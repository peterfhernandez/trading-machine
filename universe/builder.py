"""Universe membership builder.

Applies UNIVERSE_CONFIG rules to OHLCV data to produce a point-in-time daily
universe: rolling median dollar volume, minimum listing age, and
stablecoin/wrapped exclusions, ranked by liquidity and capped at target_size.

Reads OHLCV only through `ingested_ts <= asof` (point-in-time discipline) and
writes membership append-only to the "universe" dataset, keeping one row per
asset ever considered (not just members) so downstream code can see why an
asset was excluded and measure turnover over time.
"""

from datetime import datetime, timedelta, timezone
from typing import Optional

import polars as pl

from config import DATASTORE_PATH, UNIVERSE_CONFIG, UniverseConfig
from datastore import ParquetStore, latest_per_bar
from logging_config import get_logger
from universe.schema import UNIVERSE_SCHEMA

logger = get_logger(__name__)

_TURNOVER_LOOKBACK_DAYS = 30
"""How far back `build_and_store` looks for a previous snapshot to log turnover
against. Bounds a read that happens on every build and feeds only a log line."""

_EMPTY_VOLUME_SCHEMA = {"asset_id": pl.Utf8, "dollar_volume_median": pl.Float64}
_EMPTY_AGE_SCHEMA = {"asset_id": pl.Utf8, "listing_age_days": pl.Int64}


class UniverseBuilder:
    """Builds daily universe membership from `ohlcv_daily` data."""

    def __init__(
        self,
        store: Optional[ParquetStore] = None,
        config: UniverseConfig = UNIVERSE_CONFIG,
        venue: str = "binance",
    ):
        self.store = store or ParquetStore(DATASTORE_PATH)
        self.config = config
        self.venue = venue

    def _read_ohlcv(self, asof: datetime, columns: list[str]) -> pl.DataFrame:
        """Read `ohlcv_daily` for `venue`, filtered to `ingested_ts <= asof`.

        ParquetStore partitions by ingestion date, not event date, so a
        historical backfill (event_ts spanning years, ingested_ts = whenever
        the backfill ran) can't be pruned by a `date_range` on event dates --
        that would filter on the wrong timestamp. Instead we read the whole
        dataset and let the `asof` argument (which ParquetStore applies to
        `ingested_ts`) enforce point-in-time discipline; callers filter by
        `event_ts` themselves afterward.
        """
        try:
            df = self.store.read(
                "ohlcv_daily",
                asof=asof.date().isoformat(),
                columns=columns,
            )
        except FileNotFoundError:
            return pl.DataFrame()

        if len(df) == 0:
            return df

        if "venue" in df.columns:
            df = df.filter(pl.col("venue") == self.venue)

        df = df.filter(pl.col("event_ts") <= asof)

        # Overlapping loader windows store the same bar more than once. Both
        # metrics below aggregate rows -- a median dollar volume and a first-seen
        # date -- so a bar counted twice would weight the duplicated stretch of
        # history (always the most recent days) more heavily than the rest.
        # Collapse after the asof filter, never before: see datastore.dedupe.
        return latest_per_bar(df)

    def _dollar_volume(self, asof: datetime) -> pl.DataFrame:
        """Median dollar volume per asset over the lookback window, as of `asof`."""
        lookback_start = asof - timedelta(days=self.config.volume_lookback_days)
        df = self._read_ohlcv(asof, ["asset_id", "venue", "ingested_ts", "event_ts", "close", "volume"])

        if len(df) == 0:
            return pl.DataFrame(schema=_EMPTY_VOLUME_SCHEMA)

        df = df.filter(pl.col("event_ts") >= lookback_start)
        if len(df) == 0:
            return pl.DataFrame(schema=_EMPTY_VOLUME_SCHEMA)

        df = df.with_columns((pl.col("close") * pl.col("volume")).alias("dollar_volume"))
        return df.group_by("asset_id").agg(
            pl.col("dollar_volume").median().alias("dollar_volume_median")
        )

    def _listing_age(self, asof: datetime) -> pl.DataFrame:
        """Days since each asset's first observed OHLCV bar, as of `asof`."""
        df = self._read_ohlcv(asof, ["asset_id", "venue", "ingested_ts", "event_ts"])

        if len(df) == 0:
            return pl.DataFrame(schema=_EMPTY_AGE_SCHEMA)

        first_seen = df.group_by("asset_id").agg(pl.col("event_ts").min().alias("first_seen"))
        return first_seen.with_columns(
            (pl.lit(asof, dtype=pl.Datetime("us")) - pl.col("first_seen"))
            .dt.total_days()
            .alias("listing_age_days")
        ).select("asset_id", "listing_age_days")

    def _exclusion_reason(
        self,
        asset_id: str,
        listing_age_days: Optional[int],
        dollar_volume_median: Optional[float],
    ) -> Optional[str]:
        cfg = self.config
        if cfg.exclude_stablecoins and asset_id in cfg.stablecoin_symbols:
            return "stablecoin"
        if cfg.exclude_wrapped and asset_id in cfg.wrapped_symbols:
            return "wrapped"
        if listing_age_days is None or listing_age_days < cfg.min_listing_age_days:
            return "listing_age"
        if dollar_volume_median is None or dollar_volume_median < cfg.min_volume_usdt:
            return "low_volume"
        return None

    def build(self, asof: Optional[datetime] = None) -> pl.DataFrame:
        """Build point-in-time universe membership as of `asof` (default: now).

        Returns one row per asset observed in the listing-age/volume windows;
        `in_universe` marks the final target_size membership, `rank` orders
        eligible assets by liquidity, and `exclusion_reason` explains why any
        other asset was left out (stablecoin, wrapped, listing_age,
        low_volume, or rank_cutoff).
        """
        if asof is None:
            asof = datetime.now(timezone.utc).replace(tzinfo=None)

        volume_df = self._dollar_volume(asof)
        age_df = self._listing_age(asof)

        universe = age_df.join(volume_df, on="asset_id", how="full", coalesce=True)

        if len(universe) == 0:
            logger.warning(f"No OHLCV data available to build universe as of {asof.date()}")
            return pl.DataFrame(schema=UNIVERSE_SCHEMA.to_polars_schema())

        exclusion_reasons = [
            self._exclusion_reason(
                row["asset_id"], row["listing_age_days"], row["dollar_volume_median"]
            )
            for row in universe.iter_rows(named=True)
        ]
        universe = universe.with_columns(
            pl.Series("exclusion_reason", exclusion_reasons, dtype=pl.Utf8)
        )

        eligible = universe.filter(pl.col("exclusion_reason").is_null()).sort(
            "dollar_volume_median", descending=True
        )
        eligible = eligible.with_row_index("rank", offset=1).with_columns(
            pl.col("rank").cast(pl.Int64)
        )

        cutoff_ids = set(
            eligible.filter(pl.col("rank") > self.config.target_size)["asset_id"].to_list()
        )

        universe = universe.join(eligible.select("asset_id", "rank"), on="asset_id", how="left")

        universe = universe.with_columns(
            pl.when(pl.col("asset_id").is_in(cutoff_ids))
            .then(pl.lit("rank_cutoff"))
            .otherwise(pl.col("exclusion_reason"))
            .alias("exclusion_reason")
        )
        universe = universe.with_columns(
            pl.col("exclusion_reason").is_null().alias("in_universe")
        )

        now = datetime.now(timezone.utc).replace(tzinfo=None)
        event_ts = datetime(asof.year, asof.month, asof.day)

        universe = universe.with_columns(
            [
                pl.lit(self.venue).alias("venue"),
                pl.lit(event_ts, dtype=pl.Datetime("us")).alias("event_ts"),
                pl.lit(now, dtype=pl.Datetime("us")).alias("ingested_ts"),
            ]
        )

        return universe.select(
            "asset_id",
            "venue",
            "event_ts",
            "ingested_ts",
            "in_universe",
            "dollar_volume_median",
            "listing_age_days",
            "rank",
            "exclusion_reason",
        )

    def build_and_store(self, asof: Optional[datetime] = None) -> pl.DataFrame:
        """Build the universe and append it to the datastore (point-in-time)."""
        df = self.build(asof)
        if len(df) == 0:
            logger.warning("Empty universe; nothing appended")
            return df

        self.store.append("universe", df, UNIVERSE_SCHEMA)
        n_members = int(df["in_universe"].sum())
        event_date = df["event_ts"][0].date()
        logger.info(
            f"Universe built: {n_members}/{len(df)} assets in universe "
            f"as of {event_date}"
        )

        # The exclusion breakdown is what makes a membership change explicable
        # after the fact: "150 members" says nothing, "30 fewer because 30 more
        # fell below min_volume_usdt" says what happened.
        excluded = (
            df.filter(pl.col("exclusion_reason").is_not_null())
            .group_by("exclusion_reason")
            .len()
            .sort("exclusion_reason")
        )
        if len(excluded):
            breakdown = ", ".join(
                f"{row['exclusion_reason']}={row['len']}"
                for row in excluded.iter_rows(named=True)
            )
            logger.info(f"Universe {event_date} exclusions: {breakdown}")

        previous = self._previous_snapshot(event_date)
        if previous is not None:
            logger.info(
                f"Universe {event_date} turnover vs {previous[0]}: "
                f"{compute_turnover(previous[1], df):.1%}"
            )
        return df

    def _previous_snapshot(self, event_date) -> Optional[tuple[object, pl.DataFrame]]:
        """The most recent stored snapshot before `event_date`, for turnover.

        Best-effort and never fatal: turnover is an observability nicety, and a
        store that cannot answer must not break a universe build.

        Read as of `event_date` (the snapshot's own date), so the comparison is
        against what was knowable then rather than against a snapshot written
        later — the same point-in-time rule the build itself follows. One
        consequence: a research rebuild of history, whose snapshots all share
        today's `ingested_ts`, finds no prior snapshot and simply logs no
        turnover line. That is the right answer, not a gap.

        The read is bounded to a month of ingestion partitions, because this
        runs on every build and exists only to produce a log line; a store with
        years of daily snapshots should not be scanned in full for it.
        """
        lookback_start = event_date - timedelta(days=_TURNOVER_LOOKBACK_DAYS)
        try:
            df = self.store.read(
                "universe",
                date_range=(lookback_start.isoformat(), event_date.isoformat()),
                asof=event_date.isoformat(),
            )
        except Exception as e:
            logger.debug(f"No previous universe snapshot for turnover: {e}")
            return None

        if len(df) == 0:
            return None
        if "venue" in df.columns:
            df = df.filter(pl.col("venue") == self.venue)
        df = df.filter(pl.col("event_ts").dt.date() < event_date)
        if len(df) == 0:
            return None

        latest = df["event_ts"].max()
        return latest.date(), df.filter(pl.col("event_ts") == latest)


def compute_turnover(before: pl.DataFrame, after: pl.DataFrame) -> float:
    """Turnover between two universe snapshots: |joiners ∪ leavers| / |union of members|.

    Args:
        before: Universe DataFrame (as returned by `build`/`build_and_store`) for the earlier date
        after: Universe DataFrame for the later date

    Returns:
        Turnover fraction in [0, 1]; 0.0 if both snapshots have no members
    """
    before_members = set(before.filter(pl.col("in_universe"))["asset_id"].to_list())
    after_members = set(after.filter(pl.col("in_universe"))["asset_id"].to_list())

    union = before_members | after_members
    if not union:
        return 0.0

    changed = before_members.symmetric_difference(after_members)
    return len(changed) / len(union)
