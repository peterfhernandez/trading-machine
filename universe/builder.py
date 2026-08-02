"""Universe membership builder.

Applies UNIVERSE_CONFIG rules to OHLCV data to produce a point-in-time daily
universe: rolling median dollar volume, minimum listing age, and
stablecoin/wrapped exclusions, ranked by liquidity and capped at target_size.

Reads OHLCV only through `ingested_ts <= asof` (point-in-time discipline) and
writes membership append-only to the "universe" dataset, keeping one row per
asset ever considered (not just members) so downstream code can see why an
asset was excluded and measure turnover over time.

`build_and_store` writes one snapshot. Building a *history* of them is what
`python -m universe.builder --start ... --end ...` does — see `main()`. That
matters more than it sounds: the universe dataset is an **input** to everything
downstream, not a by-product. `DatastoreUniverse` reads snapshots, and the
audit's coverage denominator is the latest one, so a store with none hands every
backtest an empty universe and reports coverage as *not evaluated*.
"""

import argparse
import sys
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta

import polars as pl

from config import DATASTORE_PATH, LOG_CONFIG, UNIVERSE_CONFIG, UniverseConfig
from datastore import ParquetStore, latest_per_bar
from logging_config import get_logger, new_run_id, set_level, set_run_id
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
        store: ParquetStore | None = None,
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
        listing_age_days: int | None,
        dollar_volume_median: float | None,
    ) -> str | None:
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

    def build(self, asof: datetime | None = None) -> pl.DataFrame:
        """Build point-in-time universe membership as of `asof` (default: now).

        Returns one row per asset observed in the listing-age/volume windows;
        `in_universe` marks the final target_size membership, `rank` orders
        eligible assets by liquidity, and `exclusion_reason` explains why any
        other asset was left out (stablecoin, wrapped, listing_age,
        low_volume, or rank_cutoff).
        """
        if asof is None:
            asof = datetime.now(UTC).replace(tzinfo=None)

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

        now = datetime.now(UTC).replace(tzinfo=None)
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

    def build_and_store(self, asof: datetime | None = None) -> pl.DataFrame:
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

    def _previous_snapshot(self, event_date) -> tuple[object, pl.DataFrame] | None:
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


SUPPORTED_FREQS = ("daily", "weekly", "monthly")


def snapshot_dates(
    start: datetime,
    end: datetime,
    freq: str = "daily",
) -> list[datetime]:
    """The dates to build snapshots for, over `[start, end]` inclusive.

    Same rule as the backtester's rebalance calendar — daily is every date,
    weekly is the first date of each ISO week, monthly the first of each
    calendar month — but derived from a date range rather than from bars that
    exist, because a universe snapshot can be built for any date while a
    rebalance can only happen on a date that has a bar.

    Deliberately *not* an import of `backtest.calendar.build_rebalance_calendar`:
    `universe` does not import `backtest` (no sideways imports; the dependency
    arrow points one way). A test asserts the two agree over a date range, which
    is what keeps the duplication honest.
    """
    if freq not in SUPPORTED_FREQS:
        raise ValueError(f"Unsupported freq {freq!r}; expected one of {SUPPORTED_FREQS}")
    if start > end:
        raise ValueError(f"start {start.date()} is after end {end.date()}")

    day = datetime(start.year, start.month, start.day)
    last = datetime(end.year, end.month, end.day)
    every_day = []
    while day <= last:
        every_day.append(day)
        day += timedelta(days=1)

    if freq == "daily":
        return every_day

    def key(d: datetime) -> tuple:
        if freq == "weekly":
            iso = d.isocalendar()
            return (iso[0], iso[1])
        return (d.year, d.month)

    dates: list[datetime] = []
    seen: set[tuple] = set()
    for d in every_day:
        k = key(d)
        if k not in seen:
            seen.add(k)
            dates.append(d)
    return dates


def build_history(
    start: datetime,
    end: datetime,
    freq: str = "daily",
    venue: str = "binance",
    store: ParquetStore | None = None,
    dry_run: bool = False,
) -> tuple[int, int]:
    """Build and store a snapshot per date in `[start, end]`.

    Returns `(snapshots written, dates that failed)`.

    A date that raises is logged and skipped rather than abandoning the run:
    five years of weekly snapshots is ~260 builds, and losing 259 of them to one
    bad date is the wrong trade. The failure count comes back so the caller can
    exit non-zero on a partial build instead of reporting success.
    """
    dates = snapshot_dates(start, end, freq)
    builder = UniverseBuilder(store=store or ParquetStore(DATASTORE_PATH), venue=venue)

    logger.info(
        f"Building {len(dates)} {freq} universe snapshot(s) for {venue} over "
        f"{start.date()}..{end.date()}{' (DRY RUN)' if dry_run else ''}"
    )
    if dry_run:
        return 0, 0

    written = 0
    failed = 0
    for asof in dates:
        try:
            snapshot = builder.build_and_store(asof=asof)
        except Exception as e:
            logger.error(f"Universe build failed for {asof.date()}: {e}", exc_info=True)
            failed += 1
            continue
        if len(snapshot):
            written += 1
        else:
            # Not an error: a date before the store's first bar has nothing to
            # rank, and `build_and_store` has already logged it.
            logger.debug(f"No universe snapshot written for {asof.date()} (no data)")

    logger.info(
        f"Universe history complete: {written} snapshot(s) written, "
        f"{len(dates) - written - failed} empty, {failed} failed"
    )
    return written, failed


def _parse_date(value: str) -> datetime:
    """`YYYY-MM-DD` or full ISO 8601, as naive UTC."""
    return datetime.fromisoformat(value).replace(tzinfo=None)


def main(argv: Sequence[str] | None = None) -> int:
    """`python -m universe.builder --start 2021-09-01 --end 2026-08-01`

    The step `DATA.md` §3 calls `universe.build`: a date loop over
    `build_and_store`, which existed as a method with no way to run it from a
    shell. It lives here rather than in a second module named one letter away.
    """
    parser = argparse.ArgumentParser(
        description="Build point-in-time universe snapshots over a date range",
        epilog=(
            "The universe dataset is an input, not an output: with no snapshots "
            "every backtest runs on an empty universe and the audit reports "
            "coverage as not evaluated. Build at the frequency you intend to "
            "research at -- weekly over five years is ~260 snapshots against "
            "~1,800 daily ones, and is enough for a weekly rebalance."
        ),
    )
    parser.add_argument("--venue", default="binance")
    parser.add_argument("--start", required=True, help="First snapshot date (YYYY-MM-DD, UTC)")
    parser.add_argument("--end", help="Last snapshot date (YYYY-MM-DD, UTC; default: today)")
    parser.add_argument(
        "--freq",
        default=UNIVERSE_CONFIG.rebalance_freq,
        choices=SUPPORTED_FREQS,
        help=f"Snapshot frequency (default: UNIVERSE_CONFIG.rebalance_freq, currently "
        f"{UNIVERSE_CONFIG.rebalance_freq})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report how many snapshots would be built, and write nothing",
    )
    levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
    parser.add_argument(
        "--log-level",
        choices=levels,
        help="Level for logs/universe.log for this run (default: TM_LOG_LEVEL, else INFO)",
    )
    parser.add_argument(
        "--console-log-level",
        choices=levels,
        help="Level for stderr for this run (default: TM_CONSOLE_LOG_LEVEL, else WARNING)",
    )
    args = parser.parse_args(argv)

    if args.log_level or args.console_log_level:
        set_level(args.log_level or LOG_CONFIG.level, args.console_log_level)

    run_id = new_run_id()
    set_run_id(run_id)

    start = _parse_date(args.start)
    end = _parse_date(args.end) if args.end else datetime.now(UTC).replace(tzinfo=None)

    try:
        dates = snapshot_dates(start, end, args.freq)
    except ValueError as e:
        parser.error(str(e))

    # ASCII only: sys.stdout takes the locale encoding when it is not a
    # terminal (cp1252 on a default Windows install), and a redirected run that
    # dies on its own summary is a failure the pipeline learned about once.
    print(
        f"Universe snapshots: {len(dates)} {args.freq} date(s) "
        f"{start.date()}..{end.date()}, venue={args.venue}, run_id={run_id}"
    )

    written, failed = build_history(
        start=start,
        end=end,
        freq=args.freq,
        venue=args.venue,
        dry_run=args.dry_run,
    )

    if args.dry_run:
        print(f"  (dry run) would build {len(dates)} snapshot(s); nothing written")
        return 0

    print(f"  written: {written}   empty: {len(dates) - written - failed}   failed: {failed}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
