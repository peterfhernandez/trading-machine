"""Universe membership builder.

Applies UNIVERSE_CONFIG rules to OHLCV data to produce a point-in-time daily
universe: rolling median dollar volume, minimum listing age, and
stablecoin/wrapped exclusions, ranked by liquidity and capped at target_size.

Writes membership append-only to the "universe" dataset, keeping one row per
asset ever considered (not just members) so downstream code can see why an
asset was excluded and measure turnover over time.

**Two point-in-time modes (`pit_mode`), the same pair the backtester offers.**

- `"ingestion"` (default, strict): reads are filtered to `ingested_ts <= asof`,
  the full look-ahead defence. Correct whenever the store holds honest
  ingestion history — data the nightly pipeline collected day by day.
- `"event"` (opt-in, weaker): `event_ts <= asof` only. This exists because a
  *backfill* stamps every row it pulls with one `ingested_ts` (the moment the
  backfill ran), so under strict mode a snapshot dated 2021-09-01 built from
  history downloaded today sees literally nothing — every date logs "No OHLCV
  data available" and nothing is written. Event mode restores the ability to
  build that history, and still prevents a snapshot from seeing a bar dated
  after its own date, but it does **not** protect against vendor revisions or
  genuinely late data. Snapshots built in event mode are research inputs; feed
  them to a backtest run in the same mode, and label the results accordingly.

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
from datetime import UTC, date, datetime, timedelta

import polars as pl

from config import DATASTORE_PATH, LOG_CONFIG, UNIVERSE_CONFIG, UniverseConfig
from datastore import ParquetStore, latest_per_bar
from logging_config import get_logger, new_run_id, set_level, set_run_id
from universe.schema import UNIVERSE_SCHEMA

logger = get_logger(__name__)

OHLCV_DATASET = "ohlcv_daily"

# Restated rather than imported from `backtest.engine`: `universe` does not
# import `backtest` (no sideways imports; the dependency arrow points one way),
# exactly as `signals/panel.py` restates them. A test asserts all three copies
# stay identical — a universe built under a different definition of "knowable"
# from the backtest that consumes it would misalign silently.
PIT_INGESTION = "ingestion"
PIT_EVENT = "event"
PIT_MODES = (PIT_INGESTION, PIT_EVENT)

_TURNOVER_LOOKBACK_DAYS = 30
"""How far back `build_and_store` looks for a previous snapshot to log turnover
against. Bounds a read that happens on every build and feeds only a log line."""

_APPEND_CHUNK_SNAPSHOTS = 50
"""How many snapshots `build_history` buffers before one append.

A bulk build lands every snapshot in a *single* ingestion partition, and
`ParquetStore.read` opens every file in a partition — so appending per date
would leave five years of daily snapshots as ~1,800 parquet files that every
later universe read reopens. Buffering trades a bounded amount of re-work after
a crash (at most this many snapshots) for a partition of ~36 files instead.
Same reasoning as the archive loader's per-symbol chunking."""

_EMPTY_VOLUME_SCHEMA = {"asset_id": pl.Utf8, "dollar_volume_median": pl.Float64}
_EMPTY_AGE_SCHEMA = {"asset_id": pl.Utf8, "listing_age_days": pl.Int64}

_OHLCV_COLUMNS = ["asset_id", "venue", "ingested_ts", "event_ts", "close", "volume"]
"""Everything both metrics need, so one read serves both."""


class PreflightError(RuntimeError):
    """A history build that cannot produce a single snapshot, detected up front.

    Raised before the date loop starts rather than discovered 1,796 empty dates
    later. The message says which condition failed and what to do about it.
    """


def _validate_pit_mode(pit_mode: str) -> str:
    if pit_mode not in PIT_MODES:
        raise ValueError(f"Unknown pit_mode {pit_mode!r}; expected one of {PIT_MODES}")
    return pit_mode


class OhlcvPanel:
    """One store read of `ohlcv_daily`, re-filtered per `asof`.

    `UniverseBuilder` reads the store on every build, and the store cannot prune
    by event date — it partitions by *ingestion* date, so a backfill's five
    years of history sit in one partition that any read opens in full. Building
    a history of snapshots that way re-reads the whole dataset once per date
    (twice, before this class existed) and spends essentially all of its wall
    clock there.

    The filtering is applied per `asof`, never once up front, so the panel
    cannot leak a later bar into an earlier snapshot. A test asserts the panel
    path and the store path produce identical snapshots in both pit modes; that
    is what keeps this an optimization rather than a second implementation.
    """

    def __init__(
        self,
        store: ParquetStore,
        venue: str = "binance",
        pit_mode: str = PIT_INGESTION,
        dataset: str = OHLCV_DATASET,
    ):
        self.venue = venue
        self.pit_mode = _validate_pit_mode(pit_mode)
        self.dataset = dataset
        self._frame = self._load(store, dataset)
        self._precollapsed = False
        self._memo: tuple[datetime, pl.DataFrame] | None = None

        # In event mode the visible set is chosen by a predicate on `event_ts`,
        # and `latest_per_bar` groups by `(asset_id, event_ts)` — so the filter
        # keeps or drops whole groups and the two operations commute. Collapsing
        # once here is therefore identical to collapsing per date, and saves the
        # collapse on every one of them.
        #
        # This does NOT hold in ingestion mode: there the filter is on
        # `ingested_ts`, which varies *within* a group, so collapsing first would
        # let a later revision win the bar and then be filtered away — hiding a
        # bar that was knowable at `asof`. See `datastore.dedupe`.
        if self.pit_mode == PIT_EVENT and len(self._frame):
            self._frame = latest_per_bar(self._frame)
            self._precollapsed = True

    def _load(self, store: ParquetStore, dataset: str) -> pl.DataFrame:
        try:
            frame = store.read(dataset, columns=_OHLCV_COLUMNS)
        except FileNotFoundError:
            logger.warning("No %s in the store; universe panel is empty", dataset)
            return pl.DataFrame()

        if len(frame) == 0:
            return frame
        if "venue" in frame.columns:
            frame = frame.filter(pl.col("venue") == self.venue)
        logger.info(
            "Universe panel loaded: %d %s rows for %s (pit_mode=%s)",
            len(frame), dataset, self.venue, self.pit_mode,
        )
        return frame

    @property
    def is_empty(self) -> bool:
        return len(self._frame) == 0

    @property
    def min_ingested_ts(self) -> datetime | None:
        """The earliest ingestion in the panel, or None when it is empty.

        A strict build can see nothing on any date before this.
        """
        if self.is_empty:
            return None
        earliest: datetime = self._frame["ingested_ts"].min()
        return earliest

    def asof(self, asof: datetime) -> pl.DataFrame:
        """The bars knowable at `asof`, one row per bar.

        Memoized on the last `asof`, because a build asks twice — once for
        dollar volume, once for listing age.
        """
        if self._memo is not None and self._memo[0] == asof:
            return self._memo[1]

        df = self._frame
        if len(df):
            if self.pit_mode == PIT_INGESTION:
                # `ParquetStore.read(asof=...)` keeps anything ingested on the
                # asof calendar day, so match that boundary exactly.
                cutoff = datetime(asof.year, asof.month, asof.day) + timedelta(days=1)
                df = df.filter(pl.col("ingested_ts") < cutoff)
            df = df.filter(pl.col("event_ts") <= asof)
            if not self._precollapsed:
                df = latest_per_bar(df)

        self._memo = (asof, df)
        return df


class UniverseBuilder:
    """Builds daily universe membership from `ohlcv_daily` data."""

    def __init__(
        self,
        store: ParquetStore | None = None,
        config: UniverseConfig = UNIVERSE_CONFIG,
        venue: str = "binance",
        pit_mode: str = PIT_INGESTION,
        bars: OhlcvPanel | None = None,
    ):
        """
        Args:
            store: Datastore to read bars from and write snapshots to.
            config: Universe rules (liquidity, listing age, exclusions).
            venue: Venue whose bars to read and whose snapshots to write.
            pit_mode: "ingestion" (default, strict) or "event" — see the module
                docstring. Event mode is what makes a backfilled history
                buildable at all.
            bars: Optional pre-loaded panel, so a date loop reads the store once
                instead of once per date. Must agree with `venue`/`pit_mode`.

        Raises:
            ValueError: If `pit_mode` is unknown, or `bars` disagrees with it.
        """
        self.store = store or ParquetStore(DATASTORE_PATH)
        self.config = config
        self.venue = venue
        self.pit_mode = _validate_pit_mode(pit_mode)

        if bars is not None and (bars.venue != venue or bars.pit_mode != self.pit_mode):
            raise ValueError(
                f"Panel is {bars.venue}/{bars.pit_mode}, builder is "
                f"{venue}/{self.pit_mode}; they must agree about what was knowable"
            )
        self.bars = bars

    def _read_ohlcv(self, asof: datetime, columns: list[str]) -> pl.DataFrame:
        """The `venue`'s bars knowable at `asof`, one row per bar.

        ParquetStore partitions by ingestion date, not event date, so a
        historical backfill (event_ts spanning years, ingested_ts = whenever
        the backfill ran) can't be pruned by a `date_range` on event dates --
        that would filter on the wrong timestamp. So the whole dataset is read
        and the point-in-time filter applied afterward.

        Under `pit_mode="ingestion"` that filter is the store's own
        `ingested_ts <= asof`; under `"event"` it is dropped and only
        `event_ts <= asof` applies. When a panel was supplied it answers
        instead, applying exactly the same filters to bars already in memory.
        """
        if self.bars is not None:
            return self.bars.asof(asof)

        try:
            df = self.store.read(
                OHLCV_DATASET,
                asof=asof.date().isoformat() if self.pit_mode == PIT_INGESTION else None,
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

    def _dollar_volume(self, asof: datetime, bars: pl.DataFrame | None = None) -> pl.DataFrame:
        """Median dollar volume per asset over the lookback window, as of `asof`."""
        lookback_start = asof - timedelta(days=self.config.volume_lookback_days)
        df = self._read_ohlcv(asof, _OHLCV_COLUMNS) if bars is None else bars

        if len(df) == 0:
            return pl.DataFrame(schema=_EMPTY_VOLUME_SCHEMA)

        df = df.filter(pl.col("event_ts") >= lookback_start)
        if len(df) == 0:
            return pl.DataFrame(schema=_EMPTY_VOLUME_SCHEMA)

        df = df.with_columns((pl.col("close") * pl.col("volume")).alias("dollar_volume"))
        return df.group_by("asset_id").agg(
            pl.col("dollar_volume").median().alias("dollar_volume_median")
        )

    def _listing_age(self, asof: datetime, bars: pl.DataFrame | None = None) -> pl.DataFrame:
        """Days since each asset's first observed OHLCV bar, as of `asof`."""
        df = self._read_ohlcv(asof, _OHLCV_COLUMNS) if bars is None else bars

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

        # One read serves both metrics. They used to read independently, which
        # doubled the cost of every build for identical rows.
        bars = self._read_ohlcv(asof, _OHLCV_COLUMNS)
        volume_df = self._dollar_volume(asof, bars)
        age_df = self._listing_age(asof, bars)

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

        # Sorted because polars' group_by does not promise an output order, so
        # two builds of the same date produced the same rows in different orders
        # -- which makes a stored snapshot non-reproducible and any comparison
        # of two build paths a comparison of hash iteration order.
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
        ).sort("asset_id")

    def build_and_store(self, asof: datetime | None = None) -> pl.DataFrame:
        """Build the universe and append it to the datastore (point-in-time)."""
        df = self.build(asof)
        if len(df) == 0:
            logger.warning("Empty universe; nothing appended")
            return df

        self.store.append("universe", df, UNIVERSE_SCHEMA)
        event_date = self._log_snapshot(df)

        previous = self._previous_snapshot(event_date)
        if previous is not None:
            logger.info(
                f"Universe {event_date} turnover vs {previous[0]}: "
                f"{compute_turnover(previous[1], df):.1%}"
            )
        return df

    def _log_snapshot(self, df: pl.DataFrame) -> date:
        """Size and exclusion breakdown for one built snapshot; returns its date.

        Separate from `build_and_store` because `build_history` batches its
        appends, so the log line and the write no longer happen together.
        """
        n_members = int(df["in_universe"].sum())
        event_date: date = df["event_ts"][0].date()
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
        return event_date

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


def preflight(panel: OhlcvPanel, dates: Sequence[datetime]) -> str | None:
    """Why this history build can produce nothing, or None if it can.

    Cheap to ask (the panel is already loaded) and worth asking, because the
    failure it catches is otherwise invisible: a strict build over backfilled
    history logs "No OHLCV data available" on every one of ~1,800 dates, takes
    over an hour, writes nothing, and exits 0.
    """
    # None exactly when the panel is empty, so one check covers both.
    first_known = panel.min_ingested_ts
    if first_known is None:
        return (
            f"no {panel.dataset} rows for venue {panel.venue!r} in the store. "
            f"Load bars first (`python -m loaders.archive` or "
            f"`python -m pipeline.nightly`), and check the venue spelling."
        )

    if panel.pit_mode != PIT_INGESTION:
        return None

    last_date = max(dates)
    if first_known.date() <= last_date.date():
        return None

    # ASCII only, for the same reason `main`'s banner is: this reaches stderr,
    # which takes the locale encoding when it is not a terminal (cp1252 on a
    # default Windows install), and a diagnostic that dies on its own punctuation
    # is worse than no diagnostic.
    return (
        f"nothing in {panel.dataset} was ingested on or before {last_date.date()} "
        f"(the earliest ingested_ts is {first_known.date()}), so under "
        f"pit_mode={PIT_INGESTION!r} every date in this range sees an empty store "
        f"and no snapshot would be written. That is what a bulk backfill looks "
        f"like: it stamps every row with the moment it ran, whatever the bar's own "
        f"date. Re-run with --pit-mode {PIT_EVENT} to build over that history "
        f"(weaker: no defence against revisions, so label what comes out research "
        f"indications), and run the backtests that consume these snapshots in the "
        f"same mode."
    )


def build_history(
    start: datetime,
    end: datetime,
    freq: str = "daily",
    venue: str = "binance",
    store: ParquetStore | None = None,
    dry_run: bool = False,
    pit_mode: str = PIT_INGESTION,
    config: UniverseConfig = UNIVERSE_CONFIG,
) -> tuple[int, int]:
    """Build and store a snapshot per date in `[start, end]`.

    Returns `(snapshots written, dates that failed)`.

    The store is read **once** into an `OhlcvPanel` and re-filtered per date.
    Reading per date instead meant re-reading the entire dataset — which a
    backfill leaves in a single unprunable partition — for every snapshot.

    A date that raises is logged and skipped rather than abandoning the run:
    five years of weekly snapshots is ~260 builds, and losing 259 of them to one
    bad date is the wrong trade. The failure count comes back so the caller can
    exit non-zero on a partial build instead of reporting success.

    Raises:
        PreflightError: If the run could not produce a single snapshot. A dry
            run logs the same finding as a warning instead of raising, since it
            is reporting on a run rather than performing one.
    """
    dates = snapshot_dates(start, end, freq)
    store = store or ParquetStore(DATASTORE_PATH)

    logger.info(
        f"Building {len(dates)} {freq} universe snapshot(s) for {venue} over "
        f"{start.date()}..{end.date()} (pit_mode={pit_mode})"
        f"{' (DRY RUN)' if dry_run else ''}"
    )

    panel = OhlcvPanel(store, venue=venue, pit_mode=pit_mode)
    problem = preflight(panel, dates)
    if problem is not None:
        if not dry_run:
            raise PreflightError(problem)
        logger.warning("Preflight: %s", problem)
        return 0, 0
    if dry_run:
        return 0, 0

    builder = UniverseBuilder(
        store=store, config=config, venue=venue, pit_mode=pit_mode, bars=panel
    )

    written = 0
    failed = 0
    pending: list[pl.DataFrame] = []
    previous: tuple[date, pl.DataFrame] | None = None

    def flush() -> None:
        if pending:
            store.append("universe", pl.concat(pending, how="vertical"), UNIVERSE_SCHEMA)
            pending.clear()

    for asof in dates:
        try:
            snapshot = builder.build(asof=asof)
        except Exception as e:
            logger.error(f"Universe build failed for {asof.date()}: {e}", exc_info=True)
            failed += 1
            continue

        if not len(snapshot):
            # Not an error: a date before the store's first bar has nothing to
            # rank, and `build` has already logged it.
            logger.debug(f"No universe snapshot written for {asof.date()} (no data)")
            continue

        event_date = builder._log_snapshot(snapshot)
        if previous is not None:
            # Against the previously *built* snapshot, held in memory. Reading it
            # back would find nothing: a rebuild's snapshots all carry today's
            # `ingested_ts`, so `_previous_snapshot`'s point-in-time read cannot
            # see one dated in the past.
            logger.info(
                f"Universe {event_date} turnover vs {previous[0]}: "
                f"{compute_turnover(previous[1], snapshot):.1%}"
            )
        previous = (event_date, snapshot)

        pending.append(snapshot)
        written += 1
        if len(pending) >= _APPEND_CHUNK_SNAPSHOTS:
            flush()

    flush()

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
        "--pit-mode",
        default=PIT_INGESTION,
        choices=PIT_MODES,
        help=(
            "How a snapshot decides what was knowable: 'ingestion' (default, "
            "strict ingested_ts <= asof) or 'event' (event_ts <= asof only). "
            "Backfilled history needs 'event' -- a bulk load stamps every row "
            "with one ingested_ts, so strict mode sees nothing before that date"
        ),
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
        f"{start.date()}..{end.date()}, venue={args.venue}, "
        f"pit_mode={args.pit_mode}, run_id={run_id}"
    )

    try:
        written, failed = build_history(
            start=start,
            end=end,
            freq=args.freq,
            venue=args.venue,
            dry_run=args.dry_run,
            pit_mode=args.pit_mode,
        )
    except PreflightError as e:
        # Exit 2, not 1: "this run cannot produce anything" is a different
        # answer from "some dates failed", and a caller keying off the code
        # should be able to tell them apart.
        print(f"  preflight failed: {e}", file=sys.stderr)
        return 2

    if args.dry_run:
        print(f"  (dry run) would build {len(dates)} snapshot(s); nothing written")
        return 0

    print(f"  written: {written}   empty: {len(dates) - written - failed}   failed: {failed}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
