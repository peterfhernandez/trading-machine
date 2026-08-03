"""Acceptance checks for a bulk backfill, run before any research does.

`DATA.md` §3 step 6. The backfill (`python -m loaders.archive`) and the
snapshot rebuild (`python -m universe.builder`) both exit 0 on outcomes that
are not what was wanted — a month loop that double-counted a boundary, an
asset with a hole in the middle of its listed range, a universe dataset with
one snapshot in it — and every one of those surfaces downstream as a *quiet*
result rather than an error: a shorter signal history, a thinner cross-section,
an empty book. The point of this module is to ask the questions once, on
purpose, while the answer is still cheap to act on.

Two things it deliberately is not:

- **Not point-in-time.** Every other reader in this project filters
  `ingested_ts <= asof` or `event_ts <= asof`, because it is answering "what
  was knowable then?". These checks ask "what is on disk *now*?", which is a
  question about the load rather than about a decision, so they read the store
  raw. Duplicates are still collapsed with `datastore.latest_per_bar` before
  anything is counted — a bar stored twice is one bar.
- **Not the nightly audit.** `DataAudit` runs every night over a bounded
  lookback window and can halt trading. This runs once after a backfill, over
  the whole history, and gates *research*. The overlap is deliberate and small:
  both count duplicate bars, and they mean the same thing by it.

    python -m audit.acceptance --venue binance

Exit codes follow `universe.builder`: 0 clear, 1 a blocking check failed,
2 the run could not start (no store at that path).
"""

import argparse
import json
import statistics
import sys
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import cast

import polars as pl

from config import DATASTORE_PATH, LOADER_CONFIG, LOG_CONFIG, UNIVERSE_CONFIG
from datastore import ParquetStore, count_duplicate_bars, latest_per_bar
from loaders.window import Coverage, FetchWindow, resume_window
from logging_config import get_logger, new_run_id, set_level, set_run_id

logger = get_logger(__name__)

OHLCV_DATASET = "ohlcv_daily"
FUNDING_DATASET = "funding_rate"
UNIVERSE_DATASET = "universe"

DAYS_PER_YEAR = 365.25


class AcceptanceError(Exception):
    """The checks could not be run at all (as opposed to not passing)."""


@dataclass(frozen=True)
class AcceptanceThresholds:
    """The bar to clear, from `DATA.md` §3 step 6.

    Defaults are that checklist's numbers. They are arguments rather than
    constants because the checklist is written for the recommended pull (200
    symbols, 2021-08 onward) and a deliberately smaller one should be able to
    say so on the command line instead of being told it failed.
    """

    min_years: float = 4.0
    min_ohlcv_assets: int = 150
    min_funding_assets: int = 100

    # A "gap" is missing calendar days *between* two consecutive bars, so
    # consecutive days is a gap of 0 and this permits a three-day hole.
    max_gap_days: int = 3

    # Funding settles 8-hourly on Binance perps, but a perp listed after the
    # first bar legitimately starts late; the span comparison allows for the
    # dataset as a whole starting later than the klines, not for each asset.
    funding_span_tolerance_days: int = 45

    # A universe snapshot with no members is the failure `DATA.md` warns about;
    # a thin *early* snapshot is just 2021 having fewer listed perps, so the
    # floor applies to the median across snapshots rather than to the minimum.
    min_median_universe_members: int = 20


@dataclass(frozen=True)
class AcceptanceCheck:
    """One line of the checklist, and what the store had to say about it."""

    name: str
    passed: bool
    message: str
    blocking: bool = True

    @property
    def status(self) -> str:
        if self.passed:
            return "PASS"
        return "FAIL" if self.blocking else "WARN"


@dataclass
class AcceptanceReport:
    """Every check's verdict, and whether research may proceed."""

    checks: list[AcceptanceCheck] = field(default_factory=list)

    @property
    def blocking_failures(self) -> list[AcceptanceCheck]:
        return [c for c in self.checks if not c.passed and c.blocking]

    @property
    def warnings(self) -> list[AcceptanceCheck]:
        return [c for c in self.checks if not c.passed and not c.blocking]

    @property
    def passed(self) -> bool:
        return not self.blocking_failures

    def to_text(self) -> str:
        """A plain-ASCII report.

        ASCII for the reason `pipeline/nightly.py` and `universe/builder.py`
        learned the hard way: this goes to stdout, which takes the locale
        encoding when it is not a terminal, and a summary that dies on its own
        punctuation is worse than no summary.
        """
        lines = [f"  [{c.status}] {c.name}: {c.message}" for c in self.checks]
        passed = sum(1 for c in self.checks if c.passed)
        lines.append(
            f"  {passed}/{len(self.checks)} checks passed"
            f"{f', {len(self.warnings)} warning(s)' if self.warnings else ''}"
        )
        lines.append(
            "  ACCEPTED: the backfill is fit for research"
            if self.passed
            else f"  BLOCKED: {len(self.blocking_failures)} check(s) must be fixed first"
        )
        return "\n".join(lines)

    def to_dict(self) -> dict:
        return {
            "passed": self.passed,
            "checks": [
                {
                    "name": c.name,
                    "status": c.status,
                    "passed": c.passed,
                    "blocking": c.blocking,
                    "message": c.message,
                }
                for c in self.checks
            ],
        }


# ---------------------------------------------------------------------------
# Reading
# ---------------------------------------------------------------------------


def read_bars(store: ParquetStore, dataset: str, venue: str) -> pl.DataFrame:
    """Every bar of `dataset` for `venue`, one row per bar, plus the raw count.

    Raw rows are what `count_duplicate_bars` needs, so the collapse happens in
    the caller that wants it collapsed rather than here.
    """
    try:
        df = store.read(dataset, columns=["asset_id", "venue", "event_ts", "ingested_ts"])
    except FileNotFoundError:
        return pl.DataFrame()

    if len(df) and "venue" in df.columns:
        df = df.filter(pl.col("venue") == venue)
    return df


def _bounds(
    df: pl.DataFrame, column: str = "event_ts"
) -> tuple[datetime, datetime] | None:
    """The first and last timestamp in `column`, or None if there are none.

    `Series.min()` is typed as a union of every scalar polars can hold, so the
    cast is what keeps one narrowing in one place instead of at each of the
    four call sites. The column is a Datetime by schema in every dataset this
    module reads.
    """
    if not len(df) or column not in df.columns:
        return None
    lo, hi = df[column].min(), df[column].max()
    if lo is None or hi is None:
        return None
    return cast(datetime, lo), cast(datetime, hi)


def _span_days(df: pl.DataFrame, column: str = "event_ts") -> float:
    bounds = _bounds(df, column)
    if bounds is None:
        return 0.0
    lo, hi = bounds
    return (hi - lo).total_seconds() / 86400.0


def _span_text(df: pl.DataFrame, column: str = "event_ts") -> str:
    bounds = _bounds(df, column)
    if bounds is None:
        return "no rows"
    return f"{bounds[0].date()}..{bounds[1].date()}"


# ---------------------------------------------------------------------------
# The checks, one per bullet in DATA.md section 3 step 6
# ---------------------------------------------------------------------------


def check_ohlcv_coverage(
    bars: pl.DataFrame, thresholds: AcceptanceThresholds
) -> AcceptanceCheck:
    """`ohlcv_daily` spans >= 4 years and holds >= 150 distinct asset_ids."""
    if not len(bars):
        return AcceptanceCheck(
            name="ohlcv_daily_coverage",
            passed=False,
            message=(
                f"no {OHLCV_DATASET} rows in the store. Nothing downstream can run: "
                f"the universe builder, every price signal and the engine's price "
                f"panel all read this dataset. Run `python -m loaders.archive` first."
            ),
        )

    assets = bars["asset_id"].n_unique()
    years = _span_days(bars) / DAYS_PER_YEAR
    ok_years = years >= thresholds.min_years
    ok_assets = assets >= thresholds.min_ohlcv_assets

    shortfalls = []
    if not ok_years:
        shortfalls.append(f"span {years:.2f}y < {thresholds.min_years}y")
    if not ok_assets:
        shortfalls.append(f"{assets} assets < {thresholds.min_ohlcv_assets}")

    return AcceptanceCheck(
        name="ohlcv_daily_coverage",
        passed=ok_years and ok_assets,
        message=(
            f"{len(bars)} bars, {assets} assets, {_span_text(bars)} ({years:.2f}y)"
            + (f" -- {'; '.join(shortfalls)}" if shortfalls else "")
        ),
    )


def check_funding_coverage(
    funding: pl.DataFrame, bars: pl.DataFrame, thresholds: AcceptanceThresholds
) -> AcceptanceCheck:
    """`funding_rate` spans the same window for >= 100 assets.

    Fewer than the OHLCV asset count is expected and documented: funding exists
    on perpetuals only, so a spot listing with no perp scores `None` at every
    rebalance. That is `carry`'s breadth limitation, not a data fault -- which
    is why the floor here is 100 against OHLCV's 150.
    """
    if not len(funding):
        return AcceptanceCheck(
            name="funding_rate_coverage",
            passed=False,
            message=(
                f"no {FUNDING_DATASET} rows in the store. `carry` is the only signal "
                f"that reads it, and with none it scores None at every rebalance -- "
                f"five signals instead of six, silently."
            ),
        )

    assets = funding["asset_id"].n_unique()
    years = _span_days(funding) / DAYS_PER_YEAR
    ok_assets = assets >= thresholds.min_funding_assets

    ok_span = True
    span_note = ""
    if len(bars):
        shortfall = _span_days(bars) - _span_days(funding)
        ok_span = shortfall <= thresholds.funding_span_tolerance_days
        if not ok_span:
            span_note = (
                f"; spans {shortfall:.0f} days less than {OHLCV_DATASET} "
                f"(tolerance {thresholds.funding_span_tolerance_days})"
            )

    shortfalls = []
    if not ok_assets:
        shortfalls.append(f"{assets} assets < {thresholds.min_funding_assets}")

    return AcceptanceCheck(
        name="funding_rate_coverage",
        passed=ok_assets and ok_span,
        message=(
            f"{len(funding)} settlements, {assets} assets, {_span_text(funding)} "
            f"({years:.2f}y)"
            + (f" -- {'; '.join(shortfalls)}" if shortfalls else "")
            + span_note
        ),
    )


def check_duplicate_bars(raw: pl.DataFrame, dataset: str) -> AcceptanceCheck:
    """`count_duplicate_bars(df)` is 0 on a first archive run.

    The archive publishes one file per (symbol, month) with no overlap, so a
    first run cannot produce a duplicate: a non-zero count means the month loop
    double-counted a boundary. A *re-run* of a window already loaded produces
    them legitimately -- the store is append-only and readers collapse to the
    latest ingestion -- so the message says which of the two it cannot tell
    apart rather than pretending to know.
    """
    if not len(raw):
        return AcceptanceCheck(
            name=f"{dataset}_duplicates",
            passed=True,
            message="no rows to check",
            blocking=False,
        )

    duplicates = count_duplicate_bars(raw)
    if duplicates == 0:
        return AcceptanceCheck(
            name=f"{dataset}_duplicates",
            passed=True,
            message=f"0 duplicate bars in {len(raw)} rows",
        )

    unique = len(raw) - duplicates
    return AcceptanceCheck(
        name=f"{dataset}_duplicates",
        passed=False,
        message=(
            f"{duplicates} of {len(raw)} rows repeat one of {unique} bars "
            f"({100.0 * duplicates / len(raw):.2f}%). Expected 0 on a first archive "
            f"run; this is either a month loop double-counting a boundary or a "
            f"deliberate re-run of a window already loaded. Readers collapse to the "
            f"latest ingestion either way, so it is not a correctness problem -- but "
            f"on a first run it is a loader bug worth finding."
        ),
    )


def check_bar_gaps(
    bars: pl.DataFrame, thresholds: AcceptanceThresholds
) -> AcceptanceCheck:
    """No asset has a gap > 3 days inside its own listed range.

    This is the check most worth having, because nothing downstream reports
    it: `signals/bars.py` trims each asset to its most recent *gap-free*
    stretch, so a single hole in the middle of an asset's history silently
    shortens every price signal's usable window to whatever follows the hole --
    and a signal that then rejects the asset for insufficient history looks
    exactly like a signal working as designed.

    "Inside its own listed range" is what makes it answerable: an asset listed
    in 2023 is not missing 2021, so the range is per asset, first bar to last.
    """
    if not len(bars):
        return AcceptanceCheck(
            name="bar_gaps",
            passed=False,
            message=f"no {OHLCV_DATASET} rows to check",
        )

    per_asset = (
        bars.select(pl.col("asset_id"), pl.col("event_ts").dt.date().alias("bar_date"))
        .unique()
        .sort(["asset_id", "bar_date"])
        .with_columns(
            (pl.col("bar_date").diff().dt.total_days() - 1)
            .over("asset_id")
            .alias("missing_days")
        )
    )

    worst = (
        per_asset.group_by("asset_id")
        .agg(
            pl.col("missing_days").max().fill_null(0).alias("worst_gap"),
            pl.col("bar_date").min().alias("first_bar"),
            pl.col("bar_date").max().alias("last_bar"),
            pl.col("missing_days").sum().fill_null(0).alias("total_missing"),
        )
        .sort("worst_gap", descending=True)
    )

    offenders = worst.filter(pl.col("worst_gap") > thresholds.max_gap_days)
    if not len(offenders):
        biggest = int(cast(int, worst["worst_gap"].max())) if len(worst) else 0
        return AcceptanceCheck(
            name="bar_gaps",
            passed=True,
            message=(
                f"no asset has a gap > {thresholds.max_gap_days} days inside its "
                f"listed range (worst is {biggest} day(s), across "
                f"{len(worst)} assets)"
            ),
        )

    sample = "; ".join(
        f"{row['asset_id']} {row['worst_gap']}d gap "
        f"({row['first_bar']}..{row['last_bar']}, {row['total_missing']}d missing)"
        for row in offenders.head(5).to_dicts()
    )
    return AcceptanceCheck(
        name="bar_gaps",
        passed=False,
        message=(
            f"{len(offenders)} of {len(worst)} assets have a gap > "
            f"{thresholds.max_gap_days} days inside their own listed range. "
            f"signals/bars.py trims to the gap-free tail, so each of these has a "
            f"shorter usable history than its date range suggests. Worst: {sample}"
            + (f" (+{len(offenders) - 5} more)" if len(offenders) > 5 else "")
        ),
    )


def check_universe_snapshots(
    store: ParquetStore, venue: str, thresholds: AcceptanceThresholds
) -> AcceptanceCheck:
    """One snapshot per rebalance date over the window, with plausible members.

    The cadence is *inferred* from the spacing of the snapshots that are there,
    rather than re-derived from a frequency argument, for two reasons: it does
    not require the caller to remember which `--freq` the rebuild used, and
    `audit` must not import `universe` to borrow `snapshot_dates` (no sideways
    imports). A missing date shows up as a spacing that is a multiple of the
    modal one, which is the thing actually worth detecting.
    """
    try:
        df = store.read(
            UNIVERSE_DATASET, columns=["asset_id", "venue", "event_ts", "in_universe"]
        )
    except FileNotFoundError:
        df = pl.DataFrame()

    if len(df) and "venue" in df.columns:
        df = df.filter(pl.col("venue") == venue)

    if not len(df):
        return AcceptanceCheck(
            name="universe_snapshots",
            passed=False,
            message=(
                f"no {UNIVERSE_DATASET} snapshots for venue {venue!r}. The universe "
                f"dataset is an input, not an output: DatastoreUniverse reads these, "
                f"so every backtest runs on an empty book and the audit reports "
                f"coverage as not evaluated -- neither of which raises. This is the "
                f"DATA.md step-3 failure; the usual cause is a strict-mode rebuild "
                f"over backfilled history. Re-run `python -m universe.builder` with "
                f"--pit-mode event."
            ),
        )

    members = (
        df.filter(pl.col("in_universe"))
        .group_by("event_ts")
        .agg(pl.col("asset_id").n_unique().alias("members"))
    )
    considered = df.select("event_ts").unique()

    dates = sorted(d.date() for d in considered["event_ts"].to_list())
    counts_by_date = {
        row["event_ts"].date(): row["members"] for row in members.to_dicts()
    }
    empty = [d for d in dates if counts_by_date.get(d, 0) == 0]
    sizes = [counts_by_date.get(d, 0) for d in dates]
    median = int(statistics.median(sizes)) if sizes else 0

    if len(dates) < 2:
        return AcceptanceCheck(
            name="universe_snapshots",
            passed=False,
            message=(
                f"only {len(dates)} snapshot date(s) ({dates[0] if dates else '-'}), "
                f"median {median} members. A one-row universe is the same step-3 "
                f"failure as none at all -- a backtest sees a book that never changes."
            ),
        )

    # strict=False on purpose: `dates[1:]` is one shorter than `dates`, which is
    # the whole point of pairing consecutive elements.
    pairs = list(zip(dates, dates[1:], strict=False))
    cadence = statistics.mode((b - a).days for a, b in pairs)
    # A gap is a spacing wider than the cadence: two weekly snapshots 21 days
    # apart means two dates were never built.
    holes = [(a, b, (b - a).days) for a, b in pairs if cadence and (b - a).days > cadence]

    problems = []
    if holes:
        sample = "; ".join(f"{a}..{b} ({d}d)" for a, b, d in holes[:5])
        problems.append(
            f"{len(holes)} gap(s) in an otherwise {cadence}-day cadence: {sample}"
            + (f" (+{len(holes) - 5} more)" if len(holes) > 5 else "")
        )
    if empty:
        problems.append(
            f"{len(empty)} snapshot(s) have no members (first {empty[0]})"
        )
    if median < thresholds.min_median_universe_members:
        problems.append(
            f"median {median} members < {thresholds.min_median_universe_members}"
        )

    return AcceptanceCheck(
        name="universe_snapshots",
        passed=not problems,
        message=(
            f"{len(dates)} snapshots {dates[0]}..{dates[-1]}, every "
            f"{cadence} day(s), members min/median/max "
            f"{min(sizes)}/{median}/{max(sizes)} (target "
            f"{UNIVERSE_CONFIG.target_size})"
            + (f" -- {'; '.join(problems)}" if problems else "")
        ),
    )


def check_nightly_resume(
    bars: pl.DataFrame,
    venue: str,
    checkpoint_dir: Path | None = None,
) -> AcceptanceCheck:
    """Would `python -m pipeline.nightly --days 1` resume cleanly on top of this?

    `DATA.md` expects the checkpoint to carry "the archive's covered interval".
    It does not, and cannot: `BinanceVisionLoader` writes no checkpoint at all
    -- checkpoints belong to `BackfillRunner`, and the archive loader does not
    go through it. So this check reports what is actually there.

    The distinction that matters is between the two things a missing checkpoint
    costs. `--days 1` is unaffected: with no coverage recorded `resume_window`
    returns the request unchanged, the nightly fetches its one day, and those
    rows append beside the archive's under the same `venue` and `asset_id`.
    A `--start <archive start>` run is a different story -- it would re-fetch
    years the store already holds. That is wasted API budget and duplicate
    bars, not incorrectness, which is why this warns rather than blocks.
    """
    checkpoint_dir = checkpoint_dir or (DATASTORE_PATH.parent / "checkpoints")
    path = Path(checkpoint_dir) / f"{venue}_backfill.json"

    if not path.exists():
        return AcceptanceCheck(
            name="nightly_resume",
            passed=False,
            blocking=False,
            message=(
                f"no checkpoint at {path}. Expected: the archive loader writes none "
                f"(checkpoints belong to BackfillRunner, which it does not use). "
                f"`python -m pipeline.nightly --days 1` still resumes cleanly -- with "
                f"no coverage recorded it fetches the full day it asked for and the "
                f"rows land beside the archive's. What it costs is a wide "
                f"`--start` run, which would re-fetch history the store already holds."
            ),
        )

    try:
        checkpoint = json.loads(path.read_text())
    except (OSError, ValueError) as e:
        return AcceptanceCheck(
            name="nightly_resume",
            passed=False,
            blocking=False,
            message=f"checkpoint at {path} is unreadable ({e}); the nightly will start fresh",
        )

    requested = FetchWindow.from_lookback(1)
    notes = []
    for dataset in (OHLCV_DATASET, FUNDING_DATASET):
        coverage = Coverage.from_json(checkpoint.get(dataset))
        if coverage is None:
            notes.append(f"{dataset}: no coverage recorded, would fetch {requested}")
            continue
        planned = resume_window(
            requested, coverage, overlap_days=LOADER_CONFIG.refetch_overlap_days
        )
        covered = (
            f"{coverage.start.date() if coverage.start else '?'}..{coverage.end.date()}"
        )
        notes.append(
            f"{dataset}: covered {covered}, would fetch "
            f"{planned if planned else 'nothing'}"
        )

    # A checkpoint whose covered end predates the store's newest bar means the
    # two records disagree about what is loaded; the nightly would re-fetch the
    # difference, which is harmless but worth seeing.
    stale = ""
    bounds = _bounds(bars)
    if bounds is not None:
        newest_bar = bounds[1]
        ends = [
            c.end
            for c in (Coverage.from_json(checkpoint.get(d)) for d in (OHLCV_DATASET,))
            if c is not None
        ]
        if ends and max(ends) < newest_bar:
            stale = (
                f"; checkpoint ends {max(ends).date()} but the newest stored bar is "
                f"{newest_bar.date()} -- the archive rows are not reflected in it"
            )

    return AcceptanceCheck(
        name="nightly_resume",
        passed=True,
        message="; ".join(notes) + stale,
    )


# ---------------------------------------------------------------------------
# Running them
# ---------------------------------------------------------------------------


def run_acceptance_checks(
    store: ParquetStore | None = None,
    venue: str = "binance",
    thresholds: AcceptanceThresholds | None = None,
    checkpoint_dir: Path | None = None,
) -> AcceptanceReport:
    """Every check in `DATA.md` §3 step 6, in the order the checklist lists them.

    Raises:
        AcceptanceError: if there is no store to check at all -- a wrong
            `--datastore` path, which is a different answer from a failed check
            and would otherwise read as "the backfill produced nothing".
    """
    store = store or ParquetStore(DATASTORE_PATH)
    thresholds = thresholds or AcceptanceThresholds()

    if not store.root.exists() or not store.list_datasets():
        raise AcceptanceError(
            f"no datasets in the store at {store.root}. Check the path (config "
            f"DATASTORE_PATH, or --datastore) before reading anything into an "
            f"empty result -- a wrong path and a failed backfill look identical "
            f"from here."
        )

    logger.info("Running backfill acceptance checks for venue %s at %s", venue, store.root)

    raw_bars = read_bars(store, OHLCV_DATASET, venue)
    raw_funding = read_bars(store, FUNDING_DATASET, venue)

    # Collapse before counting anything: a bar stored twice is one bar. The
    # duplicate checks take the raw frames, since the repeat is what they mean.
    bars = latest_per_bar(raw_bars)
    funding = latest_per_bar(raw_funding)

    report = AcceptanceReport(
        checks=[
            check_ohlcv_coverage(bars, thresholds),
            check_funding_coverage(funding, bars, thresholds),
            check_duplicate_bars(raw_bars, OHLCV_DATASET),
            check_duplicate_bars(raw_funding, FUNDING_DATASET),
            check_bar_gaps(bars, thresholds),
            check_universe_snapshots(store, venue, thresholds),
            check_nightly_resume(bars, venue, checkpoint_dir),
        ]
    )

    for check in report.checks:
        logger.log(
            20 if check.passed else (40 if check.blocking else 30),
            "acceptance %s: %s -- %s",
            check.name,
            check.status,
            check.message,
        )
    logger.info(
        "Acceptance: %s (%d/%d passed)",
        "ACCEPTED" if report.passed else "BLOCKED",
        sum(1 for c in report.checks if c.passed),
        len(report.checks),
    )
    return report


def main(argv: Sequence[str] | None = None) -> int:
    """`python -m audit.acceptance --venue binance`"""
    parser = argparse.ArgumentParser(
        description="Acceptance checks for a bulk backfill (DATA.md section 3 step 6)",
        epilog=(
            "Run this after `python -m loaders.archive` and "
            "`python -m universe.builder --pit-mode event`, and before any "
            "research. Every failure it reports is one that would otherwise "
            "surface as a quiet result -- a shorter signal history, a thinner "
            "cross-section, an empty book -- rather than as an error."
        ),
    )
    parser.add_argument("--venue", default="binance")
    parser.add_argument(
        "--datastore",
        type=Path,
        help=f"Store root to check (default: config DATASTORE_PATH, {DATASTORE_PATH})",
    )
    parser.add_argument(
        "--checkpoint-dir",
        type=Path,
        help="Where the backfill checkpoints live (default: <datastore>/../checkpoints)",
    )
    parser.add_argument("--min-years", type=float, default=AcceptanceThresholds.min_years)
    parser.add_argument(
        "--min-assets", type=int, default=AcceptanceThresholds.min_ohlcv_assets
    )
    parser.add_argument(
        "--min-funding-assets",
        type=int,
        default=AcceptanceThresholds.min_funding_assets,
    )
    parser.add_argument(
        "--max-gap-days", type=int, default=AcceptanceThresholds.max_gap_days
    )
    parser.add_argument("--json", action="store_true", help="Emit the report as JSON")
    levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
    parser.add_argument("--log-level", choices=levels)
    parser.add_argument("--console-log-level", choices=levels)
    args = parser.parse_args(argv)

    if args.log_level or args.console_log_level:
        set_level(args.log_level or LOG_CONFIG.level, args.console_log_level)

    run_id = new_run_id()
    set_run_id(run_id)

    store = ParquetStore(args.datastore) if args.datastore else ParquetStore(DATASTORE_PATH)
    thresholds = AcceptanceThresholds(
        min_years=args.min_years,
        min_ohlcv_assets=args.min_assets,
        min_funding_assets=args.min_funding_assets,
        max_gap_days=args.max_gap_days,
    )

    try:
        report = run_acceptance_checks(
            store=store,
            venue=args.venue,
            thresholds=thresholds,
            checkpoint_dir=args.checkpoint_dir,
        )
    except AcceptanceError as e:
        # Exit 2, matching `universe.builder`: "could not start" is a different
        # answer from "did not pass", and a caller keying off the code should be
        # able to tell them apart.
        print(f"  cannot run acceptance checks: {e}", file=sys.stderr)
        return 2

    if args.json:
        print(json.dumps(report.to_dict(), indent=2))
    else:
        print(
            f"Backfill acceptance: venue={args.venue}, store={store.root}, "
            f"run_id={run_id}, "
            f"{datetime.now(UTC).replace(tzinfo=None).isoformat(timespec='seconds')}Z"
        )
        print(report.to_text())

    return 0 if report.passed else 1


if __name__ == "__main__":
    sys.exit(main())
