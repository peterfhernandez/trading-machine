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
- **Not a shape report.** Phase 5.9's lesson: a count of duplicated rows is
  consistent with a harmless re-run and with two defects, and a check that
  cannot separate them leaves an operator holding a red result and no decision.
  Every check here reports a verdict, and blocks only on the readings that are
  actually defects — `audit/duplicates.py` does that classification.
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

from audit.duplicates import (
    VALUE_COLUMN,
    classify_duplicates,
    disagreeing_bars,
    find_concurrent_runs,
)
from config import DATASTORE_PATH, LOADER_CONFIG, LOG_CONFIG, UNIVERSE_CONFIG
from datastore import AssetMaster, ParquetStore, latest_per_bar
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

    # A median far below `UNIVERSE_CONFIG.target_size` clears the floor above
    # while describing a materially thinner breadth machine than the config
    # claims -- 0/64/139 against a target of 150 passed silently on the first
    # real backfill, and the cause was upstream (alphabetical symbol selection).
    # A warning, not a block: a smaller universe is a legitimate choice, an
    # unnoticed one is not.
    min_universe_share_of_target: float = 0.6

    # Two copies of one bar differing by more than this are two different
    # things, not two ingestions of one. Tight on purpose: a ticker collision
    # shows a ratio in the hundreds, but spot-versus-perp -- the defect that
    # hid for a whole phase -- is a fraction of a percent, so a threshold set
    # to catch only the dramatic case would have missed the one that mattered.
    price_disagreement_pct: float = 0.1

    # Assets whose gaps the operator has looked at and accepted (a settled
    # delisting, e.g. AUDIO). Recorded here so the decision is explicit and
    # visible in the report rather than expressed by lowering max_gap_days for
    # everything.
    allow_gapped_assets: frozenset[str] = frozenset()


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


def read_bars(
    store: ParquetStore, dataset: str, venue: str, value_column: str | None = None
) -> pl.DataFrame:
    """Every raw row of `dataset` for `venue`, uncollapsed.

    Raw rows are what the duplicate and price checks need, so the collapse
    happens in the caller that wants it collapsed rather than here.
    `value_column` (`close`, `funding_rate`) is read only when a check needs it:
    the columns are the read's whole cost on a store this size.
    """
    columns = ["asset_id", "venue", "event_ts", "ingested_ts"]
    if value_column:
        columns.append(value_column)
    try:
        df = store.read(dataset, columns=columns)
    except FileNotFoundError:
        return pl.DataFrame()
    except (pl.exceptions.ColumnNotFoundError, pl.exceptions.SchemaError):
        # A dataset written before the column existed, or a fixture that only
        # carries the timestamps: the value checks report "nothing to check"
        # rather than the whole run failing to start.
        try:
            df = store.read(dataset, columns=columns[:-1] if value_column else columns)
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


def check_duplicate_bars(
    raw: pl.DataFrame, dataset: str, concurrent: bool = False
) -> AcceptanceCheck:
    """Duplicated bars, classified into the causes that mean different things.

    The raw count is not a verdict, and reporting it as one is what this check
    used to do. Three causes produce the same number and only two are defects:

    - **copies across runs that agree** -- a deliberate re-run of a window
      already loaded. Expected under append-only storage, collapsed by
      `latest_per_bar` on every read: a **warning**, not a block.
    - **copies inside one ingestion run** -- either a loader emitting a bar
      twice or (see `find_concurrent_runs`) two invocations running at once,
      which risks a *lost* write rather than merely a duplicated one. Blocks.
    - **copies that disagree on value** -- not a re-run of anything. Blocks.

    Measured on the **raw** frame, before the `latest_per_bar` collapse every
    other check runs behind: after it, this could only ever report zero.
    """
    if not len(raw):
        return AcceptanceCheck(
            name=f"{dataset}_duplicates",
            passed=True,
            message="no rows to check",
            blocking=False,
        )

    anatomy, _ = classify_duplicates(raw, dataset, concurrent=concurrent)
    if not anatomy.duplicated:
        return AcceptanceCheck(
            name=f"{dataset}_duplicates",
            passed=True,
            message=f"0 duplicate bars in {anatomy.rows} rows, {anatomy.runs} ingestion run(s)",
        )

    share = 100.0 * anatomy.duplicated / anatomy.bars
    summary = (
        f"{anatomy.duplicated} of {anatomy.bars} bars stored more than once "
        f"({share:.2f}%), across {anatomy.runs} ingestion run(s): {anatomy.verdict}"
    )

    if anatomy.blocking:
        return AcceptanceCheck(
            name=f"{dataset}_duplicates",
            passed=False,
            message=(
                summary + ". Re-pull the window rather than repairing in place: "
                "no column records which run wrote a given row."
            ),
        )

    return AcceptanceCheck(
        name=f"{dataset}_duplicates",
        passed=False,
        blocking=False,
        message=(
            summary + ". Nothing to fix -- storing a re-fetched bar twice is what "
            "append-only means, and every reader collapses to the latest ingestion."
        ),
    )


def check_price_agreement(
    raw: pl.DataFrame, dataset: str, thresholds: AcceptanceThresholds
) -> AcceptanceCheck:
    """No `(asset_id, event_ts)` carries two materially different prices.

    The check nothing performed, and the one that would have caught both
    identity defects at once. Two causes, both real and both found on the first
    backfill (`DATA.md` §9.1, §9.2):

    - **two listings under one `asset_id`** -- `1000CATUSDT` merged with
      `CATUSDT` by a canonicalisation that stripped multiplier prefixes. The
      ratio is enormous (7.1e5x for `CAT`) and impossible to miss once looked
      for.
    - **two instruments under one series** -- the ccxt loader reading spot while
      the archive pulled perpetuals. This is the one worth setting a *tight*
      threshold for: the two agree to a fraction of a percent, so it hid behind
      every check that only asked whether the numbers were wildly different.
    """
    value = VALUE_COLUMN.get(dataset)
    if not len(raw) or not value or value not in raw.columns:
        return AcceptanceCheck(
            name=f"{dataset}_price_agreement",
            passed=True,
            message="no rows to check",
            blocking=False,
        )

    offenders = disagreeing_bars(raw, dataset, thresholds.price_disagreement_pct)
    if not len(offenders):
        return AcceptanceCheck(
            name=f"{dataset}_price_agreement",
            passed=True,
            message=(
                f"every repeated bar agrees on {value} to within "
                f"{thresholds.price_disagreement_pct}%"
            ),
        )

    assets = offenders["asset_id"].n_unique()
    sample = "; ".join(
        f"{row['asset_id']} {row['event_ts'].date()} {value} {row['low']:.8g} vs "
        f"{row['high']:.8g} ({row['ratio']:.4g}x)"
        for row in offenders.head(3).to_dicts()
    )
    return AcceptanceCheck(
        name=f"{dataset}_price_agreement",
        passed=False,
        message=(
            f"{len(offenders)} bar(s) across {assets} asset(s) carry two {value} "
            f"values differing by more than {thresholds.price_disagreement_pct}%. "
            f"A ratio of hundreds or thousands means two listings merged onto one "
            f"asset_id; a fraction of a percent means two instruments (spot and "
            f"perpetual) in one series. Worst: {sample}"
        ),
    )


def check_asset_identity(store: ParquetStore, venue: str) -> AcceptanceCheck:
    """No two venue symbols resolve to one `asset_id` in the asset master.

    The store-side symptom of a collision is a price series that switches scale;
    this asks the master directly, which is cheaper and names both symbols.
    `AssetMaster.add_mapping` refuses new collisions, so a finding here is a
    master built before the guard existed -- which is precisely the state the
    re-pull is meant to leave behind.
    """
    path = store.root / "asset_master.parquet"
    if not path.exists():
        return AcceptanceCheck(
            name="asset_identity",
            passed=False,
            blocking=False,
            message=(
                f"no asset master at {path}; nothing to check. The loaders write "
                f"it as they register symbols, so an absent one means nothing has "
                f"been ingested through them."
            ),
        )

    collisions = [c for c in AssetMaster(path).find_collisions() if c.venue == venue]
    if not collisions:
        return AcceptanceCheck(
            name="asset_identity",
            passed=True,
            message=f"no asset_id maps two different {venue} listings",
        )

    # Simultaneous listings are the defect; sequential ones are a rename, and
    # also the Phase 10a trigger. Blocking on the second would stop every
    # legitimate ticker change, which is the thing the asset master is for.
    overlapping = [c for c in collisions if c.relation == "overlap"]
    abutting = [c for c in collisions if c.is_redenomination_candidate]

    if overlapping:
        return AcceptanceCheck(
            name="asset_identity",
            passed=False,
            message=(
                f"{len(overlapping)} asset_id(s) map two {venue} listings that were "
                f"live at the same time, so they are different assets sharing a "
                f"ticker. Their bars are interleaved under one asset_id and "
                f"latest_per_bar picks between them by ingestion time. "
                + "; ".join(c.describe() for c in overlapping[:3])
            ),
        )

    return AcceptanceCheck(
        name="asset_identity",
        passed=False,
        blocking=False,
        message=(
            f"{len(abutting)} asset_id(s) map two sequential {venue} listings. That "
            f"is a rename as far as the store is concerned, and the validity ranges "
            f"express it -- but it is also the shape of a redenomination or a reused "
            f"ticker, which need price adjustment (DATA.md section 10, Phase 10a). "
            f"Check the seam before researching across it: "
            + "; ".join(c.describe() for c in abutting[:3])
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

    # An accepted delisting is an operator decision, and recording it as one is
    # better than lowering the threshold for every asset: the exemption is
    # per asset, named, and printed in the message either way.
    allowed = sorted(
        set(offenders["asset_id"].to_list()) & set(thresholds.allow_gapped_assets)
    )
    if allowed:
        offenders = offenders.filter(~pl.col("asset_id").is_in(allowed))
    allowance = (
        f" ({len(allowed)} allowed by --allow-gapped-assets: {', '.join(allowed)})"
        if allowed
        else ""
    )

    if not len(offenders):
        biggest = int(cast(int, worst["worst_gap"].max())) if len(worst) else 0
        return AcceptanceCheck(
            name="bar_gaps",
            passed=True,
            message=(
                f"no asset has a gap > {thresholds.max_gap_days} days inside its "
                f"listed range (worst is {biggest} day(s), across "
                f"{len(worst)} assets)" + allowance
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
            + allowance
            + ". scratch/scratch_backfill_forensics.py --list-archive separates a "
            "delisting (nothing to refetch; pass --allow-gapped-assets once you "
            "have decided to keep the asset) from a month the loader skipped"
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
    sizes = [counts_by_date.get(d, 0) for d in dates]
    median = int(statistics.median(sizes)) if sizes else 0

    # Empties *before* the first populated snapshot are the
    # `min_listing_age_days` warm-up, not the step-3 failure this check exists
    # for: a history built from the same date as the first bar has no asset old
    # enough to qualify for 30 days, and no amount of rebuilding removes that
    # (append-only). Empties *after* it are a real failure -- the universe went
    # from populated to empty, which nothing legitimate does.
    populated = [d for d in dates if counts_by_date.get(d, 0) > 0]
    first_populated = populated[0] if populated else None
    empty = [d for d in dates if counts_by_date.get(d, 0) == 0]
    warm_up = [d for d in empty if first_populated is None or d < first_populated]
    empty_after = [d for d in empty if first_populated is not None and d > first_populated]

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
    if first_populated is None:
        problems.append(f"no snapshot has any members ({len(empty)} empty)")
    if empty_after:
        problems.append(
            f"{len(empty_after)} snapshot(s) have no members after the universe "
            f"was populated on {first_populated} (first {empty_after[0]})"
        )
    if median < thresholds.min_median_universe_members:
        problems.append(
            f"median {median} members < {thresholds.min_median_universe_members}"
        )

    note = ""
    if warm_up:
        note = (
            f"; {len(warm_up)} leading snapshot(s) {warm_up[0]}..{warm_up[-1]} have no "
            f"members -- the UNIVERSE_CONFIG.min_listing_age_days "
            f"({UNIVERSE_CONFIG.min_listing_age_days}d) warm-up, not a failure. Build "
            f"from a start date that far past the first bar to avoid them"
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
            + note
        ),
    )


def check_universe_breadth(
    store: ParquetStore, venue: str, thresholds: AcceptanceThresholds
) -> AcceptanceCheck:
    """Is the universe anywhere near the size the config asks for?

    A separate, non-blocking check because it is a separate question. The
    snapshot check asks whether the rebuild *ran*; this asks whether the machine
    it produced is the one `UNIVERSE_CONFIG.target_size` describes. The first
    real backfill came back at a median of 64 against a target of 150 and
    nothing said so -- the floor of 20 was cleared, the cadence was right, and
    the shortfall was upstream, in an alphabetical symbol selection.

    A smaller universe is a legitimate choice; an unnoticed one is a breadth
    machine quietly running at 40%.
    """
    try:
        df = store.read(UNIVERSE_DATASET, columns=["asset_id", "venue", "event_ts", "in_universe"])
    except FileNotFoundError:
        df = pl.DataFrame()

    if len(df) and "venue" in df.columns:
        df = df.filter(pl.col("venue") == venue)
    if not len(df):
        return AcceptanceCheck(
            name="universe_breadth",
            passed=True,
            blocking=False,
            message="no snapshots to size (universe_snapshots covers that)",
        )

    per_date = (
        df.filter(pl.col("in_universe"))
        .group_by("event_ts")
        .agg(pl.col("asset_id").n_unique().alias("members"))
    )
    sizes = per_date["members"].to_list() or [0]
    median = int(statistics.median(sizes))
    target = UNIVERSE_CONFIG.target_size
    floor = int(target * thresholds.min_universe_share_of_target)

    if median >= floor:
        return AcceptanceCheck(
            name="universe_breadth",
            passed=True,
            message=f"median {median} members against a target of {target}",
        )

    return AcceptanceCheck(
        name="universe_breadth",
        passed=False,
        blocking=False,
        message=(
            f"median {median} members is {100.0 * median / target:.0f}% of "
            f"UNIVERSE_CONFIG.target_size ({target}); expected at least "
            f"{floor}. The universe ranks by liquidity and cuts at the target, so "
            f"a shortfall means it had too few candidates: the loaders pulled "
            f"fewer symbols than the target, or pulled the wrong ones (the "
            f"archive selects alphabetically unless --rank-by-liquidity or an "
            f"explicit --symbols list is given). IR scales with the square root "
            f"of breadth, so this is a real cost, not a cosmetic one."
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

    # The duplicate and price checks need the value column, so these reads are
    # wider than the coverage checks require.
    raw_bars = read_bars(store, OHLCV_DATASET, venue, value_column="close")
    raw_funding = read_bars(store, FUNDING_DATASET, venue, value_column="funding_rate")

    # Collapse before counting anything: a bar stored twice is one bar. The
    # duplicate and price checks take the raw frames, since the repeat is what
    # they mean.
    bars = latest_per_bar(raw_bars)
    funding = latest_per_bar(raw_funding)

    # Asked once, of both datasets together, because that is the only place the
    # answer exists: one invocation loads its datasets in sequence, so clusters
    # that overlap across datasets prove two processes were live.
    concurrent = find_concurrent_runs(
        {OHLCV_DATASET: raw_bars, FUNDING_DATASET: raw_funding}
    )
    if concurrent:
        first, run_a, second, run_b = concurrent[0]
        logger.warning(
            "Concurrent ingestion detected: %s run %s..%s overlaps %s run %s..%s",
            first, run_a.start, run_a.end, second, run_b.start, run_b.end,
        )

    report = AcceptanceReport(
        checks=[
            check_ohlcv_coverage(bars, thresholds),
            check_funding_coverage(funding, bars, thresholds),
            check_duplicate_bars(raw_bars, OHLCV_DATASET, concurrent=bool(concurrent)),
            check_duplicate_bars(raw_funding, FUNDING_DATASET, concurrent=bool(concurrent)),
            check_price_agreement(raw_bars, OHLCV_DATASET, thresholds),
            check_asset_identity(store, venue),
            check_bar_gaps(bars, thresholds),
            check_universe_snapshots(store, venue, thresholds),
            check_universe_breadth(store, venue, thresholds),
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
    parser.add_argument(
        "--allow-gapped-assets",
        default="",
        help=(
            "Comma-separated asset_ids whose gaps are an accepted delisting "
            "(e.g. AUDIO). They are exempted from bar_gaps by name and listed "
            "in the report -- an explicit operator decision rather than a "
            "loosened threshold for everything"
        ),
    )
    parser.add_argument(
        "--max-price-disagreement-pct",
        type=float,
        default=AcceptanceThresholds.price_disagreement_pct,
        help=(
            "How far two copies of one bar may differ before they are two "
            "different things (default: %(default)s%%)"
        ),
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
        price_disagreement_pct=args.max_price_disagreement_pct,
        allow_gapped_assets=frozenset(
            a.strip().upper() for a in args.allow_gapped_assets.split(",") if a.strip()
        ),
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
