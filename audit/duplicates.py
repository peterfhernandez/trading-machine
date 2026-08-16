"""What a duplicate bar *is*, as opposed to how many there are.

`count_duplicate_bars` answers "how many rows repeat a bar", and the first real
backfill showed that number cannot be acted on: 122,650 of 266,772 rows was
consistent with a deliberate re-run (harmless — the store is append-only and
every reader collapses to the latest ingestion), with a month loop
double-counting a boundary (a loader bug), and with two symbols merged onto one
`asset_id` (a correctness defect that silently picks between two price series).
The acceptance gate said as much in its own message and stopped there, which
left an operator with a red check and no decision.

This module makes the distinction the gate was missing. It lives in `audit/`
rather than in `scratch/` — where it was first written — because a gate that
reports shapes and a diagnostic that reports verdicts should not be two
different pieces of code with two different ideas of what a duplicate means.

Three questions, and they are independent:

1. **One invocation or several?** `ingested_ts` is stamped per file parsed, so
   an invocation leaves a cluster of ingestion timestamps and the gap between
   clusters is however long the operator took to type the next command.
2. **Did the invocations overlap?** The clustering above cannot see this on its
   own, and that is exactly how the first diagnosis went wrong (`DATA.md` §9.3):
   two concurrent processes look like one long run. Clusters from *different*
   datasets that overlap in time do prove it, because one invocation loads its
   datasets in sequence.
3. **Do the copies agree?** Orthogonal to both, and the one that means a defect
   whatever the answer to the other two: two rows for one `(asset_id, event_ts)`
   carrying different closes are not a re-run of anything.
"""

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta

import polars as pl

from logging_config import get_logger

logger = get_logger(__name__)

# Two appends further apart than this belong to different invocations. An
# archive run appends per symbol as its downloads complete, so within a run the
# spacing is seconds; between runs it is minutes or hours. Half an hour is
# comfortably outside the first and inside the second — but see
# `find_concurrent_runs`: no gap threshold can separate invocations that overlap
# in time, and pretending otherwise is what produced a wrong diagnosis once.
RUN_GAP_MINUTES = 30

# The column whose disagreement between two copies of one bar means the copies
# are not copies at all.
VALUE_COLUMN = {"ohlcv_daily": "close", "ohlcv_hourly": "close", "funding_rate": "funding_rate"}


@dataclass(frozen=True)
class IngestionRun:
    """One cluster of ingestion timestamps: an invocation, as far as we can tell."""

    start: datetime
    end: datetime

    @property
    def minutes(self) -> float:
        return (self.end - self.start).total_seconds() / 60.0

    def overlaps(self, other: "IngestionRun") -> bool:
        """Strictly: one run began before the other ended, and vice versa.

        Strict on both sides so that two *instantaneous* clusters — a fixture
        that stamps every row with one timestamp, or a dataset written in a
        single append — never read as concurrent. A point carries no interval
        to overlap with, and inferring concurrency from one would recreate the
        original error in the opposite direction.
        """
        return self.start < other.end and other.start < self.end


def cluster_runs(
    stamps: list[datetime], gap_minutes: int = RUN_GAP_MINUTES
) -> list[IngestionRun]:
    """Group ingestion timestamps into the invocations that produced them."""
    bounds: list[list[datetime]] = []
    for ts in sorted(stamps):
        if bounds and (ts - bounds[-1][1]) <= timedelta(minutes=gap_minutes):
            bounds[-1][1] = ts
        else:
            bounds.append([ts, ts])
    return [IngestionRun(lo, hi) for lo, hi in bounds]


def label_runs(df: pl.DataFrame, gap_minutes: int = RUN_GAP_MINUTES) -> pl.DataFrame:
    """Add a `run` column naming which ingestion cluster each row belongs to."""
    stamps = df.select("ingested_ts").unique()["ingested_ts"].to_list()
    runs = cluster_runs(stamps, gap_minutes)

    index: dict[datetime, int] = {}
    for i, run in enumerate(runs):
        for ts in stamps:
            if run.start <= ts <= run.end:
                index[ts] = i

    mapping = pl.DataFrame(
        {"ingested_ts": list(index.keys()), "run": list(index.values())},
        schema={"ingested_ts": df.schema["ingested_ts"], "run": pl.Int64},
    )
    return df.join(mapping, on="ingested_ts", how="left")


def find_concurrent_runs(
    frames: Mapping[str, pl.DataFrame], gap_minutes: int = RUN_GAP_MINUTES
) -> list[tuple[str, IngestionRun, str, IngestionRun]]:
    """Pairs of clusters, in different datasets, that were live at the same time.

    This is the signal the gap threshold cannot provide. One
    `python -m loaders.archive` invocation loads its datasets **in sequence**
    (`ohlcv_daily`, then `funding_rate`), so their ingestion clusters cannot
    overlap. If they do, at least two processes were running — which is a
    different verdict from "one run emitted the bar twice", needs a different
    fix, and is what actually happened.

    Returns `(dataset_a, run_a, dataset_b, run_b)` for each overlapping pair.
    """
    runs_by_dataset: dict[str, list[IngestionRun]] = {}
    for dataset, frame in frames.items():
        if frame is None or not len(frame) or "ingested_ts" not in frame.columns:
            continue
        stamps = frame.select("ingested_ts").unique()["ingested_ts"].to_list()
        runs_by_dataset[dataset] = cluster_runs(stamps, gap_minutes)

    names = sorted(runs_by_dataset)
    overlapping = []
    for i, first in enumerate(names):
        for second in names[i + 1 :]:
            for run_a in runs_by_dataset[first]:
                for run_b in runs_by_dataset[second]:
                    if run_a.overlaps(run_b):
                        overlapping.append((first, run_a, second, run_b))
    return overlapping


@dataclass(frozen=True)
class DuplicateAnatomy:
    """What a dataset's duplicate bars are, split by the questions above."""

    dataset: str
    rows: int
    bars: int
    duplicated: int
    cross_run: int
    same_run: int
    same_run_on_month_boundary: int
    disagreeing: int
    worst_ratio: float | None
    value_column: str | None
    runs: int
    concurrent: bool = False

    @property
    def blocking(self) -> int:
        """Duplicated bars that are a defect rather than an expected re-run."""
        return self.disagreeing + self.same_run

    @property
    def verdict(self) -> str:
        if not self.duplicated:
            return "no duplicates"

        parts = []
        if self.disagreeing:
            detail = (
                f"{self.disagreeing} bar(s) whose copies DISAGREE on "
                f"{self.value_column} -- two symbols on one asset_id, or two "
                f"instruments in one series"
            )
            if self.worst_ratio:
                detail += f" (worst {self.worst_ratio:.4g}x)"
            parts.append(detail)
        if self.same_run:
            if self.concurrent:
                # The honest verdict, and the one the first diagnosis missed:
                # overlapping clusters read as one run, so "within a run" here
                # means "within a stretch of time when two processes were live".
                parts.append(
                    f"{self.same_run} bar(s) inside one ingestion cluster, but "
                    f"clusters overlap across datasets -- concurrent invocations, "
                    f"not necessarily a within-run double-emit"
                )
            else:
                parts.append(
                    f"{self.same_run} bar(s) inside a single ingestion run -- a "
                    f"loader bug, emitting the same bar twice"
                )
        if self.cross_run:
            parts.append(
                f"{self.cross_run} bar(s) across runs, agreeing on value -- a "
                f"re-run of a window already loaded, collapsed on read"
            )
        return "; ".join(parts)


def classify_duplicates(
    raw: pl.DataFrame,
    dataset: str,
    gap_minutes: int = RUN_GAP_MINUTES,
    concurrent: bool = False,
) -> tuple[DuplicateAnatomy, pl.DataFrame]:
    """Split duplicated bars into the categories that mean different things.

    Takes the **raw** frame, before `latest_per_bar`: the repeat is the subject.
    Returns the counts and the per-bar frame behind them (`asset_id`,
    `event_ts`, `copies`, `runs`, and `distinct_values` where the dataset has a
    value column), so a caller can print examples without recomputing.
    """
    value = VALUE_COLUMN.get(dataset)

    if not len(raw):
        return (
            DuplicateAnatomy(dataset, 0, 0, 0, 0, 0, 0, 0, None, value, 0, concurrent),
            pl.DataFrame(),
        )

    labelled = label_runs(raw, gap_minutes)
    # None unless the dataset has a value column *and* this frame carries it: a
    # caller that read only the timestamps gets the run classification and no
    # disagreement check, rather than an error.
    value_column = value if value and value in labelled.columns else None

    aggs = [pl.len().alias("copies"), pl.col("run").n_unique().alias("runs")]
    if value_column:
        aggs += [
            pl.col(value_column).n_unique().alias("distinct_values"),
            pl.col(value_column).min().alias("low"),
            pl.col(value_column).max().alias("high"),
        ]

    per_bar = labelled.group_by(["asset_id", "event_ts"]).agg(aggs)
    duplicated = per_bar.filter(pl.col("copies") > 1)
    same_run = duplicated.filter(pl.col("runs") == 1)

    # A month loop that double-counts its boundary repeats the first and/or last
    # bar of a month and nothing else, so the boundary rate among the within-run
    # offenders separates that from a symbol planned twice.
    boundary = 0
    if len(same_run):
        boundary = int(
            same_run.select(
                (
                    (pl.col("event_ts").dt.day() == 1)
                    | (
                        pl.col("event_ts").dt.offset_by("1d").dt.month()
                        != pl.col("event_ts").dt.month()
                    )
                ).sum()
            ).item()
        )

    disagreeing_frame = (
        duplicated.filter(pl.col("distinct_values") > 1)
        if value_column
        else duplicated.head(0)
    )
    worst_ratio = None
    if value_column and len(disagreeing_frame):
        ratios = disagreeing_frame.filter(pl.col("low").abs() > 0).select(
            (pl.col("high") / pl.col("low")).abs().max()
        )
        if len(ratios) and ratios.item() is not None:
            worst_ratio = float(ratios.item())

    anatomy = DuplicateAnatomy(
        dataset=dataset,
        rows=len(raw),
        bars=len(per_bar),
        duplicated=len(duplicated),
        cross_run=len(duplicated.filter(pl.col("runs") > 1)),
        same_run=len(same_run),
        same_run_on_month_boundary=boundary,
        disagreeing=len(disagreeing_frame),
        worst_ratio=worst_ratio,
        value_column=value_column,
        runs=int(labelled["run"].n_unique()),
        concurrent=concurrent,
    )
    return anatomy, per_bar


def disagreeing_bars(
    raw: pl.DataFrame, dataset: str, tolerance_pct: float
) -> pl.DataFrame:
    """Bars whose copies differ by more than `tolerance_pct`, worst ratio first.

    Independent of the duplicate classification above, and of ingestion
    timestamps entirely: this asks only whether one `(asset_id, event_ts)`
    carries two materially different prices. Two things produce that, and both
    are defects — two listings merged onto one `asset_id` (§9.1, a ratio in the
    hundreds or thousands) and two *instruments* under one series (§9.2, spot
    against perp, a fraction of a percent and therefore invisible in any check
    that only looks for the dramatic case).

    Columns: `asset_id`, `event_ts`, `low`, `high`, `ratio`, `diff_pct`.
    """
    value = VALUE_COLUMN.get(dataset)
    if not len(raw) or not value or value not in raw.columns:
        return pl.DataFrame()

    return (
        raw.group_by(["asset_id", "event_ts"])
        .agg(
            pl.col(value).min().alias("low"),
            pl.col(value).max().alias("high"),
            pl.len().alias("copies"),
        )
        .filter(pl.col("copies") > 1)
        .with_columns(
            pl.when(pl.col("low").abs() > 0)
            .then((pl.col("high") - pl.col("low")).abs() / pl.col("low").abs() * 100.0)
            .otherwise(None)
            .alias("diff_pct"),
            pl.when(pl.col("low").abs() > 0)
            .then((pl.col("high") / pl.col("low")).abs())
            .otherwise(None)
            .alias("ratio"),
        )
        .filter(pl.col("diff_pct") > tolerance_pct)
        .sort("diff_pct", descending=True, nulls_last=True)
    )
