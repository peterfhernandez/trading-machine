#!/usr/bin/env python3
"""Scratch script: why did the acceptance gate block this backfill?

`python -m audit.acceptance` reports what is wrong and, deliberately, does not
say why. Two of its checks say so in as many words: the duplicate check cannot
tell "a deliberate re-run of a window already loaded" from "a month loop
double-counting a boundary", and the gap check cannot tell a venue delisting
from a month the loader skipped. Both distinctions matter — one pair is
harmless and the other is a bug — and neither is answerable from the numbers
the gate prints. Both are answerable from the store.

So this asks the four follow-up questions the gate leaves open:

1. **Duplicates — one run or two?** Every archive append stamps `ingested_ts`
   at the moment its file was parsed, so one `python -m loaders.archive`
   invocation leaves a tight cluster of ingestion timestamps and two
   invocations are minutes or hours apart. If a duplicated bar's copies fall
   in *different* clusters the store was loaded twice, which is expected under
   append-only storage and collapsed on read. If they fall in the *same*
   cluster, one run emitted the bar twice, which is the loader bug worth
   finding.
2. **Duplicates — do the copies agree?** Two rows for one `(asset_id,
   event_ts)` carrying different closes are not a re-run of anything. The
   likeliest cause is two archive symbols collapsing onto one `asset_id`
   (`asset_id_for` strips `1000`/`1M` multipliers, so a re-denominated or
   renamed contract can collide with its own predecessor), and that one *is* a
   correctness problem: `latest_per_bar` picks the latest ingestion, which
   between two price scales is an arbitrary choice.
3. **Gaps — did the archive publish those months at all?** A hole spanning
   months the bucket never published is a delisting, and the only decision to
   make is whether to keep the asset. A hole inside months that *are*
   published is a file the loader skipped — `loaders/archive.py` logs and
   continues on a corrupt or missing file, by design — and it can be refetched.
   `--list-archive` answers this live, for the offending symbols only.
4. **Universe — which rule emptied the early snapshots?** Every snapshot row
   carries its own `exclusion_reason`, so an empty snapshot says why it is
   empty without anything having to be re-derived.

Read-only: it opens the store, the asset master and (with `--list-archive`) the
public archive listing, and writes nothing anywhere.

    PAPER=true python scratch/scratch_backfill_forensics.py
    PAPER=true python scratch/scratch_backfill_forensics.py --list-archive
    PAPER=true python -m scratch.scratch_backfill_forensics --datastore D:/store
"""

import argparse
import logging
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

import polars as pl

# Imported first: log_demo puts the repository root on sys.path, so the
# project imports below resolve when this script is run directly.
from log_demo import start_demo_run

from audit.acceptance import AcceptanceThresholds
from config import DATASTORE_PATH, PAPER
from datastore import ParquetStore, latest_per_bar
from logging_config import get_logger

logging.basicConfig(
    level=logging.ERROR,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
# Named explicitly rather than from `__name__`: this is a diagnostic for what
# `audit/acceptance.py` reported, so its records belong beside the gate's in
# `logs/audit.log`. `__name__` here is `scratch.…`, which is not a component
# and would reach the console only.
logger = get_logger("audit.backfill_forensics")

OHLCV_DATASET = "ohlcv_daily"
FUNDING_DATASET = "funding_rate"
UNIVERSE_DATASET = "universe"

# Two appends this far apart belong to different invocations. An archive run
# appends per symbol as its downloads complete, so within a run the spacing is
# seconds; between runs it is however long the operator took to type the next
# command. Half an hour is comfortably outside the first and inside the second.
RUN_GAP_MINUTES = 30

# The value column whose disagreement means two symbols collided on one
# asset_id, per dataset.
VALUE_COLUMN = {OHLCV_DATASET: "close", FUNDING_DATASET: "funding_rate"}

SEPARATOR = "=" * 78


def heading(text: str) -> None:
    print(f"\n{SEPARATOR}\n{text}\n{SEPARATOR}")


# ---------------------------------------------------------------------------
# Ingestion runs
# ---------------------------------------------------------------------------


def cluster_runs(
    stamps: list[datetime], gap_minutes: int = RUN_GAP_MINUTES
) -> list[tuple[datetime, datetime]]:
    """Group ingestion timestamps into the invocations that produced them.

    One `[start, end]` per cluster, oldest first. The clustering is the whole
    diagnostic: `ingested_ts` is stamped per file parsed, so it is the only
    record the store keeps of *which run* a row came from.
    """
    runs: list[list[datetime]] = []
    for ts in sorted(stamps):
        if runs and (ts - runs[-1][1]) <= timedelta(minutes=gap_minutes):
            runs[-1][1] = ts
        else:
            runs.append([ts, ts])
    return [(lo, hi) for lo, hi in runs]


def label_runs(df: pl.DataFrame, gap_minutes: int = RUN_GAP_MINUTES) -> pl.DataFrame:
    """Add a `run` column naming which ingestion cluster each row belongs to."""
    stamps = df.select("ingested_ts").unique()["ingested_ts"].to_list()
    runs = cluster_runs(stamps, gap_minutes)

    index: dict[datetime, int] = {}
    for i, (lo, hi) in enumerate(runs):
        for ts in stamps:
            if lo <= ts <= hi:
                index[ts] = i

    mapping = pl.DataFrame(
        {
            "ingested_ts": list(index.keys()),
            "run": list(index.values()),
        },
        schema={"ingested_ts": df.schema["ingested_ts"], "run": pl.Int64},
    )
    return df.join(mapping, on="ingested_ts", how="left")


def describe_runs(df: pl.DataFrame, gap_minutes: int = RUN_GAP_MINUTES) -> None:
    """Print one line per ingestion run: when it ran, and how much it wrote."""
    labelled = label_runs(df, gap_minutes)
    summary = (
        labelled.group_by("run")
        .agg(
            pl.len().alias("rows"),
            pl.col("asset_id").n_unique().alias("assets"),
            pl.col("ingested_ts").min().alias("started"),
            pl.col("ingested_ts").max().alias("finished"),
            pl.col("event_ts").min().alias("first_bar"),
            pl.col("event_ts").max().alias("last_bar"),
        )
        .sort("run")
    )
    print(f"  {len(summary)} ingestion run(s) in this dataset:")
    for row in summary.iter_rows(named=True):
        minutes = (row["finished"] - row["started"]).total_seconds() / 60.0
        print(
            f"    run {row['run']}: {row['rows']:>8,} rows, {row['assets']:>4} assets, "
            f"{row['started']:%Y-%m-%d %H:%M} +{minutes:5.1f}min, "
            f"bars {row['first_bar'].date()}..{row['last_bar'].date()}"
        )


# ---------------------------------------------------------------------------
# 1 + 2. What the duplicates are
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class DuplicateAnatomy:
    """What a dataset's duplicate bars are, split by the two questions.

    `cross_run` and `same_run` partition the duplicated bars by whether their
    copies came from one `loaders.archive` invocation or more than one — the
    distinction `audit/acceptance.py` says in its own message that it cannot
    make. `disagreeing` is orthogonal to both and is the one that means a
    defect regardless of the answer.
    """

    rows: int
    bars: int
    duplicated: int
    cross_run: int
    same_run: int
    same_run_on_month_boundary: int
    disagreeing: int
    value_column: str | None
    runs: int

    @property
    def verdict(self) -> str:
        if not self.duplicated:
            return "no duplicates"
        if self.disagreeing:
            return "copies disagree on value -- two symbols on one asset_id"
        if self.same_run and not self.cross_run:
            return "one run emitted each bar twice -- a loader bug"
        if self.cross_run and not self.same_run:
            return "a re-run of a window already loaded -- expected, collapsed on read"
        return "both a re-run and within-run repeats"


def classify_duplicates(
    raw: pl.DataFrame, dataset: str, gap_minutes: int = RUN_GAP_MINUTES
) -> tuple[DuplicateAnatomy, pl.DataFrame]:
    """Split the duplicated bars into the categories that mean different things.

    Returns the counts and the per-bar frame behind them (`asset_id`,
    `event_ts`, `copies`, `runs`, and `distinct_values` where the dataset has a
    value column), so a caller can print examples without recomputing.
    """
    if not len(raw):
        return (
            DuplicateAnatomy(0, 0, 0, 0, 0, 0, 0, VALUE_COLUMN.get(dataset), 0),
            pl.DataFrame(),
        )

    labelled = label_runs(raw, gap_minutes)
    value = VALUE_COLUMN.get(dataset)
    aggs = [pl.len().alias("copies"), pl.col("run").n_unique().alias("runs")]
    if value and value in labelled.columns:
        aggs.append(pl.col(value).n_unique().alias("distinct_values"))

    per_bar = labelled.group_by(["asset_id", "event_ts"]).agg(aggs)
    duplicated = per_bar.filter(pl.col("copies") > 1)
    same_run = duplicated.filter(pl.col("runs") == 1)

    # A month loop that double-counts its boundary repeats the first and/or
    # last bar of a month and nothing else, so the boundary rate among the
    # within-run offenders separates that from a symbol planned twice.
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

    disagreeing = (
        len(duplicated.filter(pl.col("distinct_values") > 1))
        if "distinct_values" in duplicated.columns
        else 0
    )

    anatomy = DuplicateAnatomy(
        rows=len(raw),
        bars=len(per_bar),
        duplicated=len(duplicated),
        cross_run=len(duplicated.filter(pl.col("runs") > 1)),
        same_run=len(same_run),
        same_run_on_month_boundary=boundary,
        disagreeing=disagreeing,
        value_column=value if value in labelled.columns else None,
        runs=int(labelled["run"].n_unique()),
    )
    return anatomy, per_bar


def diagnose_duplicates(
    raw: pl.DataFrame, dataset: str, gap_minutes: int = RUN_GAP_MINUTES
) -> None:
    """Separate a re-run from a loader bug, and both from a symbol collision."""
    heading(f"1-2. Duplicate bars in {dataset}")

    if not len(raw):
        print("  no rows")
        return

    describe_runs(raw, gap_minutes)
    anatomy, per_bar = classify_duplicates(raw, dataset, gap_minutes)
    duplicated = per_bar.filter(pl.col("copies") > 1)

    print(
        f"\n  {anatomy.rows:,} rows -> {anatomy.bars:,} distinct bars; "
        f"{anatomy.duplicated:,} bar(s) stored more than once "
        f"({100.0 * anatomy.duplicated / anatomy.bars:.1f}% of bars)"
    )
    logger.info(
        "forensics %s: %d rows, %d bars, %d duplicated (%d cross-run, %d same-run, "
        "%d disagreeing) -- %s",
        dataset, anatomy.rows, anatomy.bars, anatomy.duplicated, anatomy.cross_run,
        anatomy.same_run, anatomy.disagreeing, anatomy.verdict,
    )
    if not anatomy.duplicated:
        print("  nothing further to explain")
        return

    histogram = duplicated.group_by("copies").agg(pl.len().alias("bars")).sort("copies")
    print(
        "  copies per duplicated bar: "
        + ", ".join(f"{r['copies']}x{r['bars']:,}" for r in histogram.iter_rows(named=True))
    )

    # (1) One run or two? This is the question the gate says it cannot answer.
    print(
        f"\n  copies span different ingestion runs: {anatomy.cross_run:,} bar(s)"
        f"  <- a re-run of a window already loaded"
    )
    print(
        f"  copies within a single ingestion run:  {anatomy.same_run:,} bar(s)"
        f"  <- one run emitted the bar twice"
    )

    if anatomy.same_run:
        share = 100.0 * anatomy.same_run_on_month_boundary / anatomy.same_run
        print(
            f"    of those, {anatomy.same_run_on_month_boundary:,} fall on the first "
            f"or last day of a month ({share:.0f}%) -- a month-boundary double-count "
            f"would be at or near 100%"
        )
        worst = (
            duplicated.filter(pl.col("runs") == 1)
            .group_by("asset_id")
            .agg(pl.len().alias("bars"))
            .sort("bars", descending=True)
            .head(5)
        )
        print(
            "    worst assets: "
            + ", ".join(f"{r['asset_id']} ({r['bars']:,})" for r in worst.iter_rows(named=True))
        )

    # (2) Do the copies agree? A disagreement is not a re-run of anything.
    if anatomy.value_column:
        print(
            f"\n  copies that disagree on {anatomy.value_column}: "
            f"{anatomy.disagreeing:,} bar(s)"
            f"  <- NOT a re-run; two symbols on one asset_id, or a revision"
        )
        if anatomy.disagreeing:
            disagreeing = duplicated.filter(pl.col("distinct_values") > 1)
            assets = (
                disagreeing.group_by("asset_id")
                .agg(pl.len().alias("bars"))
                .sort("bars", descending=True)
            )
            print(
                "    affected assets: "
                + ", ".join(
                    f"{r['asset_id']} ({r['bars']:,})" for r in assets.head(10).iter_rows(named=True)
                )
                + (f" (+{len(assets) - 10} more)" if len(assets) > 10 else "")
            )
            _show_disagreement(raw, disagreeing, anatomy.value_column)

    # Which assets escaped duplication tells you what changed between the runs:
    # a widened --max-symbols shows up as a set of assets present in the later
    # run only.
    only_once = set(
        per_bar.filter(pl.col("copies") == 1)["asset_id"].unique().to_list()
    ) - set(duplicated["asset_id"].unique().to_list())
    if only_once:
        listed = sorted(only_once)
        print(
            f"\n  {len(listed)} asset(s) have no duplicated bar at all: "
            + ", ".join(listed[:15])
            + (f" (+{len(listed) - 15} more)" if len(listed) > 15 else "")
        )
        print(
            "    one run loaded these and the other did not -- either the symbol\n"
            "    set changed between runs (a widened --max-symbols) or an earlier\n"
            "    run stopped before reaching them"
        )

    print(f"\n  verdict: {anatomy.verdict}")


def _show_disagreement(raw: pl.DataFrame, disagreeing: pl.DataFrame, value: str) -> None:
    """Print both copies of a few disagreeing bars, largest ratio first.

    The ratio is the tell: a multiplier collision shows a clean factor of ten
    or a thousand, while a vendor revision shows a fraction of a percent.
    """
    sample = disagreeing.head(200).select("asset_id", "event_ts")
    rows = raw.join(sample, on=["asset_id", "event_ts"], how="inner")
    spread = (
        rows.group_by(["asset_id", "event_ts"])
        .agg(
            pl.col(value).min().alias("low"),
            pl.col(value).max().alias("high"),
        )
        .with_columns(
            pl.when(pl.col("low").abs() > 0)
            .then(pl.col("high") / pl.col("low"))
            .otherwise(None)
            .alias("ratio")
        )
        .sort("ratio", descending=True, nulls_last=True)
        .head(5)
    )
    for row in spread.iter_rows(named=True):
        ratio = f"{row['ratio']:.4g}x" if row["ratio"] is not None else "n/a"
        print(
            f"      {row['asset_id']} {row['event_ts'].date()}: "
            f"{value} {row['low']:.8g} vs {row['high']:.8g}  ({ratio})"
        )


# ---------------------------------------------------------------------------
# 3. What the gaps are
# ---------------------------------------------------------------------------


def find_gaps(bars: pl.DataFrame, max_gap_days: int) -> pl.DataFrame:
    """Every gap over the threshold, one row each — not one row per asset.

    `audit/acceptance.py` reports each asset's *worst* gap, which is the right
    summary for a gate and the wrong one for a diagnosis: an asset with one
    81-day hole and an asset with nine 9-day holes read identically there and
    have completely different causes. Columns: `asset_id`, `previous`,
    `bar_date`, `missing`.
    """
    if not len(bars):
        return pl.DataFrame()

    per_asset = (
        bars.select(pl.col("asset_id"), pl.col("event_ts").dt.date().alias("bar_date"))
        .unique()
        .sort(["asset_id", "bar_date"])
        .with_columns(pl.col("bar_date").shift().over("asset_id").alias("previous"))
        .with_columns(
            ((pl.col("bar_date") - pl.col("previous")).dt.total_days() - 1).alias("missing")
        )
    )
    return per_asset.filter(pl.col("missing") > max_gap_days).sort(
        "missing", descending=True
    )


def diagnose_gaps(
    bars: pl.DataFrame,
    max_gap_days: int,
    datastore: Path,
    list_archive: bool = False,
) -> None:
    """Enumerate every gap, not just the worst per asset, and name its months."""
    heading(f"3. Gaps longer than {max_gap_days} day(s) inside an asset's listed range")

    if not len(bars):
        print("  no rows")
        return

    holes = find_gaps(bars, max_gap_days)

    if not len(holes):
        print("  none")
        return

    symbols = _symbols_by_asset(datastore)
    print(f"  {len(holes)} gap(s) across {holes['asset_id'].n_unique()} asset(s):")
    for row in holes.iter_rows(named=True):
        mapped = ", ".join(symbols.get(row["asset_id"], [])) or "unmapped"
        print(
            f"    {row['asset_id']:<12} {row['missing']:>5}d missing between "
            f"{row['previous']} and {row['bar_date']}   [{mapped}]"
        )

    print(
        "\n  A gap covering months the archive never published is a delisting: the\n"
        "  contract was not trading, there is nothing to refetch, and the decision\n"
        "  is whether the asset is worth keeping. A gap inside months that ARE\n"
        "  published is a file `loaders/archive.py` skipped -- it logs and continues\n"
        "  on a corrupt or missing file by design -- and re-running that window fixes\n"
        "  it. Check `logs/loaders.log` for 'Skipping <SYM> <month>' first."
    )

    if list_archive:
        _compare_against_archive(holes, symbols)
    else:
        print("  Pass --list-archive to ask the bucket which months exist for these.")


def _symbols_by_asset(datastore: Path) -> dict[str, list[str]]:
    """Every venue symbol mapped to each `asset_id`.

    Read from the parquet rather than through `AssetMaster.asset_info`, which
    returns one symbol per venue — and more than one symbol per venue is
    precisely the case worth seeing here.
    """
    path = Path(datastore) / "asset_master.parquet"
    if not path.exists():
        return {}
    frame = pl.read_parquet(path)
    mapping: dict[str, list[str]] = defaultdict(list)
    for row in frame.select("asset_id", "symbol").unique().iter_rows(named=True):
        mapping[row["asset_id"]].append(row["symbol"])
    return {k: sorted(v) for k, v in mapping.items()}


def _compare_against_archive(holes: pl.DataFrame, symbols: dict[str, list[str]]) -> None:
    """Ask the bucket which months it publishes for each offending symbol.

    Live, and the only part of this script that touches the network. It is a
    listing call per affected symbol — a handful of requests, no key, no rate
    limit — and it is what turns "there is a hole" into "the venue delisted it"
    or "we failed to download these months".
    """
    from loaders.archive import KLINES, BinanceVisionLoader

    print("\n  Months published by the archive for the offending symbols:")
    loader = BinanceVisionLoader(market="um")
    for asset_id in holes["asset_id"].unique().to_list():
        for symbol in symbols.get(asset_id, []):
            try:
                months = loader.list_months(symbol, KLINES)
            except Exception as e:  # a listing failure must not end the report
                print(f"    {symbol}: could not list ({e})")
                continue
            if not months:
                print(f"    {symbol}: nothing published")
                continue
            published = set(months)
            expected = _months_between(months[0], months[-1])
            unpublished = sorted(expected - published)
            print(
                f"    {symbol}: {len(months)} month(s) {months[0]}..{months[-1]}"
                + (
                    f"; NOT published: {', '.join(unpublished)}  <- delisted, nothing to refetch"
                    if unpublished
                    else "; contiguous  <- the hole is ours, refetch that window"
                )
            )


def _months_between(first: str, last: str) -> set[str]:
    """Every `YYYY-MM` from `first` to `last` inclusive."""
    cursor = datetime.strptime(first, "%Y-%m")
    end = datetime.strptime(last, "%Y-%m")
    months = set()
    while cursor <= end:
        months.add(f"{cursor.year:04d}-{cursor.month:02d}")
        cursor = (cursor + timedelta(days=32)).replace(day=1)
    return months


# ---------------------------------------------------------------------------
# 4. Why the early universe snapshots are empty
# ---------------------------------------------------------------------------


def diagnose_universe(store: ParquetStore, venue: str, first_n: int = 10) -> None:
    """Print the exclusion breakdown for the earliest snapshots.

    `UniverseBuilder.build` writes one row per asset *considered*, each with
    the rule that excluded it, so an empty snapshot is self-explaining — no
    rule has to be re-derived here.
    """
    heading("4. The empty universe snapshots")

    try:
        df = store.read(UNIVERSE_DATASET)
    except FileNotFoundError:
        print(f"  no {UNIVERSE_DATASET} dataset in the store")
        return

    if len(df) and "venue" in df.columns:
        df = df.filter(pl.col("venue") == venue)
    if not len(df):
        print(f"  no {UNIVERSE_DATASET} snapshots for venue {venue!r}")
        return

    df = latest_per_bar(df)
    per_date = (
        df.group_by("event_ts")
        .agg(
            pl.col("in_universe").sum().alias("members"),
            pl.len().alias("considered"),
        )
        .sort("event_ts")
    )
    empty = per_date.filter(pl.col("members") == 0)
    print(
        f"  {len(per_date)} snapshot date(s) {per_date['event_ts'][0].date()}.."
        f"{per_date['event_ts'][-1].date()}; {len(empty)} with no members"
    )

    print(f"\n  Earliest {first_n} snapshots, and why assets were excluded:")
    for row in per_date.head(first_n).iter_rows(named=True):
        snapshot = df.filter(pl.col("event_ts") == row["event_ts"])
        breakdown = (
            snapshot.filter(pl.col("exclusion_reason").is_not_null())
            .group_by("exclusion_reason")
            .agg(pl.len().alias("n"))
            .sort("n", descending=True)
        )
        reasons = ", ".join(
            f"{r['exclusion_reason']}={r['n']}" for r in breakdown.iter_rows(named=True)
        )
        print(
            f"    {row['event_ts'].date()}  members={row['members']:>4}  "
            f"considered={row['considered']:>4}  {reasons or '-'}"
        )

    if len(empty):
        first_member_date = per_date.filter(pl.col("members") > 0)
        if len(first_member_date):
            print(
                f"\n  First snapshot with members: "
                f"{first_member_date['event_ts'][0].date()}"
            )
        trailing = empty.filter(
            pl.col("event_ts") > per_date.filter(pl.col("members") > 0)["event_ts"].min()
        ) if len(first_member_date) else empty
        print(
            f"  Empty snapshots after that date: {len(trailing)}  "
            f"(leading empties are the warm-up; later ones are not)"
        )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def build_report(
    datastore: Path,
    venue: str,
    max_gap_days: int,
    list_archive: bool,
    gap_minutes: int,
) -> int:
    store = ParquetStore(datastore)
    if not store.root.exists() or not store.list_datasets():
        print(f"  no datasets in the store at {store.root}; nothing to diagnose")
        return 2

    print(f"Backfill forensics: store={store.root}, venue={venue}")
    print(f"Datasets: {', '.join(sorted(store.list_datasets()))}")

    for dataset in (OHLCV_DATASET, FUNDING_DATASET):
        try:
            raw = store.read(dataset)
        except FileNotFoundError:
            heading(f"1-2. Duplicate bars in {dataset}")
            print("  dataset not in the store")
            continue
        if len(raw) and "venue" in raw.columns:
            raw = raw.filter(pl.col("venue") == venue)
        diagnose_duplicates(raw, dataset, gap_minutes)

    try:
        bars = latest_per_bar(
            store.read(OHLCV_DATASET, columns=["asset_id", "venue", "event_ts", "ingested_ts"])
        )
        if len(bars) and "venue" in bars.columns:
            bars = bars.filter(pl.col("venue") == venue)
    except FileNotFoundError:
        bars = pl.DataFrame()
    diagnose_gaps(bars, max_gap_days, datastore, list_archive)

    diagnose_universe(store, venue)

    heading("What to do with this")
    print(
        "  - duplicates that span runs and agree on value: expected under\n"
        "    append-only storage, collapsed by `latest_per_bar` on every read.\n"
        "    Nothing to fix in the loader.\n"
        "  - duplicates inside one run, or that disagree on value: a real defect.\n"
        "    Disagreeing copies mean two archive symbols share one asset_id, and\n"
        "    `latest_per_bar` is choosing between them by ingestion time.\n"
        "  - gaps over months the archive never published: a delisting.\n"
        "  - empty universe snapshots before the first populated one: the\n"
        "    min_listing_age_days warm-up, and rebuilding from a later --start is\n"
        "    the fix. Empty ones after it are not, and need explaining."
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Explain the failures `python -m audit.acceptance` reports",
    )
    parser.add_argument("--venue", default="binance")
    parser.add_argument("--datastore", type=Path, default=DATASTORE_PATH)
    parser.add_argument(
        "--max-gap-days", type=int, default=AcceptanceThresholds.max_gap_days
    )
    parser.add_argument(
        "--run-gap-minutes",
        type=int,
        default=RUN_GAP_MINUTES,
        help="Ingestion timestamps further apart than this belong to different runs",
    )
    parser.add_argument(
        "--list-archive",
        action="store_true",
        help="Ask data.binance.vision which months exist for the gapped symbols (network)",
    )
    args = parser.parse_args(argv)

    start_demo_run("audit")

    if not PAPER:
        logger.error("PAPER mode is False; scratch scripts must not run in production")
        return 1

    return build_report(
        datastore=args.datastore,
        venue=args.venue,
        max_gap_days=args.max_gap_days,
        list_archive=args.list_archive,
        gap_minutes=args.run_gap_minutes,
    )


if __name__ == "__main__":
    sys.exit(main())
