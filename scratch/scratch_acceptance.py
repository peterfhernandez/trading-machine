#!/usr/bin/env python3
"""Scratch script: the backfill acceptance gate (`DATA.md` §3 step 6).

Every check in `audit/acceptance.py` exists because some upstream command
reports success on the outcome it catches. So this demo does not show a store
passing — that proves nothing. It builds four stores, each broken in one
specific way that `loaders/archive.py` or `universe/builder.py` would exit 0
on, and shows what the gate says about each:

1. a clean backfill — accepted
2. one asset with a hole in the middle of its listed range — every
   dataset-level number stays plausible, and `signals/bars.py` would silently
   trim that asset to whatever follows the hole
3. a universe dataset with one snapshot in it — the `--pit-mode` failure, which
   surfaces downstream as an empty backtest rather than as an error
4. a month loop that double-counted its boundary — duplicate bars the
   append-only store keeps by design

Then it prints what the gate says about the real store, if there is one.
"""

import logging
import shutil
import sys
import tempfile
from datetime import datetime, timedelta
from pathlib import Path

import polars as pl

# Imported first: log_demo puts the repository root on sys.path, so the
# project imports below resolve when this script is run directly.
from log_demo import start_demo_run

from audit.acceptance import AcceptanceError, AcceptanceThresholds, run_acceptance_checks
from config import DATASTORE_PATH, PAPER
from datastore import ParquetStore
from loaders.schemas import FUNDING_RATE_SCHEMA, OHLCV_SCHEMA
from universe.schema import UNIVERSE_SCHEMA

logging.basicConfig(
    level=logging.ERROR,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

FIRST_BAR = datetime(2021, 8, 1)
N_BARS = 900
N_ASSETS = 12
BACKFILL_RAN_AT = datetime(2026, 8, 2, 9, 30)

# Scaled to the demo's store rather than to a real 200-symbol pull, so the
# interesting output is the check that fires rather than "everything is short".
DEMO_THRESHOLDS = AcceptanceThresholds(
    min_years=2.0,
    min_ohlcv_assets=10,
    min_funding_assets=6,
    min_median_universe_members=5,
)


def bars(assets: list[str], skip: dict[str, set[int]] | None = None) -> pl.DataFrame:
    skip = skip or {}
    rows = [
        {
            "asset_id": asset,
            "venue": "binance",
            "timeframe": "1d",
            "event_ts": FIRST_BAR + timedelta(days=offset),
            "ingested_ts": BACKFILL_RAN_AT,
            "open": 100.0,
            "high": 101.0,
            "low": 99.0,
            "close": 100.0,
            "volume": 5_000_000.0,
        }
        for asset in assets
        for offset in range(N_BARS)
        if offset not in skip.get(asset, set())
    ]
    return pl.DataFrame(rows, schema=OHLCV_SCHEMA.to_polars_schema())


def funding(assets: list[str]) -> pl.DataFrame:
    rows = [
        {
            "asset_id": asset,
            "venue": "binance",
            "event_ts": FIRST_BAR + timedelta(days=offset),
            "ingested_ts": BACKFILL_RAN_AT,
            "funding_rate": 0.0001,
            # Null by construction: the archive's funding files carry the rate
            # alone. AUDIT_CONFIG.nullable_columns_by_dataset already permits it.
            "mark_price": None,
            "index_price": None,
        }
        for asset in assets
        for offset in range(N_BARS)
    ]
    return pl.DataFrame(rows, schema=FUNDING_RATE_SCHEMA.to_polars_schema())


def universe(n_snapshots: int, members: int = 8) -> pl.DataFrame:
    rows = [
        {
            "asset_id": f"A{i:02d}",
            "venue": "binance",
            "event_ts": FIRST_BAR + timedelta(days=30 + 7 * snapshot),
            "ingested_ts": BACKFILL_RAN_AT,
            "in_universe": True,
            "dollar_volume_median": 5_000_000.0,
            "listing_age_days": 400,
            "rank": i,
            "exclusion_reason": None,
        }
        for snapshot in range(n_snapshots)
        for i in range(members)
    ]
    return pl.DataFrame(rows, schema=UNIVERSE_SCHEMA.to_polars_schema())


def build_store(
    root: Path,
    skip: dict[str, set[int]] | None = None,
    n_snapshots: int = 110,
    duplicate_months: bool = False,
) -> ParquetStore:
    assets = [f"A{i:02d}" for i in range(N_ASSETS)]
    store = ParquetStore(root)
    store.append("ohlcv_daily", bars(assets, skip=skip), OHLCV_SCHEMA)
    if duplicate_months:
        # What a month loop that re-reads its own boundary leaves behind: the
        # same 30 bars again, under a later ingestion. The store keeps both by
        # design, so nothing upstream of the gate raises.
        again = bars(assets).head(30 * N_ASSETS).with_columns(
            pl.lit(BACKFILL_RAN_AT + timedelta(hours=2)).alias("ingested_ts")
        )
        store.append("ohlcv_daily", again, OHLCV_SCHEMA)
    store.append("funding_rate", funding(assets[:8]), FUNDING_RATE_SCHEMA)
    store.append("universe", universe(n_snapshots), UNIVERSE_SCHEMA)
    return store


def show(title: str, store: ParquetStore) -> None:
    print(f"\n--- {title} " + "-" * max(0, 66 - len(title)))
    report = run_acceptance_checks(
        store=store, venue="binance", thresholds=DEMO_THRESHOLDS
    )
    print(report.to_text())


def section_1_clean(root: Path) -> None:
    print("\n" + "=" * 70)
    print("1. A clean backfill")
    print("=" * 70)
    print(
        "\nThe baseline. Note that `nightly_resume` warns rather than passes:\n"
        "the archive loader writes no checkpoint (checkpoints belong to\n"
        "BackfillRunner, which it does not use), and that costs a wide --start\n"
        "re-run, not the nightly's --days 1."
    )
    show("clean", build_store(root / "clean"))


def section_2_a_hole(root: Path) -> None:
    print("\n" + "=" * 70)
    print("2. One asset with a hole in its listed range")
    print("=" * 70)
    print(
        "\nA05 is missing nine days in the middle. The asset count, the span and\n"
        "the duplicate count are all unchanged -- and `signals/bars.py` trims to\n"
        "the most recent gap-free stretch, so every price signal would see A05's\n"
        "history as starting *after* the hole. A signal that then rejects it for\n"
        "insufficient history looks exactly like a signal working as designed."
    )
    show("gap", build_store(root / "gap", skip={"A05": set(range(400, 409))}))


def section_3_no_universe(root: Path) -> None:
    print("\n" + "=" * 70)
    print("3. A universe dataset with one snapshot in it")
    print("=" * 70)
    print(
        "\nThe DATA.md step-3 failure. `DatastoreUniverse` reads these snapshots,\n"
        "so a book that never changes is what a backtest gets -- and the audit's\n"
        "coverage denominator is the latest snapshot, so coverage reports itself\n"
        "not evaluated. Neither raises. The usual cause is a strict-mode rebuild\n"
        "over backfilled history, which the builder's own preflight now refuses."
    )
    show("one snapshot", build_store(root / "thin", n_snapshots=1))


def section_4_duplicates(root: Path) -> None:
    print("\n" + "=" * 70)
    print("4. A month loop that double-counted its boundary")
    print("=" * 70)
    print(
        "\nThe archive publishes one file per (symbol, month) with no overlap, so\n"
        "a first run cannot produce a duplicate. Readers collapse to the latest\n"
        "ingestion, so this is not a correctness problem -- but on a first run it\n"
        "is a loader bug, and the gate cannot tell it apart from a deliberate\n"
        "re-run, so it says so rather than guessing."
    )
    show("duplicates", build_store(root / "dupes", duplicate_months=True))


def section_5_the_real_store() -> None:
    print("\n" + "=" * 70)
    print("5. The real store, if there is one")
    print("=" * 70)
    print(f"\n  {DATASTORE_PATH}\n")
    try:
        report = run_acceptance_checks(store=ParquetStore(DATASTORE_PATH))
    except AcceptanceError as e:
        print(f"  not checked: {e}")
        print("\n  This is what a machine with no backfill says. Run:")
        print("    python -m loaders.archive --market um --start 2021-08-01 ...")
        print("    python -m universe.builder --pit-mode event --start 2021-09-01 ...")
        return
    print(report.to_text())


def main() -> int:
    start_demo_run("audit")
    if not PAPER:
        logger.error("PAPER mode is False; scratch scripts must not run in production")
        return 1

    print("\n" + "=" * 70)
    print("Backfill acceptance checks (DATA.md section 3 step 6)")
    print("=" * 70)

    tmpdir = tempfile.mkdtemp()
    try:
        root = Path(tmpdir)
        section_1_clean(root)
        section_2_a_hole(root)
        section_3_no_universe(root)
        section_4_duplicates(root)
        section_5_the_real_store()
    finally:
        # Explicit rather than a context manager: Windows will not delete a file
        # another handle has open, and this demo opens a good few parquet files.
        shutil.rmtree(tmpdir, ignore_errors=True)

    print("\n" + "=" * 70)
    print("Demo complete")
    print("=" * 70 + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
