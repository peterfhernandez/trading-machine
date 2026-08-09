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
4. duplicate bars, in both the harmless shape (a window loaded twice) and the
   defective one (the same bar emitted twice inside one run) — the gate blocks
   on the second and warns on the first
5. one bar carrying two prices — two listings merged onto one `asset_id`, and
   the fraction-of-a-percent version of it, spot closes spliced with perpetual
   closes (`DATA.md` §9)

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
from datastore import AssetMaster, ParquetStore
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
    # 12 assets against a target of 150 means every snapshot here is thin by
    # construction, so the breadth warning would fire in every section and say
    # nothing about the defect that section is showing. It gets its own
    # demonstration in section 3 instead, at the real threshold.
    min_universe_share_of_target=0.0,
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
    same_run_duplicates: bool = False,
    second_price_scale: float | None = None,
    merged_ticker: bool = False,
    universe_members: int = 8,
) -> ParquetStore:
    assets = [f"A{i:02d}" for i in range(N_ASSETS)]
    store = ParquetStore(root)
    store.append("ohlcv_daily", bars(assets, skip=skip), OHLCV_SCHEMA)
    if duplicate_months:
        # A window loaded twice: the same 30 bars again, hours later. The store
        # keeps both by design and every reader collapses them, so the gate
        # warns rather than blocking.
        again = bars(assets).head(30 * N_ASSETS).with_columns(
            pl.lit(BACKFILL_RAN_AT + timedelta(hours=2)).alias("ingested_ts")
        )
        store.append("ohlcv_daily", again, OHLCV_SCHEMA)
    if same_run_duplicates:
        # The same 30 bars again, five seconds later: inside one ingestion run,
        # which is a loader defect rather than a re-run.
        again = bars(assets).head(30 * N_ASSETS).with_columns(
            pl.lit(BACKFILL_RAN_AT + timedelta(seconds=5)).alias("ingested_ts")
        )
        store.append("ohlcv_daily", again, OHLCV_SCHEMA)
    if second_price_scale is not None:
        # Two listings merged onto one asset_id: the same bars at a different
        # price scale, which `latest_per_bar` then chooses between by ingestion
        # time -- arbitrarily.
        other = (
            bars(assets[:3])
            .head(60 * 3)
            .with_columns(
                pl.lit(BACKFILL_RAN_AT + timedelta(hours=5)).alias("ingested_ts"),
                (pl.col("close") * second_price_scale).alias("close"),
            )
        )
        store.append("ohlcv_daily", other, OHLCV_SCHEMA)
    store.append("funding_rate", funding(assets[:8]), FUNDING_RATE_SCHEMA)
    store.append("universe", universe(n_snapshots, members=universe_members), UNIVERSE_SCHEMA)
    write_asset_master(store, assets, merged_ticker=merged_ticker)
    return store


def write_asset_master(
    store: ParquetStore, assets: list[str], merged_ticker: bool = False
) -> None:
    """The mapping the loaders would have written, so `asset_identity` has
    something to check rather than reporting an absent master.

    With `merged_ticker`, it carries the pre-5.9 shape: two simultaneously
    listed symbols under one `asset_id`. `add_mapping` refuses that now, so
    reproducing it takes `allow_collision=True` — which is the point.
    """
    master = AssetMaster(store.root / "asset_master.parquet")
    for asset in assets:
        master.add_mapping(asset, "binance", f"{asset}USDT", FIRST_BAR)
    if merged_ticker:
        master.add_mapping(
            assets[0], "binance", f"1000{assets[0]}USDT", FIRST_BAR, allow_collision=True
        )


def show(
    title: str, store: ParquetStore, thresholds: AcceptanceThresholds | None = None
) -> None:
    print(f"\n--- {title} " + "-" * max(0, 66 - len(title)))
    report = run_acceptance_checks(
        store=store, venue="binance", thresholds=thresholds or DEMO_THRESHOLDS
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

    print(
        "\nAnd the finding nothing flagged on the first real backfill: member\n"
        "counts of 0/64/139 against a target_size of 150 cleared the median floor\n"
        "of 20 and described a breadth machine running at 43%. The cause is\n"
        "upstream -- the archive picks symbols alphabetically unless told\n"
        "otherwise -- so this warns rather than blocking, and names it."
    )
    show(
        "a thin universe",
        build_store(root / "thinuniverse", universe_members=64),
        thresholds=AcceptanceThresholds(
            min_years=2.0, min_ohlcv_assets=10, min_funding_assets=6
        ),
    )


def section_4_duplicates(root: Path) -> None:
    print("\n" + "=" * 70)
    print("4. Duplicate bars, and the three things they can mean")
    print("=" * 70)
    print(
        "\nThe same count, three causes, and until Phase 5.9 the gate reported the\n"
        "count and said it could not tell them apart -- which left an operator\n"
        "holding a red check and no decision. `audit/duplicates.py` classifies by\n"
        "ingestion cluster and by whether the copies agree, and the gate blocks\n"
        "only on the readings that are defects.\n"
        "\nFirst: a window loaded twice, hours apart, agreeing on value. Expected\n"
        "under append-only storage and collapsed on every read -- a WARN."
    )
    show("re-run (warn)", build_store(root / "dupes", duplicate_months=True))
    print(
        "\nSecond: the same bars five seconds apart, inside one ingestion run.\n"
        "Nothing legitimate emits a bar twice within a run -- a FAIL."
    )
    show("within one run (block)", build_store(root / "samerun", same_run_duplicates=True))


def section_5_two_prices_for_one_bar(root: Path) -> None:
    print("\n" + "=" * 70)
    print("5. Two prices for one bar")
    print("=" * 70)
    print(
        "\nThe check nothing performed, and the one that catches both identity\n"
        "defects the first real backfill hid (DATA.md section 9). A thousandfold\n"
        "ratio is two listings merged onto one asset_id (`1000CATUSDT` beside\n"
        "`CATUSDT`); a fraction of a percent is two *instruments* -- spot closes\n"
        "and perpetual closes -- written into one series, which is why the\n"
        "tolerance is 0.1% rather than something comfortable."
    )
    show(
        "a merged ticker",
        build_store(root / "merged", second_price_scale=1000.0, merged_ticker=True),
    )
    print(
        "\nAnd the version that hid for a whole phase: a 40bp basis, which every\n"
        "dataset-level number survives unchanged. Note that `asset_identity`\n"
        "passes here -- there is no second symbol to find, because both series\n"
        "are the same asset on two market types. Only the prices give it away."
    )
    show(
        "spot against perp",
        build_store(root / "twoinstruments", second_price_scale=1.004),
    )


def section_6_the_real_store() -> None:
    print("\n" + "=" * 70)
    print("6. The real store, if there is one")
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
        section_5_two_prices_for_one_bar(root)
        section_6_the_real_store()
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
