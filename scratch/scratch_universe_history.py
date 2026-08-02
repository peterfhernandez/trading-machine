#!/usr/bin/env python3
"""Scratch script: building a universe *history* over backfilled bars.

Reproduces, on synthetic data, the thing that actually happens after running
`loaders/archive.py` and then `python -m universe.builder`:

    WARNING  No OHLCV data available to build universe as of 2021-09-01
    WARNING  Empty universe; nothing appended
    ... 1,796 more times, for over an hour, exiting 0 with nothing written

The cause is not a missing backfill. `ParquetStore.read(asof=...)` filters
`ingested_ts`, and a bulk archive load stamps every row with the moment it ran
(2026, say) whatever the bar's own date is. So a snapshot dated 2021-09-01 asks
"what did I know on 2021-09-01?" and the honest answer is: nothing.

Four sections:

1. the symptom, strict mode against a backfilled store
2. the preflight that now refuses that run in seconds instead of an hour
3. `--pit-mode event`, which is what a backfill needs
4. what the one-read panel and the batched append are worth
"""

import logging
import shutil
import sys
import tempfile
import time
from datetime import datetime, timedelta
from pathlib import Path

import polars as pl

# Imported first: log_demo puts the repository root on sys.path, so the
# project imports below resolve when this script is run directly.
from log_demo import start_demo_run

from config import PAPER, UniverseConfig
from datastore import ParquetStore
from loaders.schemas import OHLCV_SCHEMA
from universe import OhlcvPanel, PreflightError, UniverseBuilder, build_history

logging.basicConfig(
    level=logging.ERROR,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Small enough to run in seconds, large enough that the panel's effect is
# visible rather than lost in noise.
N_ASSETS = 60
FIRST_BAR = datetime(2021, 1, 1)
N_BARS = 1200
BACKFILL_RAN_AT = datetime(2026, 8, 2, 9, 30)

CONFIG = UniverseConfig(target_size=10, min_volume_usdt=1_000_000.0)


def make_backfilled_store(root: Path) -> ParquetStore:
    """Bars spanning 2021-2022, every one stamped with today's `ingested_ts`.

    This is what the archive loader leaves behind, and the shape of it is the
    whole point: `event_ts` spans years, `ingested_ts` is a single instant.
    """
    store = ParquetStore(root)
    rows = []
    for i in range(N_ASSETS):
        asset_id = f"AST{i:02d}"
        base_volume = 50_000_000.0 / (i + 1)
        for offset in range(N_BARS):
            price = 100.0 + (offset % 17) - (i % 5)
            rows.append(
                (
                    asset_id, "binance", "1d",
                    FIRST_BAR + timedelta(days=offset), BACKFILL_RAN_AT,
                    price, price, price, price, base_volume / price,
                )
            )

    frame = pl.DataFrame(
        rows,
        schema=[
            "asset_id", "venue", "timeframe", "event_ts", "ingested_ts",
            "open", "high", "low", "close", "volume",
        ],
        orient="row",
    ).with_columns(
        pl.col("event_ts").cast(pl.Datetime("us")),
        pl.col("ingested_ts").cast(pl.Datetime("us")),
    )
    store.append("ohlcv_daily", frame, OHLCV_SCHEMA)
    return store


def section_1_the_symptom(store: ParquetStore) -> None:
    print("1. Strict mode against a backfilled store (the reported symptom)")
    print("-" * 70)

    builder = UniverseBuilder(store, config=CONFIG)
    asof = datetime(2021, 9, 1)
    snapshot = builder.build(asof=asof)

    print(f"   bars in the store:        {len(store.read('ohlcv_daily')):,}")
    print(f"   earliest ingested_ts:     {BACKFILL_RAN_AT.date()}")
    print(f"   snapshot rows at {asof.date()}: {len(snapshot)}")
    print("   -> every row was ingested in 2026, so nothing was knowable in 2021.\n")


def section_2_the_preflight(store: ParquetStore) -> None:
    print("2. The preflight refuses the run instead of discovering it 1,796 times")
    print("-" * 70)

    started = time.perf_counter()
    try:
        build_history(
            start=datetime(2021, 9, 1),
            end=datetime(2022, 8, 1),
            freq="daily",
            store=store,
            config=CONFIG,
        )
    except PreflightError as e:
        elapsed = time.perf_counter() - started
        print(f"   refused after {elapsed:.2f}s, before building anything:\n")
        for line in _wrap(str(e), 66):
            print(f"     {line}")
    print()


def section_3_event_mode(store: ParquetStore) -> None:
    print("3. --pit-mode event: what a backfill actually needs")
    print("-" * 70)

    written, failed = build_history(
        start=datetime(2021, 9, 1),
        end=datetime(2022, 8, 1),
        freq="weekly",
        store=store,
        pit_mode="event",
        config=CONFIG,
    )

    stored = store.read("universe")
    members = stored.filter(pl.col("in_universe"))
    print(f"   snapshots written:  {written}   failed: {failed}")
    print(f"   universe rows:      {len(stored):,} ({len(members):,} memberships)")
    print(f"   partition files:    {len(list((store.root / 'universe').glob('date=*/*.parquet')))}")
    print("   (batched: one file per 50 snapshots, not one per snapshot -- every")
    print("    later universe read reopens every file in the partition.)\n")

    first = stored.filter(pl.col("event_ts") == stored["event_ts"].min())
    print(f"   first snapshot {first['event_ts'][0].date()}: "
          f"{int(first['in_universe'].sum())} members, "
          f"max listing age {first['listing_age_days'].max()} days")
    print("   -> event mode is still point-in-time in event time: a snapshot")
    print("      cannot see a bar dated after itself.\n")


def section_4_what_the_panel_is_worth(store: ParquetStore) -> None:
    print("4. One store read for the whole history, not two per date")
    print("-" * 70)

    dates = [datetime(2022, 1, 1) + timedelta(days=7 * i) for i in range(26)]

    per_date = UniverseBuilder(store, config=CONFIG, pit_mode="event")
    started = time.perf_counter()
    for asof in dates:
        per_date.build(asof=asof)
    slow = time.perf_counter() - started

    panel = OhlcvPanel(store, pit_mode="event")
    pooled = UniverseBuilder(store, config=CONFIG, pit_mode="event", bars=panel)
    started = time.perf_counter()
    for asof in dates:
        pooled.build(asof=asof)
    fast = time.perf_counter() - started

    print(f"   {len(dates)} snapshots, reading per date: {slow:6.2f}s")
    print(f"   {len(dates)} snapshots, one panel read:   {fast:6.2f}s")
    if fast > 0:
        print(f"   speedup: {slow / fast:.1f}x")
    print("   The store partitions by *ingestion* date, so a backfill leaves one")
    print("   unprunable partition: every per-date read opens all of it.")
    print("   Identical snapshots either way -- pinned by a test, not by this demo.")
    print()
    print("   This understates it. The demo store is one small parquet file that")
    print("   stays in page cache; a real archive backfill is ~200 files and")
    print("   ~1.8M rows, where the read is nearly all of the per-date cost.")
    print("   Measured there: ~2.3s per date, against 1,796 dates.\n")


def _wrap(text: str, width: int) -> list[str]:
    lines, current = [], ""
    for word in text.split():
        if len(current) + len(word) + 1 > width:
            lines.append(current)
            current = word
        else:
            current = f"{current} {word}".strip()
    if current:
        lines.append(current)
    return lines


def main() -> int:
    start_demo_run("universe")
    if not PAPER:
        logger.error("PAPER mode is False; scratch scripts must not run in production")
        return 1

    print("\n" + "=" * 70)
    print("Universe history over a bulk backfill")
    print("=" * 70 + "\n")

    tmpdir = tempfile.mkdtemp()
    try:
        store = make_backfilled_store(Path(tmpdir) / "store")
        section_1_the_symptom(store)
        section_2_the_preflight(store)
        section_3_event_mode(store)
        section_4_what_the_panel_is_worth(store)
    finally:
        # Explicit rather than a context manager: Windows will not delete a file
        # another handle has open, and this demo has opened a good few parquet
        # files. `ignore_errors` keeps a cleanup failure from masking the run.
        shutil.rmtree(tmpdir, ignore_errors=True)

    print("=" * 70)
    print("Demo complete")
    print("=" * 70 + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
