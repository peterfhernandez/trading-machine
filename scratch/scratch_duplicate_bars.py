#!/usr/bin/env python3
"""Scratch script: what happens when the same bar is ingested twice.

Simulates three nightly runs whose fetch windows overlap (each pulls the last
two days), which is what the pipeline does today, and shows:

1. what lands on disk -- the store is append-only, so the overlap is stored,
2. what the audit reports about it (`duplicate_bars`, a warning, never a halt),
3. what readers see -- `latest_per_bar` collapses each bar to its latest
   ingestion, so a rolling median is not skewed by the duplicated stretch,
4. that a *revision* (the venue restating a bar) wins over the original.

No network: builds a temporary store from synthetic bars.
"""

import logging
import sys
import tempfile
from datetime import datetime, timedelta

import polars as pl

# Imported first: log_demo puts the repository root on sys.path, so the
# project imports below resolve when this script is run directly.
# isort: off
from log_demo import start_demo_run

# isort: on

from audit.auditor import DataAudit
from config import PAPER
from datastore import ParquetStore, count_duplicate_bars, latest_per_bar
from loaders.schemas import OHLCV_SCHEMA
from universe import UniverseBuilder

logging.basicConfig(level=logging.WARNING, format="%(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

ASOF = datetime(2024, 2, 1)


def write_bars(store, event_dates, ingested_ts, volume=1_000.0, close=100.0):
    """Append one BTC bar per date, all ingested at `ingested_ts`."""
    store.append(
        "ohlcv_daily",
        pl.DataFrame(
            {
                "asset_id": ["BTC"] * len(event_dates),
                "venue": ["binance"] * len(event_dates),
                "timeframe": ["1d"] * len(event_dates),
                "event_ts": list(event_dates),
                "ingested_ts": [ingested_ts] * len(event_dates),
                "open": [close] * len(event_dates),
                "high": [close] * len(event_dates),
                "low": [close] * len(event_dates),
                "close": [close] * len(event_dates),
                "volume": [volume] * len(event_dates),
            },
            schema=OHLCV_SCHEMA.to_polars_schema(),
        ),
        OHLCV_SCHEMA,
    )


def main():
    start_demo_run("datastore")
    if not PAPER:
        logger.error("PAPER mode is False; scratch scripts must not run in production")
        sys.exit(1)

    print("\n" + "=" * 70)
    print("Duplicate bars: append-only storage, read-side collapse")
    print("=" * 70)

    with tempfile.TemporaryDirectory() as tmpdir:
        store = ParquetStore(tmpdir)

        # Three nightly runs, each fetching a 2-day window: days 1..3 of history
        # get stored, and the overlap means day 2 and day 3 arrive twice.
        history = [ASOF - timedelta(days=n) for n in (3, 2, 1)]
        for run, day in enumerate(history, start=1):
            window = [d for d in history if day - timedelta(days=1) <= d <= day]
            write_bars(store, window, ingested_ts=day + timedelta(hours=1))
            print(f"\nnightly run {run} (window ending {day.date()}): "
                  f"fetched {len(window)} bars")

        raw = store.read("ohlcv_daily")
        collapsed = latest_per_bar(raw)

        print("\n--- on disk vs. what readers see ---")
        print(f"  rows stored            : {len(raw)}")
        print(f"  unique bars            : {len(collapsed)}")
        print(f"  repeat ingestions      : {count_duplicate_bars(raw)}")

        print("\n--- audit ---")
        audit = DataAudit(store, venue="binance")
        for result in audit.audit_dataset("ohlcv_daily", ASOF):
            if result.check_name in ("duplicate_bars", "freshness"):
                status = "✓ PASS" if result.passed else "✗ FAIL"
                print(f"  {status} [{result.severity:7s}] {result.check_name}: {result.message}")
        print(f"  should halt trading    : {'YES' if audit.should_halt_trading() else 'NO'}")

        # A rolling median over duplicated rows would weight the overlapping
        # (most recent) days more heavily than the rest of the window.
        builder = UniverseBuilder(store=store, venue="binance")
        median = builder._dollar_volume(ASOF)["dollar_volume_median"][0]
        print("\n--- universe metrics read through the collapse ---")
        print(f"  median dollar volume   : {median:,.0f}  (every bar weighted once)")

        # A revision: the venue restates the most recent bar's volume.
        write_bars(store, [history[-1]], ingested_ts=ASOF + timedelta(hours=1), volume=9_999.0)
        revised = latest_per_bar(store.read("ohlcv_daily"))
        latest_row = revised.filter(pl.col("event_ts") == history[-1])
        print("\n--- after the venue revises the latest bar ---")
        print(f"  volume readers now see : {latest_row['volume'][0]:,.0f} "
              f"(ingested {latest_row['ingested_ts'][0]})")
        print(f"  rows still on disk     : {len(store.read('ohlcv_daily'))} "
              f"(nothing is ever deleted)")

    print("\n" + "=" * 70)
    print("Demo complete")
    print("=" * 70 + "\n")


if __name__ == "__main__":
    main()
