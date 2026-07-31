#!/usr/bin/env python3
"""Scratch script: resumable, windowed fetching.

Shows the three things item 3 changed, against a stub venue (no network):

1. A loader is told a `[start, end]` interval, not "how many days back from
   now", so a specific stretch of history can be asked for.
2. A window longer than one venue page is walked forward instead of silently
   truncating at the page limit.
3. The checkpoint is read back: a re-run fetches only what is missing, plus a
   deliberate one-day overlap for the incomplete trailing bar.

The last section shows the same window against the real venue's *capabilities*
(read-only, no orders) if the network allows it.
"""

import logging
import sys
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

# Imported first: log_demo puts the repository root on sys.path, so the
# project imports below resolve when this script is run directly.
# isort: off
from log_demo import start_demo_run

# isort: on

from config import LOADER_CONFIG, PAPER
from datastore import AssetMaster, ParquetStore, count_duplicate_bars, latest_per_bar
from loaders.backfill import BackfillRunner
from loaders.base import paginate_time_series
from loaders.window import Coverage, FetchWindow, resume_window

logging.basicConfig(level=logging.WARNING, format="%(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

DAY_MS = 86_400_000


def stub_venue(symbols, first_bar: datetime, n_bars: int, page_size: int):
    """A venue with a finite daily record, served `page_size` bars at a time."""
    exchange = MagicMock()
    exchange.symbols = list(symbols)
    exchange.load_markets = MagicMock()
    exchange.has = {}
    stamps = [
        FetchWindow(first_bar + timedelta(days=i), first_bar + timedelta(days=i)).start_ms
        for i in range(n_bars)
    ]

    def fetch_ohlcv(symbol, timeframe, since=None, limit=None):
        available = [t for t in stamps if since is None or t >= since]
        return [[t, 100.0, 101.0, 99.0, 100.5, 10.0] for t in available[:page_size]]

    exchange.fetch_ohlcv = MagicMock(side_effect=fetch_ohlcv)
    return exchange


def section(title: str) -> None:
    print(f"\n--- {title} ---")


def main():
    start_demo_run("loaders")
    if not PAPER:
        logger.error("PAPER mode is False; scratch scripts must not run in production")
        sys.exit(1)

    print("\n" + "=" * 70)
    print("Windowed, resumable fetching")
    print("=" * 70)

    first_bar = datetime(2024, 1, 1)
    venue = stub_venue(["BTC/USDT"], first_bar, n_bars=90, page_size=30)

    section("a window longer than one page is walked forward")
    window = FetchWindow(first_bar, first_bar + timedelta(days=89))
    one_call = venue.fetch_ohlcv("BTC/USDT", "1d", since=window.start_ms, limit=30)
    paged = paginate_time_series(
        lambda since: venue.fetch_ohlcv("BTC/USDT", "1d", since=since, limit=30),
        window,
        timestamp_of=lambda c: c[0],
    )
    print(f"  window                 : {window} ({window.days + 1} bars)")
    print(f"  one call (page size 30): {len(one_call)} bars  <- silently truncated")
    print(f"  paginated              : {len(paged)} bars")

    section("resuming from a checkpoint")
    covered = Coverage(start=first_bar, end=first_bar + timedelta(days=59))
    for label, requested in (
        ("same window again", FetchWindow(first_bar, first_bar + timedelta(days=59))),
        ("extended forward", FetchWindow(first_bar, first_bar + timedelta(days=89))),
        ("older history", FetchWindow(datetime(2023, 6, 1), datetime(2023, 7, 1))),
    ):
        resumed = resume_window(requested, covered, LOADER_CONFIG.refetch_overlap_days)
        print(f"  {label:20s}: {requested} -> "
              f"{resumed if resumed else 'nothing to fetch'}")

    section("through the runner, end to end")
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        store = ParquetStore(tmp / "parquet")
        master = AssetMaster(tmp / "asset_master.parquet")
        master.add_mapping("BTC", "binance", "BTC/USDT", datetime(2020, 1, 1))

        # The stub only implements OHLCV; funding/OI warn that they got nothing,
        # which is not what this section is about.
        logging.getLogger("loaders").setLevel(logging.ERROR)

        with patch("loaders.ohlcv.ccxt.binance", return_value=venue), \
             patch("loaders.funding_rate.ccxt.binance", return_value=venue), \
             patch("loaders.open_interest.ccxt.binance", return_value=venue), \
             patch("loaders.backfill.time.sleep"):
            runner = BackfillRunner("binance", tmp / "checkpoints", store, master)

            end = first_bar + timedelta(days=59)
            runner.run(start_date=first_bar, end_date=end)
            after_first = store.read("ohlcv_daily")
            print(f"  run 1 ({first_bar.date()}..{end.date()}): "
                  f"{len(after_first)} rows stored")

            runner.run(start_date=first_bar, end_date=end)
            after_second = store.read("ohlcv_daily")
            print(f"  run 2 (same window)       : "
                  f"{len(after_second) - len(after_first)} rows added "
                  f"(the deliberate {LOADER_CONFIG.refetch_overlap_days}-day overlap)")

            end2 = first_bar + timedelta(days=89)
            runner.run(start_date=first_bar, end_date=end2)
            after_third = store.read("ohlcv_daily")
            print(f"  run 3 (extended to {end2.date()}): "
                  f"{len(after_third) - len(after_second)} rows added")

        raw = store.read("ohlcv_daily")
        print(f"\n  rows on disk           : {len(raw)}")
        print(f"  unique bars            : {len(latest_per_bar(raw))}")
        print(f"  repeat ingestions      : {count_duplicate_bars(raw)} "
              f"(collapsed on read)")

        checkpoint = runner.load_checkpoint()
        print(f"  checkpoint             : {checkpoint.get('ohlcv_daily')}")

    section("what the real venue supports")
    try:
        import ccxt

        real = ccxt.binance({"options": {"defaultType": LOADER_CONFIG.perp_market_type}})
        for capability in ("fetchOHLCV", "fetchFundingRateHistory", "fetchOpenInterestHistory"):
            print(f"  {capability:28s}: {real.has.get(capability)}")
    except Exception as e:
        print(f"  (venue unreachable: {type(e).__name__})")

    print("\n" + "=" * 70)
    print("Demo complete")
    print("=" * 70 + "\n")


if __name__ == "__main__":
    main()
