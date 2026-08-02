#!/usr/bin/env python3
"""Scratch script: pull three months of one symbol from data.binance.vision.

The smoke test that the archive route still works before committing to a
twenty-minute run: it lists the bucket, downloads three monthly files, verifies
their published SHA-256s, parses them, registers the symbol in a temporary
asset master and appends to a temporary datastore.

Real network, real files — that is the point, since the URL layout and the CSV
format are the two things a unit test cannot tell you have moved. Nothing is
written outside a temporary directory, and like every demo here it refuses to
run when PAPER is False.

    PAPER=true python scratch/scratch_archive_backfill.py
    PAPER=true python -m scratch.scratch_archive_backfill

An unreachable archive exits non-zero and prints why. That is a finding, not a
crash: `api.binance.com` answers 451 from a US address while this host answers
200, so "which of the two am I talking to" is exactly what this demo is for.
"""

import logging
import sys
import tempfile
from datetime import datetime
from pathlib import Path

# Imported first: log_demo puts the repository root on sys.path, so the
# project imports below resolve when this script is run directly.
from log_demo import start_demo_run

from config import PAPER
from datastore import AssetMaster, ParquetStore, count_duplicate_bars
from loaders.archive import KLINES, ArchiveError, BinanceVisionLoader
from loaders.window import FetchWindow

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

SYMBOL = "BTCUSDT"
WINDOW = FetchWindow(datetime(2024, 1, 1), datetime(2024, 3, 31, 23, 59))


def main() -> int:
    """Demo the archive loader against the live bucket, into a temp store."""
    start_demo_run("loaders")
    if not PAPER:
        logger.error("PAPER mode is False; scratch scripts must not run in production")
        return 1

    print("\n" + "=" * 70)
    print("Binance public archive (data.binance.vision) — one symbol, three months")
    print("=" * 70 + "\n")

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        store = ParquetStore(tmp / "parquet")
        loader = BinanceVisionLoader(
            market="um",
            symbols=[SYMBOL],
            store=store,
            # Explicit, and under the temp directory: the default is the real
            # asset master, and a demo must not write to it.
            asset_master=AssetMaster(tmp / "asset_master.parquet"),
            workers=3,
        )

        try:
            # 1. Enumerate rather than probe: the listing also tells us the
            #    symbol's first published month, i.e. its listing date.
            months = loader.list_months(SYMBOL, KLINES)
            print(f"1. Listing: {len(months)} monthly kline files published for {SYMBOL}")
            print(f"   first: {months[0]}   latest: {months[-1]}")
            print(f"   url:   {loader.url_for(loader.key_for(SYMBOL, months[0], KLINES))}\n")

            # 2. One file, downloaded, checksum-verified and parsed.
            frame = loader.download(SYMBOL, "2024-01", KLINES)
            print(f"2. One month parsed and checksum-verified: {len(frame)} daily bars")
            print(frame.head(3))
            print()

            # 3. The window, appended to the store the way a backfill would.
            daily_rows = loader.run_daily(window=WINDOW)
            funding_rows = loader.run_funding(window=WINDOW)
            print(f"3. Appended {daily_rows} ohlcv_daily and {funding_rows} funding_rate rows")
            print(f"   ohlcv_daily:  {store.dataset_info('ohlcv_daily')}")
            print(f"   funding_rate: {store.dataset_info('funding_rate')}\n")

            # 4. The acceptance checks DATA.md asks for, on this small pull.
            stored = store.read("ohlcv_daily")
            funding = store.read("funding_rate")
            print("4. Acceptance checks on this sample:")
            print(f"   asset_id:            {stored['asset_id'].unique().to_list()}")
            print(f"   event_ts range:      {stored['event_ts'].min()} .. {stored['event_ts'].max()}")
            print(f"   duplicate bars:      {count_duplicate_bars(stored)} (expected 0)")
            print(
                f"   funding mark/index:  {funding['mark_price'].null_count()} nulls of "
                f"{len(funding)} (the archive publishes the rate alone)"
            )
            print(
                "   ingested_ts:         "
                f"{stored['ingested_ts'].max()} — today, not 2024. A backtest over "
                "this history\n                        needs pit_mode='event' "
                "(DATA.md section 4)."
            )
        except ArchiveError as e:
            print(f"\nArchive unreachable or changed: {e}")
            logger.error(f"Archive demo failed: {e}", exc_info=True)
            return 1

    print("\n" + "=" * 70)
    print("Archive demo complete")
    print("=" * 70 + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
