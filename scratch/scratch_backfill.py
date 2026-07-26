#!/usr/bin/env python3
"""Scratch script: demo Backfill runner (checkpoint persistence)."""

import logging
import sys
import tempfile
from datetime import datetime

from config import PAPER
from datastore import ParquetStore, AssetMaster
from loaders.backfill import BackfillRunner


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def main():
    """Demo Backfill runner on temp data (no production writes)."""
    if not PAPER:
        logger.error("PAPER mode is False; scratch scripts must not run in production")
        sys.exit(1)

    print("\n" + "=" * 60)
    print("Backfill Runner Demo (30 days)")
    print("=" * 60 + "\n")

    with tempfile.TemporaryDirectory() as tmpdir:
        logger.info(f"Using temporary directory: {tmpdir}")

        store = ParquetStore(tmpdir)
        asset_master_path = f"{tmpdir}/asset_master.parquet"
        am = AssetMaster(asset_master_path)

        base_date = datetime(2024, 1, 1)
        am.add_mapping("BTC", "binance", "BTC/USDT", base_date)
        am.add_mapping("ETH", "binance", "ETH/USDT", base_date)
        am.add_mapping("BNB", "binance", "BNB/USDT", base_date)

        logger.info("Asset master initialized with BTC, ETH, BNB")

        try:
            checkpoint_dir = f"{tmpdir}/checkpoints"
            logger.info(f"Initializing BackfillRunner for Binance...")
            runner = BackfillRunner("binance", checkpoint_dir, store, am)

            logger.info("Running 30-day backfill (mock mode)...")
            runner.run(days_back=30)

            checkpoint = runner.load_checkpoint()
            print("\nCheckpoint state after backfill:")
            for loader_name, last_date in checkpoint.items():
                print(f"  {loader_name}: {last_date}")

            info_daily = store.dataset_info("ohlcv_daily")
            info_hourly = store.dataset_info("ohlcv_hourly")
            info_fr = store.dataset_info("funding_rate")
            info_oi = store.dataset_info("open_interest")

            print("\nDataset sizes after backfill:")
            print(f"  OHLCV Daily: {info_daily['row_count']} rows")
            print(f"  OHLCV Hourly: {info_hourly['row_count']} rows")
            print(f"  Funding Rate: {info_fr['row_count']} rows")
            print(f"  Open Interest: {info_oi['row_count']} rows")
            print()

        except Exception as e:
            logger.error(f"Error during backfill: {e}", exc_info=True)
            sys.exit(1)

    print("=" * 60)
    print("Backfill Demo Complete")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    main()
