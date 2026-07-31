#!/usr/bin/env python3
"""Scratch script: demo Open Interest loader."""

import logging
import sys
import tempfile
from datetime import datetime

# Imported first: log_demo puts the repository root on sys.path, so the
# project imports below resolve when this script is run directly.
from log_demo import start_demo_run

from config import PAPER
from datastore import ParquetStore, AssetMaster
from loaders.open_interest import OpenInterestLoader
from loaders.schemas import OPEN_INTEREST_SCHEMA



logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def main():
    """Demo Open Interest loader on temp data (no production writes)."""
    start_demo_run("loaders")
    if not PAPER:
        logger.error("PAPER mode is False; scratch scripts must not run in production")
        sys.exit(1)

    print("\n" + "=" * 60)
    print("Open Interest Loader Demo")
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
            logger.info("Initializing Open Interest loader for Binance...")
            loader = OpenInterestLoader("binance", lookback_days=7, store=store, asset_master=am)

            logger.info("Fetching open interest data...")
            df = loader.fetch()

            if len(df) > 0:
                logger.info(f"Fetched {len(df)} open interest snapshots")
                logger.info(f"Assets covered: {df['asset_id'].n_unique()} unique")
                logger.info(f"Date range: {df['event_ts'].min()} to {df['event_ts'].max()}")
                print("\nOpen Interest Sample (first 5 rows):")
                print(df.select(["asset_id", "event_ts", "open_interest", "open_interest_usd"]).head())

                loader.append("open_interest", df, OPEN_INTEREST_SCHEMA)
                info = store.dataset_info("open_interest")
                print(f"\nOpen Interest Dataset Info: {info}\n")
            else:
                logger.warning("No open interest data fetched; check asset master resolution")

        except Exception as e:
            logger.error(f"Error during open interest load: {e}", exc_info=True)
            sys.exit(1)

    print("=" * 60)
    print("Open Interest Demo Complete")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    main()
