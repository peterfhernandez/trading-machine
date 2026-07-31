#!/usr/bin/env python3
"""Scratch script: demo OHLCV loader (daily + hourly)."""

import logging
import sys
import tempfile
from datetime import datetime

# Imported first: log_demo puts the repository root on sys.path, so the
# project imports below resolve when this script is run directly.
from log_demo import start_demo_run

from config import PAPER
from datastore import ParquetStore, AssetMaster
from loaders.ohlcv import OHLCVLoader
from loaders.schemas import OHLCV_SCHEMA



logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def main():
    """Demo OHLCV loader on temp data (no production writes)."""
    start_demo_run("loaders")
    if not PAPER:
        logger.error("PAPER mode is False; scratch scripts must not run in production")
        sys.exit(1)

    print("\n" + "=" * 60)
    print("OHLCV Loader Demo")
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
            logger.info("Initializing OHLCV loader for Binance (daily)...")
            loader = OHLCVLoader("binance", lookback_days=7, store=store, asset_master=am)

            logger.info("Fetching last 7 days of daily OHLCV...")
            df = loader.fetch(timeframe="1d")

            if len(df) > 0:
                logger.info(f"Fetched {len(df)} daily candles")
                logger.info(f"Assets covered: {df['asset_id'].n_unique()} unique")
                logger.info(f"Date range: {df['event_ts'].min()} to {df['event_ts'].max()}")
                print("\nDaily OHLCV Sample (first 5 rows):")
                print(df.select(["asset_id", "event_ts", "open", "close", "volume"]).head())

                loader.append("ohlcv_daily", df, OHLCV_SCHEMA)
                info = store.dataset_info("ohlcv_daily")
                print(f"\nOHLCV Daily Dataset Info: {info}")
            else:
                logger.warning("No daily data fetched; check asset master resolution")

            logger.info("Fetching last 7 days of hourly OHLCV...")
            df_hourly = loader.fetch(timeframe="1h")

            if len(df_hourly) > 0:
                logger.info(f"Fetched {len(df_hourly)} hourly candles")
                loader.append("ohlcv_hourly", df_hourly, OHLCV_SCHEMA)
                info = store.dataset_info("ohlcv_hourly")
                print(f"OHLCV Hourly Dataset Info: {info}\n")
            else:
                logger.warning("No hourly data fetched")

        except Exception as e:
            logger.error(f"Error during OHLCV load: {e}", exc_info=True)
            sys.exit(1)

    print("=" * 60)
    print("OHLCV Demo Complete")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    main()
