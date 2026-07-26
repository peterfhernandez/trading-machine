#!/usr/bin/env python3
"""Scratch script: demo Funding Rate loader."""

import logging
import sys
import tempfile
from datetime import datetime

from config import PAPER
from datastore import ParquetStore, AssetMaster
from loaders.funding_rate import FundingRateLoader
from loaders.schemas import FUNDING_RATE_SCHEMA


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def main():
    """Demo Funding Rate loader on temp data (no production writes)."""
    if not PAPER:
        logger.error("PAPER mode is False; scratch scripts must not run in production")
        sys.exit(1)

    print("\n" + "=" * 60)
    print("Funding Rate Loader Demo")
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
            logger.info("Initializing Funding Rate loader for Binance...")
            loader = FundingRateLoader("binance", lookback_days=7, store=store, asset_master=am)

            logger.info("Fetching funding rates...")
            df = loader.fetch()

            if len(df) > 0:
                logger.info(f"Fetched {len(df)} funding rate records")
                logger.info(f"Assets covered: {df['asset_id'].n_unique()} unique")
                logger.info(f"Date range: {df['event_ts'].min()} to {df['event_ts'].max()}")
                print("\nFunding Rate Sample (first 5 rows):")
                print(df.select(["asset_id", "event_ts", "funding_rate", "mark_price"]).head())

                loader.append("funding_rate", df, FUNDING_RATE_SCHEMA)
                info = store.dataset_info("funding_rate")
                print(f"\nFunding Rate Dataset Info: {info}\n")
            else:
                logger.warning("No funding rate data fetched; check asset master resolution")

        except Exception as e:
            logger.error(f"Error during funding rate load: {e}", exc_info=True)
            sys.exit(1)

    print("=" * 60)
    print("Funding Rate Demo Complete")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    main()
