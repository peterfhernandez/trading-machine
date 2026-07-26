#!/usr/bin/env python3
"""Scratch script: demo Nightly pipeline end-to-end."""

import logging
import sys
import tempfile
from datetime import datetime

import polars as pl

from config import PAPER
from datastore import ParquetStore, DatasetSchema, AssetMaster
from pipeline.nightly import NightlyPipeline


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def main():
    """Demo Nightly pipeline end-to-end."""
    if not PAPER:
        logger.error("PAPER mode is False; scratch scripts must not run in production")
        sys.exit(1)

    print("\n" + "=" * 70)
    print("Nightly Pipeline Demo (Dry Run)")
    print("=" * 70 + "\n")

    with tempfile.TemporaryDirectory() as tmpdir:
        logger.info(f"Using temporary directory: {tmpdir}")

        store = ParquetStore(tmpdir)

        base_date = datetime(2024, 1, 1)
        now = base_date.replace(microsecond=0)

        schema = DatasetSchema(
            name="ohlcv_daily",
            fields={
                "asset_id": pl.Utf8,
                "venue": pl.Utf8,
                "event_ts": pl.Datetime("us"),
                "ingested_ts": pl.Datetime("us"),
                "close": pl.Float64,
            },
        )

        assets = ["BTC", "ETH", "BNB"]
        df = pl.DataFrame({
            "asset_id": assets * 10,
            "venue": ["binance"] * 30,
            "event_ts": [now] * 30,
            "ingested_ts": [now] * 30,
            "close": [42000.0, 2500.0, 600.0] * 10,
        })

        store.append("ohlcv_daily", df, schema)
        logger.info("Created test dataset with 30 rows")

        try:
            logger.info("\n" + "=" * 70)
            logger.info("Running Nightly Pipeline (dry-run mode)...")
            logger.info("=" * 70)

            pipeline = NightlyPipeline("binance", dry_run=True)
            success = pipeline.run(days=1)

            print(f"\n✓ Pipeline result: {'SUCCESS' if success else 'FAILED'}")
            print()

        except Exception as e:
            logger.error(f"Error during pipeline run: {e}", exc_info=True)
            sys.exit(1)

    print("=" * 70)
    print("Nightly Pipeline Demo Complete")
    print("=" * 70 + "\n")


if __name__ == "__main__":
    main()
