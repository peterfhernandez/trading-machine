#!/usr/bin/env python3
"""Scratch script: demo Audit module (data quality checks)."""

import logging
import sys
import tempfile
from datetime import datetime

import polars as pl

from config import PAPER
from datastore import ParquetStore, DatasetSchema
from audit.auditor import DataAudit


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def main():
    """Demo Audit module on temp data."""
    if not PAPER:
        logger.error("PAPER mode is False; scratch scripts must not run in production")
        sys.exit(1)

    print("\n" + "=" * 60)
    print("Audit Module Demo")
    print("=" * 60 + "\n")

    with tempfile.TemporaryDirectory() as tmpdir:
        logger.info(f"Using temporary directory: {tmpdir}")

        store = ParquetStore(tmpdir)

        base_date = datetime(2024, 1, 1)
        now = base_date.replace(microsecond=0)

        schema = DatasetSchema(
            name="ohlcv_demo",
            fields={
                "asset_id": pl.Utf8,
                "venue": pl.Utf8,
                "event_ts": pl.Datetime("us"),
                "ingested_ts": pl.Datetime("us"),
                "close": pl.Float64,
                "volume": pl.Float64,
            },
        )

        assets = ["BTC", "ETH", "BNB", "SOL", "ADA"]
        df = pl.DataFrame({
            "asset_id": assets * 20,
            "venue": ["binance"] * 100,
            "event_ts": [now] * 100,
            "ingested_ts": [now] * 100,
            "close": [42000.0, 2500.0, 600.0, 100.0, 50.0] * 20,
            "volume": [1000000.0] * 100,
        })

        store.append("ohlcv_demo", df, schema)
        logger.info("Created test dataset with 100 rows")

        try:
            logger.info("Running audit on ohlcv_demo...")
            audit = DataAudit(store)
            results = audit.audit_dataset("ohlcv_demo", base_date)

            print("\nAudit Results:")
            for result in results:
                status = "✓ PASS" if result.passed else "✗ FAIL"
                print(f"  {status} | {result.check_name}")
                print(f"      └─ {result.message}")

            print()
            halt = audit.should_halt_trading()
            print(f"Should halt trading: {'YES' if halt else 'NO'}")
            print()

        except Exception as e:
            logger.error(f"Error during audit: {e}", exc_info=True)
            sys.exit(1)

    print("=" * 60)
    print("Audit Demo Complete")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    main()
