#!/usr/bin/env python3
"""Scratch script: demo Audit module (data quality checks)."""

import logging
import sys
import tempfile
from datetime import datetime

import polars as pl

# Imported first: log_demo puts the repository root on sys.path, so the
# project imports below resolve when this script is run directly.
from log_demo import start_demo_run

from audit.auditor import DataAudit
from config import PAPER
from datastore import DatasetSchema, ParquetStore
from universe import UNIVERSE_SCHEMA

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def main():
    """Demo Audit module on temp data."""
    start_demo_run("audit")
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

        def run_audit(label: str) -> None:
            print(f"\n--- {label} ---")
            audit = DataAudit(store, venue="binance")
            size, source = audit.resolve_universe_size(base_date)
            print(f"Coverage denominator: {size if size is not None else 'n/a'} ({source})")

            results = audit.audit_dataset("ohlcv_demo", base_date)

            print("Audit Results:")
            for result in results:
                status = "✓ PASS" if result.passed else "✗ FAIL"
                print(f"  {status} | {result.check_name} [{result.severity}]")
                print(f"      └─ {result.message}")

            print(f"Should halt trading: {'YES' if audit.should_halt_trading() else 'NO'}")

        try:
            # 1. No universe snapshot: coverage has no honest denominator, so it
            #    reports itself unevaluated instead of halting on a made-up one.
            run_audit("no universe snapshot in the store")

            # 2. With a point-in-time universe snapshot the check becomes real:
            #    5 of 8 members present is below the 90% threshold.
            members = ["BTC", "ETH", "BNB", "SOL", "ADA", "XRP", "DOT", "LINK"]
            universe_df = pl.DataFrame(
                {
                    "asset_id": members,
                    "venue": ["binance"] * len(members),
                    "event_ts": [base_date] * len(members),
                    "ingested_ts": [base_date] * len(members),
                    "in_universe": [True] * len(members),
                    "dollar_volume_median": [1e7] * len(members),
                    "listing_age_days": [365] * len(members),
                    "rank": list(range(1, len(members) + 1)),
                    "exclusion_reason": [None] * len(members),
                },
                schema=UNIVERSE_SCHEMA.to_polars_schema(),
            )
            store.append("universe", universe_df, UNIVERSE_SCHEMA)
            logger.info(f"Wrote universe snapshot with {len(members)} members")

            run_audit("universe snapshot present (8 members, 5 with data)")
            print()

        except Exception as e:
            logger.error(f"Error during audit: {e}", exc_info=True)
            sys.exit(1)

    print("=" * 60)
    print("Audit Demo Complete")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    main()
