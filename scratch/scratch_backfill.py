#!/usr/bin/env python3
"""Scratch script: demo Backfill runner (checkpoint persistence)."""

import logging
import sys
import tempfile
from datetime import datetime

# Imported first: log_demo puts the repository root on sys.path, so the
# project imports below resolve when this script is run directly.
from log_demo import start_demo_run

from config import PAPER
from datastore import AssetMaster, ParquetStore
from loaders.backfill import BackfillRunner

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def main():
    """Demo Backfill runner on temp data (no production writes)."""
    start_demo_run("loaders")
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

        # Dynamically fetch symbols from binance and populate asset master
        try:
            import ccxt
            exchange = ccxt.binance()
            exchange.load_markets()
            symbols = exchange.symbols

            # Map every exact USDT-quoted symbol string to its base asset (not a
            # reconstructed "{asset}/USDT" string) so resolve_symbol()'s literal
            # match works regardless of spot/perpetual/quarterly notation.
            usdt_symbols = [s for s in symbols if "/USDT" in s][:50]  # cap for demo speed

            base_date = datetime(2024, 1, 1)
            for symbol in usdt_symbols:
                market = exchange.markets.get(symbol, {})
                base = market.get("base") or symbol.split("/")[0]
                am.add_mapping(base, "binance", symbol, base_date)

            logger.info(f"Asset master initialized with {len(usdt_symbols)} binance symbols")
        except Exception as e:
            logger.warning(f"Failed to fetch symbols from binance: {e}; using fallback")
            base_date = datetime(2024, 1, 1)
            am.add_mapping("BTC", "binance", "BTC/USDT", base_date)
            am.add_mapping("ETH", "binance", "ETH/USDT", base_date)
            am.add_mapping("BNB", "binance", "BNB/USDT", base_date)
            logger.info("Asset master initialized with fallback symbols")

        try:
            checkpoint_dir = f"{tmpdir}/checkpoints"
            logger.info("Initializing BackfillRunner for Binance...")
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
            print(f"  OHLCV Daily: {info_daily.get('row_count', 0)} rows")
            print(f"  OHLCV Hourly: {info_hourly.get('row_count', 0)} rows")
            print(f"  Funding Rate: {info_fr.get('row_count', 0)} rows")
            print(f"  Open Interest: {info_oi.get('row_count', 0)} rows")
            print()

        except Exception as e:
            logger.error(f"Error during backfill: {e}", exc_info=True)
            sys.exit(1)

    print("=" * 60)
    print("Backfill Demo Complete")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    main()
