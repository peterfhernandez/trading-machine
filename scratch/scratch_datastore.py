"""
Scratch script demonstrating ParquetStore and AssetMaster functionality.

This script:
1. Creates a temporary datastore
2. Populates it with sample OHLCV data across multiple dates
3. Demonstrates point-in-time reads and symbol resolution
4. Never places real trades (PAPER-only demo)
"""

from datetime import datetime, timedelta
from pathlib import Path
import sys
import tempfile
import shutil

import polars as pl

sys.path.insert(0, str(Path(__file__).parent.parent))

from log_demo import start_demo_run  # noqa: E402

from config import DATASTORE_PATH, PAPER
from datastore import ParquetStore, DatasetSchema, AssetMaster


def demo_parquet_store():
    """Demonstrate the ParquetStore with sample OHLCV data."""
    print("\n" + "=" * 70)
    print("PARQUETSTORE DEMO")
    print("=" * 70)

    # Use a temporary store for this demo
    with tempfile.TemporaryDirectory() as tmpdir:
        store_path = Path(tmpdir) / "parquet"
        store = ParquetStore(store_path)

        print("\n1. Defining dataset schema...")
        schema = DatasetSchema(
            name="ohlcv",
            fields={
                "asset_id": pl.Utf8,
                "exchange": pl.Utf8,
                "open": pl.Float64,
                "high": pl.Float64,
                "low": pl.Float64,
                "close": pl.Float64,
                "volume": pl.Float64,
                "event_ts": pl.Datetime("us"),
                "ingested_ts": pl.Datetime("us"),
            },
        )
        print(f"   Schema: {list(schema.fields.keys())}")

        print("\n2. Appending historical OHLCV data...")
        # Simulate 5 days of daily candles for BTC and ETH
        for day_offset in range(5):
            date = datetime(2024, 1, 1) + timedelta(days=day_offset)
            ingested = date + timedelta(hours=1)  # Ingested slightly after event

            data = []
            for asset_id in ["BTC", "ETH"]:
                price_base = 40000.0 if asset_id == "BTC" else 2000.0
                data.append(
                    {
                        "asset_id": asset_id,
                        "exchange": "binance",
                        "open": float(price_base + day_offset * 100),
                        "high": float(price_base + day_offset * 100 + 500),
                        "low": float(price_base + day_offset * 100 - 500),
                        "close": float(price_base + day_offset * 100 + 200),
                        "volume": 1000.0 + day_offset * 100,
                        "event_ts": date,
                        "ingested_ts": ingested,
                    }
                )

            df = pl.DataFrame(data)
            store.append("ohlcv", df, schema)
            print(f"   Day {day_offset + 1}: Appended 2 rows (BTC, ETH)")

        print("\n3. Querying dataset info...")
        info = store.dataset_info("ohlcv")
        print(f"   Dataset: {info['name']}")
        print(f"   Total rows: {info['row_count']}")
        print(f"   Date range: {info['date_range']}")
        print(f"   Columns: {info['columns']}")

        print("\n4. Point-in-time read (asof='2024-01-02')...")
        df = store.read("ohlcv", asof="2024-01-02")
        print(f"   Rows visible at this date: {len(df)}")
        print("\n   Data preview:")
        print(df.select(["asset_id", "close", "event_ts", "ingested_ts"]))

        print("\n5. Date range read (2024-01-02 to 2024-01-04)...")
        df = store.read("ohlcv", date_range=("2024-01-02", "2024-01-04"))
        print(f"   Rows in this range: {len(df)}")

        print("\n6. Column selection (asset_id, close only)...")
        df = store.read("ohlcv", columns=["asset_id", "close"])
        print(f"   Columns: {df.columns}")
        print(df)

        print("\n7. List datasets...")
        datasets = store.list_datasets()
        print(f"   Datasets: {datasets}")

    print("\n✓ ParquetStore demo complete")



def demo_asset_master():
    """Demonstrate the AssetMaster with symbol mappings."""
    print("\n" + "=" * 70)
    print("ASSETMASTER DEMO")
    print("=" * 70)

    with tempfile.TemporaryDirectory() as tmpdir:
        master_path = Path(tmpdir) / "asset_master.parquet"
        master = AssetMaster(master_path)

        base_time = datetime(2024, 1, 1, 0, 0, 0)

        print("\n1. Adding venue symbol mappings...")
        mappings = [
            ("BTC", "binance", "BTC/USDT"),
            ("BTC", "deribit", "BTC-PERPETUAL"),
            ("ETH", "binance", "ETH/USDT"),
            ("ETH", "deribit", "ETH-PERPETUAL"),
        ]
        for asset_id, venue, symbol in mappings:
            master.add_mapping(asset_id, venue, symbol, base_time, is_primary=True)
            print(f"   {asset_id} -> {venue}:{symbol}")

        print("\n2. Resolving symbols to asset IDs...")
        tests = [
            ("BTC/USDT", "binance"),
            ("BTC-PERPETUAL", "deribit"),
            ("ETH/USDT", "binance"),
        ]
        for symbol, venue in tests:
            asset = master.resolve_symbol(symbol, venue, asof=base_time)
            print(f"   {venue}:{symbol} -> {asset}")

        print("\n3. Getting primary symbol for an asset...")
        for asset_id in ["BTC", "ETH"]:
            for venue in ["binance", "deribit"]:
                symbol = master.get_symbol(asset_id, venue, asof=base_time)
                print(f"   {asset_id} on {venue} -> {symbol}")

        print("\n4. Listing all assets and venues...")
        assets = master.list_assets()
        venues = master.list_venues()
        print(f"   Assets: {assets}")
        print(f"   Venues: {venues}")

        print("\n5. Asset info (all venue symbols for BTC)...")
        info = master.asset_info("BTC", asof=base_time)
        for venue, symbol in info.items():
            print(f"   {venue}: {symbol}")

        print("\n6. Simulating a symbol rename...")
        rename_time = datetime(2024, 2, 1, 0, 0, 0)
        after_rename = datetime(2024, 2, 2, 0, 0, 0)

        # Mark old symbol as ended
        master.add_mapping(
            asset_id="TEST",
            venue="binance",
            symbol="TEST/USDT",
            validity_start=base_time,
            validity_end=rename_time,
            is_primary=True,
        )

        # Add new symbol starting at rename
        master.add_mapping(
            asset_id="TEST",
            venue="binance",
            symbol="NEWTEST/USDT",
            validity_start=rename_time,
            is_primary=True,
        )

        # Before rename
        result = master.resolve_symbol("TEST/USDT", "binance", asof=base_time)
        print(f"   Before rename (2024-01-01): TEST/USDT -> {result}")

        # After rename
        result = master.resolve_symbol("NEWTEST/USDT", "binance", asof=after_rename)
        print(f"   After rename (2024-02-02): NEWTEST/USDT -> {result}")

        # Old symbol should not resolve after rename
        result = master.resolve_symbol("TEST/USDT", "binance", asof=after_rename)
        print(f"   After rename (2024-02-02): TEST/USDT -> {result} (None expected)")

    print("\n✓ AssetMaster demo complete")


def main():
    """Run all demos."""
    start_demo_run("datastore")
    print("\n")
    print("╔" + "=" * 68 + "╗")
    print("║" + " " * 68 + "║")
    print("║" + "  POOR MAN'S TRADING MACHINE — DATASTORE DEMO".center(68) + "║")
    print("║" + " " * 68 + "║")
    print("╚" + "=" * 68 + "╝")

    if not PAPER:
        print(
            "\n[!] PAPER=False: Skipping demo (this is demo code, not production)"
        )
        return

    print(f"\n[+] Configuration:")
    print(f"    DATASTORE_PATH: {DATASTORE_PATH}")
    print(f"    PAPER mode: {PAPER}")

    demo_parquet_store()
    demo_asset_master()

    print("\n" + "=" * 70)
    print("ALL DEMOS COMPLETE")
    print("=" * 70)
    print("\nPhase 1 (Datastore & asset master) is ready for use!")
    print("Next: Phase 2 (Loaders & audit)\n")


if __name__ == "__main__":
    main()
