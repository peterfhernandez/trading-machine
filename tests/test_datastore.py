"""
Tests for the datastore module (ParquetStore and AssetMaster).
"""

import pytest
from datetime import datetime, timedelta
from pathlib import Path
import tempfile
import shutil

import polars as pl

from datastore import ParquetStore, DatasetSchema, AssetMaster


@pytest.fixture
def tmp_store_path(tmp_path):
    """Create a temporary directory for the datastore."""
    store_path = tmp_path / "parquet"
    yield store_path
    # Cleanup
    if store_path.exists():
        shutil.rmtree(store_path)


@pytest.fixture
def tmp_asset_master_path(tmp_path):
    """Create a temporary path for the asset master."""
    path = tmp_path / "asset_master.parquet"
    yield path
    if path.exists():
        path.unlink()


class TestParquetStore:
    """Tests for ParquetStore append-only behavior and point-in-time reads."""

    def test_append_basic(self, tmp_store_path):
        """Test basic append operation."""
        store = ParquetStore(tmp_store_path)

        # Create a simple OHLCV dataset
        schema = DatasetSchema(
            name="ohlcv",
            fields={
                "asset_id": pl.Utf8,
                "open": pl.Float64,
                "high": pl.Float64,
                "low": pl.Float64,
                "close": pl.Float64,
                "volume": pl.Float64,
                "event_ts": pl.Datetime("us"),
                "ingested_ts": pl.Datetime("us"),
            },
        )

        base_time = datetime(2024, 1, 1, 0, 0, 0)
        df = pl.DataFrame(
            {
                "asset_id": ["BTC", "BTC"],
                "open": [40000.0, 40100.0],
                "high": [40500.0, 40600.0],
                "low": [39900.0, 40000.0],
                "close": [40200.0, 40300.0],
                "volume": [100.0, 150.0],
                "event_ts": [base_time, base_time + timedelta(hours=1)],
                "ingested_ts": [base_time, base_time],
            }
        )

        store.append("ohlcv", df, schema)

        # Verify data was written
        info = store.dataset_info("ohlcv")
        assert info["row_count"] == 2
        assert info["name"] == "ohlcv"

    def test_append_multiple_dates(self, tmp_store_path):
        """Test appending data across multiple dates (partitions)."""
        store = ParquetStore(tmp_store_path)

        schema = DatasetSchema(
            name="ohlcv",
            fields={
                "asset_id": pl.Utf8,
                "close": pl.Float64,
                "event_ts": pl.Datetime("us"),
                "ingested_ts": pl.Datetime("us"),
            },
        )

        # Append data from two different dates
        date1 = datetime(2024, 1, 1, 0, 0, 0)
        date2 = datetime(2024, 1, 2, 0, 0, 0)

        df1 = pl.DataFrame(
            {
                "asset_id": ["BTC"],
                "close": [40000.0],
                "event_ts": [date1],
                "ingested_ts": [date1],
            }
        )

        df2 = pl.DataFrame(
            {
                "asset_id": ["BTC"],
                "close": [41000.0],
                "event_ts": [date2],
                "ingested_ts": [date2],
            }
        )

        store.append("ohlcv", df1, schema)
        store.append("ohlcv", df2, schema)

        # Verify partitions were created
        dataset_path = tmp_store_path / "ohlcv"
        partitions = list(dataset_path.glob("date=*"))
        assert len(partitions) == 2

        # Verify data is readable
        info = store.dataset_info("ohlcv")
        assert info["row_count"] == 2

    def test_no_overwrite_guarantee(self, tmp_store_path):
        """Test that the store never overwrites existing data."""
        store = ParquetStore(tmp_store_path)

        schema = DatasetSchema(
            name="ohlcv",
            fields={
                "asset_id": pl.Utf8,
                "close": pl.Float64,
                "event_ts": pl.Datetime("us"),
                "ingested_ts": pl.Datetime("us"),
            },
        )

        # Append once
        base_time = datetime(2024, 1, 1, 0, 0, 0)
        df1 = pl.DataFrame(
            {
                "asset_id": ["BTC"],
                "close": [40000.0],
                "event_ts": [base_time],
                "ingested_ts": [base_time],
            }
        )

        store.append("ohlcv", df1, schema)

        # Check the file count
        partition = tmp_store_path / "ohlcv" / "date=2024-01-01"
        files_after_first = list(partition.glob("*.parquet"))
        assert len(files_after_first) == 1

        # Append again to the same date
        df2 = pl.DataFrame(
            {
                "asset_id": ["ETH"],
                "close": [2000.0],
                "event_ts": [base_time + timedelta(hours=1)],
                "ingested_ts": [base_time],
            }
        )

        store.append("ohlcv", df2, schema)

        # Check that a new file was created, not overwritten
        files_after_second = list(partition.glob("*.parquet"))
        assert len(files_after_second) == 2

        # Verify both datasets are present
        info = store.dataset_info("ohlcv")
        assert info["row_count"] == 2

    def test_point_in_time_read(self, tmp_store_path):
        """Test point-in-time (asof) reads."""
        store = ParquetStore(tmp_store_path)

        schema = DatasetSchema(
            name="ohlcv",
            fields={
                "asset_id": pl.Utf8,
                "close": pl.Float64,
                "event_ts": pl.Datetime("us"),
                "ingested_ts": pl.Datetime("us"),
            },
        )

        # Append data ingested at different times
        event_time = datetime(2024, 1, 1, 0, 0, 0)
        ingest_time1 = datetime(2024, 1, 1, 6, 0, 0)
        ingest_time2 = datetime(2024, 1, 2, 6, 0, 0)

        # First batch: ingested on Jan 1
        df1 = pl.DataFrame(
            {
                "asset_id": ["BTC"],
                "close": [40000.0],
                "event_ts": [event_time],
                "ingested_ts": [ingest_time1],
            }
        )

        # Second batch: ingested on Jan 2
        df2 = pl.DataFrame(
            {
                "asset_id": ["BTC"],
                "close": [41000.0],
                "event_ts": [event_time],
                "ingested_ts": [ingest_time2],
            }
        )

        store.append("ohlcv", df1, schema)
        store.append("ohlcv", df2, schema)

        # Read as of Jan 1 (should only see first batch)
        result = store.read("ohlcv", asof="2024-01-01")
        assert len(result) == 1
        assert result["close"][0] == 40000.0

        # Read as of Jan 2 (should see both batches)
        result = store.read("ohlcv", asof="2024-01-02")
        assert len(result) == 2

    def test_date_range_filter(self, tmp_store_path):
        """Test date range filtering on reads."""
        store = ParquetStore(tmp_store_path)

        schema = DatasetSchema(
            name="ohlcv",
            fields={
                "asset_id": pl.Utf8,
                "close": pl.Float64,
                "event_ts": pl.Datetime("us"),
                "ingested_ts": pl.Datetime("us"),
            },
        )

        # Append data from three days
        for day in range(1, 4):
            base_time = datetime(2024, 1, day, 0, 0, 0)
            df = pl.DataFrame(
                {
                    "asset_id": ["BTC"],
                    "close": [40000.0 + day * 1000],
                    "event_ts": [base_time],
                    "ingested_ts": [base_time],
                }
            )
            store.append("ohlcv", df, schema)

        # Read only day 2 (closes at 42000.0 since close = 40000 + 2*1000)
        result = store.read("ohlcv", date_range=("2024-01-02", "2024-01-02"))
        assert len(result) == 1
        assert result["close"][0] == 42000.0

        # Read days 2-3
        result = store.read("ohlcv", date_range=("2024-01-02", "2024-01-03"))
        assert len(result) == 2

    def test_schema_validation(self, tmp_store_path):
        """Test that schema validation catches missing required columns."""
        store = ParquetStore(tmp_store_path)

        schema = DatasetSchema(
            name="ohlcv",
            fields={
                "asset_id": pl.Utf8,
                "close": pl.Float64,
                "event_ts": pl.Datetime("us"),
                "ingested_ts": pl.Datetime("us"),
            },
        )

        # Missing 'asset_id' column
        df = pl.DataFrame(
            {
                "close": [40000.0],
                "event_ts": [datetime(2024, 1, 1)],
                "ingested_ts": [datetime(2024, 1, 1)],
            }
        )

        with pytest.raises(ValueError, match="Missing required column"):
            store.append("ohlcv", df, schema)

    def test_column_selection(self, tmp_store_path):
        """Test column selection on reads."""
        store = ParquetStore(tmp_store_path)

        schema = DatasetSchema(
            name="ohlcv",
            fields={
                "asset_id": pl.Utf8,
                "open": pl.Float64,
                "close": pl.Float64,
                "event_ts": pl.Datetime("us"),
                "ingested_ts": pl.Datetime("us"),
            },
        )

        base_time = datetime(2024, 1, 1, 0, 0, 0)
        df = pl.DataFrame(
            {
                "asset_id": ["BTC"],
                "open": [40000.0],
                "close": [40200.0],
                "event_ts": [base_time],
                "ingested_ts": [base_time],
            }
        )

        store.append("ohlcv", df, schema)

        # Read only specific columns
        result = store.read("ohlcv", columns=["asset_id", "close"])
        assert result.columns == ["asset_id", "close"]
        assert len(result) == 1

    def test_list_datasets(self, tmp_store_path):
        """Test listing all datasets."""
        store = ParquetStore(tmp_store_path)

        schema = DatasetSchema(
            name="ohlcv",
            fields={"asset_id": pl.Utf8, "event_ts": pl.Datetime("us"), "ingested_ts": pl.Datetime("us")},
        )

        base_time = datetime(2024, 1, 1, 0, 0, 0)

        # Add two datasets
        df1 = pl.DataFrame(
            {
                "asset_id": ["BTC"],
                "event_ts": [base_time],
                "ingested_ts": [base_time],
            }
        )

        df2 = pl.DataFrame(
            {
                "asset_id": ["ETH"],
                "event_ts": [base_time],
                "ingested_ts": [base_time],
            }
        )

        store.append("ohlcv", df1, schema)
        store.append("funding_rates", df2, schema)

        datasets = store.list_datasets()
        assert "ohlcv" in datasets
        assert "funding_rates" in datasets


class TestAssetMaster:
    """Tests for AssetMaster symbol resolution and point-in-time mapping."""

    def test_add_and_resolve_mapping(self, tmp_asset_master_path):
        """Test adding a mapping and resolving a symbol."""
        master = AssetMaster(tmp_asset_master_path)

        base_time = datetime(2024, 1, 1, 0, 0, 0)
        master.add_mapping(
            asset_id="BTC",
            venue="binance",
            symbol="BTC/USDT",
            validity_start=base_time,
        )

        # Resolve symbol -> asset_id
        asset_id = master.resolve_symbol("BTC/USDT", "binance", asof=base_time)
        assert asset_id == "BTC"

    def test_get_primary_symbol(self, tmp_asset_master_path):
        """Test retrieving the primary symbol for an asset on a venue."""
        master = AssetMaster(tmp_asset_master_path)

        base_time = datetime(2024, 1, 1, 0, 0, 0)
        master.add_mapping("BTC", "binance", "BTC/USDT", base_time)
        master.add_mapping("BTC", "deribit", "BTC-PERPETUAL", base_time)

        # Get symbol on Binance
        symbol = master.get_symbol("BTC", "binance", asof=base_time)
        assert symbol == "BTC/USDT"

        # Get symbol on Deribit
        symbol = master.get_symbol("BTC", "deribit", asof=base_time)
        assert symbol == "BTC-PERPETUAL"

    def test_symbol_rename_point_in_time(self, tmp_asset_master_path):
        """Test point-in-time resolution across a symbol rename."""
        master = AssetMaster(tmp_asset_master_path)

        # Original symbol valid until Jan 15
        old_time = datetime(2024, 1, 1, 0, 0, 0)
        rename_time = datetime(2024, 1, 15, 0, 0, 0)
        new_time = datetime(2024, 1, 16, 0, 0, 0)

        master.add_mapping(
            asset_id="TOKEN",
            venue="binance",
            symbol="TOKEN/USDT",
            validity_start=old_time,
            validity_end=rename_time,
            is_primary=True,
        )

        master.add_mapping(
            asset_id="TOKEN",
            venue="binance",
            symbol="TOKEN2/USDT",
            validity_start=rename_time,
            is_primary=True,
        )

        # Resolve before rename
        symbol = master.resolve_symbol("TOKEN/USDT", "binance", asof=old_time)
        assert symbol == "TOKEN"

        # Resolve after rename
        symbol = master.resolve_symbol("TOKEN2/USDT", "binance", asof=new_time)
        assert symbol == "TOKEN"

        # Old symbol should not resolve after rename
        symbol = master.resolve_symbol("TOKEN/USDT", "binance", asof=new_time)
        assert symbol is None

    def test_list_assets_and_venues(self, tmp_asset_master_path):
        """Test listing all assets and venues."""
        master = AssetMaster(tmp_asset_master_path)

        base_time = datetime(2024, 1, 1, 0, 0, 0)
        master.add_mapping("BTC", "binance", "BTC/USDT", base_time)
        master.add_mapping("ETH", "binance", "ETH/USDT", base_time)
        master.add_mapping("BTC", "deribit", "BTC-PERPETUAL", base_time)

        assets = master.list_assets()
        venues = master.list_venues()

        assert set(assets) == {"BTC", "ETH"}
        assert set(venues) == {"binance", "deribit"}

    def test_asset_info(self, tmp_asset_master_path):
        """Test getting all venue symbols for an asset at a point in time."""
        master = AssetMaster(tmp_asset_master_path)

        base_time = datetime(2024, 1, 1, 0, 0, 0)
        master.add_mapping("BTC", "binance", "BTC/USDT", base_time, is_primary=True)
        master.add_mapping("BTC", "deribit", "BTC-PERPETUAL", base_time, is_primary=True)

        # Get all current symbols
        info = master.asset_info("BTC", asof=base_time)
        assert info == {"binance": "BTC/USDT", "deribit": "BTC-PERPETUAL"}

    def test_persistence(self, tmp_asset_master_path):
        """Test that asset master persists to disk and can be reloaded."""
        # Create and populate
        master1 = AssetMaster(tmp_asset_master_path)
        base_time = datetime(2024, 1, 1, 0, 0, 0)
        master1.add_mapping("BTC", "binance", "BTC/USDT", base_time)

        # Reload from disk
        master2 = AssetMaster(tmp_asset_master_path)
        symbol = master2.resolve_symbol("BTC/USDT", "binance", asof=base_time)
        assert symbol == "BTC"

    def test_delistings(self, tmp_asset_master_path):
        """Test handling of delisted symbols with validity ranges."""
        master = AssetMaster(tmp_asset_master_path)

        start = datetime(2024, 1, 1, 0, 0, 0)
        delisted = datetime(2024, 3, 1, 0, 0, 0)

        # Asset was listed, then delisted
        master.add_mapping(
            asset_id="DEADCOIN",
            venue="binance",
            symbol="DEADCOIN/USDT",
            validity_start=start,
            validity_end=delisted,
        )

        # Should resolve before delisting
        result = master.resolve_symbol("DEADCOIN/USDT", "binance", asof=datetime(2024, 2, 1))
        assert result == "DEADCOIN"

        # Should not resolve after delisting
        result = master.resolve_symbol("DEADCOIN/USDT", "binance", asof=datetime(2024, 4, 1))
        assert result is None
