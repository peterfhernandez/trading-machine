"""Tests for Open Interest loader."""

from datetime import datetime
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from datastore import ParquetStore, AssetMaster
from loaders.open_interest import OpenInterestLoader
from loaders.schemas import OPEN_INTEREST_SCHEMA


@pytest.fixture
def mock_asset_master(tmp_path):
    """Create a temporary asset master with BTC and ETH mappings."""
    asset_master_path = tmp_path / "asset_master.parquet"
    am = AssetMaster(asset_master_path)

    base_date = datetime(2024, 1, 1)
    am.add_mapping("BTC", "binance", "BTC/USDT", base_date)
    am.add_mapping("ETH", "binance", "ETH/USDT", base_date)

    return am


@pytest.fixture
def temp_store(tmp_path):
    """Create a temporary ParquetStore."""
    return ParquetStore(tmp_path)


@pytest.fixture
def mock_ccxt_binance():
    """Mock ccxt.binance exchange with open interest."""
    exchange = MagicMock()
    exchange.symbols = ["BTC/USDT", "ETH/USDT"]
    exchange.load_markets = MagicMock()

    base_time = int(datetime(2024, 1, 1).timestamp() * 1000)
    exchange.fetch_open_interest = MagicMock(
        side_effect=lambda symbol: {
            "symbol": symbol,
            "timestamp": base_time,
            "openInterest": 1000000.0,
            "openInterestUsd": 42000000000.0,
        }
    )

    return exchange


class TestOpenInterestLoader:
    """Tests for OpenInterestLoader."""

    @patch("loaders.open_interest.ccxt.binance")
    def test_init(self, mock_binance_class, mock_asset_master, temp_store, mock_ccxt_binance):
        """Test loader initialization."""
        mock_binance_class.return_value = mock_ccxt_binance

        loader = OpenInterestLoader("binance", lookback_days=7, store=temp_store, asset_master=mock_asset_master)
        assert loader.venue == "binance"
        assert loader.lookback_days == 7

    @patch("loaders.open_interest.ccxt.binance")
    def test_fetch(self, mock_binance_class, mock_asset_master, temp_store, mock_ccxt_binance):
        """Test fetching open interest."""
        mock_binance_class.return_value = mock_ccxt_binance

        loader = OpenInterestLoader("binance", lookback_days=7, store=temp_store, asset_master=mock_asset_master)
        df = loader.fetch()

        assert len(df) > 0
        assert "asset_id" in df.columns
        assert "open_interest" in df.columns
        assert "open_interest_usd" in df.columns
        assert "event_ts" in df.columns
        assert "ingested_ts" in df.columns

    @patch("loaders.open_interest.ccxt.binance")
    def test_append(self, mock_binance_class, mock_asset_master, temp_store, mock_ccxt_binance):
        """Test appending open interest to datastore."""
        mock_binance_class.return_value = mock_ccxt_binance

        loader = OpenInterestLoader("binance", lookback_days=7, store=temp_store, asset_master=mock_asset_master)
        loader.run()

        info = temp_store.dataset_info("open_interest")
        assert info["row_count"] > 0

    @patch("loaders.open_interest.ccxt.binance")
    def test_has_required_columns(self, mock_binance_class, mock_asset_master, temp_store, mock_ccxt_binance):
        """Test that fetched data has all required columns."""
        mock_binance_class.return_value = mock_ccxt_binance

        loader = OpenInterestLoader("binance", lookback_days=7, store=temp_store, asset_master=mock_asset_master)
        df = loader.fetch()

        required = ["asset_id", "venue", "event_ts", "ingested_ts", "open_interest", "open_interest_usd"]
        for col in required:
            assert col in df.columns, f"Missing required column: {col}"

    @patch("loaders.open_interest.ccxt.binance")
    def test_both_oi_fields_populated(self, mock_binance_class, mock_asset_master, temp_store, mock_ccxt_binance):
        """Test that both open_interest and open_interest_usd are populated."""
        mock_binance_class.return_value = mock_ccxt_binance

        loader = OpenInterestLoader("binance", lookback_days=7, store=temp_store, asset_master=mock_asset_master)
        df = loader.fetch()

        assert df["open_interest"].null_count() == 0
        assert df["open_interest_usd"].null_count() == 0
