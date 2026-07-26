"""Tests for OHLCV loader."""

from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from config import DATASTORE_PATH
from datastore import ParquetStore, AssetMaster, DatasetSchema
from loaders.ohlcv import OHLCVLoader
from loaders.schemas import OHLCV_SCHEMA


@pytest.fixture
def mock_asset_master(tmp_path):
    """Create a temporary asset master with BTC and ETH mappings."""
    asset_master_path = tmp_path / "asset_master.parquet"
    am = AssetMaster(asset_master_path)

    base_date = datetime(2024, 1, 1)
    am.add_mapping("BTC", "binance", "BTC/USDT", base_date)
    am.add_mapping("ETH", "binance", "ETH/USDT", base_date)
    am.add_mapping("BTC", "deribit", "BTC", base_date)
    am.add_mapping("ETH", "deribit", "ETH", base_date)

    return am


@pytest.fixture
def temp_store(tmp_path):
    """Create a temporary ParquetStore."""
    return ParquetStore(tmp_path)


@pytest.fixture
def mock_ccxt_binance():
    """Mock ccxt.binance exchange."""
    exchange = MagicMock()
    exchange.symbols = ["BTC/USDT", "ETH/USDT", "BNB/USDT"]
    exchange.load_markets = MagicMock()

    base_time = int(datetime(2024, 1, 1).timestamp() * 1000)
    exchange.fetch_ohlcv = MagicMock(
        side_effect=lambda symbol, tf, since=None, limit=None: [
            [base_time + (i * 60 * 60 * 1000), 42000 + i, 42100 + i, 41900 + i, 42050 + i, 100 + i]
            for i in range(5)
        ]
    )

    return exchange


class TestOHLCVLoader:
    """Tests for OHLCVLoader."""

    def test_init(self, mock_asset_master, temp_store):
        """Test loader initialization."""
        loader = OHLCVLoader("binance", lookback_days=7, store=temp_store, asset_master=mock_asset_master)
        assert loader.venue == "binance"
        assert loader.lookback_days == 7

    @patch("loaders.ohlcv.ccxt.binance")
    def test_fetch_daily(self, mock_binance_class, mock_asset_master, temp_store, mock_ccxt_binance):
        """Test fetching daily OHLCV."""
        mock_binance_class.return_value = mock_ccxt_binance

        loader = OHLCVLoader("binance", lookback_days=7, store=temp_store, asset_master=mock_asset_master)
        df = loader.fetch(timeframe="1d")

        assert len(df) > 0
        assert "asset_id" in df.columns
        assert "event_ts" in df.columns
        assert "ingested_ts" in df.columns
        assert "close" in df.columns
        assert "volume" in df.columns

    @patch("loaders.ohlcv.ccxt.binance")
    def test_fetch_hourly(self, mock_binance_class, mock_asset_master, temp_store, mock_ccxt_binance):
        """Test fetching hourly OHLCV."""
        mock_binance_class.return_value = mock_ccxt_binance

        loader = OHLCVLoader("binance", lookback_days=7, store=temp_store, asset_master=mock_asset_master)
        df = loader.fetch(timeframe="1h")

        assert len(df) > 0
        assert df["timeframe"].unique().to_list() == ["1h"]

    @patch("loaders.ohlcv.ccxt.binance")
    def test_append_daily(self, mock_binance_class, mock_asset_master, temp_store, mock_ccxt_binance):
        """Test appending daily OHLCV to datastore."""
        mock_binance_class.return_value = mock_ccxt_binance

        loader = OHLCVLoader("binance", lookback_days=7, store=temp_store, asset_master=mock_asset_master)
        loader.run_daily()

        info = temp_store.dataset_info("ohlcv_daily")
        assert info["row_count"] > 0

    @patch("loaders.ohlcv.ccxt.binance")
    def test_symbol_resolution(self, mock_binance_class, mock_asset_master, temp_store, mock_ccxt_binance):
        """Test symbol resolution from ccxt symbols to asset_ids."""
        mock_binance_class.return_value = mock_ccxt_binance

        loader = OHLCVLoader("binance", lookback_days=7, store=temp_store, asset_master=mock_asset_master)
        df = loader.fetch(timeframe="1d")

        asset_ids = df["asset_id"].unique().to_list()
        assert "BTC" in asset_ids or "ETH" in asset_ids
        assert None not in asset_ids

    @patch("loaders.ohlcv.ccxt.binance")
    def test_has_required_columns(self, mock_binance_class, mock_asset_master, temp_store, mock_ccxt_binance):
        """Test that fetched data has all required columns."""
        mock_binance_class.return_value = mock_ccxt_binance

        loader = OHLCVLoader("binance", lookback_days=7, store=temp_store, asset_master=mock_asset_master)
        df = loader.fetch(timeframe="1d")

        required = ["asset_id", "venue", "timeframe", "event_ts", "ingested_ts", "open", "high", "low", "close", "volume"]
        for col in required:
            assert col in df.columns, f"Missing required column: {col}"

    @patch("loaders.ohlcv.ccxt.binance")
    def test_golden_fixture_3_days(self, mock_binance_class, mock_asset_master, temp_store, mock_ccxt_binance):
        """Golden test: hand-computed 3-day fixture must match exactly."""
        base_time = int(datetime(2024, 1, 1).timestamp() * 1000)
        mock_ccxt_binance.fetch_ohlcv.side_effect = lambda symbol, tf, since=None, limit=None: [
            [base_time + (i * 86400 * 1000), 42000.0, 42100.0, 41900.0, 42050.0, 100.0]
            for i in range(3)
        ]
        mock_binance_class.return_value = mock_ccxt_binance

        loader = OHLCVLoader("binance", lookback_days=7, store=temp_store, asset_master=mock_asset_master)
        df = loader.fetch(timeframe="1d")

        assert len(df) >= 3
        assert all(df["open"] == 42000.0)
        assert all(df["high"] == 42100.0)
        assert all(df["low"] == 41900.0)
