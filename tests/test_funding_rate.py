"""Tests for Funding Rate loader."""

from datetime import datetime
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from datastore import ParquetStore, AssetMaster
from loaders.funding_rate import FundingRateLoader
from loaders.schemas import FUNDING_RATE_SCHEMA


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
    """Mock ccxt.binance exchange with funding rates."""
    exchange = MagicMock()
    exchange.symbols = ["BTC/USDT", "ETH/USDT"]
    exchange.load_markets = MagicMock()

    base_time = int(datetime(2024, 1, 1).timestamp() * 1000)
    exchange.fetch_funding_rate = MagicMock(
        side_effect=lambda symbol: {
            "symbol": symbol,
            "timestamp": base_time,
            "fundingRate": 0.00001,
            "markPrice": 42000.0,
            "indexPrice": 41950.0,
        }
    )

    return exchange


class TestFundingRateLoader:
    """Tests for FundingRateLoader."""

    @patch("loaders.funding_rate.ccxt.binance")
    def test_init(self, mock_binance_class, mock_asset_master, temp_store, mock_ccxt_binance):
        """Test loader initialization."""
        mock_binance_class.return_value = mock_ccxt_binance

        loader = FundingRateLoader("binance", lookback_days=7, store=temp_store, asset_master=mock_asset_master)
        assert loader.venue == "binance"
        assert loader.lookback_days == 7

    @patch("loaders.funding_rate.ccxt.binance")
    def test_fetch(self, mock_binance_class, mock_asset_master, temp_store, mock_ccxt_binance):
        """Test fetching funding rates."""
        mock_binance_class.return_value = mock_ccxt_binance

        loader = FundingRateLoader("binance", lookback_days=7, store=temp_store, asset_master=mock_asset_master)
        df = loader.fetch()

        assert len(df) > 0
        assert "asset_id" in df.columns
        assert "funding_rate" in df.columns
        assert "mark_price" in df.columns
        assert "index_price" in df.columns
        assert "event_ts" in df.columns
        assert "ingested_ts" in df.columns

    @patch("loaders.funding_rate.ccxt.binance")
    def test_append(self, mock_binance_class, mock_asset_master, temp_store, mock_ccxt_binance):
        """Test appending funding rates to datastore."""
        mock_binance_class.return_value = mock_ccxt_binance

        loader = FundingRateLoader("binance", lookback_days=7, store=temp_store, asset_master=mock_asset_master)
        loader.run()

        info = temp_store.dataset_info("funding_rate")
        assert info["row_count"] > 0

    @patch("loaders.funding_rate.ccxt.binance")
    def test_has_required_columns(self, mock_binance_class, mock_asset_master, temp_store, mock_ccxt_binance):
        """Test that fetched data has all required columns."""
        mock_binance_class.return_value = mock_ccxt_binance

        loader = FundingRateLoader("binance", lookback_days=7, store=temp_store, asset_master=mock_asset_master)
        df = loader.fetch()

        required = ["asset_id", "venue", "event_ts", "ingested_ts", "funding_rate", "mark_price", "index_price"]
        for col in required:
            assert col in df.columns, f"Missing required column: {col}"

    @patch("loaders.funding_rate.ccxt.binance")
    def test_symbol_resolution(self, mock_binance_class, mock_asset_master, temp_store, mock_ccxt_binance):
        """Test symbol resolution."""
        mock_binance_class.return_value = mock_ccxt_binance

        loader = FundingRateLoader("binance", lookback_days=7, store=temp_store, asset_master=mock_asset_master)
        df = loader.fetch()

        asset_ids = df["asset_id"].unique().to_list()
        assert "BTC" in asset_ids or "ETH" in asset_ids
        assert None not in asset_ids
