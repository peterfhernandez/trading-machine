"""Tests for Funding Rate loader."""

from datetime import datetime
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from config import LOADER_CONFIG
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
    def test_opens_exchange_against_perp_markets(
        self, mock_binance_class, mock_asset_master, temp_store, mock_ccxt_binance
    ):
        """Funding rates only exist for derivatives; the ccxt default (spot)
        would raise for every symbol and produce an empty dataset."""
        mock_binance_class.return_value = mock_ccxt_binance

        FundingRateLoader("binance", store=temp_store, asset_master=mock_asset_master)

        mock_binance_class.assert_called_once_with(
            {"options": {"defaultType": LOADER_CONFIG.perp_market_type}}
        )

    @patch("loaders.funding_rate.ccxt.binance")
    def test_falls_back_when_venue_rejects_market_type(
        self, mock_binance_class, mock_asset_master, temp_store, mock_ccxt_binance
    ):
        """A venue that rejects the options dict still gets initialized."""
        mock_binance_class.side_effect = [TypeError("unsupported option"), mock_ccxt_binance]

        loader = FundingRateLoader("binance", store=temp_store, asset_master=mock_asset_master)

        assert loader.exchange is mock_ccxt_binance
        assert mock_binance_class.call_count == 2

    @patch("loaders.funding_rate.ccxt.binance")
    def test_filters_usdt_symbols_before_applying_the_cap(
        self, mock_binance_class, mock_asset_master, temp_store, mock_ccxt_binance
    ):
        """Regression: the loader used to slice exchange.symbols[:50] and filter
        afterwards, so an alphabetically-interleaved symbol list yielded ~15
        assets out of a 50-symbol budget."""
        symbols = []
        for i in range(60):
            base = f"A{i:03d}"
            symbols.extend([
                f"{base}/BNB:BNB", f"{base}/BTC:BTC", f"{base}/USDC:USDC", f"{base}/USDT:USDT"
            ])
        mock_ccxt_binance.symbols = sorted(symbols)
        mock_binance_class.return_value = mock_ccxt_binance

        loader = FundingRateLoader(
            "binance", store=temp_store, asset_master=mock_asset_master, max_symbols=50
        )
        loader.fetch()

        queried = [c.args[0] for c in mock_ccxt_binance.fetch_funding_rate.call_args_list]
        assert len(queried) == 50
        assert all(s.endswith(":USDT") for s in queried)

    @patch("loaders.funding_rate.ccxt.binance")
    def test_prefers_perp_symbols_over_spot(
        self, mock_binance_class, mock_asset_master, temp_store, mock_ccxt_binance
    ):
        """With perps available, spot duplicates are not queried for funding."""
        mock_ccxt_binance.symbols = ["BTC/USDT", "BTC/USDT:USDT", "ETH/USDT", "ETH/USDT:USDT"]
        mock_binance_class.return_value = mock_ccxt_binance

        loader = FundingRateLoader("binance", store=temp_store, asset_master=mock_asset_master)
        loader.fetch()

        queried = [c.args[0] for c in mock_ccxt_binance.fetch_funding_rate.call_args_list]
        assert queried == ["BTC/USDT:USDT", "ETH/USDT:USDT"]

    @patch("loaders.funding_rate.ccxt.binance")
    def test_symbol_resolution(self, mock_binance_class, mock_asset_master, temp_store, mock_ccxt_binance):
        """Test symbol resolution."""
        mock_binance_class.return_value = mock_ccxt_binance

        loader = FundingRateLoader("binance", lookback_days=7, store=temp_store, asset_master=mock_asset_master)
        df = loader.fetch()

        asset_ids = df["asset_id"].unique().to_list()
        assert "BTC" in asset_ids or "ETH" in asset_ids
        assert None not in asset_ids
