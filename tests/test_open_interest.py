"""Tests for Open Interest loader."""

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from config import LOADER_CONFIG
from datastore import ParquetStore, AssetMaster
from loaders.open_interest import OpenInterestLoader
from loaders.window import FetchWindow, utc_now
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

    # A snapshot endpoint reports the current value, so the mock stamps it
    # with now -- a timestamp fixed in the past falls outside any window.
    base_time = int(utc_now().timestamp() * 1000)
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
    def test_opens_exchange_against_perp_markets(
        self, mock_binance_class, mock_asset_master, temp_store, mock_ccxt_binance
    ):
        """Open interest only exists for derivatives, so the loader must not run
        against ccxt's default spot markets."""
        mock_binance_class.return_value = mock_ccxt_binance

        OpenInterestLoader("binance", store=temp_store, asset_master=mock_asset_master)

        mock_binance_class.assert_called_once_with(
            {"options": {"defaultType": LOADER_CONFIG.perp_market_type}}
        )

    @patch("loaders.open_interest.ccxt.binance")
    def test_falls_back_when_venue_rejects_market_type(
        self, mock_binance_class, mock_asset_master, temp_store, mock_ccxt_binance
    ):
        """A venue that rejects the options dict still gets initialized."""
        mock_binance_class.side_effect = [TypeError("unsupported option"), mock_ccxt_binance]

        loader = OpenInterestLoader("binance", store=temp_store, asset_master=mock_asset_master)

        assert loader.exchange is mock_ccxt_binance
        assert mock_binance_class.call_count == 2

    @patch("loaders.open_interest.ccxt.binance")
    def test_filters_usdt_symbols_before_applying_the_cap(
        self, mock_binance_class, mock_asset_master, temp_store, mock_ccxt_binance
    ):
        """Regression: the loader used to slice exchange.symbols[:50] and filter
        afterwards, covering ~15 assets out of a 50-symbol budget."""
        symbols = []
        for i in range(60):
            base = f"A{i:03d}"
            symbols.extend([
                f"{base}/BNB:BNB", f"{base}/BTC:BTC", f"{base}/USDC:USDC", f"{base}/USDT:USDT"
            ])
        mock_ccxt_binance.symbols = sorted(symbols)
        mock_binance_class.return_value = mock_ccxt_binance

        loader = OpenInterestLoader(
            "binance", store=temp_store, asset_master=mock_asset_master, max_symbols=50
        )
        loader.fetch()

        queried = [c.args[0] for c in mock_ccxt_binance.fetch_open_interest.call_args_list]
        assert len(queried) == 50
        assert all(s.endswith(":USDT") for s in queried)

    @patch("loaders.open_interest.ccxt.binance")
    def test_prefers_perp_symbols_over_spot(
        self, mock_binance_class, mock_asset_master, temp_store, mock_ccxt_binance
    ):
        """With perps available, spot duplicates are not queried for OI."""
        mock_ccxt_binance.symbols = ["BTC/USDT", "BTC/USDT:USDT", "ETH/USDT", "ETH/USDT:USDT"]
        mock_binance_class.return_value = mock_ccxt_binance

        loader = OpenInterestLoader("binance", store=temp_store, asset_master=mock_asset_master)
        loader.fetch()

        queried = [c.args[0] for c in mock_ccxt_binance.fetch_open_interest.call_args_list]
        assert queried == ["BTC/USDT:USDT", "ETH/USDT:USDT"]

    @patch("loaders.open_interest.ccxt.binance")
    def test_both_oi_fields_populated(self, mock_binance_class, mock_asset_master, temp_store, mock_ccxt_binance):
        """Test that both open_interest and open_interest_usd are populated."""
        mock_binance_class.return_value = mock_ccxt_binance

        loader = OpenInterestLoader("binance", lookback_days=7, store=temp_store, asset_master=mock_asset_master)
        df = loader.fetch()

        assert df["open_interest"].null_count() == 0
        assert df["open_interest_usd"].null_count() == 0

    @patch("loaders.open_interest.ccxt.binance")
    def test_run_returns_rows_appended(
        self, mock_binance_class, mock_asset_master, temp_store, mock_ccxt_binance
    ):
        """The backfill runner uses this count instead of a second fetch()."""
        mock_binance_class.return_value = mock_ccxt_binance

        loader = OpenInterestLoader("binance", store=temp_store, asset_master=mock_asset_master)
        rows = loader.run()

        assert rows == 2
        assert temp_store.dataset_info("open_interest")["row_count"] == rows

    @patch("loaders.open_interest.ccxt.binance")
    def test_run_returns_zero_when_nothing_fetched(
        self, mock_binance_class, mock_asset_master, temp_store, mock_ccxt_binance
    ):
        mock_ccxt_binance.symbols = []
        mock_binance_class.return_value = mock_ccxt_binance

        loader = OpenInterestLoader("binance", store=temp_store, asset_master=mock_asset_master)

        assert loader.run() == 0


@pytest.fixture
def mock_binance_with_history():
    """A venue that advertises and serves open interest history."""
    exchange = MagicMock()
    exchange.symbols = ["BTC/USDT:USDT"]
    exchange.load_markets = MagicMock()
    exchange.has = {"fetchOpenInterestHistory": True}

    stamps = [
        int(datetime(2024, 1, day, tzinfo=timezone.utc).timestamp() * 1000)
        for day in (1, 2, 3)
    ]

    def history(symbol, timeframe="1d", since=None, limit=None):
        return [
            {
                "symbol": symbol,
                "timestamp": ts,
                # ccxt's history structure, not the snapshot one
                "openInterestAmount": 100.0 + i,
                "openInterestValue": 5000.0 + i,
            }
            for i, ts in enumerate(stamps)
            if since is None or ts >= since
        ]

    exchange.fetch_open_interest_history = MagicMock(side_effect=history)
    exchange.fetch_open_interest = MagicMock(return_value={
        "timestamp": int(utc_now().timestamp() * 1000),
        "openInterest": 1.0,
        "openInterestUsd": 2.0,
    })
    return exchange


class TestOpenInterestHistory:
    """A window means history, not a snapshot stamped with today's date."""

    @patch("loaders.open_interest.ccxt.binance")
    def test_uses_the_history_endpoint_when_available(
        self, mock_binance_class, mock_asset_master, temp_store, mock_binance_with_history
    ):
        mock_binance_class.return_value = mock_binance_with_history
        mock_asset_master.add_mapping("BTC", "binance", "BTC/USDT:USDT", datetime(2024, 1, 1))

        loader = OpenInterestLoader("binance", store=temp_store, asset_master=mock_asset_master)
        df = loader.fetch(window=FetchWindow(datetime(2024, 1, 1), datetime(2024, 1, 3)))

        assert mock_binance_with_history.fetch_open_interest_history.called
        assert not mock_binance_with_history.fetch_open_interest.called
        assert len(df) == 3
        assert df["open_interest"].to_list() == [100.0, 101.0, 102.0]
        assert df["open_interest_usd"].to_list() == [5000.0, 5001.0, 5002.0]

    @patch("loaders.open_interest.ccxt.binance")
    def test_history_timeframe_is_passed_to_the_venue(
        self, mock_binance_class, mock_asset_master, temp_store, mock_binance_with_history
    ):
        mock_binance_class.return_value = mock_binance_with_history
        mock_asset_master.add_mapping("BTC", "binance", "BTC/USDT:USDT", datetime(2024, 1, 1))

        loader = OpenInterestLoader(
            "binance", store=temp_store, asset_master=mock_asset_master, history_timeframe="4h"
        )
        loader.fetch(window=FetchWindow(datetime(2024, 1, 1), datetime(2024, 1, 3)))

        assert mock_binance_with_history.fetch_open_interest_history.call_args.args[1] == "4h"

    @patch("loaders.open_interest.ccxt.binance")
    def test_window_bounds_the_history(
        self, mock_binance_class, mock_asset_master, temp_store, mock_binance_with_history
    ):
        mock_binance_class.return_value = mock_binance_with_history
        mock_asset_master.add_mapping("BTC", "binance", "BTC/USDT:USDT", datetime(2024, 1, 1))

        loader = OpenInterestLoader("binance", store=temp_store, asset_master=mock_asset_master)
        df = loader.fetch(window=FetchWindow(datetime(2024, 1, 2), datetime(2024, 1, 2, 12)))

        assert len(df) == 1
        assert df["event_ts"][0] == datetime(2024, 1, 2)

    @patch("loaders.open_interest.ccxt.binance")
    def test_snapshot_fallback_ignores_a_historical_window(
        self, mock_binance_class, mock_asset_master, temp_store, mock_ccxt_binance
    ):
        """A snapshot can only answer for now."""
        mock_binance_class.return_value = mock_ccxt_binance

        loader = OpenInterestLoader("binance", store=temp_store, asset_master=mock_asset_master)
        df = loader.fetch(window=FetchWindow(datetime(2024, 1, 1), datetime(2024, 1, 2)))

        assert len(df) == 0
