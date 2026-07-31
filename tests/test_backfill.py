"""Tests for Backfill runner."""

import json
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from datastore import ParquetStore, AssetMaster
from loaders.backfill import BackfillRunner


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
def checkpoint_dir(tmp_path):
    """Create a temporary checkpoint directory."""
    cp_dir = tmp_path / "checkpoints"
    cp_dir.mkdir()
    return cp_dir


class TestBackfillRunner:
    """Tests for BackfillRunner."""

    def test_init(self, temp_store, mock_asset_master, checkpoint_dir):
        """Test runner initialization."""
        runner = BackfillRunner("binance", checkpoint_dir, temp_store, mock_asset_master)
        assert runner.venue == "binance"
        assert runner.checkpoint_dir.exists()

    def test_checkpoint_save_load(self, temp_store, mock_asset_master, checkpoint_dir):
        """Test checkpoint persistence."""
        runner = BackfillRunner("binance", checkpoint_dir, temp_store, mock_asset_master)

        checkpoint = {
            "ohlcv_daily": "2024-01-31T12:00:00",
            "funding_rate": "2024-01-31T12:00:00",
        }
        runner.save_checkpoint(checkpoint)

        loaded = runner.load_checkpoint()
        assert loaded == checkpoint
        assert (checkpoint_dir / "binance_backfill.json").exists()

    def test_checkpoint_file_path(self, temp_store, mock_asset_master, checkpoint_dir):
        """Test checkpoint file naming."""
        runner = BackfillRunner("binance", checkpoint_dir, temp_store, mock_asset_master)
        assert runner._checkpoint_file() == checkpoint_dir / "binance_backfill.json"

    def test_load_checkpoint_nonexistent(self, temp_store, mock_asset_master, checkpoint_dir):
        """Test loading nonexistent checkpoint returns empty dict."""
        runner = BackfillRunner("binance", checkpoint_dir, temp_store, mock_asset_master)
        checkpoint = runner.load_checkpoint()
        assert checkpoint == {}

    @patch("loaders.backfill.OHLCVLoader")
    @patch("loaders.backfill.FundingRateLoader")
    @patch("loaders.backfill.OpenInterestLoader")
    def test_run_backfill(
        self,
        mock_oi_class,
        mock_fr_class,
        mock_ohlcv_class,
        temp_store,
        mock_asset_master,
        checkpoint_dir,
    ):
        """Test running backfill with mocked loaders."""
        mock_ohlcv = MagicMock()
        mock_ohlcv.run_daily.return_value = 10
        mock_ohlcv.run_hourly.return_value = 24
        mock_ohlcv_class.return_value = mock_ohlcv

        mock_fr = MagicMock()
        mock_fr.run.return_value = 5
        mock_fr_class.return_value = mock_fr

        mock_oi = MagicMock()
        mock_oi.run.return_value = 5
        mock_oi_class.return_value = mock_oi

        runner = BackfillRunner("binance", checkpoint_dir, temp_store, mock_asset_master)
        runner.run(days_back=7)

        checkpoint_file = checkpoint_dir / "binance_backfill.json"
        assert checkpoint_file.exists()

        with open(checkpoint_file) as f:
            saved_checkpoint = json.load(f)
        assert "ohlcv_daily" in saved_checkpoint
        assert "ohlcv_hourly" in saved_checkpoint
        assert "funding_rate" in saved_checkpoint
        assert "open_interest" in saved_checkpoint

    @patch("loaders.backfill.OHLCVLoader")
    @patch("loaders.backfill.FundingRateLoader")
    @patch("loaders.backfill.OpenInterestLoader")
    def test_each_dataset_is_fetched_exactly_once(
        self,
        mock_oi_class,
        mock_fr_class,
        mock_ohlcv_class,
        temp_store,
        mock_asset_master,
        checkpoint_dir,
    ):
        """Regression: the runner used to call fetch() to test for emptiness and
        then run(), which fetched again -- pulling every symbol from the venue
        twice per dataset, per run."""
        mock_ohlcv = MagicMock()
        mock_ohlcv.run_daily.return_value = 10
        mock_ohlcv.run_hourly.return_value = 24
        mock_ohlcv_class.return_value = mock_ohlcv

        mock_fr = MagicMock()
        mock_fr.run.return_value = 5
        mock_fr_class.return_value = mock_fr

        mock_oi = MagicMock()
        mock_oi.run.return_value = 5
        mock_oi_class.return_value = mock_oi

        runner = BackfillRunner("binance", checkpoint_dir, temp_store, mock_asset_master)
        runner.run(days_back=7)

        assert mock_ohlcv.run_daily.call_count == 1
        assert mock_ohlcv.run_hourly.call_count == 1
        assert mock_fr.run.call_count == 1
        assert mock_oi.run.call_count == 1
        # No separate probe fetch on any loader
        assert mock_ohlcv.fetch.call_count == 0
        assert mock_fr.fetch.call_count == 0
        assert mock_oi.fetch.call_count == 0

    @patch("loaders.backfill.OHLCVLoader")
    @patch("loaders.backfill.FundingRateLoader")
    @patch("loaders.backfill.OpenInterestLoader")
    def test_empty_run_does_not_checkpoint(
        self,
        mock_oi_class,
        mock_fr_class,
        mock_ohlcv_class,
        temp_store,
        mock_asset_master,
        checkpoint_dir,
    ):
        """A loader that appended nothing must not advance its checkpoint."""
        mock_ohlcv = MagicMock()
        mock_ohlcv.run_daily.return_value = 0
        mock_ohlcv.run_hourly.return_value = 0
        mock_ohlcv_class.return_value = mock_ohlcv

        mock_fr = MagicMock()
        mock_fr.run.return_value = 0
        mock_fr_class.return_value = mock_fr

        mock_oi = MagicMock()
        mock_oi.run.return_value = 3
        mock_oi_class.return_value = mock_oi

        runner = BackfillRunner("binance", checkpoint_dir, temp_store, mock_asset_master)
        runner.run(days_back=7)

        with open(checkpoint_dir / "binance_backfill.json") as f:
            saved_checkpoint = json.load(f)

        assert "ohlcv_daily" not in saved_checkpoint
        assert "ohlcv_hourly" not in saved_checkpoint
        assert "funding_rate" not in saved_checkpoint
        assert "open_interest" in saved_checkpoint

    def test_backfill_runner_multiple_venues(self, temp_store, mock_asset_master, checkpoint_dir):
        """Test that different venues have separate checkpoints."""
        runner_binance = BackfillRunner("binance", checkpoint_dir, temp_store, mock_asset_master)
        runner_deribit = BackfillRunner("deribit", checkpoint_dir, temp_store, mock_asset_master)

        assert runner_binance._checkpoint_file() != runner_deribit._checkpoint_file()
        assert runner_binance._checkpoint_file().name == "binance_backfill.json"
        assert runner_deribit._checkpoint_file().name == "deribit_backfill.json"
