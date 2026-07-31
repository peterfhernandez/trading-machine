"""Tests for Backfill runner."""

import json
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from datastore import ParquetStore, AssetMaster
from loaders.backfill import BackfillRunner
from loaders.window import FetchWindow


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


@pytest.fixture(autouse=True)
def no_rate_limit_pause():
    """The runner pauses between datasets to be kind to the venue; tests are
    talking to mocks and must not pay for it (100ms budget per test)."""
    with patch("loaders.backfill.time.sleep"):
        yield


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


class TestResumableWindows:
    """The checkpoint is read back, so a re-run fetches only what is missing."""

    @pytest.fixture
    def loaders(self):
        """Patch all three loader classes; yields the runner-visible mocks."""
        with patch("loaders.backfill.OHLCVLoader") as ohlcv_class, \
             patch("loaders.backfill.FundingRateLoader") as fr_class, \
             patch("loaders.backfill.OpenInterestLoader") as oi_class:
            ohlcv = MagicMock()
            ohlcv.run_daily.return_value = 10
            ohlcv.run_hourly.return_value = 24
            ohlcv_class.return_value = ohlcv

            fr = MagicMock()
            fr.run.return_value = 5
            fr_class.return_value = fr

            oi = MagicMock()
            oi.run.return_value = 5
            oi_class.return_value = oi

            yield ohlcv, fr, oi

    def test_loaders_receive_the_requested_window(
        self, loaders, temp_store, mock_asset_master, checkpoint_dir
    ):
        """A day count is no longer what reaches the loader -- explicit dates are."""
        ohlcv, fr, oi = loaders
        start, end = datetime(2024, 3, 1), datetime(2024, 3, 8)

        runner = BackfillRunner("binance", checkpoint_dir, temp_store, mock_asset_master)
        runner.run(start_date=start, end_date=end)

        window = ohlcv.run_daily.call_args.kwargs["window"]
        assert (window.start, window.end) == (start, end)
        assert fr.run.call_args.kwargs["window"].start == start
        assert oi.run.call_args.kwargs["window"].end == end

    def test_checkpoint_records_the_covered_interval(
        self, loaders, temp_store, mock_asset_master, checkpoint_dir
    ):
        runner = BackfillRunner("binance", checkpoint_dir, temp_store, mock_asset_master)
        runner.run(start_date=datetime(2024, 3, 1), end_date=datetime(2024, 3, 8))

        with open(checkpoint_dir / "binance_backfill.json") as f:
            saved = json.load(f)

        assert saved["ohlcv_daily"]["covered_start"] == "2024-03-01T00:00:00"
        assert saved["ohlcv_daily"]["covered_end"] == "2024-03-08T00:00:00"

    def test_rerunning_the_same_window_fetches_only_the_overlap(
        self, loaders, temp_store, mock_asset_master, checkpoint_dir
    ):
        """The overlap is deliberate: the trailing bar of the first run was
        probably incomplete, and venues revise recent bars."""
        ohlcv, _, _ = loaders
        start, end = datetime(2024, 3, 1), datetime(2024, 3, 8)

        runner = BackfillRunner("binance", checkpoint_dir, temp_store, mock_asset_master)
        runner.run(start_date=start, end_date=end)
        first = ohlcv.run_daily.call_args.kwargs["window"]

        runner.run(start_date=start, end_date=end)
        second = ohlcv.run_daily.call_args.kwargs["window"]

        assert first.days == 7          # the whole window
        assert second.days == 1         # only the overlap
        assert second == FetchWindow(datetime(2024, 3, 7), end)

    def test_extending_the_window_fetches_only_the_new_part(
        self, loaders, temp_store, mock_asset_master, checkpoint_dir
    ):
        ohlcv, _, _ = loaders

        runner = BackfillRunner("binance", checkpoint_dir, temp_store, mock_asset_master)
        runner.run(start_date=datetime(2024, 3, 1), end_date=datetime(2024, 3, 8))
        runner.run(start_date=datetime(2024, 3, 1), end_date=datetime(2024, 3, 15))

        second = ohlcv.run_daily.call_args.kwargs["window"]
        assert second.start == datetime(2024, 3, 7)  # 8th minus the 1-day overlap
        assert second.end == datetime(2024, 3, 15)

    def test_older_history_is_still_fetched(
        self, loaders, temp_store, mock_asset_master, checkpoint_dir
    ):
        """A high-water mark cannot prove older history is present."""
        ohlcv, _, _ = loaders

        runner = BackfillRunner("binance", checkpoint_dir, temp_store, mock_asset_master)
        runner.run(start_date=datetime(2024, 3, 1), end_date=datetime(2024, 3, 8))
        runner.run(start_date=datetime(2023, 1, 1), end_date=datetime(2023, 2, 1))

        second = ohlcv.run_daily.call_args.kwargs["window"]
        assert second.start == datetime(2023, 1, 1)
        assert second.end == datetime(2023, 2, 1)

    def test_ignore_checkpoint_refetches_everything(
        self, loaders, temp_store, mock_asset_master, checkpoint_dir
    ):
        ohlcv, _, _ = loaders
        start, end = datetime(2024, 3, 1), datetime(2024, 3, 8)

        BackfillRunner("binance", checkpoint_dir, temp_store, mock_asset_master).run(
            start_date=start, end_date=end
        )
        BackfillRunner(
            "binance", checkpoint_dir, temp_store, mock_asset_master, ignore_checkpoint=True
        ).run(start_date=start, end_date=end)

        assert ohlcv.run_daily.call_count == 2
        assert ohlcv.run_daily.call_args.kwargs["window"].start == start

    def test_a_failed_dataset_does_not_advance_its_checkpoint(
        self, loaders, temp_store, mock_asset_master, checkpoint_dir
    ):
        """Nothing appended means the window is still missing; the next run
        must ask for it again."""
        ohlcv, fr, _ = loaders
        fr.run.return_value = 0
        start, end = datetime(2024, 3, 1), datetime(2024, 3, 8)

        runner = BackfillRunner("binance", checkpoint_dir, temp_store, mock_asset_master)
        runner.run(start_date=start, end_date=end)
        runner.run(start_date=start, end_date=end)

        assert fr.run.call_count == 2
        assert fr.run.call_args.kwargs["window"].start == start

    def test_legacy_checkpoint_is_honoured(
        self, loaders, temp_store, mock_asset_master, checkpoint_dir
    ):
        """Checkpoints written before this change stored a bare end date."""
        ohlcv, _, _ = loaders
        with open(checkpoint_dir / "binance_backfill.json", "w") as f:
            json.dump({"ohlcv_daily": "2024-03-08T00:00:00"}, f)

        runner = BackfillRunner("binance", checkpoint_dir, temp_store, mock_asset_master)
        runner.run(start_date=datetime(2024, 3, 1), end_date=datetime(2024, 3, 15))

        window = ohlcv.run_daily.call_args.kwargs["window"]
        assert window.start == datetime(2024, 3, 7)
