"""Tests for Nightly pipeline."""

from datetime import datetime
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from datastore import ParquetStore, DatasetSchema, AssetMaster
from pipeline.nightly import NightlyPipeline


@pytest.fixture
def temp_store(tmp_path):
    """Create a temporary ParquetStore."""
    return ParquetStore(tmp_path)


@pytest.fixture
def mock_asset_master(tmp_path):
    """Create a temporary asset master."""
    asset_master_path = tmp_path / "asset_master.parquet"
    am = AssetMaster(asset_master_path)
    base_date = datetime(2024, 1, 1)
    am.add_mapping("BTC", "binance", "BTC/USDT", base_date)
    am.add_mapping("ETH", "binance", "ETH/USDT", base_date)
    return am


class TestNightlyPipeline:
    """Tests for NightlyPipeline."""

    def test_init(self, temp_store):
        """Test pipeline initialization."""
        pipeline = NightlyPipeline("binance", dry_run=False)
        assert pipeline.venue == "binance"
        assert not pipeline.dry_run
        assert not pipeline.trading_halted

    def test_dry_run_mode(self, temp_store):
        """Test dry-run mode doesn't write data."""
        pipeline = NightlyPipeline("binance", dry_run=True)
        result = pipeline.run(days=1)
        assert result

    @patch("pipeline.nightly.BackfillRunner")
    @patch("pipeline.nightly.DataAudit")
    def test_run_pipeline_success(self, mock_audit_class, mock_backfill_class, temp_store):
        """Test successful pipeline run."""
        mock_backfill = MagicMock()
        mock_backfill_class.return_value = mock_backfill

        mock_audit = MagicMock()
        mock_audit.audit_dataset.return_value = []
        mock_audit.should_halt_trading.return_value = False
        mock_audit_class.return_value = mock_audit

        pipeline = NightlyPipeline("binance", dry_run=False)
        result = pipeline.run(days=1)

        assert result
        assert not pipeline.trading_halted

    @patch("pipeline.nightly.BackfillRunner")
    @patch("pipeline.nightly.DataAudit")
    def test_trading_halt_on_critical_failure(self, mock_audit_class, mock_backfill_class):
        """Test trading is halted on critical audit failure."""
        mock_backfill = MagicMock()
        mock_backfill_class.return_value = mock_backfill

        mock_audit = MagicMock()
        mock_audit.audit_dataset.return_value = []
        mock_audit.should_halt_trading.return_value = True
        mock_audit_class.return_value = mock_audit

        pipeline = NightlyPipeline("binance", dry_run=False)
        result = pipeline.run(days=1)

        assert not result
        assert pipeline.trading_halted

    def test_load_stage_dry_run(self, temp_store):
        """Test load stage in dry-run mode."""
        pipeline = NightlyPipeline("binance", dry_run=True)
        pipeline._load_stage(days=1)
        assert not pipeline.trading_halted

    def test_audit_stage_dry_run(self, temp_store):
        """Test audit stage in dry-run mode."""
        pipeline = NightlyPipeline("binance", dry_run=True)
        pipeline._audit_stage()
        assert not pipeline.trading_halted

    def test_report_stage(self, temp_store):
        """Test report stage."""
        schema = DatasetSchema(
            name="ohlcv_daily",
            fields={
                "asset_id": pl.Utf8,
                "event_ts": pl.Datetime("us"),
                "ingested_ts": pl.Datetime("us"),
                "close": pl.Float64,
            },
        )

        base_date = datetime(2024, 1, 1)
        df = pl.DataFrame({
            "asset_id": ["BTC"] * 10,
            "event_ts": [base_date] * 10,
            "ingested_ts": [base_date] * 10,
            "close": [42000.0] * 10,
        })

        temp_store.append("ohlcv_daily", df, schema)

        pipeline = NightlyPipeline("binance", dry_run=True)
        pipeline._report_stage()

    def test_pipeline_with_dry_run_disabled_trading_halt(self):
        """Test that trading halt is not allowed on dry-run."""
        pipeline = NightlyPipeline("binance", dry_run=True)
        pipeline._audit_stage()
        assert not pipeline.trading_halted
