"""Tests for Audit module."""

from datetime import datetime, timedelta

import polars as pl
import pytest

from config import AUDIT_CONFIG
from datastore import ParquetStore, DatasetSchema
from audit.auditor import DataAudit, AuditResult


@pytest.fixture
def temp_store(tmp_path):
    """Create a temporary ParquetStore."""
    return ParquetStore(tmp_path)


@pytest.fixture
def sample_ohlcv_schema():
    """Sample OHLCV schema."""
    return DatasetSchema(
        name="test_ohlcv",
        fields={
            "asset_id": pl.Utf8,
            "venue": pl.Utf8,
            "event_ts": pl.Datetime("us"),
            "ingested_ts": pl.Datetime("us"),
            "close": pl.Float64,
        },
    )


class TestDataAudit:
    """Tests for DataAudit."""

    def test_init(self, temp_store):
        """Test audit initialization."""
        audit = DataAudit(temp_store)
        assert audit.store == temp_store
        assert audit.results == []

    def test_audit_missing_data(self, temp_store, sample_ohlcv_schema):
        """Test audit when dataset has no data for date."""
        audit = DataAudit(temp_store)
        results = audit.audit_dataset("missing_dataset")

        assert len(results) > 0
        assert not results[0].passed
        assert "No data found" in results[0].message

    def test_audit_with_data(self, temp_store, sample_ohlcv_schema):
        """Test audit on actual dataset."""
        base_date = datetime(2024, 1, 1)
        now = base_date.replace(microsecond=0)

        df = pl.DataFrame({
            "asset_id": ["BTC", "ETH"] * 10,
            "venue": ["binance"] * 20,
            "event_ts": [now] * 20,
            "ingested_ts": [now] * 20,
            "close": [42000.0, 2500.0] * 10,
        })

        temp_store.append("test_ohlcv", df, sample_ohlcv_schema)

        audit = DataAudit(temp_store)
        results = audit.audit_dataset("test_ohlcv", base_date)

        assert len(results) > 0
        assert "coverage" in [r.check_name for r in results]

    def test_coverage_check(self, temp_store, sample_ohlcv_schema):
        """Test coverage check."""
        base_date = datetime(2024, 1, 1)
        now = base_date.replace(microsecond=0)

        assets = ["BTC", "ETH", "BNB"]
        df = pl.DataFrame({
            "asset_id": assets * 5,
            "venue": ["binance"] * 15,
            "event_ts": [now] * 15,
            "ingested_ts": [now] * 15,
            "close": [42000.0] * 15,
        })

        temp_store.append("test_ohlcv", df, sample_ohlcv_schema)

        audit = DataAudit(temp_store)
        results = audit.audit_dataset("test_ohlcv", base_date)

        coverage_results = [r for r in results if r.check_name == "coverage"]
        assert len(coverage_results) > 0

    def test_null_rate_check(self, temp_store, sample_ohlcv_schema):
        """Test null rate detection."""
        base_date = datetime(2024, 1, 1)
        now = base_date.replace(microsecond=0)

        df = pl.DataFrame({
            "asset_id": ["BTC"] * 100,
            "venue": ["binance"] * 100,
            "event_ts": [now] * 100,
            "ingested_ts": [now] * 100,
            "close": [42000.0] * 95 + [None] * 5,  # 5% nulls
        })

        temp_store.append("test_ohlcv", df, sample_ohlcv_schema)

        audit = DataAudit(temp_store)
        results = audit.audit_dataset("test_ohlcv", base_date)

        null_results = [r for r in results if "null_rate" in r.check_name]
        assert len(null_results) > 0

    def test_freshness_check(self, temp_store, sample_ohlcv_schema):
        """Test freshness check."""
        audit_date = datetime(2024, 1, 2)
        old_date = datetime(2024, 1, 1)  # 1 day old

        df = pl.DataFrame({
            "asset_id": ["BTC"] * 10,
            "venue": ["binance"] * 10,
            "event_ts": [old_date] * 10,
            "ingested_ts": [old_date] * 10,
            "close": [42000.0] * 10,
        })

        temp_store.append("test_ohlcv", df, sample_ohlcv_schema)

        audit = DataAudit(temp_store)
        results = audit.audit_dataset("test_ohlcv", audit_date)

        freshness_results = [r for r in results if r.check_name == "freshness"]
        assert len(freshness_results) > 0

    def test_price_outliers_check(self, temp_store, sample_ohlcv_schema):
        """Test price outlier detection."""
        base_date = datetime(2024, 1, 1)
        now = base_date.replace(microsecond=0)

        closes = [42000.0, 42000.0, 50000.0, 42000.0, 42000.0]

        df = pl.DataFrame({
            "asset_id": ["BTC"] * 5,
            "venue": ["binance"] * 5,
            "event_ts": [now] * 5,
            "ingested_ts": [now] * 5,
            "close": closes,
        })

        temp_store.append("test_ohlcv", df, sample_ohlcv_schema)

        audit = DataAudit(temp_store)
        results = audit.audit_dataset("test_ohlcv", base_date)

        outlier_results = [r for r in results if r.check_name == "price_outliers"]
        assert len(outlier_results) > 0

    def test_halt_trading_on_error(self, temp_store):
        """Test halt trading flag on critical failures."""
        audit = DataAudit(temp_store)

        audit.results = [
            AuditResult("check1", False, "failed", "error"),
            AuditResult("check2", True, "passed", "info"),
        ]

        assert audit.should_halt_trading()

    def test_no_halt_on_warning(self, temp_store):
        """Test that warnings don't halt trading."""
        audit = DataAudit(temp_store)

        audit.results = [
            AuditResult("check1", False, "failed", "warning"),
            AuditResult("check2", True, "passed", "info"),
        ]

        assert not audit.should_halt_trading()
