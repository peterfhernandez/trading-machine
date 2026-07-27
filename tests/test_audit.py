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

    def test_coverage_threshold_is_percentage_not_raw_multiplier(
        self, temp_store, sample_ohlcv_schema
    ):
        """coverage_threshold_pct=90.0 means 90%, i.e. int(150 * 0.90) = 135,
        not int(150 * 90.0) = 13500 -- the latter can never pass for any
        realistic universe size."""
        base_date = datetime(2024, 1, 1)
        now = base_date.replace(microsecond=0)

        assets = [f"ASSET{i}" for i in range(140)]  # above 135, below 150
        df = pl.DataFrame({
            "asset_id": assets,
            "venue": ["binance"] * len(assets),
            "event_ts": [now] * len(assets),
            "ingested_ts": [now] * len(assets),
            "close": [100.0] * len(assets),
        })
        temp_store.append("test_ohlcv", df, sample_ohlcv_schema)

        audit = DataAudit(temp_store)
        results = audit.audit_dataset("test_ohlcv", base_date)

        coverage_results = [r for r in results if r.check_name == "coverage"]
        assert len(coverage_results) == 1
        assert coverage_results[0].passed
        assert "threshold: 135" in coverage_results[0].message

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

    def test_price_outliers_ignores_cross_asset_price_differences(
        self, temp_store, sample_ohlcv_schema
    ):
        """Interleaved rows for different assets at wildly different price
        levels (e.g. BTC ~$100k next to a $0.10 altcoin) must not be read as
        a price jump -- each asset's series should be checked independently.
        A genuine large jump within one asset's own series should still be
        caught."""
        t0 = datetime(2024, 1, 1, 0)
        t1 = datetime(2024, 1, 1, 1)

        df = pl.DataFrame({
            "asset_id": ["BTC", "DOGE", "BTC", "DOGE"],
            "venue": ["binance"] * 4,
            "event_ts": [t0, t0, t1, t1],
            "ingested_ts": [t0, t0, t1, t1],
            # BTC: 100000 -> 101000 (1% move, not an outlier)
            # DOGE: 0.10 -> 0.10 (no move)
            "close": [100000.0, 0.10, 101000.0, 0.10],
        })
        temp_store.append("test_ohlcv", df, sample_ohlcv_schema)

        audit = DataAudit(temp_store)
        results = audit.audit_dataset("test_ohlcv", t1)

        outlier_results = [r for r in results if r.check_name == "price_outliers"]
        assert len(outlier_results) == 1
        assert outlier_results[0].passed

    def test_price_outliers_catches_genuine_within_asset_jump(
        self, temp_store, sample_ohlcv_schema
    ):
        """A real jump within a single asset's series should still be flagged,
        even interleaved with another asset's unrelated price level."""
        t0 = datetime(2024, 1, 1, 0)
        t1 = datetime(2024, 1, 1, 1)

        df = pl.DataFrame({
            "asset_id": ["BTC", "DOGE", "BTC", "DOGE"],
            "venue": ["binance"] * 4,
            "event_ts": [t0, t0, t1, t1],
            "ingested_ts": [t0, t0, t1, t1],
            # BTC: 100000 -> 150000 (50% move, a genuine outlier)
            "close": [100000.0, 0.10, 150000.0, 0.10],
        })
        temp_store.append("test_ohlcv", df, sample_ohlcv_schema)

        audit = DataAudit(temp_store)
        results = audit.audit_dataset("test_ohlcv", t1)

        outlier_results = [r for r in results if r.check_name == "price_outliers"]
        assert len(outlier_results) == 1
        assert not outlier_results[0].passed
        assert "1 price jumps" in outlier_results[0].message

    def test_freshness_check_flags_stale_data_within_lookback(self, temp_store, sample_ohlcv_schema):
        """Data ingested 2 days ago, audited today, is stale under the 24h default
        threshold but still within the 7-day lookback window: the audit should
        find it and report it as stale via the freshness check, not report it
        as "no data" (which would hide the actual cause)."""
        audit_date = datetime(2024, 1, 3)
        stale_date = datetime(2024, 1, 1)  # 2 days before audit_date

        df = pl.DataFrame({
            "asset_id": ["BTC"] * 10,
            "venue": ["binance"] * 10,
            "event_ts": [stale_date] * 10,
            "ingested_ts": [stale_date] * 10,
            "close": [42000.0] * 10,
        })
        temp_store.append("test_ohlcv", df, sample_ohlcv_schema)

        audit = DataAudit(temp_store)
        results = audit.audit_dataset("test_ohlcv", audit_date)

        assert not any(r.check_name == "data_presence" for r in results)
        freshness_results = [r for r in results if r.check_name == "freshness"]
        assert len(freshness_results) == 1
        assert not freshness_results[0].passed
        assert freshness_results[0].severity == "error"
        assert "48.0h old" in freshness_results[0].message

    def test_data_presence_fails_outside_lookback_window(self, temp_store, sample_ohlcv_schema):
        """Data ingested well beyond the lookback window (8 days ago, window is
        7 days) should be reported as missing, not silently found stale."""
        audit_date = datetime(2024, 1, 10)
        very_old_date = datetime(2024, 1, 1)  # 9 days before audit_date

        df = pl.DataFrame({
            "asset_id": ["BTC"] * 10,
            "venue": ["binance"] * 10,
            "event_ts": [very_old_date] * 10,
            "ingested_ts": [very_old_date] * 10,
            "close": [42000.0] * 10,
        })
        temp_store.append("test_ohlcv", df, sample_ohlcv_schema)

        audit = DataAudit(temp_store)
        results = audit.audit_dataset("test_ohlcv", audit_date)

        assert len(results) == 1
        assert results[0].check_name == "data_presence"
        assert not results[0].passed

    def test_per_dataset_freshness_threshold(self, temp_store):
        """ohlcv_hourly has a tighter freshness bar than the 24h default."""
        assert AUDIT_CONFIG.freshness_threshold_for("ohlcv_hourly") == 2.0
        assert AUDIT_CONFIG.freshness_threshold_for("ohlcv_daily") == 24.0
        assert AUDIT_CONFIG.freshness_threshold_for("some_unlisted_dataset") == (
            AUDIT_CONFIG.freshness_threshold_hours
        )

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
