"""Tests for Audit module."""

from datetime import datetime, timedelta

import polars as pl
import pytest

from config import AUDIT_CONFIG
from datastore import ParquetStore, DatasetSchema
from audit.auditor import DataAudit, AuditResult
from universe import UNIVERSE_SCHEMA


@pytest.fixture
def temp_store(tmp_path):
    """Create a temporary ParquetStore."""
    return ParquetStore(tmp_path)


def write_universe_snapshot(
    store: ParquetStore,
    event_ts: datetime,
    n_members: int,
    venue: str = "binance",
    n_excluded: int = 0,
    ingested_ts: datetime | None = None,
) -> None:
    """Append a point-in-time universe snapshot with `n_members` members.

    Mirrors what UniverseBuilder writes: one row per asset considered, with
    non-members carrying an exclusion_reason.
    """
    ingested_ts = ingested_ts or event_ts
    members = [f"MEM{i:03d}" for i in range(n_members)]
    excluded = [f"EXC{i:03d}" for i in range(n_excluded)]
    assets = members + excluded

    df = pl.DataFrame(
        {
            "asset_id": assets,
            "venue": [venue] * len(assets),
            "event_ts": [event_ts] * len(assets),
            "ingested_ts": [ingested_ts] * len(assets),
            "in_universe": [True] * n_members + [False] * n_excluded,
            "dollar_volume_median": [1e7] * len(assets),
            "listing_age_days": [365] * len(assets),
            "rank": list(range(1, n_members + 1)) + [None] * n_excluded,
            "exclusion_reason": [None] * n_members + ["low_volume"] * n_excluded,
        },
        schema=UNIVERSE_SCHEMA.to_polars_schema(),
    )

    store.append("universe", df, UNIVERSE_SCHEMA)


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

        write_universe_snapshot(temp_store, base_date, n_members=150)

        assets = [f"MEM{i:03d}" for i in range(140)]  # above 135, below 150
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

        # One row per bar: 100 rows sharing an event_ts would be 100 ingestions
        # of the *same* bar, which the audit collapses before counting nulls.
        bars = [now - timedelta(hours=i) for i in range(100)]
        df = pl.DataFrame({
            "asset_id": ["BTC"] * 100,
            "venue": ["binance"] * 100,
            "event_ts": bars,
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

        # Five consecutive bars, not five ingestions of one bar -- a "jump"
        # between two copies of the same bar is a revision, not a price move.
        df = pl.DataFrame({
            "asset_id": ["BTC"] * 5,
            "venue": ["binance"] * 5,
            "event_ts": [now + timedelta(hours=i) for i in range(5)],
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


class TestCoverageDenominator:
    """The coverage check's denominator is the point-in-time universe, not a
    hardcoded target size."""

    def test_denominator_comes_from_universe_snapshot(self, temp_store, sample_ohlcv_schema):
        """20 of 20 universe members present -> coverage passes with a
        threshold of 18 (90% of 20), not of some hardcoded 150."""
        base_date = datetime(2024, 1, 1)

        write_universe_snapshot(temp_store, base_date, n_members=20, n_excluded=30)

        assets = [f"MEM{i:03d}" for i in range(20)]
        df = pl.DataFrame({
            "asset_id": assets,
            "venue": ["binance"] * len(assets),
            "event_ts": [base_date] * len(assets),
            "ingested_ts": [base_date] * len(assets),
            "close": [100.0] * len(assets),
        })
        temp_store.append("test_ohlcv", df, sample_ohlcv_schema)

        audit = DataAudit(temp_store)
        results = audit.audit_dataset("test_ohlcv", base_date)

        coverage = [r for r in results if r.check_name == "coverage"][0]
        assert coverage.passed
        assert "20/20 universe assets" in coverage.message
        assert "threshold: 18" in coverage.message
        assert not audit.should_halt_trading()

    def test_short_coverage_against_known_universe_halts(self, temp_store, sample_ohlcv_schema):
        """A real shortfall against a known universe is still a halting error."""
        base_date = datetime(2024, 1, 1)

        write_universe_snapshot(temp_store, base_date, n_members=100)

        assets = [f"MEM{i:03d}" for i in range(15)]
        df = pl.DataFrame({
            "asset_id": assets,
            "venue": ["binance"] * len(assets),
            "event_ts": [base_date] * len(assets),
            "ingested_ts": [base_date] * len(assets),
            "close": [100.0] * len(assets),
        })
        temp_store.append("test_ohlcv", df, sample_ohlcv_schema)

        audit = DataAudit(temp_store)
        results = audit.audit_dataset("test_ohlcv", base_date)

        coverage = [r for r in results if r.check_name == "coverage"][0]
        assert not coverage.passed
        assert coverage.severity == "error"
        assert "15/100 universe assets" in coverage.message
        assert "threshold: 90" in coverage.message
        assert audit.should_halt_trading()

    def test_no_universe_snapshot_is_not_evaluated_and_does_not_halt(
        self, temp_store, sample_ohlcv_schema
    ):
        """With no universe dataset there is no honest denominator: report the
        observed asset count as a warning rather than halting trading on a
        fabricated threshold."""
        base_date = datetime(2024, 1, 1)

        assets = [f"ASSET{i}" for i in range(15)]
        df = pl.DataFrame({
            "asset_id": assets,
            "venue": ["binance"] * len(assets),
            "event_ts": [base_date] * len(assets),
            "ingested_ts": [base_date] * len(assets),
            "close": [100.0] * len(assets),
        })
        temp_store.append("test_ohlcv", df, sample_ohlcv_schema)

        audit = DataAudit(temp_store)
        results = audit.audit_dataset("test_ohlcv", base_date)

        coverage = [r for r in results if r.check_name == "coverage"][0]
        assert coverage.passed
        assert coverage.severity == "warning"
        assert "not evaluated" in coverage.message
        assert "15 assets" in coverage.message
        assert not audit.should_halt_trading()

    def test_non_members_do_not_count_toward_coverage(self, temp_store, sample_ohlcv_schema):
        """A dataset full of assets that are not in the universe covers none of
        it -- coverage counts members present, not distinct asset_ids."""
        base_date = datetime(2024, 1, 1)

        write_universe_snapshot(temp_store, base_date, n_members=10)

        assets = ["MEM000", "MEM001"] + [f"OTHER{i}" for i in range(50)]
        df = pl.DataFrame({
            "asset_id": assets,
            "venue": ["binance"] * len(assets),
            "event_ts": [base_date] * len(assets),
            "ingested_ts": [base_date] * len(assets),
            "close": [100.0] * len(assets),
        })
        temp_store.append("test_ohlcv", df, sample_ohlcv_schema)

        audit = DataAudit(temp_store)
        results = audit.audit_dataset("test_ohlcv", base_date)

        coverage = [r for r in results if r.check_name == "coverage"][0]
        assert not coverage.passed
        assert "2/10 universe assets" in coverage.message

    def test_uses_latest_snapshot_at_or_before_audit_date(self, temp_store, sample_ohlcv_schema):
        """Point-in-time: a later snapshot must not set today's denominator."""
        audit_date = datetime(2024, 1, 5)

        write_universe_snapshot(temp_store, datetime(2024, 1, 1), n_members=10)
        write_universe_snapshot(temp_store, datetime(2024, 1, 4), n_members=20)
        write_universe_snapshot(temp_store, datetime(2024, 1, 6), n_members=200)

        audit = DataAudit(temp_store)
        size, source = audit.resolve_universe_size(audit_date)

        assert size == 20
        assert "2024-01-04" in source

    def test_ignores_snapshot_ingested_after_audit_date(self, temp_store):
        """A snapshot whose ingested_ts is later than the audit date was not
        knowable then, so it cannot define the denominator."""
        audit_date = datetime(2024, 1, 5)

        write_universe_snapshot(
            temp_store,
            datetime(2024, 1, 4),
            n_members=20,
            ingested_ts=datetime(2024, 1, 9),
        )

        audit = DataAudit(temp_store)
        size, source = audit.resolve_universe_size(audit_date)

        assert size is None
        assert "no universe snapshot" in source

    def test_ignores_other_venues(self, temp_store):
        """Only the audited venue's universe counts."""
        audit_date = datetime(2024, 1, 5)

        write_universe_snapshot(temp_store, datetime(2024, 1, 4), n_members=30, venue="deribit")

        audit = DataAudit(temp_store, venue="binance")
        size, source = audit.resolve_universe_size(audit_date)
        assert size is None

        audit_deribit = DataAudit(temp_store, venue="deribit")
        size, source = audit_deribit.resolve_universe_size(audit_date)
        assert size == 30

    def test_excluded_assets_do_not_count_as_universe_members(self, temp_store):
        """Rows are written for every asset considered; only in_universe counts."""
        audit_date = datetime(2024, 1, 5)

        write_universe_snapshot(
            temp_store, datetime(2024, 1, 4), n_members=12, n_excluded=88
        )

        audit = DataAudit(temp_store)
        size, _ = audit.resolve_universe_size(audit_date)

        assert size == 12

    def test_explicit_universe_size_override(self, temp_store, sample_ohlcv_schema):
        """A caller with a known expected asset count can set the denominator."""
        base_date = datetime(2024, 1, 1)

        assets = ["BTC", "ETH", "SOL", "ADA"]
        df = pl.DataFrame({
            "asset_id": assets,
            "venue": ["binance"] * len(assets),
            "event_ts": [base_date] * len(assets),
            "ingested_ts": [base_date] * len(assets),
            "close": [100.0] * len(assets),
        })
        temp_store.append("test_ohlcv", df, sample_ohlcv_schema)

        audit = DataAudit(temp_store, universe_size=4)
        results = audit.audit_dataset("test_ohlcv", base_date)

        coverage = [r for r in results if r.check_name == "coverage"][0]
        assert coverage.passed
        assert "4/4 universe assets" in coverage.message
        assert "explicit universe_size" in coverage.message

    def test_empty_universe_snapshot_is_not_a_denominator(self, temp_store):
        """A snapshot with no members (e.g. built before any data existed)
        cannot be divided by."""
        audit_date = datetime(2024, 1, 5)

        write_universe_snapshot(temp_store, datetime(2024, 1, 4), n_members=0, n_excluded=5)

        audit = DataAudit(temp_store)
        size, source = audit.resolve_universe_size(audit_date)

        assert size is None
        assert "no members" in source


class TestDuplicateBars:
    """Repeat ingestions of a bar are reported, and never counted twice."""

    def _write(self, store, schema, rows):
        """rows: (asset_id, event_ts, ingested_ts, close) tuples."""
        store.append(
            "test_ohlcv",
            pl.DataFrame(
                {
                    "asset_id": [r[0] for r in rows],
                    "venue": ["binance"] * len(rows),
                    "event_ts": [r[1] for r in rows],
                    "ingested_ts": [r[2] for r in rows],
                    "close": [r[3] for r in rows],
                },
                schema=schema.to_polars_schema(),
            ),
            schema,
        )

    def test_duplicates_are_reported_but_do_not_halt(self, temp_store, sample_ohlcv_schema):
        base_date = datetime(2024, 1, 2)
        bar = datetime(2024, 1, 1)

        self._write(temp_store, sample_ohlcv_schema, [
            ("BTC", bar, datetime(2024, 1, 1, 12), 100.0),
            ("BTC", bar, datetime(2024, 1, 2, 12), 100.0),  # re-ingested
            ("ETH", bar, datetime(2024, 1, 2, 12), 10.0),
        ])

        audit = DataAudit(temp_store)
        results = audit.audit_dataset("test_ohlcv", base_date)

        dupes = [r for r in results if r.check_name == "duplicate_bars"][0]
        assert dupes.passed
        assert dupes.severity == "warning"
        assert "1 of 3 rows" in dupes.message
        assert "2 unique bars" in dupes.message
        assert not audit.should_halt_trading()

    def test_clean_dataset_reports_no_duplicates(self, temp_store, sample_ohlcv_schema):
        base_date = datetime(2024, 1, 2)

        self._write(temp_store, sample_ohlcv_schema, [
            ("BTC", datetime(2024, 1, 1), datetime(2024, 1, 1, 12), 100.0),
            ("ETH", datetime(2024, 1, 1), datetime(2024, 1, 1, 12), 10.0),
        ])

        audit = DataAudit(temp_store)
        results = audit.audit_dataset("test_ohlcv", base_date)

        dupes = [r for r in results if r.check_name == "duplicate_bars"][0]
        assert dupes.passed
        assert dupes.severity == "info"

    def test_null_rate_is_computed_per_bar_not_per_row(self, temp_store, sample_ohlcv_schema):
        """10 bars, one with a null close, is a 10% null rate. Re-ingesting the
        nine good bars must not dilute it to 5%."""
        base_date = datetime(2024, 1, 2)
        bars = [datetime(2024, 1, 1, h) for h in range(10)]

        rows = [("BTC", ts, datetime(2024, 1, 1, 20), 100.0) for ts in bars[:9]]
        rows.append(("BTC", bars[9], datetime(2024, 1, 1, 20), None))
        rows += [("BTC", ts, datetime(2024, 1, 2, 20), 100.0) for ts in bars[:9]]
        self._write(temp_store, sample_ohlcv_schema, rows)

        audit = DataAudit(temp_store)
        results = audit.audit_dataset("test_ohlcv", base_date)

        null_close = [r for r in results if r.check_name == "null_rate_close"][0]
        assert "10.00%" in null_close.message

    def test_revision_is_not_read_as_a_price_jump(self, temp_store, sample_ohlcv_schema):
        """A bar re-ingested with a corrected close is one bar, not a 50% move
        between two bars."""
        base_date = datetime(2024, 1, 2)
        bar = datetime(2024, 1, 1)

        self._write(temp_store, sample_ohlcv_schema, [
            ("BTC", bar, datetime(2024, 1, 1, 12), 100.0),
            ("BTC", bar, datetime(2024, 1, 2, 12), 150.0),  # revision, same bar
        ])

        audit = DataAudit(temp_store)
        results = audit.audit_dataset("test_ohlcv", base_date)

        outliers = [r for r in results if r.check_name == "price_outliers"]
        assert outliers == [] or outliers[0].passed

    def test_freshness_uses_the_latest_ingestion(self, temp_store, sample_ohlcv_schema):
        """Collapsing to the latest ingestion must not make data look stale."""
        audit_date = datetime(2024, 1, 2, 12)
        bar = datetime(2024, 1, 1)

        self._write(temp_store, sample_ohlcv_schema, [
            ("BTC", bar, datetime(2024, 1, 1, 0), 100.0),
            ("BTC", bar, datetime(2024, 1, 2, 6), 100.0),
        ])

        audit = DataAudit(temp_store)
        results = audit.audit_dataset("test_ohlcv", audit_date)

        freshness = [r for r in results if r.check_name == "freshness"][0]
        assert freshness.passed
        assert "6.0h old" in freshness.message
