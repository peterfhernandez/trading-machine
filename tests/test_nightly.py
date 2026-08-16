"""Tests for Nightly pipeline."""

from datetime import datetime
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from datastore import AssetMaster, DatasetSchema, ParquetStore
from pipeline.nightly import NightlyPipeline, parse_window_arg


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

    @patch("pipeline.nightly.BackfillRunner")
    @patch("pipeline.nightly.DataAudit")
    def test_audit_is_given_the_pipeline_venue(
        self, mock_audit_class, mock_backfill_class
    ):
        """The coverage check resolves its denominator from the venue's universe
        snapshots, so the audit must know which venue is running."""
        mock_backfill_class.return_value = MagicMock()

        mock_audit = MagicMock()
        mock_audit.audit_dataset.return_value = []
        mock_audit.should_halt_trading.return_value = False
        mock_audit_class.return_value = mock_audit

        pipeline = NightlyPipeline("binance", dry_run=False)
        pipeline._audit_stage()

        for call in mock_audit_class.call_args_list:
            assert call.kwargs["venue"] == "binance"

    def test_pipeline_with_dry_run_disabled_trading_halt(self):
        """Test that trading halt is not allowed on dry-run."""
        pipeline = NightlyPipeline("binance", dry_run=True)
        pipeline._audit_stage()
        assert not pipeline.trading_halted


def _mock_exchange(symbols: list[str]) -> MagicMock:
    """A ccxt-like exchange exposing `symbols` and a real markets dict."""
    exchange = MagicMock()
    exchange.symbols = symbols
    exchange.markets = {s: {"base": s.split("/")[0], "quote": "USDT"} for s in symbols}
    exchange.load_markets = MagicMock()
    return exchange


class TestAssetMasterPopulation:
    """The asset master must cover the symbols the loaders actually iterate.

    Phase 5.9: that is **one market type per venue** — perpetuals. Until then
    the OHLCV loader read spot while the other three read derivatives, so
    `ohlcv_daily` carried two instruments under one `asset_id` with no column
    saying which (`DATA.md` §9.2).
    """

    @patch("ccxt.binance")
    def test_populates_the_perp_namespace(self, mock_binance_class, tmp_path):
        perp = _mock_exchange(["BTC/USDT:USDT", "ETH/USDT:USDT", "ETH/BTC"])
        mock_binance_class.side_effect = [perp]

        with patch("pipeline.nightly.DATASTORE_PATH", tmp_path):
            pipeline = NightlyPipeline("binance", dry_run=False)
            am = pipeline._populate_asset_master("binance")

        assert am.resolve_symbol("BTC/USDT:USDT", "binance") == "BTC"
        assert am.resolve_symbol("ETH/USDT:USDT", "binance") == "ETH"
        # Non-USDT pairs are not mapped
        assert am.resolve_symbol("ETH/BTC", "binance") is None

    @patch("ccxt.binance")
    def test_a_dated_future_is_not_mapped(self, mock_binance_class, tmp_path):
        """A quarterly has its own basis and expiry. Mapping it to the same
        asset_id as the perp puts two instruments in one series, which is the
        defect this phase exists to remove."""
        perp = _mock_exchange(["BTC/USDT:USDT", "BTC/USDT:USDT-260327"])
        mock_binance_class.side_effect = [perp]

        with patch("pipeline.nightly.DATASTORE_PATH", tmp_path):
            pipeline = NightlyPipeline("binance", dry_run=False)
            am = pipeline._populate_asset_master("binance")

        assert am.resolve_symbol("BTC/USDT:USDT", "binance") == "BTC"
        assert am.resolve_symbol("BTC/USDT:USDT-260327", "binance") is None

    @patch("ccxt.binance")
    def test_a_multiplier_prefix_is_kept(self, mock_binance_class, tmp_path):
        """The same rule `loaders/archive.py` applies, which is the whole point:
        `1000CAT` and `CAT` are two assets on both paths or on neither."""
        perp = _mock_exchange(["1000CAT/USDT:USDT", "CAT/USDT:USDT"])
        mock_binance_class.side_effect = [perp]

        with patch("pipeline.nightly.DATASTORE_PATH", tmp_path):
            pipeline = NightlyPipeline("binance", dry_run=False)
            am = pipeline._populate_asset_master("binance")

        assert am.resolve_symbol("1000CAT/USDT:USDT", "binance") == "1000CAT"
        assert am.resolve_symbol("CAT/USDT:USDT", "binance") == "CAT"

    @patch("ccxt.binance")
    def test_existing_mappings_are_not_duplicated(self, mock_binance_class, tmp_path):
        """add_mapping appends unconditionally, so a nightly re-run must skip
        symbols that are already mapped rather than growing the master."""
        mock_binance_class.side_effect = [
            _mock_exchange(["BTC/USDT:USDT"]),
            _mock_exchange(["BTC/USDT:USDT"]),
        ]

        with patch("pipeline.nightly.DATASTORE_PATH", tmp_path):
            pipeline = NightlyPipeline("binance", dry_run=False)
            pipeline._populate_asset_master("binance")
            am = pipeline._populate_asset_master("binance")

        mappings = pl.read_parquet(tmp_path / "asset_master.parquet")
        assert len(mappings) == 1
        assert am.resolve_symbol("BTC/USDT:USDT", "binance") == "BTC"

    @patch("ccxt.binance")
    def test_a_spot_only_venue_still_gets_its_symbols(
        self, mock_binance_class, tmp_path
    ):
        """One market type per venue does not mean futures everywhere: a venue
        with no derivatives falls back to its defaults rather than mapping
        nothing and leaving every loader unable to resolve a symbol."""
        mock_binance_class.side_effect = [
            RuntimeError("no futures API"),
            _mock_exchange(["BTC/USDT"]),
        ]

        with patch("pipeline.nightly.DATASTORE_PATH", tmp_path):
            pipeline = NightlyPipeline("binance", dry_run=False)
            am = pipeline._populate_asset_master("binance")

        assert am.resolve_symbol("BTC/USDT", "binance") == "BTC"


class TestWindowArguments:
    """--start/--end/--ignore-checkpoint reach the backfill runner."""

    def test_parses_date_and_iso_timestamps(self):
        assert parse_window_arg("2024-03-01", "start") == datetime(2024, 3, 1)
        assert parse_window_arg("2024-03-01T06:30:00", "start") == datetime(2024, 3, 1, 6, 30)
        assert parse_window_arg(None, "start") is None

    def test_strips_timezone_to_naive_utc(self):
        """The datastore stores naive UTC throughout."""
        parsed = parse_window_arg("2024-03-01T00:00:00+00:00", "start")
        assert parsed == datetime(2024, 3, 1)
        assert parsed.tzinfo is None

    def test_rejects_nonsense(self):
        with pytest.raises(SystemExit, match="--start must be"):
            parse_window_arg("last tuesday", "start")

    @patch("pipeline.nightly.BackfillRunner")
    @patch("pipeline.nightly.DataAudit")
    def test_explicit_window_is_passed_through(self, mock_audit_class, mock_backfill_class):
        mock_backfill = MagicMock()
        mock_backfill_class.return_value = mock_backfill
        mock_audit = MagicMock()
        mock_audit.audit_dataset.return_value = []
        mock_audit.should_halt_trading.return_value = False
        mock_audit_class.return_value = mock_audit

        with patch.object(NightlyPipeline, "_populate_asset_master", return_value=MagicMock()):
            pipeline = NightlyPipeline("binance", dry_run=False)
            pipeline.run(days=1, start=datetime(2024, 3, 1), end=datetime(2024, 3, 8))

        kwargs = mock_backfill.run.call_args.kwargs
        assert kwargs["start_date"] == datetime(2024, 3, 1)
        assert kwargs["end_date"] == datetime(2024, 3, 8)

    @patch("pipeline.nightly.BackfillRunner")
    @patch("pipeline.nightly.DataAudit")
    def test_ignore_checkpoint_reaches_the_runner(self, mock_audit_class, mock_backfill_class):
        mock_backfill_class.return_value = MagicMock()
        mock_audit = MagicMock()
        mock_audit.audit_dataset.return_value = []
        mock_audit.should_halt_trading.return_value = False
        mock_audit_class.return_value = mock_audit

        with patch.object(NightlyPipeline, "_populate_asset_master", return_value=MagicMock()):
            NightlyPipeline("binance", dry_run=False, ignore_checkpoint=True).run(days=1)

        assert mock_backfill_class.call_args.kwargs["ignore_checkpoint"] is True

    @patch("pipeline.nightly.BackfillRunner")
    @patch("pipeline.nightly.DataAudit")
    def test_days_still_works_without_a_window(self, mock_audit_class, mock_backfill_class):
        mock_backfill = MagicMock()
        mock_backfill_class.return_value = mock_backfill
        mock_audit = MagicMock()
        mock_audit.audit_dataset.return_value = []
        mock_audit.should_halt_trading.return_value = False
        mock_audit_class.return_value = mock_audit

        with patch.object(NightlyPipeline, "_populate_asset_master", return_value=MagicMock()):
            NightlyPipeline("binance", dry_run=False).run(days=3)

        kwargs = mock_backfill.run.call_args.kwargs
        assert kwargs["days_back"] == 3
        assert kwargs["start_date"] is None
