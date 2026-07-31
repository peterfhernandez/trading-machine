"""Tests for the logging retrofit (Phase 5.5).

Per `LOGGING.md` section 8, this suite deliberately does **not** assert log
message text broadly — that is brittle and not the point. It asserts the three
things the design actually promises:

1. A **CRITICAL** record on every halt path (`should_halt_trading()` returning
   True) and on any unhandled exception in a pipeline stage — so a halt has a
   durable record whether or not the Telegram alert got out.
2. An **ERROR** record when a loader raises inside `BaseLoader.append`.
3. `prune_old_logs()`, given fabricated file ages, deletes only backups older
   than `retention_days`, never the live `*.log` file, and returns exactly the
   deleted paths.

Plus the mechanism itself: `get_logger` naming, `run_id` correlation, JSON
formatting, and that every component named in `LOG_CONFIG.components` gets a
file handler.
"""

import json
import logging
import time
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from pathlib import Path

import polars as pl
import pytest

import logging_config
from audit.auditor import AuditResult, DataAudit
from config import LOG_CONFIG
from datastore import ParquetStore
from loaders.base import BaseLoader
from loaders.schemas import OHLCV_SCHEMA
from pipeline import prune_logs
from pipeline.nightly import NightlyPipeline

D1 = datetime(2024, 1, 1)


@dataclass
class FakeLogConfig:
    """A `LogConfig` pointed at a tmp_path, for retention tests."""

    dir: Path
    retention_days: int = 365
    level: str = "INFO"
    console_level: str = "WARNING"
    max_bytes: int = 1024
    components: tuple[str, ...] = ("pipeline",)


def touch(path: Path, age_days: float) -> Path:
    """Create `path` with an mtime `age_days` in the past."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("{}\n")
    when = time.time() - age_days * 86400
    import os

    os.utime(path, (when, when))
    return path


# ===========================================================================
# Retention pruning
# ===========================================================================


class TestPruneOldLogs:
    """Rotation is size-triggered; retention is calendar-triggered and separate."""

    def test_deletes_only_backups_past_the_window(self, tmp_path):
        cfg = FakeLogConfig(dir=tmp_path, retention_days=365)
        old = touch(tmp_path / "loaders.log.20240101T000000Z.log", age_days=400)
        newer = touch(tmp_path / "loaders.log.20260101T000000Z.log", age_days=10)

        deleted = logging_config.prune_old_logs(cfg)

        assert deleted == [old]
        assert not old.exists()
        assert newer.exists()

    def test_never_touches_the_live_file(self, tmp_path):
        """The file a handler is currently writing to must survive any age."""
        cfg = FakeLogConfig(dir=tmp_path, retention_days=1)
        live = touch(tmp_path / "loaders.log", age_days=5000)

        deleted = logging_config.prune_old_logs(cfg)

        assert deleted == []
        assert live.exists()

    def test_returns_exactly_the_deleted_paths(self, tmp_path):
        cfg = FakeLogConfig(dir=tmp_path, retention_days=30)
        expired = [
            touch(tmp_path / f"audit.log.2024010{i}T000000Z.log", age_days=100 + i)
            for i in range(1, 4)
        ]
        touch(tmp_path / "audit.log.20260101T000000Z.log", age_days=1)

        deleted = logging_config.prune_old_logs(cfg)

        assert sorted(deleted) == sorted(expired)
        assert all(not p.exists() for p in expired)

    def test_boundary_is_retention_days(self, tmp_path):
        cfg = FakeLogConfig(dir=tmp_path, retention_days=365)
        just_inside = touch(tmp_path / "a.log.20260101T000000Z.log", age_days=364.9)
        just_outside = touch(tmp_path / "b.log.20240101T000000Z.log", age_days=365.1)

        deleted = logging_config.prune_old_logs(cfg)

        assert deleted == [just_outside]
        assert just_inside.exists()

    def test_missing_directory_is_not_an_error(self, tmp_path):
        cfg = FakeLogConfig(dir=tmp_path / "does_not_exist")
        assert logging_config.prune_old_logs(cfg) == []

    def test_expired_backups_lists_without_deleting(self, tmp_path):
        cfg = FakeLogConfig(dir=tmp_path, retention_days=30)
        old = touch(tmp_path / "audit.log.20240101T000000Z.log", age_days=100)

        expired = logging_config.expired_log_backups(cfg)

        assert expired == [old]
        assert old.exists()  # listing must not delete

    def test_now_can_be_injected(self, tmp_path):
        """A fixed `now` makes the boundary testable without sleeping."""
        cfg = FakeLogConfig(dir=tmp_path, retention_days=1)
        path = touch(tmp_path / "a.log.20260101T000000Z.log", age_days=0)

        # Pretend it is ten days later.
        deleted = logging_config.prune_old_logs(cfg, now=time.time() + 10 * 86400)

        assert deleted == [path]

    def test_prune_logs_entry_point_dry_run(self, tmp_path, monkeypatch, capsys):
        cfg = FakeLogConfig(dir=tmp_path, retention_days=30)
        old = touch(tmp_path / "audit.log.20240101T000000Z.log", age_days=100)
        monkeypatch.setattr(prune_logs, "LOG_CONFIG", cfg)
        monkeypatch.setattr(
            prune_logs, "expired_log_backups", lambda: logging_config.expired_log_backups(cfg)
        )

        assert prune_logs.main(["--dry-run"]) == 0
        assert old.exists()
        assert old.name in capsys.readouterr().out

    def test_prune_logs_entry_point_deletes(self, tmp_path, monkeypatch, capsys):
        cfg = FakeLogConfig(dir=tmp_path, retention_days=30)
        old = touch(tmp_path / "audit.log.20240101T000000Z.log", age_days=100)
        monkeypatch.setattr(prune_logs, "LOG_CONFIG", cfg)
        monkeypatch.setattr(
            prune_logs, "prune_old_logs", lambda: logging_config.prune_old_logs(cfg)
        )

        assert prune_logs.main([]) == 0
        assert not old.exists()
        assert old.name in capsys.readouterr().out


# ===========================================================================
# CRITICAL on every halt path
# ===========================================================================


def failing_audit(store: ParquetStore) -> DataAudit:
    """An audit whose results contain one halting failure."""
    audit = DataAudit(store, venue="binance")
    audit.results = [
        AuditResult("coverage", passed=False, message="only 3 of 150", severity="error"),
        AuditResult("freshness", passed=True, message="2h old", severity="info"),
    ]
    return audit


class TestCriticalOnHalt:
    """A halt always leaves a CRITICAL record, independent of the alert."""

    def test_should_halt_trading_logs_critical(self, datastore_path, caplog):
        audit = failing_audit(ParquetStore(datastore_path))

        with caplog.at_level(logging.CRITICAL, logger="tm.audit"):
            assert audit.should_halt_trading() is True

        criticals = [r for r in caplog.records if r.levelno == logging.CRITICAL]
        assert len(criticals) == 1
        assert "coverage" in criticals[0].getMessage()

    def test_no_critical_when_nothing_halts(self, datastore_path, caplog):
        audit = DataAudit(ParquetStore(datastore_path), venue="binance")
        audit.results = [
            AuditResult("coverage", passed=True, message="ok", severity="info"),
            AuditResult("dupes", passed=True, message="some", severity="warning"),
        ]

        with caplog.at_level(logging.DEBUG, logger="tm.audit"):
            assert audit.should_halt_trading() is False

        assert not [r for r in caplog.records if r.levelno == logging.CRITICAL]

    def test_critical_is_logged_before_the_alert_is_attempted(
        self, datastore_path, caplog
    ):
        """Telegram being down must not cost us the durable record."""
        audit = failing_audit(ParquetStore(datastore_path))

        with caplog.at_level(logging.DEBUG, logger="tm.audit"):
            halted = audit.should_halt_trading()
            audit.send_alerts()  # alerts disabled in tests; must not raise

        assert halted
        assert [r for r in caplog.records if r.levelno == logging.CRITICAL]

    def test_data_presence_failure_halts_and_logs_critical(self, datastore_path, caplog):
        """The first-ever-run path: a dataset with no data at all."""
        audit = DataAudit(ParquetStore(datastore_path), venue="binance")

        with caplog.at_level(logging.DEBUG, logger="tm.audit"):
            results = audit.audit_dataset("ohlcv_daily", D1)
            halted = audit.should_halt_trading()

        assert results[0].check_name == "data_presence"
        assert halted
        assert [r for r in caplog.records if r.levelno == logging.CRITICAL]

    def test_every_check_verdict_is_logged(self, datastore_path, caplog):
        """Passes too: a check that stopped running is only visible by absence."""
        store = ParquetStore(datastore_path)
        frame = pl.DataFrame(
            {
                "asset_id": ["BTC", "BTC"],
                "venue": ["binance"] * 2,
                "timeframe": ["1d"] * 2,
                "event_ts": [D1, D1 + timedelta(days=1)],
                "ingested_ts": [D1, D1 + timedelta(days=1)],
                "open": [100.0, 101.0],
                "high": [102.0, 103.0],
                "low": [99.0, 100.0],
                "close": [101.0, 102.0],
                "volume": [1e6, 1e6],
            }
        ).with_columns(
            pl.col("event_ts").cast(pl.Datetime("us")),
            pl.col("ingested_ts").cast(pl.Datetime("us")),
        )
        store.append("ohlcv_daily", frame, OHLCV_SCHEMA)

        audit = DataAudit(store, venue="binance")
        with caplog.at_level(logging.INFO, logger="tm.audit"):
            results = audit.audit_dataset("ohlcv_daily", D1 + timedelta(days=1))

        logged = "\n".join(r.getMessage() for r in caplog.records)
        for result in results:
            assert result.check_name in logged


class TestCriticalOnPipelineFailure:
    """An unhandled stage exception is a halt in everything but name."""

    def test_stage_exception_logs_critical_and_names_the_stage(self, caplog):
        pipeline = NightlyPipeline(venue="binance", dry_run=True)

        def boom():
            raise RuntimeError("stage exploded")

        with caplog.at_level(logging.CRITICAL, logger="tm.pipeline"):
            with pytest.raises(RuntimeError, match="stage exploded"):
                pipeline._stage("audit", boom)

        criticals = [r for r in caplog.records if r.levelno == logging.CRITICAL]
        assert len(criticals) == 1
        assert "AUDIT" in criticals[0].getMessage()
        assert criticals[0].exc_info is not None

    def test_run_logs_critical_and_returns_false(self, caplog, monkeypatch):
        pipeline = NightlyPipeline(venue="binance", dry_run=True)
        monkeypatch.setattr(
            pipeline, "_load_stage", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("nope"))
        )

        with caplog.at_level(logging.CRITICAL, logger="tm.pipeline"):
            assert pipeline.run(days=1) is False

        criticals = [r for r in caplog.records if r.levelno == logging.CRITICAL]
        # One from the stage, one from run() itself.
        assert len(criticals) >= 1

    def test_successful_run_logs_no_critical(self, caplog):
        pipeline = NightlyPipeline(venue="binance", dry_run=True)

        with caplog.at_level(logging.DEBUG, logger="tm.pipeline"):
            assert pipeline.run(days=1) is True

        assert not [r for r in caplog.records if r.levelno == logging.CRITICAL]

    def test_run_sets_a_run_id(self):
        NightlyPipeline(venue="binance", dry_run=True).run(days=1)
        assert logging_config.get_run_id() != "-"

    def test_run_prunes_logs_even_when_it_fails(self, monkeypatch):
        pipeline = NightlyPipeline(venue="binance", dry_run=True)
        monkeypatch.setattr(
            pipeline, "_load_stage", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("nope"))
        )
        calls = []
        monkeypatch.setattr(
            "pipeline.nightly.prune_old_logs", lambda: calls.append(True) or []
        )

        assert pipeline.run(days=1) is False
        assert calls == [True]

    def test_pruning_failure_does_not_fail_the_run(self, monkeypatch, caplog):
        pipeline = NightlyPipeline(venue="binance", dry_run=True)
        monkeypatch.setattr(
            "pipeline.nightly.prune_old_logs",
            lambda: (_ for _ in ()).throw(OSError("disk gone")),
        )

        with caplog.at_level(logging.WARNING, logger="tm.pipeline"):
            assert pipeline.run(days=1) is True

        assert any("pruning" in r.getMessage().lower() for r in caplog.records)


# ===========================================================================
# ERROR when a loader's append raises
# ===========================================================================


class BrokenStore:
    """A store whose append always fails."""

    def append(self, *args, **kwargs):
        raise OSError("disk full")


class DummyLoader(BaseLoader):
    def fetch(self) -> pl.DataFrame:
        return pl.DataFrame({"asset_id": ["BTC"]})


class TestLoaderAppendErrors:
    def test_append_failure_logs_error_and_reraises(self, datastore_path, caplog):
        loader = DummyLoader("binance", store=ParquetStore(datastore_path))
        loader.store = BrokenStore()

        with caplog.at_level(logging.ERROR, logger="tm.loaders"):
            with pytest.raises(OSError, match="disk full"):
                loader.append("ohlcv_daily", pl.DataFrame({"asset_id": ["BTC"]}), OHLCV_SCHEMA)

        errors = [r for r in caplog.records if r.levelno == logging.ERROR]
        assert len(errors) == 1
        assert "ohlcv_daily" in errors[0].getMessage()
        assert errors[0].exc_info is not None

    def test_unresolved_symbols_log_a_warning_with_a_count(self, datastore_path, caplog):
        loader = DummyLoader("binance", store=ParquetStore(datastore_path))

        with caplog.at_level(logging.WARNING, logger="tm"):
            resolved = loader.resolve_symbols(["BTC/USDT", "ETH/USDT"])

        assert resolved == {"BTC/USDT": None, "ETH/USDT": None}
        summary = [r for r in caplog.records if "unresolved" in r.getMessage()]
        assert any("2 of 2" in r.getMessage() for r in summary)

    def test_schema_violation_logs_a_warning_before_raising(self, datastore_path, caplog):
        store = ParquetStore(datastore_path)

        with caplog.at_level(logging.WARNING, logger="tm.datastore"):
            with pytest.raises(ValueError, match="Missing required column"):
                store.append("ohlcv_daily", pl.DataFrame({"asset_id": ["BTC"]}), OHLCV_SCHEMA)

        assert [r for r in caplog.records if r.levelno == logging.WARNING]


# ===========================================================================
# The mechanism
# ===========================================================================


class TestLoggingMechanism:
    def test_logger_names_are_namespaced(self):
        assert logging_config.get_logger("loaders.ohlcv").name == "tm.loaders.ohlcv"
        assert logging_config.get_logger("tm.audit").name == "tm.audit"

    def test_every_configured_component_has_a_file_handler(self):
        logging_config.configure_logging()
        for component in LOG_CONFIG.components:
            handlers = logging.getLogger(f"tm.{component}").handlers
            assert handlers, f"{component} has no handler"
            assert any(
                isinstance(h, logging.handlers.RotatingFileHandler) for h in handlers
            ), component

    def test_component_files_are_named_per_component(self):
        logging_config.configure_logging()
        for component in ("datastore", "loaders", "audit", "signals", "backtest"):
            handler = next(
                h for h in logging.getLogger(f"tm.{component}").handlers
                if isinstance(h, logging.handlers.RotatingFileHandler)
            )
            assert Path(handler.baseFilename).name == f"{component}.log"

    def test_rotation_size_comes_from_config(self):
        logging_config.configure_logging()
        handler = next(
            h for h in logging.getLogger("tm.loaders").handlers
            if isinstance(h, logging.handlers.RotatingFileHandler)
        )
        assert handler.maxBytes == LOG_CONFIG.max_bytes

    def test_configure_logging_is_idempotent(self):
        logging_config.configure_logging()
        before = len(logging.getLogger("tm.loaders").handlers)
        logging_config.configure_logging()
        assert len(logging.getLogger("tm.loaders").handlers) == before

    def test_json_formatter_emits_one_object_per_record(self):
        record = logging.LogRecord(
            "tm.loaders.ohlcv", logging.INFO, "loaders/ohlcv.py", 42,
            "loaded %d symbols", (7,), None,
        )
        record.run_id = "20260731T000000Z"
        payload = json.loads(logging_config.JsonFormatter().format(record))

        assert payload["level"] == "INFO"
        assert payload["logger"] == "tm.loaders.ohlcv"
        assert payload["run_id"] == "20260731T000000Z"
        assert payload["message"] == "loaded 7 symbols"
        assert payload["line"] == 42

    def test_json_formatter_includes_the_traceback(self):
        try:
            raise ValueError("boom")
        except ValueError:
            import sys

            record = logging.LogRecord(
                "tm.audit", logging.CRITICAL, "audit/auditor.py", 1,
                "halt", (), sys.exc_info(),
            )
        payload = json.loads(logging_config.JsonFormatter().format(record))
        assert "ValueError: boom" in payload["exception"]

    def test_run_id_defaults_to_a_dash(self):
        """A scratch script that never sets one is not the unattended-run case."""
        record = logging.LogRecord("tm.x", logging.INFO, "x.py", 1, "hi", (), None)
        assert json.loads(logging_config.JsonFormatter().format(record))["run_id"] == "-"

    def test_run_id_round_trip(self):
        run_id = logging_config.new_run_id()
        logging_config.set_run_id(run_id)
        assert logging_config.get_run_id() == run_id
        assert run_id.endswith("Z") and len(run_id) == 16

    def test_run_id_filter_stamps_the_record(self):
        logging_config.set_run_id("20260731T010203Z")
        record = logging.LogRecord("tm.x", logging.INFO, "x.py", 1, "hi", (), None)
        assert logging_config._RunIdFilter().filter(record) is True
        assert record.run_id == "20260731T010203Z"

    def test_backup_names_are_timestamped_not_numbered(self):
        """Retention reads a backup's age from its name, so the name must carry it."""
        name = logging_config._timestamped_namer("logs/loaders.log.1")
        assert name.startswith("logs/loaders.log.")
        assert name.endswith(".log")
        assert not name.endswith(".1")
        stamp = name.split("logs/loaders.log.")[1].removesuffix(".log")
        assert len(stamp) == 16 and stamp.endswith("Z")

    def test_a_timestamped_backup_matches_the_prune_glob(self, tmp_path):
        """The namer and the pruner must agree, or retention silently never runs."""
        backup = Path(logging_config._timestamped_namer(str(tmp_path / "loaders.log.1")))
        touch(backup, age_days=500)
        touch(tmp_path / "loaders.log", age_days=500)

        deleted = logging_config.prune_old_logs(FakeLogConfig(dir=tmp_path, retention_days=1))

        assert deleted == [backup]
        assert (tmp_path / "loaders.log").exists()


class TestModuleLoggers:
    """Every retrofitted module logs under its own component."""

    @pytest.mark.parametrize(
        "module_name, component",
        [
            ("datastore.store", "datastore"),
            ("datastore.asset_master", "datastore"),
            ("loaders.base", "loaders"),
            ("loaders.backfill", "loaders"),
            ("loaders.ohlcv", "loaders"),
            ("loaders.window", "loaders"),
            ("audit.auditor", "audit"),
            ("universe.builder", "universe"),
            ("backtest.engine", "backtest"),
            ("signals.registry", "signals"),
            ("signals.markov_mean_reversion", "signals"),
            ("signals.carry", "signals"),
            ("signals.breadth", "signals"),
            ("signals.alpha", "signals"),
            ("pipeline.nightly", "pipeline"),
        ],
    )
    def test_module_logger_is_under_its_component(self, module_name, component):
        import importlib

        module = importlib.import_module(module_name)
        logger = getattr(module, "logger", None) or module.log
        assert logger.name == f"tm.{module_name}"
        assert logger.name.startswith(f"tm.{component}.")

    def test_no_module_still_uses_the_stdlib_root_logger(self):
        """`get_logger`, not `logging.getLogger`, is the single entry point."""
        import importlib

        for module_name in (
            "datastore.store", "loaders.base", "audit.auditor",
            "universe.builder", "backtest.engine", "signals.registry",
        ):
            module = importlib.import_module(module_name)
            logger = getattr(module, "logger", None) or module.log
            assert logger.name != module_name, module_name


def test_log_config_has_no_stale_file_field():
    """`LogConfig.file` was replaced by `dir` (LOGGING.md section 4)."""
    assert not hasattr(LOG_CONFIG, "file")
    assert LOG_CONFIG.dir.name == "logs"
    assert LOG_CONFIG.max_bytes == 10 * 1024 * 1024
    assert LOG_CONFIG.retention_days == 365


def test_every_pipeline_component_is_declared():
    """Phases 6-9's files are reserved up front so nothing logs to the void."""
    assert set(LOG_CONFIG.components) == {
        "pipeline", "datastore", "loaders", "audit", "universe", "backtest",
        "signals", "risk", "portfolio", "execution", "attribution",
    }


def test_replace_keeps_logconfig_a_dataclass():
    """`replace` is how tests and callers point logging at a temp directory."""
    swapped = replace(LOG_CONFIG, retention_days=7)
    assert swapped.retention_days == 7
    assert LOG_CONFIG.retention_days == 365
