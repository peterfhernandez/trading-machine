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

import importlib
import importlib.machinery
import json
import logging
import os
import subprocess
import sys
import time
import types
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from pathlib import Path

import polars as pl
import pytest

import config
import logging_config
from audit.auditor import AuditResult, DataAudit
from config import LOG_CONFIG
from datastore import AssetMaster, ParquetStore
from loaders.base import BaseLoader
from loaders.schemas import OHLCV_SCHEMA
from pipeline import prune_logs
from pipeline.nightly import NightlyPipeline

PROJECT_ROOT = Path(__file__).parent.parent
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
        # The asset master is passed explicitly: without it `BaseLoader` falls
        # back to the production `data/parquet/asset_master.parquet`, and on a
        # machine that has run the pipeline these symbols resolve.
        loader = DummyLoader(
            "binance",
            store=ParquetStore(datastore_path),
            asset_master=AssetMaster(datastore_path / "asset_master.parquet"),
        )

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

    def test_every_configured_component_gets_a_file_handler_on_first_use(self):
        for component in LOG_CONFIG.components:
            logging_config.get_logger(f"{component}.probe")
            handlers = logging.getLogger(f"tm.{component}").handlers
            assert handlers, f"{component} has no handler"
            assert any(isinstance(h, logging.FileHandler) for h in handlers), component

    def test_a_component_that_never_logs_gets_no_file(self):
        """Handlers are lazy: an empty risk.log would claim Phase 6 exists."""
        unused = "attribution"
        logging.Logger.manager.loggerDict.pop(f"tm.{unused}", None)
        logging_config.configure_logging(force=True)

        assert not [
            h
            for h in logging.getLogger(f"tm.{unused}").handlers
            if isinstance(h, logging.FileHandler)
        ]

    def test_component_files_are_named_per_component(self):
        for component in ("datastore", "loaders", "audit", "signals", "backtest"):
            logging_config.get_logger(f"{component}.probe")
            handler = next(
                h for h in logging.getLogger(f"tm.{component}").handlers
                if isinstance(h, logging.FileHandler)
            )
            assert Path(handler.baseFilename).name == f"{component}.log"

    def test_rotation_size_comes_from_config(self):
        logging_config.get_logger("loaders.probe")
        handler = next(
            h for h in logging.getLogger("tm.loaders").handlers
            if isinstance(h, logging.FileHandler)
        )
        assert handler.maxBytes == LOG_CONFIG.max_bytes

    def test_configure_logging_is_idempotent(self):
        logging_config.get_logger("loaders.probe")
        before = len(logging.getLogger("tm.loaders").handlers)
        logging_config.configure_logging()
        logging_config.get_logger("loaders.probe")
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


# ===========================================================================
# Running as `python -m ...` (the documented way to run the pipeline)
# ===========================================================================


class TestMainModuleLoggerBinding:
    """`__name__` is `"__main__"` under `-m`, which used to mean no log file.

    The whole `logs/pipeline.log` story — the run_id banner, per-stage
    entry/exit/duration, CRITICAL on an unhandled stage failure — was written
    to `tm.__main__`, a logger under no component: no file handler, and INFO is
    below the console threshold. The tests could not see it because importing
    the module gives it the right name; only actually running it reproduces it.
    """

    def test_main_resolves_to_the_module_it_was_invoked_as(self, monkeypatch):
        spec = importlib.machinery.ModuleSpec("pipeline.nightly", loader=None)
        fake_main = types.SimpleNamespace(__spec__=spec)
        monkeypatch.setitem(sys.modules, "__main__", fake_main)

        assert logging_config.resolve_logger_name("__main__") == "tm.pipeline.nightly"

    def test_a_plain_script_without_a_spec_still_works(self, monkeypatch):
        monkeypatch.setitem(sys.modules, "__main__", types.SimpleNamespace(__spec__=None))
        assert logging_config.resolve_logger_name("__main__") == "tm.__main__"

    def test_ordinary_module_names_are_untouched(self):
        assert logging_config.resolve_logger_name("loaders.ohlcv") == "tm.loaders.ohlcv"
        assert logging_config.resolve_logger_name("tm.audit") == "tm.audit"

    @pytest.mark.integration
    def test_running_the_pipeline_as_a_module_writes_pipeline_log(self, tmp_path):
        """The end-to-end version: run the CLI, then look in the log directory."""
        env = {
            **os.environ,
            "PAPER": "true",
            "TM_LOG_DIR": str(tmp_path),
            "PYTHONPATH": str(PROJECT_ROOT),
        }
        result = subprocess.run(
            [sys.executable, "-m", "pipeline.nightly", "--dry-run", "--days", "1"],
            cwd=PROJECT_ROOT, env=env, capture_output=True, text=True, timeout=180,
        )

        assert result.returncode == 0, result.stderr
        written = (tmp_path / "pipeline.log").read_text().splitlines()
        assert written, "running as `-m` produced no pipeline.log records"

        records = [json.loads(line) for line in written]
        assert all(r["logger"].startswith("tm.pipeline") for r in records)
        assert any("run_id=" in r["message"] for r in records)
        assert {r["run_id"] for r in records} != {"-"}


class TestLevelOverride:
    """DEBUG has to be reachable without editing config.py."""

    def test_log_level_comes_from_the_environment(self, monkeypatch):
        monkeypatch.setenv("TM_LOG_LEVEL", "debug")
        importlib.reload(config)
        try:
            assert config.LOG_CONFIG.level == "DEBUG"
        finally:
            monkeypatch.delenv("TM_LOG_LEVEL")
            importlib.reload(config)

    def test_default_is_still_info(self):
        assert logging.getLevelName(logging.getLogger("tm").level) in ("INFO", "DEBUG")

    def test_set_level_reaches_component_loggers_and_their_handlers(self):
        original = LOG_CONFIG.level
        logging_config.get_logger("signals.probe")
        try:
            logging_config.set_level("DEBUG")

            component = logging.getLogger("tm.signals")
            assert component.level == logging.DEBUG
            assert logging_config.get_logger("signals.probe").isEnabledFor(logging.DEBUG)
            assert all(h.level <= logging.DEBUG for h in component.handlers)
        finally:
            logging_config.set_level(original)

    def test_debug_records_are_dropped_at_the_default_level(self):
        """The other half: INFO must still mean INFO, so DEBUG stays off by
        default and a long backtest does not flood the log."""
        original = LOG_CONFIG.level
        try:
            logging_config.set_level("INFO")
            probe = logging_config.get_logger("signals.probe")
            assert not probe.isEnabledFor(logging.DEBUG)
            assert probe.isEnabledFor(logging.INFO)
        finally:
            logging_config.set_level(original)


class TestRotation:
    """Rotation must not eat a backup, and must not cost 100k syscalls."""

    def _handler(self, tmp_path, max_bytes=200):
        cfg = FakeLogConfig(dir=tmp_path, max_bytes=max_bytes)
        return logging_config._make_handler("loaders", cfg)

    def test_two_rotations_in_the_same_second_keep_both_backups(self, tmp_path, monkeypatch):
        """One-second stamps collide; the loser used to be os.remove'd."""
        monkeypatch.setattr(
            logging_config.time, "strftime", lambda *a, **k: "20260801T000000Z"
        )
        handler = self._handler(tmp_path, max_bytes=400)
        logger = logging.getLogger("tm.test.rotation")
        logger.handlers = [handler]
        logger.propagate = False
        logger.setLevel(logging.INFO)

        for _ in range(3):
            logger.info("x" * 300)
        handler.close()

        backups = sorted(tmp_path.glob(logging_config.BACKUP_GLOB))
        assert len({p.name for p in backups}) == len(backups), "same-second names collided"

        # The property that matters: rotating three times loses no record.
        # The stdlib rollover would have removed the earlier backup and renamed
        # onto its name, so two of the three would be gone.
        survived = sum(
            len([line for line in path.read_text().splitlines() if line.strip()])
            for path in [*backups, tmp_path / "loaders.log"]
        )
        assert survived == 3, [p.name for p in backups]

    def test_every_backup_still_matches_the_prune_glob(self, tmp_path, monkeypatch):
        """Uniquifying the name must not put it outside retention's reach."""
        monkeypatch.setattr(
            logging_config.time, "strftime", lambda *a, **k: "20260801T000000Z"
        )
        name = logging_config._unique_backup_name(str(tmp_path / "loaders.log.1"))
        Path(name).write_text("{}\n")
        collision = logging_config._unique_backup_name(str(tmp_path / "loaders.log.1"))

        assert collision != name
        assert Path(collision).match(logging_config.BACKUP_GLOB)

    def test_rollover_makes_exactly_one_backup_and_walks_no_window(self, tmp_path):
        """backupCount is not consulted: how many to keep is retention_days' job.

        The stdlib rollover shuffles `<file>.1 -> <file>.2 -> ...` across the
        whole `backupCount` range, one namer call and one stat per step — 100k
        of them at the count this module used to carry, ~320 ms per rotation.
        """
        handler = self._handler(tmp_path)
        assert isinstance(handler, logging_config.TimestampedRotatingFileHandler)
        assert handler.backupCount == 0

        handler.stream.write("x" * 300)
        handler.doRollover()
        handler.close()

        assert len(list(tmp_path.glob(logging_config.BACKUP_GLOB))) == 1
        assert not list(tmp_path.glob("*.log.1"))
        assert (tmp_path / "loaders.log").exists()


class TestCaplogCanary:
    """`tm` does not propagate, so pytest must attach to it directly.

    pytest gained that behaviour for non-propagating loggers in 8.x; on an
    older pytest every `caplog` assertion in this file either fails or — worse,
    for the `assert not [criticals]` ones — passes without capturing anything.
    `pyproject.toml` pins the floor; this is the canary that fires if the
    mechanism ever stops working.
    """

    def test_caplog_actually_captures_tm_records(self, caplog):
        log = logging_config.get_logger("audit.canary")
        with caplog.at_level(logging.CRITICAL, logger="tm.audit"):
            log.critical("canary")

        assert any(r.getMessage() == "canary" for r in caplog.records), (
            "caplog captured nothing from a non-propagating tm.* logger — every "
            "halt-logging assertion in this file is now vacuous"
        )
    assert LOG_CONFIG.retention_days == 365
