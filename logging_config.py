"""
Central logging configuration for the Poor Man's Trading Machine.

Implements the design in LOGGING.md. Every module gets a logger through
`get_logger(__name__)`. One rotating, JSON-structured file per pipeline
component lives under `LOG_CONFIG.dir` (10 MB rotation); a separate,
time-based mechanism (`prune_old_logs`) enforces 12-month retention on the
rotated backups, decoupled from the size-triggered rotation itself — see
LOGGING.md section 3.2 for why those two are not the same knob.

Wired into `datastore/`, `loaders/`, `audit/`, `universe/`, `backtest/`,
`signals/` and `pipeline/` as of Phase 5.5; `risk/`, `portfolio/`,
`execution/` and `attribution/` have files reserved and build logging in as
they are written (Phases 6-9).

Usage:

    from logging_config import get_logger

    log = get_logger(__name__)
    log.info("loaded %d symbols", len(symbols))

At the top of a pipeline run (e.g. NightlyPipeline.run()):

    from logging_config import new_run_id, set_run_id

    set_run_id(new_run_id())
"""

from __future__ import annotations

import contextvars
import json
import logging
import logging.handlers
import time
from pathlib import Path

from config import LOG_CONFIG

# LOG_CONFIG (class LogConfig) lives in config.py, alongside LOADER_CONFIG /
# AUDIT_CONFIG / UNIVERSE_CONFIG — this module only owns the logging
# *mechanism* (handlers, formatting, get_logger, retention pruning), not the
# config values themselves. Expected shape, extended from the original
# level/file stub:
#
#   @dataclass
#   class LogConfig:
#       level: str = "INFO"
#       console_level: str = "WARNING"
#       dir: Path = LOGS_PATH
#       max_bytes: int = 10 * 1024 * 1024   # 10 MB
#       retention_days: int = 365           # 12 months
#       components: tuple[str, ...] = (
#           "pipeline", "datastore", "loaders", "audit", "universe",
#           "backtest", "signals", "risk", "portfolio", "execution",
#           "attribution",
#       )
#
#   LOG_CONFIG = LogConfig()


# ---------------------------------------------------------------------------
# run_id correlation
# ---------------------------------------------------------------------------

_run_id_var: "contextvars.ContextVar[str]" = contextvars.ContextVar("run_id", default="-")


def new_run_id() -> str:
    """UTC-timestamp run id, e.g. '20260731T041502Z'."""
    return time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())


def set_run_id(run_id: str) -> None:
    _run_id_var.set(run_id)


def get_run_id() -> str:
    return _run_id_var.get()


class _RunIdFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        record.run_id = get_run_id()
        return True


# ---------------------------------------------------------------------------
# JSON formatter (file handlers) — see LOGGING.md 3.3 for why file logs are
# structured while the console handler stays human-readable.
# ---------------------------------------------------------------------------


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": self.formatTime(record, "%Y-%m-%dT%H:%M:%S%z"),
            "level": record.levelname,
            "logger": record.name,
            "run_id": getattr(record, "run_id", "-"),
            "message": record.getMessage(),
            "module": record.module,
            "line": record.lineno,
        }
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


# ---------------------------------------------------------------------------
# Rotation: size-triggered, with timestamped backup names so retention
# pruning can read a backup's age straight from its filename.
# ---------------------------------------------------------------------------


def _timestamped_namer(default_name: str) -> str:
    # RotatingFileHandler's default_name looks like "logs/loaders.log.1".
    # Replace the bare numeric suffix with the rotation time.
    base, _, _ = default_name.rpartition(".")
    stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    return f"{base}.{stamp}.log"


# backupCount just has to be large enough that the size-triggered rotation
# never silently drops a backup before prune_old_logs() gets to judge it by
# age — retention is governed by retention_days, not by this count.
_BACKUP_COUNT = 100_000


def _make_handler(component: str, cfg=LOG_CONFIG) -> logging.handlers.RotatingFileHandler:
    cfg.dir.mkdir(parents=True, exist_ok=True)
    path = cfg.dir / f"{component}.log"
    handler = logging.handlers.RotatingFileHandler(
        path, maxBytes=cfg.max_bytes, backupCount=_BACKUP_COUNT, encoding="utf-8"
    )
    handler.namer = _timestamped_namer
    handler.setFormatter(JsonFormatter())
    handler.addFilter(_RunIdFilter())
    return handler


# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------

_configured = False


def configure_logging(cfg=LOG_CONFIG) -> None:
    """Idempotent. Wires one rotating file handler per component under
    `tm.<component>`, plus a shared human-readable console handler on the
    `tm` root logger."""
    global _configured
    if _configured:
        return

    root = logging.getLogger("tm")
    root.setLevel(cfg.level)
    root.propagate = False

    console = logging.StreamHandler()
    console.setLevel(cfg.console_level)
    console.setFormatter(logging.Formatter("%(asctime)s %(levelname)-8s %(name)s: %(message)s"))
    console.addFilter(_RunIdFilter())
    root.addHandler(console)

    for component in cfg.components:
        logger = logging.getLogger(f"tm.{component}")
        logger.setLevel(cfg.level)
        logger.addHandler(_make_handler(component, cfg))
        logger.propagate = True  # bubble up to tm's console handler too

    _configured = True


def get_logger(name: str) -> logging.Logger:
    """Standard entry point for every module. `name` is normally __name__,
    e.g. 'loaders.ohlcv' -> logger 'tm.loaders.ohlcv', which inherits the
    'loaders' component's rotating file handler plus the shared console
    handler. Component loggers not in LOG_CONFIG.components (a typo, or a
    module added ahead of its phase) still work — they just have no file
    handler of their own and only reach the console."""
    configure_logging()
    qualified = name if name.startswith("tm.") else f"tm.{name}"
    return logging.getLogger(qualified)


# ---------------------------------------------------------------------------
# Retention pruning — the time-based half of the rotation/retention split.
# Wired as the last step of NightlyPipeline.run() (Phase 5.5 retrofit); also
# runnable standalone as `python -m pipeline.prune_logs`.
# ---------------------------------------------------------------------------


BACKUP_GLOB = "*.log.*.log"
"""Rotated backups only. The live `<component>.log` cannot match this pattern,
which is what keeps pruning from deleting the file a handler is writing to."""


def expired_log_backups(cfg=LOG_CONFIG, now: float | None = None) -> list[Path]:
    """Rotated backups older than `cfg.retention_days`, oldest first.

    Split out from `prune_old_logs` so the standalone entry point
    (`python -m pipeline.prune_logs --dry-run`) can show what would be deleted
    without deleting it — and so the two can never disagree about which files
    are expired.
    """
    now = now if now is not None else time.time()
    cutoff = now - cfg.retention_days * 86400
    if not cfg.dir.exists():
        return []
    expired = [p for p in cfg.dir.glob(BACKUP_GLOB) if p.stat().st_mtime < cutoff]
    return sorted(expired, key=lambda p: p.stat().st_mtime)


def prune_old_logs(cfg=LOG_CONFIG, now: float | None = None) -> list[Path]:
    """Delete rotated log backups older than cfg.retention_days. Only matches
    the timestamped backup pattern (*.log.<stamp>.log), so the live,
    currently-written *.log file is never touched. Returns the deleted paths
    so callers can log the action and tests can assert on it."""
    deleted: list[Path] = []
    for path in expired_log_backups(cfg, now):
        path.unlink()
        deleted.append(path)
    return deleted
