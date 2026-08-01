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
import sys
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


def _unique_backup_name(default_name: str, namer=_timestamped_namer) -> str:
    """A timestamped backup name that is not already taken.

    The stamp has one-second resolution, so two rotations inside the same
    second produce the same name. `RotatingFileHandler` would `os.remove` the
    collision and rename onto it — silently destroying a full backup, which is
    the one thing a retention mechanism must never do. A `-1`, `-2`, ... suffix
    is appended instead; the result still ends in `.log`, so it still matches
    `BACKUP_GLOB` and the pruner still judges it by mtime.
    """
    candidate = namer(default_name)
    if not Path(candidate).exists():
        return candidate

    stem = candidate[: -len(".log")] if candidate.endswith(".log") else candidate
    for n in range(1, 1000):
        alternative = f"{stem}-{n}.log"
        if not Path(alternative).exists():
            return alternative
    return f"{stem}-{time.time_ns()}.log"


class TimestampedRotatingFileHandler(logging.handlers.RotatingFileHandler):
    """Size-triggered rotation straight to a timestamped backup.

    `RotatingFileHandler.doRollover` exists to maintain a numbered window of
    backups: it shuffles `<file>.1` -> `<file>.2` -> ... and deletes what falls
    off the end. Retention here is calendar-based (`prune_old_logs`), so that
    shuffle has nothing to do — but it is not free, and it is not harmless:

    * it walks `range(backupCount - 1, 0, -1)`, calling the namer and `stat`
      once per step. At the `backupCount` this module used to carry (100,000)
      one 10 MB rotation cost ~320 ms of pure syscalls, measured.
    * the last step renames onto `namer(<file>.1)`, removing whatever is
      already there. With a one-second stamp, two rotations in the same second
      resolve to the same name and the earlier backup is destroyed.

    So the rollover is done directly: close, rename to a unique timestamped
    name, reopen. `backupCount` is deliberately not consulted — how many
    backups to keep is `retention_days`' business (LOGGING.md section 3.2).
    """

    def doRollover(self) -> None:
        if self.stream:
            self.stream.close()
            self.stream = None

        destination = _unique_backup_name(
            f"{self.baseFilename}.1", namer=self.rotation_filename
        )
        if Path(self.baseFilename).exists():
            self.rotate(self.baseFilename, destination)

        if not self.delay:
            self.stream = self._open()


def _make_handler(component: str, cfg=LOG_CONFIG) -> logging.Handler:
    cfg.dir.mkdir(parents=True, exist_ok=True)
    path = cfg.dir / f"{component}.log"
    handler = TimestampedRotatingFileHandler(
        path, maxBytes=cfg.max_bytes, backupCount=0, encoding="utf-8"
    )
    handler.namer = _timestamped_namer
    handler.setFormatter(JsonFormatter())
    handler.addFilter(_RunIdFilter())
    handler.setLevel(cfg.level)
    return handler


# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------

_configured = False
_active_cfg = LOG_CONFIG


def configure_logging(cfg=LOG_CONFIG, force: bool = False) -> None:
    """Idempotent. Puts the shared human-readable console handler on the `tm`
    root logger; each component's rotating file handler is created on first use
    by `get_logger` (see `_ensure_component_handler`).

    Args:
        cfg: Logging configuration to apply.
        force: Re-apply even if logging was already configured — used by
            `set_level` and by tests that swap `cfg`.
    """
    global _configured, _active_cfg
    if _configured and not force:
        return

    _active_cfg = cfg
    root = logging.getLogger("tm")
    root.setLevel(cfg.level)
    root.propagate = False

    for handler in list(root.handlers):
        root.removeHandler(handler)

    console = logging.StreamHandler()
    console.setLevel(cfg.console_level)
    console.setFormatter(logging.Formatter("%(asctime)s %(levelname)-8s %(name)s: %(message)s"))
    console.addFilter(_RunIdFilter())
    root.addHandler(console)

    _configured = True


def _ensure_component_handler(component: str, cfg=None) -> None:
    """Give `tm.<component>` its rotating file handler, once, on first use.

    Created lazily rather than for every name in `cfg.components` up front:
    eager creation left `risk.log`, `portfolio.log`, `execution.log` and
    `attribution.log` sitting in `logs/` at 0 bytes from Phase 5.5 onwards,
    which reads as "wired up and silent" when the truth is "not written yet"
    (those modules arrive in Phases 6-9). A file in `logs/` now means that
    component has actually logged something.
    """
    cfg = cfg or _active_cfg
    if component not in cfg.components:
        return

    logger = logging.getLogger(f"tm.{component}")
    if any(isinstance(h, logging.FileHandler) for h in logger.handlers):
        return

    logger.setLevel(cfg.level)
    logger.addHandler(_make_handler(component, cfg))
    logger.propagate = True  # bubble up to tm's console handler too


def resolve_logger_name(name: str) -> str:
    """Map a module's `__name__` onto its `tm.<component>...` logger name.

    The subtlety this exists for: a module run as `python -m pipeline.nightly`
    has `__name__ == "__main__"`, so `get_logger(__name__)` asked for
    `tm.__main__` — a logger under no component, with no file handler, and
    below the console threshold at INFO. The pipeline's own records (the
    `run_id` banner, every stage entry/exit/duration, the CRITICAL on an
    unhandled stage failure) went nowhere at all when the pipeline was run the
    documented way. Tests never saw it because they import the class instead.

    `__main__.__spec__.name` is the dotted name `-m` was invoked with, which is
    exactly the name the module would have had if it had been imported.
    """
    if name == "__main__":
        spec = getattr(sys.modules.get("__main__"), "__spec__", None)
        resolved = getattr(spec, "name", None)
        if resolved and resolved != "__main__":
            name = resolved
    return name if name.startswith("tm.") else f"tm.{name}"


def get_logger(name: str) -> logging.Logger:
    """Standard entry point for every module. `name` is normally __name__,
    e.g. 'loaders.ohlcv' -> logger 'tm.loaders.ohlcv', which inherits the
    'loaders' component's rotating file handler plus the shared console
    handler. Component loggers not in LOG_CONFIG.components (a typo, or a
    module added ahead of its phase) still work — they just have no file
    handler of their own and only reach the console."""
    configure_logging()
    qualified = resolve_logger_name(name)
    _ensure_component_handler(qualified.split(".")[1] if "." in qualified else "")
    return logging.getLogger(qualified)


def set_level(level: str, console_level: str | None = None, cfg=None) -> None:
    """Change the log level at runtime — what `--log-level` is wired to.

    Without this the DEBUG records the Phase 5.5 retrofit added (every signal's
    per-asset reject reason, the backtester's per-rebalance detail) could only
    be reached by editing `config.py`, which meant they were never reachable in
    practice on a machine running the nightly job.
    """
    cfg = cfg or _active_cfg
    cfg = replace_log_level(cfg, level, console_level)
    configure_logging(cfg, force=True)

    for component in cfg.components:
        logger = logging.getLogger(f"tm.{component}")
        logger.setLevel(cfg.level)
        for handler in logger.handlers:
            handler.setLevel(cfg.level)


def replace_log_level(cfg, level: str, console_level: str | None = None):
    """A copy of `cfg` with the levels replaced (dataclasses.replace, guarded
    so a non-dataclass stand-in in a test still works)."""
    import dataclasses

    updates = {"level": level.upper()}
    if console_level is not None:
        updates["console_level"] = console_level.upper()

    if dataclasses.is_dataclass(cfg):
        return dataclasses.replace(cfg, **updates)
    for key, value in updates.items():
        setattr(cfg, key, value)
    return cfg


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
