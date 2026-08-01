#!/usr/bin/env python3
"""Scratch script: the observability fixes, demonstrated end to end.

Five things that were each broken or unreachable before, shown working:

1. **`python -m pipeline.nightly` writes to `logs/pipeline.log`.** Run as a
   module, the pipeline's `__name__` is `"__main__"`, so its logger used to be
   `tm.__main__` — under no component, with no file handler, and below the
   console threshold at INFO. Every stage timing and the `run_id` banner went
   nowhere on the one code path the README documents.
2. **DEBUG is reachable.** `--log-level DEBUG` / `TM_LOG_LEVEL`, instead of
   editing `config.py` to see a signal's per-asset reject reasons.
3. **Rotation keeps every backup.** Two rotations inside one second used to
   resolve to the same timestamped filename, and the older backup was deleted.
4. **Retention still finds them.** The uniquified names must stay inside the
   pruner's glob, or retention silently never runs.
5. **A signal's status is data.** `signal_statuses()` answers "which signals
   have backtest evidence?" without anyone re-reading six documents.

No network, no store writes: a temporary log directory and synthetic records.
"""

import json
import logging
import os
import subprocess
import sys
import tempfile
from pathlib import Path

# Imported first: log_demo puts the repository root on sys.path, so the
# project imports below resolve when this script is run directly.
# isort: off
from log_demo import start_demo_run

# isort: on

import logging_config
from config import LOG_CONFIG, PAPER, PROJECT_ROOT, LogConfig


def section(title: str) -> None:
    print(f"\n{'=' * 78}\n{title}\n{'=' * 78}")


def child_env(log_dir: Path) -> dict[str, str]:
    """Environment for the child pipeline: the parent's, plus three overrides.

    Inherited rather than hand-built. A minimal dict looks tidy and is a trap:
    the child imports `ccxt`, and therefore `ssl` and `socket`, whose extension
    modules do not load on Windows when `SYSTEMROOT` is absent. Passing only
    PATH killed the child at import — before any logging was configured, so
    there was no `pipeline.log` to read and no clue in the demo's output as to
    why. Anything the interpreter needs to start is the parent's business, not
    a list this file has to keep correct on three platforms.

    `PYTHONIOENCODING` is the second half: the pipeline's report prints `✓` and
    `🛑`, and a captured stdout takes the locale encoding (cp1252 on a default
    Windows install), where those characters cannot be encoded at all.
    """
    return {
        **os.environ,
        "PAPER": "true",
        "TM_LOG_DIR": str(log_dir),
        "PYTHONPATH": str(PROJECT_ROOT),
        "PYTHONIOENCODING": "utf-8",
    }


def demo_module_invocation(tmp: Path) -> bool:
    section("1. `python -m pipeline.nightly` leaves a record")

    log_dir = tmp / "module-run"
    result = subprocess.run(
        [sys.executable, "-m", "pipeline.nightly", "--dry-run", "--days", "1"],
        cwd=PROJECT_ROOT,
        env=child_env(log_dir),
        capture_output=True,
        encoding="utf-8",
        errors="replace",
        timeout=300,
    )
    print(f"exit code: {result.returncode}")

    written = log_dir / "pipeline.log"
    if result.returncode != 0 or not written.exists():
        # The demo's whole claim is that this file gets written, so a missing
        # file is the finding — report it with the child's own diagnosis
        # attached, rather than raising FileNotFoundError from the parent and
        # burying the reason in a discarded stderr pipe.
        print(f"  FAILED: no {written.name} at {log_dir}")
        print(f"  the child's stderr ({len(result.stderr.splitlines())} line(s)):")
        for line in result.stderr.splitlines()[-15:]:
            print(f"    {line}")
        return False

    records = [json.loads(line) for line in written.read_text().splitlines() if line.strip()]
    print(f"{written.name}: {len(records)} record(s)")
    print(f"  logger:  {records[0]['logger']}   (was tm.__main__, which had no file)")
    print(f"  run_id:  {records[0]['run_id']}")
    for record in records[:4]:
        print(f"    {record['level']:8s} {record['message'][:60]}")
    return True


def demo_level_override(tmp: Path) -> None:
    section("2. DEBUG without editing config.py")

    cfg = LogConfig(dir=tmp / "levels", level="INFO")
    logging_config.configure_logging(cfg, force=True)
    log = logging_config.get_logger("signals.demo")

    log.debug("a signal's reject reason, at the INFO default")
    print(f"INFO  -> DEBUG enabled? {log.isEnabledFor(10)}")

    logging_config.set_level("DEBUG", cfg=cfg)
    log = logging_config.get_logger("signals.demo")
    log.debug("a signal's reject reason, at DEBUG")
    print(f"DEBUG -> DEBUG enabled? {log.isEnabledFor(10)}")

    written = (cfg.dir / "signals.log").read_text().splitlines()
    print(f"\n{len(written)} record(s) in signals.log — the first call was dropped:")
    for line in written:
        record = json.loads(line)
        print(f"  {record['level']:8s} {record['message']}")


def demo_rotation_collision(tmp: Path) -> None:
    section("3 & 4. Two rotations in one second, and retention still sees them")

    cfg = LogConfig(dir=tmp / "rotation", max_bytes=400, retention_days=365)
    handler = logging_config._make_handler("loaders", cfg)
    logger = logging_config.logging.getLogger("tm.scratch.rotation")
    logger.handlers = [handler]
    logger.propagate = False
    logger.setLevel(20)

    for i in range(4):
        logger.info("record %d %s", i, "x" * 300)
    handler.close()

    backups = sorted(cfg.dir.glob(logging_config.BACKUP_GLOB))
    print(f"{len(backups)} backup(s), all distinct: {len({p.name for p in backups}) == len(backups)}")
    for path in backups:
        print(f"  {path.name:44s} {path.stat().st_size:>5} bytes")

    surviving = sum(
        len([ln for ln in p.read_text().splitlines() if ln.strip()])
        for p in [*backups, cfg.dir / "loaders.log"]
    )
    print(f"\n4 records written, {surviving} survived rotation "
          f"(the stdlib rollover would have deleted the same-second collisions)")

    expired = logging_config.expired_log_backups(cfg)
    print(f"expired under a 365-day window: {len(expired)}")
    stale = logging_config.expired_log_backups(
        LogConfig(dir=cfg.dir, retention_days=0)
    )
    print(f"expired under a 0-day window:   {len(stale)}  <- the pruner's glob matches")


def release_temp_log_handlers() -> None:
    """Close every `tm` file handler and put the real `LOG_CONFIG` back.

    Sections 2 and 3 point log files at a temporary directory, and a
    `RotatingFileHandler` holds its file open. Windows refuses to delete a file
    another handle has open, so the leak turned `TemporaryDirectory`'s cleanup
    into `PermissionError: [WinError 32]` *after* every section had printed —
    the demo looked like it worked and the interpreter looked like it failed.
    POSIX unlinks an open file without complaint, so no local run and no
    GitHub-hosted CI job could see it; the self-hosted deploy gate did.

    The handlers are not only the ones this file creates. `configure_logging`
    in section 2 replaces the module-level *active* config, so every
    `get_logger` after it opens its component file under the temp directory
    too — which is how `datastore.log`, from nothing more than
    `import signals`, ended up being the handle that blocked the cleanup.
    """
    names = ["tm", *(n for n in logging.root.manager.loggerDict if n.startswith("tm."))]
    for name in names:
        logger = logging.getLogger(name)
        for handler in list(logger.handlers):
            if isinstance(handler, logging.FileHandler):
                logger.removeHandler(handler)
                handler.close()

    logging_config.configure_logging(LOG_CONFIG, force=True)


def demo_signal_status() -> None:
    section("5. Which signals actually have evidence?")

    import signals

    print(f"{'signal':28s} {'family':11s} {'status':12s} evidenced")
    for signal_id, signal in sorted(signals.all_signals().items()):
        print(
            f"{signal_id:28s} {signal.family:11s} {signal.status:12s} "
            f"{signal.is_evidenced}"
        )

    statuses = set(signals.signal_statuses().values())
    print(f"\ndistinct statuses: {statuses}")
    print("Every signal is still `draft` — implemented and tested, not evidenced.")
    print("Filling in section 5 of a doc from a real backfill is what changes this.")


def main() -> int:
    start_demo_run("pipeline")

    if not PAPER:
        print("PAPER is False; scratch scripts do not run against live config.")
        return 1

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        try:
            ok = demo_module_invocation(tmp)
            demo_level_override(tmp)
            demo_rotation_collision(tmp)
        finally:
            # Before the directory is removed, and before anything else can
            # open a component file inside it.
            release_temp_log_handlers()

    # Outside the temp directory on purpose: this section imports `signals`,
    # which logs, and those records belong in the real component files the way
    # every other demo's do.
    demo_signal_status()

    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
