"""Standalone log retention pruning: `python -m pipeline.prune_logs`.

Rotation and retention are two different mechanisms (LOGGING.md section 3.2).
`RotatingFileHandler` rotates on **size** at `LOG_CONFIG.max_bytes`; this module
enforces the **calendar** half, deleting rotated backups older than
`LOG_CONFIG.retention_days`. The two cannot be composed through `backupCount`
alone, because how many 10 MB rotations equal a year depends on log volume,
which varies by component and by how much is happening.

`NightlyPipeline.run()` already calls `prune_old_logs()` as its final step, so
a machine running the nightly job needs nothing else. This entry point is for
everyone else: a research box that runs backtests and sweeps writes to
`logs/backtest.log` and `logs/signals.log` without ever running the pipeline,
and would otherwise never prune.

    python -m pipeline.prune_logs             # delete expired backups
    python -m pipeline.prune_logs --dry-run   # list what would go
"""

import argparse
import sys

from config import LOG_CONFIG
from logging_config import expired_log_backups, get_logger, prune_old_logs

logger = get_logger(__name__)


def main(argv: list[str] | None = None) -> int:
    """Prune expired log backups. Returns a process exit code."""
    parser = argparse.ArgumentParser(
        description=(
            "Delete rotated log backups older than LOG_CONFIG.retention_days. "
            "The live *.log file each component is currently writing is never "
            "touched."
        )
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List the backups that would be deleted, and delete nothing",
    )
    args = parser.parse_args(argv)

    if args.dry_run:
        expired = expired_log_backups()
        print(
            f"{len(expired)} backup(s) in {LOG_CONFIG.dir} older than "
            f"{LOG_CONFIG.retention_days} days:"
        )
        for path in expired:
            print(f"  {path.name}")
        if not expired:
            print("  (none)")
        return 0

    deleted = prune_old_logs()
    logger.info(
        "Pruned %d log backup(s) older than %d days from %s",
        len(deleted), LOG_CONFIG.retention_days, LOG_CONFIG.dir,
    )
    print(
        f"Deleted {len(deleted)} backup(s) older than "
        f"{LOG_CONFIG.retention_days} days from {LOG_CONFIG.dir}"
    )
    for path in deleted:
        print(f"  {path.name}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
