"""Shared logging helper for the scratch demos (Phase 5.5).

Every scratch demo ends by showing that the work it just did left a durable
record: which component file it went to, and what one line looks like. That is
the point of Phase 5.5 — an unattended run can be reconstructed afterwards —
and a demo that only prints to stdout does not demonstrate it.

Not a `scratch_*.py` demo itself: it is imported by them. One call at the top
of `main()` does both halves — a `run_id` for the demo's records, and a tail of
the component's log printed when the script exits:

    from log_demo import start_demo_run

    def main():
        start_demo_run("loaders")
        ...

The tail is printed from an `atexit` hook rather than at the end of `main()`
so a demo that exits early — `sys.exit(1)` when `PAPER` is False, an
unreachable venue — still shows what it managed to write. That is the case
worth seeing.
"""

import atexit
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from config import LOG_CONFIG  # noqa: E402
from logging_config import get_run_id, new_run_id, set_run_id  # noqa: E402


def start_demo_run(component: str | None = None, lines: int = 3) -> str:
    """Give this demo a `run_id`, and print `component`'s log tail on exit.

    Without a `run_id` every record carries `"-"`, which is fine for a demo but
    hides the correlation the field exists for.

    Args:
        component: A name from `LOG_CONFIG.components`; None skips the tail.
        lines: How many trailing records to show at exit.

    Returns:
        The `run_id` that was set.
    """
    run_id = new_run_id()
    set_run_id(run_id)
    if component is not None:
        atexit.register(show_log_tail, component, lines)
    return run_id


def show_log_tail(component: str, lines: int = 3) -> None:
    """Print the last few records from `logs/<component>.log`.

    Args:
        component: A name from `LOG_CONFIG.components` (e.g. "loaders").
        lines: How many trailing records to show.
    """
    path = LOG_CONFIG.dir / f"{component}.log"
    print(f"\n{'-' * 78}")
    print(f"Logging (Phase 5.5): {path}")
    print(f"{'-' * 78}")

    if not path.exists():
        print(f"  Nothing written to {path.name} yet.")
        return

    records = path.read_text().splitlines()
    if not records:
        print(f"  {path.name} is empty.")
        return

    print(f"  {len(records)} record(s); last {min(lines, len(records))}:\n")
    for record in records[-lines:]:
        print(f"    {record}")
    print(
        f"\n  One JSON object per line, each tagged run_id={get_run_id()}. Grepping "
        f"that\n  value across every file in {LOG_CONFIG.dir.name}/ reconstructs "
        "one run end to end."
    )
