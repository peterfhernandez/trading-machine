"""Rebalance calendar construction.

A rebalance calendar is a subset of the available bar dates. Rebalancing can
only happen on a date for which a bar exists, because the engine fills at the
*next* bar's open — a rebalance date with no following bar cannot be executed
and is dropped by the engine.
"""

from collections.abc import Sequence
from datetime import datetime

SUPPORTED_FREQS = ("daily", "weekly", "monthly")


def build_rebalance_calendar(
    dates: Sequence[datetime],
    freq: str = "daily",
) -> list[datetime]:
    """Select rebalance dates from the available bar dates.

    Args:
        dates: Sorted, unique bar timestamps.
        freq: "daily" (every bar), "weekly" (first bar of each ISO week), or
            "monthly" (first bar of each calendar month).

    Returns:
        Sorted list of rebalance timestamps, a subset of `dates`.

    Raises:
        ValueError: If `freq` is not supported.
    """
    if freq not in SUPPORTED_FREQS:
        raise ValueError(f"Unsupported rebalance_freq {freq!r}; expected one of {SUPPORTED_FREQS}")

    ordered = sorted(dates)
    if freq == "daily":
        return ordered

    if freq == "weekly":
        def key(d: datetime) -> tuple:
            iso = d.isocalendar()
            return (iso[0], iso[1])
    else:
        def key(d: datetime) -> tuple:
            return (d.year, d.month)

    calendar: list[datetime] = []
    seen: set[tuple] = set()
    for d in ordered:
        k = key(d)
        if k not in seen:
            seen.add(k)
            calendar.append(d)
    return calendar
