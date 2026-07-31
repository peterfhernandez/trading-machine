"""Explicit fetch windows, and resuming from what a previous run covered.

A loader used to be told "how many days back from now" — a count, re-derived
against the clock on every run, which meant every run re-fetched the same
window and no run could ever be asked for a *specific* stretch of history.
This module replaces the count with a `[start, end]` interval, and works out
which part of a requested interval a previous run has already covered.

Two deliberate properties:

- **The overlap is re-fetched on purpose.** Resuming trims the window to what
  is new, minus `refetch_overlap_days`. The trailing bar of a run is usually
  incomplete (a daily candle fetched at 00:10 UTC covers ten minutes of the
  current day) and vendors revise recent bars, so the last day covered has to
  be pulled again. Append-only storage keeps both copies and readers collapse
  to the latest ingestion (`datastore.latest_per_bar`), so the overlap costs
  API calls and disk, not correctness.
- **Coverage is an interval, not a high-water mark.** A single "last date"
  cannot answer "do we already have March 2024?", so a request for older
  history than the checkpoint's start is honoured in full rather than skipped.
"""

import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Optional

logger = logging.getLogger(__name__)


def utc_now() -> datetime:
    """Naive UTC now — the datastore stores naive UTC timestamps throughout."""
    return datetime.now(UTC).replace(tzinfo=None)


@dataclass(frozen=True)
class FetchWindow:
    """A closed `[start, end]` interval of event time, in naive UTC."""

    start: datetime
    end: datetime

    def __post_init__(self) -> None:
        if self.start > self.end:
            raise ValueError(f"FetchWindow start {self.start} is after end {self.end}")

    @classmethod
    def from_lookback(cls, days: int, end: datetime | None = None) -> "FetchWindow":
        """The last `days` days, ending now unless `end` is given."""
        end = end or utc_now()
        return cls(start=end - timedelta(days=days), end=end)

    @property
    def start_ms(self) -> int:
        return int(self.start.replace(tzinfo=UTC).timestamp() * 1000)

    @property
    def end_ms(self) -> int:
        return int(self.end.replace(tzinfo=UTC).timestamp() * 1000)

    @property
    def days(self) -> int:
        return max(0, (self.end - self.start).days)

    def contains(self, moment: datetime) -> bool:
        return self.start <= moment <= self.end

    def __str__(self) -> str:
        return f"{self.start.date()}..{self.end.date()}"


@dataclass(frozen=True)
class Coverage:
    """What a dataset's previous runs have already fetched, as one interval."""

    start: datetime | None  # None = unknown (legacy checkpoint format)
    end: datetime

    def to_json(self) -> dict[str, str | None]:
        return {
            "covered_start": self.start.isoformat() if self.start else None,
            "covered_end": self.end.isoformat(),
        }

    @classmethod
    def from_json(cls, value: Any) -> Optional["Coverage"]:
        """Parse a checkpoint entry, tolerating the older plain-string format.

        Older checkpoints stored just the end date as an ISO string, which says
        nothing about where coverage began; that is represented as `start=None`
        and only ever trims the *end* of a requested window.
        """
        if value is None:
            return None

        try:
            if isinstance(value, str):
                return cls(start=None, end=datetime.fromisoformat(value))
            if isinstance(value, dict):
                end = value.get("covered_end")
                if not end:
                    return None
                start = value.get("covered_start")
                return cls(
                    start=datetime.fromisoformat(start) if start else None,
                    end=datetime.fromisoformat(end),
                )
        except (TypeError, ValueError) as e:
            logger.warning(f"Unreadable checkpoint entry {value!r}: {e}; ignoring it")

        return None

    def merge(self, window: FetchWindow, touch_tolerance_days: int = 1) -> "Coverage":
        """Extend coverage with a window that was just fetched.

        Only merges when the two touch or overlap. A disjoint older fetch would
        otherwise make the gap between them look covered, so the interval with
        the later end wins and the fragmentation is logged rather than hidden.
        """
        tolerance = timedelta(days=touch_tolerance_days)
        start = self.start
        contiguous = (
            window.start <= self.end + tolerance
            and (start is None or window.end >= start - tolerance)
        )

        if not contiguous:
            logger.warning(
                f"Fetched window {window} is disjoint from covered range "
                f"{start.date() if start else '?'}..{self.end.date()}; "
                f"coverage tracking keeps only the later interval"
            )
            if window.end >= self.end:
                return Coverage(start=window.start, end=window.end)
            return self

        merged_start = window.start if start is None else min(start, window.start)
        return Coverage(start=merged_start, end=max(self.end, window.end))


def resume_window(
    requested: FetchWindow,
    coverage: Coverage | None,
    overlap_days: int = 1,
) -> FetchWindow | None:
    """Trim `requested` to the part a previous run has not already fetched.

    Returns None when there is nothing left to fetch.

    The rules, in order:
    1. No coverage recorded -> fetch the whole request.
    2. The request reaches back before coverage starts -> fetch it in full; a
       high-water mark cannot prove that older history is present.
    3. The request ends within what is covered (allowing for the re-fetch
       overlap) -> nothing to do.
    4. Otherwise -> fetch from `covered_end - overlap` forward, so the
       incomplete trailing bar and any revisions are picked up again.
    """
    if coverage is None:
        return requested

    if coverage.start is not None and requested.start < coverage.start:
        logger.info(
            f"Requested window {requested} starts before covered range "
            f"{coverage.start.date()}..{coverage.end.date()}; fetching it in full"
        )
        return requested

    overlap = timedelta(days=max(0, overlap_days))
    resume_from = coverage.end - overlap

    if requested.end <= resume_from:
        return None

    if resume_from <= requested.start:
        return requested

    return FetchWindow(start=resume_from, end=requested.end)
