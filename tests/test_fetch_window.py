"""Tests for explicit fetch windows and checkpoint-based resumption.

The behaviour being pinned: a run asks for an interval, the checkpoint says what
is already covered, and only the missing part is fetched -- minus a deliberate
re-fetch overlap, because the trailing bar of a run is usually incomplete and
venues revise recent bars.
"""

from datetime import UTC, datetime, timedelta

import pytest

from loaders.window import Coverage, FetchWindow, resume_window, utc_now

JAN1 = datetime(2024, 1, 1)
JAN5 = datetime(2024, 1, 5)
JAN10 = datetime(2024, 1, 10)
JAN20 = datetime(2024, 1, 20)


class TestFetchWindow:
    """Tests for FetchWindow."""

    def test_rejects_inverted_window(self):
        with pytest.raises(ValueError, match="after end"):
            FetchWindow(start=JAN10, end=JAN1)

    def test_single_instant_window_is_allowed(self):
        window = FetchWindow(start=JAN1, end=JAN1)
        assert window.days == 0

    def test_from_lookback(self):
        window = FetchWindow.from_lookback(7, end=JAN10)
        assert window.start == datetime(2024, 1, 3)
        assert window.end == JAN10
        assert window.days == 7

    def test_from_lookback_defaults_to_now(self):
        window = FetchWindow.from_lookback(1)
        assert (utc_now() - window.end).total_seconds() < 5

    def test_millisecond_bounds_are_utc(self):
        window = FetchWindow(start=JAN1, end=JAN1)
        expected = int(datetime(2024, 1, 1, tzinfo=UTC).timestamp() * 1000)
        assert window.start_ms == expected
        assert window.end_ms == expected

    def test_contains(self):
        window = FetchWindow(start=JAN1, end=JAN10)
        assert window.contains(JAN5)
        assert window.contains(JAN1)
        assert window.contains(JAN10)
        assert not window.contains(JAN20)
        assert not window.contains(datetime(2023, 12, 31))


class TestCoverageSerialization:
    """Coverage round-trips through the checkpoint file."""

    def test_round_trip(self):
        coverage = Coverage(start=JAN1, end=JAN10)
        assert Coverage.from_json(coverage.to_json()) == coverage

    def test_legacy_plain_string_is_read_as_an_end_only(self):
        """Older checkpoints stored just the end date; that says nothing about
        where coverage began."""
        coverage = Coverage.from_json("2024-01-10T00:00:00")

        assert coverage.start is None
        assert coverage.end == JAN10

    def test_unreadable_values_are_ignored(self):
        assert Coverage.from_json(None) is None
        assert Coverage.from_json("not-a-date") is None
        assert Coverage.from_json({"covered_end": "nonsense"}) is None
        assert Coverage.from_json({}) is None
        assert Coverage.from_json(42) is None


class TestCoverageMerge:
    """Extending coverage with a window that was just fetched."""

    def test_extends_forward(self):
        merged = Coverage(JAN1, JAN10).merge(FetchWindow(JAN5, JAN20))
        assert merged == Coverage(JAN1, JAN20)

    def test_extends_backward(self):
        merged = Coverage(JAN10, JAN20).merge(FetchWindow(JAN5, JAN10))
        assert merged == Coverage(JAN5, JAN20)

    def test_contained_window_changes_nothing(self):
        merged = Coverage(JAN1, JAN20).merge(FetchWindow(JAN5, JAN10))
        assert merged == Coverage(JAN1, JAN20)

    def test_touching_windows_merge(self):
        """A gap inside the one-day tolerance is contiguous, not disjoint."""
        merged = Coverage(JAN1, JAN10).merge(FetchWindow(datetime(2024, 1, 11), JAN20))
        assert merged == Coverage(JAN1, JAN20)

    def test_disjoint_window_does_not_claim_the_gap(self):
        """Fetching an old window must not make everything since look covered."""
        merged = Coverage(JAN10, JAN20).merge(FetchWindow(datetime(2023, 1, 1), datetime(2023, 2, 1)))

        assert merged == Coverage(JAN10, JAN20)

    def test_disjoint_later_window_replaces_coverage(self):
        merged = Coverage(JAN1, JAN5).merge(FetchWindow(datetime(2024, 3, 1), datetime(2024, 3, 10)))

        assert merged == Coverage(datetime(2024, 3, 1), datetime(2024, 3, 10))

    def test_legacy_coverage_gains_a_start(self):
        merged = Coverage(start=None, end=JAN10).merge(FetchWindow(JAN5, JAN20))
        assert merged == Coverage(JAN5, JAN20)


class TestResumeWindow:
    """Trimming a requested window to what is actually missing."""

    def test_no_coverage_fetches_everything(self):
        requested = FetchWindow(JAN1, JAN10)
        assert resume_window(requested, None) == requested

    def test_fully_covered_window_is_skipped(self):
        requested = FetchWindow(JAN1, JAN5)
        coverage = Coverage(JAN1, JAN20)

        assert resume_window(requested, coverage) is None

    def test_resumes_from_the_end_minus_overlap(self):
        """The last covered day is re-fetched: its trailing bar was probably
        incomplete when it was stored."""
        requested = FetchWindow(JAN1, JAN20)
        coverage = Coverage(JAN1, JAN10)

        window = resume_window(requested, coverage, overlap_days=1)

        assert window == FetchWindow(datetime(2024, 1, 9), JAN20)

    def test_zero_overlap_resumes_exactly_at_the_end(self):
        window = resume_window(FetchWindow(JAN1, JAN20), Coverage(JAN1, JAN10), overlap_days=0)
        assert window == FetchWindow(JAN10, JAN20)

    def test_request_older_than_coverage_is_honoured_in_full(self):
        """A high-water mark cannot prove older history is present, so asking
        for it must fetch it rather than skip it."""
        requested = FetchWindow(datetime(2023, 6, 1), datetime(2023, 7, 1))
        coverage = Coverage(JAN1, JAN20)

        assert resume_window(requested, coverage) == requested

    def test_request_starting_after_the_overlap_is_untouched(self):
        requested = FetchWindow(JAN20, datetime(2024, 1, 25))
        coverage = Coverage(JAN1, JAN10)

        assert resume_window(requested, coverage) == requested

    def test_legacy_coverage_only_trims_the_end(self):
        """With no recorded start, an older request cannot be ruled out."""
        coverage = Coverage(start=None, end=JAN10)

        assert resume_window(FetchWindow(JAN1, JAN20), coverage) == FetchWindow(
            datetime(2024, 1, 9), JAN20
        )
        assert resume_window(FetchWindow(JAN1, JAN5), coverage) is None

    def test_nightly_rerun_refetches_only_the_overlap(self):
        """The everyday case: yesterday's run covered through yesterday, so
        today's run fetches from one day back."""
        today = datetime(2024, 2, 1)
        requested = FetchWindow(today - timedelta(days=1), today)
        coverage = Coverage(datetime(2024, 1, 1), today - timedelta(days=1))

        window = resume_window(requested, coverage, overlap_days=1)

        # The overlap reaches back further than the request, so the request
        # stands as asked: one day, re-fetched.
        assert window == requested
        assert window.days == 1
