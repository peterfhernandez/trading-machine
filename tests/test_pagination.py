"""Tests for walking a venue's paged endpoints across a window.

A venue caps one response (1000 bars on Binance), so a window longer than a
page truncates silently unless the caller pages through it: a five-year daily
request comes back as the first 1000 days with nothing to say the rest is
missing.
"""

from datetime import datetime, timedelta

from loaders.base import paginate_time_series, venue_supports
from loaders.window import FetchWindow

START = datetime(2024, 1, 1)
DAY_MS = 86_400_000


def ms(moment: datetime) -> int:
    return FetchWindow(moment, moment).start_ms


def daily_pages(page_size: int, total_days: int):
    """A venue serving `total_days` daily bars, `page_size` at a time."""
    all_ts = [ms(START + timedelta(days=i)) for i in range(total_days)]

    def fetch_page(since: int):
        available = [t for t in all_ts if t >= since]
        return [[t, 1.0] for t in available[:page_size]]

    return fetch_page


class TestPaginateTimeSeries:
    """Tests for paginate_time_series."""

    def test_single_page_window(self):
        window = FetchWindow(START, START + timedelta(days=4))
        entries = paginate_time_series(daily_pages(1000, 5), window, lambda e: e[0])

        assert len(entries) == 5

    def test_walks_past_the_page_limit(self):
        """The regression: one call returns 3 bars, the window needs 10."""
        window = FetchWindow(START, START + timedelta(days=9))

        entries = paginate_time_series(daily_pages(3, 10), window, lambda e: e[0])

        assert len(entries) == 10
        assert entries[0][0] == ms(START)
        assert entries[-1][0] == ms(START + timedelta(days=9))

    def test_one_call_would_have_truncated(self):
        """What the old single-call behaviour would have returned."""
        assert len(daily_pages(3, 10)(ms(START))) == 3

    def test_entries_outside_the_window_are_dropped(self):
        window = FetchWindow(START + timedelta(days=2), START + timedelta(days=4))

        entries = paginate_time_series(daily_pages(1000, 10), window, lambda e: e[0])

        assert [e[0] for e in entries] == [
            ms(START + timedelta(days=d)) for d in (2, 3, 4)
        ]

    def test_stops_when_a_page_repeats(self):
        """A venue that keeps returning the same page must not loop forever."""
        calls = []

        def stuck(since):
            calls.append(since)
            return [[ms(START), 1.0]]

        window = FetchWindow(START, START + timedelta(days=30))
        entries = paginate_time_series(stuck, window, lambda e: e[0])

        assert len(entries) == 1
        assert len(calls) == 2  # first page, then one that made no progress

    def test_stops_on_empty_page(self):
        calls = []

        def empty(since):
            calls.append(since)
            return []

        window = FetchWindow(START, START + timedelta(days=30))

        assert paginate_time_series(empty, window, lambda e: e[0]) == []
        assert len(calls) == 1

    def test_page_budget_is_bounded(self):
        """max_pages stops a very long window from running away."""
        window = FetchWindow(START, START + timedelta(days=1000))

        entries = paginate_time_series(
            daily_pages(1, 1000), window, lambda e: e[0], max_pages=5
        )

        assert len(entries) == 5

    def test_duplicate_timestamps_within_a_page_are_kept_once(self):
        def doubled(since):
            return [[ms(START), 1.0], [ms(START), 2.0], [ms(START + timedelta(days=1)), 3.0]]

        window = FetchWindow(START, START + timedelta(days=1))
        entries = paginate_time_series(doubled, window, lambda e: e[0])

        assert len(entries) == 2

    def test_entries_without_a_timestamp_are_skipped(self):
        def messy(since):
            return [{"timestamp": None}, {"timestamp": ms(START)}]

        window = FetchWindow(START, START + timedelta(days=1))
        entries = paginate_time_series(messy, window, lambda e: e.get("timestamp"))

        assert entries == [{"timestamp": ms(START)}]

    def test_first_call_starts_at_the_window_start(self):
        seen = []

        def record(since):
            seen.append(since)
            return []

        window = FetchWindow(START, START + timedelta(days=5))
        paginate_time_series(record, window, lambda e: e[0])

        assert seen == [ms(START)]


class TestVenueSupports:
    """Capability detection, deliberately conservative."""

    def test_reads_the_has_mapping(self):
        class Exchange:
            has = {"fetchFundingRateHistory": True, "fetchOpenInterestHistory": False}

        assert venue_supports(Exchange(), "fetchFundingRateHistory")
        assert not venue_supports(Exchange(), "fetchOpenInterestHistory")
        assert not venue_supports(Exchange(), "fetchSomethingElse")

    def test_unknown_capability_shape_reads_as_unsupported(self):
        """A test double or an exchange without the flag falls back to the
        endpoint known to work rather than calling a method that may not exist."""

        class NoHas:
            pass

        class WeirdHas:
            has = "not a mapping"

        assert not venue_supports(NoHas(), "fetchFundingRateHistory")
        assert not venue_supports(WeirdHas(), "fetchFundingRateHistory")
