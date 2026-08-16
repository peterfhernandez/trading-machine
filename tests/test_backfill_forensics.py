"""The forensics that explain what the acceptance gate reports.

Two parts, and the second is the interesting one. `find_gaps` enumerates every
hole rather than the worst per asset — the gate's summary is right for a gate
and wrong for a diagnosis, since one 81-day hole and nine 9-day holes read
identically there and have different causes.

`classify_gap` is the fix for a blind spot found by *using* the tool. Its first
version asked only whether the archive's published months were contiguous
between a symbol's first and last, and never compared them against the gap's
own dates — so for `AUDIO`, whose hole begins in the last month the archive
ever published, it reported "contiguous, the hole is ours, refetch". The perp
had been delisted; there was nothing to refetch. A diagnostic's own assumptions
are part of what a diagnosis has to check, which is why these tests exist.

The duplicate classification these tests used to cover now lives in
`audit/duplicates.py` and is tested in `tests/test_duplicates.py`.
"""

from datetime import datetime, timedelta

import polars as pl
import pytest

from scratch.scratch_backfill_forensics import (
    classify_duplicates,
    classify_gap,
    find_gaps,
)

FIRST = datetime(2021, 8, 1)
RUN_A = datetime(2026, 8, 3, 8, 0)
RUN_B = datetime(2026, 8, 3, 14, 0)


def bars(
    asset_id: str,
    n_days: int,
    ingested: datetime,
    close: float = 100.0,
    start: datetime = FIRST,
    spacing_seconds: int = 1,
) -> pl.DataFrame:
    """`n_days` consecutive daily bars for one asset, ingested in one run.

    `spacing_seconds` mimics the archive loader stamping `ingested_ts` per file
    parsed rather than once per run — the clustering has to survive that.
    """
    return pl.DataFrame(
        {
            "asset_id": [asset_id] * n_days,
            "event_ts": [start + timedelta(days=d) for d in range(n_days)],
            "ingested_ts": [
                ingested + timedelta(seconds=d * spacing_seconds) for d in range(n_days)
            ],
            "close": [close + d for d in range(n_days)],
        }
    )


def on_dates(asset_id: str, days: list[int], ingested: datetime, close: float = 100.0):
    """Bars on an explicit set of day offsets from `FIRST`."""
    return pl.DataFrame(
        {
            "asset_id": [asset_id] * len(days),
            "event_ts": [FIRST + timedelta(days=d) for d in days],
            "ingested_ts": [ingested] * len(days),
            "close": [close] * len(days),
        }
    )


class TestEveryGapNotJustTheWorstOne:
    """The gate reports one number per asset; a diagnosis needs each hole."""

    def test_a_contiguous_series_has_none(self):
        assert not len(find_gaps(bars("BTC", 60, RUN_A), max_gap_days=3))

    def test_three_missing_days_pass_and_four_fail(self):
        """Both sides of the boundary, because "a gap > 3 days" reads either
        way and the two readings differ by one bar."""
        three = on_dates("BTC", [0, 4], RUN_A)  # 1,2,3 missing
        four = on_dates("BTC", [0, 5], RUN_A)  # 1,2,3,4 missing

        assert not len(find_gaps(three, max_gap_days=3))
        assert len(find_gaps(four, max_gap_days=3)) == 1

    def test_each_hole_is_its_own_row(self):
        frame = on_dates("BTC", [0, 1, 20, 21, 60], RUN_A)

        holes = find_gaps(frame, max_gap_days=3)

        assert len(holes) == 2
        assert holes["missing"].to_list() == [38, 18]

    def test_a_late_listing_is_not_a_gap(self):
        """The range is per asset, first bar to last: an asset listed in 2023
        is not missing 2021."""
        frame = pl.concat(
            [bars("BTC", 60, RUN_A), bars("NEW", 10, RUN_A, start=FIRST + timedelta(days=50))]
        )

        assert not len(find_gaps(frame, max_gap_days=3))

    def test_a_bar_stored_twice_is_not_a_zero_day_spacing(self):
        """Bar dates are made unique before the diff, so a duplicated bar
        cannot manufacture a spacing of zero and hide a real hole behind it."""
        frame = pl.concat([on_dates("BTC", [0, 20], RUN_A), on_dates("BTC", [0, 20], RUN_B)])

        holes = find_gaps(frame, max_gap_days=3)

        assert len(holes) == 1
        assert holes["missing"][0] == 19

    def test_the_gap_is_reported_between_the_bars_that_bound_it(self):
        holes = find_gaps(on_dates("BTC", [0, 30], RUN_A), max_gap_days=3)

        assert holes["previous"][0] == FIRST.date()
        assert holes["bar_date"][0] == (FIRST + timedelta(days=30)).date()

    def test_an_empty_frame_answers_rather_than_raising(self):
        assert not len(find_gaps(pl.DataFrame(), max_gap_days=3))


@pytest.mark.parametrize("gap_minutes", [1, 30, 120])
def test_a_single_run_is_one_run_at_any_plausible_threshold(gap_minutes):
    """The default must not be load-bearing: a run appending every second is
    one run whatever the operator sets."""
    frame = bars("BTC", 100, RUN_A, spacing_seconds=1)

    anatomy, _ = classify_duplicates(frame, "ohlcv_daily", gap_minutes=gap_minutes)

    assert anatomy.runs == 1


class TestWhatTheArchiveSaysAboutAGap:
    """The blind spot, and both sides of the distinction it could not make."""

    def test_a_gap_starting_after_the_last_published_month_is_a_delisting(self):
        """`AUDIO`, exactly: last published month 2024-05, hole from
        2024-05-28 to 2026-06-01, and the 2026 bars are spot rows the ccxt
        loader supplied for a perp that stopped trading. The old comparison
        called this 'contiguous -- the hole is ours, refetch that window'."""
        published = ["2021-10", "2021-11", "2024-04", "2024-05"]

        verdict = classify_gap(
            "AUDIOUSDT", published, datetime(2024, 5, 28), datetime(2026, 6, 1)
        )

        assert verdict.starts_after_last_published
        assert "DELISTED" in verdict.verdict
        assert "refetch" in verdict.verdict  # says there is nothing to

    def test_a_gap_inside_published_months_is_ours(self):
        """`BNX`: the hole sits inside 2022-04..2026-07, so those files exist
        and the loader skipped them -- it logs and continues on a corrupt or
        missing file by design."""
        published = ["2022-12", "2023-01", "2023-02", "2023-03"]

        verdict = classify_gap(
            "BNXUSDT", published, datetime(2023, 1, 31), datetime(2023, 2, 22)
        )

        assert not verdict.starts_after_last_published
        assert "OURS" in verdict.verdict

    def test_contiguity_alone_does_not_decide_it(self):
        """The negative control for the fix. Both symbols publish an unbroken
        run of months -- the property the old check tested -- and the verdicts
        are opposite, because only one gap falls inside the published range."""
        published = ["2023-01", "2023-02", "2023-03"]

        inside = classify_gap(
            "X", published, datetime(2023, 2, 3), datetime(2023, 2, 25)
        )
        after = classify_gap(
            "Y", published, datetime(2023, 3, 20), datetime(2024, 6, 1)
        )

        assert "OURS" in inside.verdict
        assert "DELISTED" in after.verdict

    def test_a_partly_published_gap_names_both_halves(self):
        published = ["2023-01", "2023-02", "2023-06"]

        verdict = classify_gap(
            "Z", published, datetime(2023, 2, 10), datetime(2023, 6, 20)
        )

        assert verdict.missing_from_archive == ("2023-03", "2023-04", "2023-05")
        assert verdict.available_in_archive == ("2023-02", "2023-06")
        assert "mixed" in verdict.verdict

    def test_a_symbol_the_archive_never_published_says_so(self):
        verdict = classify_gap("Q", [], datetime(2023, 1, 1), datetime(2023, 6, 1))

        assert "nothing published" in verdict.verdict

    def test_the_gap_months_span_both_bounding_bars(self):
        verdict = classify_gap(
            "X", ["2023-01"], datetime(2023, 1, 20), datetime(2023, 4, 2)
        )

        assert verdict.gap_months == ("2023-01", "2023-02", "2023-03", "2023-04")
