"""The forensics that separate a harmless duplicate from a real one.

`audit/acceptance.py` reports duplicate bars and says, in the check's own
message, that it cannot tell "a deliberate re-run of a window already loaded"
from "a month loop double-counting a boundary". That is honest and it is also
where the operator is left. `scratch/scratch_backfill_forensics.py` answers it
from the one record the store keeps of which run a row came from — its
`ingested_ts` — and these tests pin the three answers apart, because on the
counts alone all three look identical.

The classification is deliberately not a judgement about intent: it is
arithmetic on ingestion timestamps and on the values the copies carry. So the
tests fabricate each shape exactly and assert the verdict flips, which is what
makes the tool worth trusting on a store nobody here can see.
"""

from datetime import datetime, timedelta

import polars as pl
import pytest

from scratch.scratch_backfill_forensics import (
    classify_duplicates,
    cluster_runs,
    find_gaps,
    label_runs,
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


class TestClusteringIngestionRuns:
    """`ingested_ts` is the only thing that says which invocation wrote a row."""

    def test_no_stamps_is_no_runs(self):
        assert cluster_runs([]) == []

    def test_stamps_seconds_apart_are_one_run(self):
        stamps = [RUN_A + timedelta(seconds=s) for s in range(0, 600, 30)]

        assert len(cluster_runs(stamps)) == 1

    def test_stamps_hours_apart_are_two_runs(self):
        assert len(cluster_runs([RUN_A, RUN_B])) == 2

    def test_the_gap_threshold_is_what_decides(self):
        """A single knob, so a machine that appends slowly can widen it."""
        stamps = [RUN_A, RUN_A + timedelta(minutes=45)]

        assert len(cluster_runs(stamps, gap_minutes=30)) == 2
        assert len(cluster_runs(stamps, gap_minutes=60)) == 1

    def test_a_long_run_is_still_one_run_when_no_single_gap_is_wide(self):
        """Chained, not bounded: an eight-hour backfill appending every minute
        is one run, and comparing against the first stamp would call it 480."""
        stamps = [RUN_A + timedelta(minutes=m) for m in range(0, 480, 5)]

        assert len(cluster_runs(stamps, gap_minutes=30)) == 1

    def test_labelling_puts_every_row_in_a_run(self):
        frame = pl.concat([bars("BTC", 5, RUN_A), bars("BTC", 5, RUN_B)])

        labelled = label_runs(frame)

        assert labelled["run"].null_count() == 0
        assert labelled["run"].n_unique() == 2


class TestTheThreeThingsDuplicatesCanMean:
    """Same duplicate count, three causes, three different responses."""

    def test_a_clean_load_has_none(self):
        anatomy, _ = classify_duplicates(bars("BTC", 30, RUN_A), "ohlcv_daily")

        assert anatomy.duplicated == 0
        assert anatomy.verdict == "no duplicates"

    def test_a_rerun_is_cross_run_and_expected(self):
        """The whole window loaded twice: every bar repeats, and the copies
        come from different invocations. Append-only storage does this on
        purpose and `latest_per_bar` collapses it on every read."""
        frame = pl.concat([bars("BTC", 30, RUN_A), bars("BTC", 30, RUN_B)])

        anatomy, _ = classify_duplicates(frame, "ohlcv_daily")

        assert anatomy.duplicated == 30
        assert anatomy.cross_run == 30
        assert anatomy.same_run == 0
        assert "re-run" in anatomy.verdict

    def test_one_run_emitting_a_bar_twice_is_a_loader_bug(self):
        """Identical rows, seconds apart, inside one invocation — the shape a
        month loop that double-counts its boundary would leave."""
        frame = pl.concat(
            [
                bars("BTC", 30, RUN_A),
                bars("BTC", 30, RUN_A + timedelta(seconds=5)),
            ]
        )

        anatomy, _ = classify_duplicates(frame, "ohlcv_daily")

        assert anatomy.duplicated == 30
        assert anatomy.cross_run == 0
        assert anatomy.same_run == 30
        assert "loader bug" in anatomy.verdict

    def test_only_the_ingestion_spacing_separates_those_two(self):
        """The negative control for the whole tool: identical bars, identical
        values, identical counts — one timestamp apart in when they were
        written, and opposite conclusions."""
        rerun = pl.concat([bars("BTC", 30, RUN_A), bars("BTC", 30, RUN_B)])
        one_run = pl.concat(
            [bars("BTC", 30, RUN_A), bars("BTC", 30, RUN_A + timedelta(seconds=5))]
        )

        assert classify_duplicates(rerun, "ohlcv_daily")[0].cross_run == 30
        assert classify_duplicates(one_run, "ohlcv_daily")[0].same_run == 30

    def test_disagreeing_copies_outrank_both(self):
        """Two archive symbols collapsing onto one asset_id — a re-denominated
        contract, or a rename whose directories overlap. `latest_per_bar` picks
        by ingestion time, which between two price scales is arbitrary, so this
        is a correctness problem however the copies were written."""
        frame = pl.concat(
            [bars("SATS", 30, RUN_A, close=100.0), bars("SATS", 30, RUN_B, close=1000.0)]
        )

        anatomy, _ = classify_duplicates(frame, "ohlcv_daily")

        assert anatomy.disagreeing == 30
        assert "two symbols on one asset_id" in anatomy.verdict

    def test_a_rerun_of_identical_rows_does_not_count_as_disagreeing(self):
        frame = pl.concat([bars("BTC", 30, RUN_A), bars("BTC", 30, RUN_B)])

        assert classify_duplicates(frame, "ohlcv_daily")[0].disagreeing == 0

    def test_the_month_boundary_share_is_measured_on_within_run_repeats(self):
        """A boundary double-count repeats the first and last bar of a month
        and nothing else; a symbol planned twice repeats everything. The share
        is what tells them apart, and it is only meaningful within one run."""
        month_ends = [0, 30, 31, 61]  # 2021-08-01, 08-31, 09-01, 10-01
        frame = pl.concat(
            [
                bars("BTC", 90, RUN_A),
                on_dates("BTC", month_ends, RUN_A + timedelta(seconds=5)),
            ]
        )

        anatomy, _ = classify_duplicates(frame, "ohlcv_daily")

        assert anatomy.same_run == len(month_ends)
        assert anatomy.same_run_on_month_boundary == len(month_ends)

    def test_a_symbol_planned_twice_does_not_look_like_a_boundary_problem(self):
        frame = pl.concat(
            [bars("BTC", 90, RUN_A), bars("BTC", 90, RUN_A + timedelta(seconds=5))]
        )

        anatomy, _ = classify_duplicates(frame, "ohlcv_daily")

        assert anatomy.same_run == 90
        assert anatomy.same_run_on_month_boundary < anatomy.same_run / 2

    def test_funding_rate_is_judged_on_its_own_value_column(self):
        """`close` does not exist in `funding_rate`; asking for it would make
        the disagreement check silently vacuous on that dataset."""
        frame = pl.concat(
            [bars("BTC", 10, RUN_A), bars("BTC", 10, RUN_B)]
        ).rename({"close": "funding_rate"})

        anatomy, _ = classify_duplicates(frame, "funding_rate")

        assert anatomy.value_column == "funding_rate"
        assert anatomy.disagreeing == 0

    def test_an_empty_frame_answers_rather_than_raising(self):
        anatomy, per_bar = classify_duplicates(pl.DataFrame(), "ohlcv_daily")

        assert anatomy.rows == 0
        assert anatomy.verdict == "no duplicates"
        assert not len(per_bar)


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
