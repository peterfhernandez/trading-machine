"""Tests for `audit/duplicates.py` — what a duplicate bar *is*.

Same duplicate count, three causes, three different responses: a re-run of a
loaded window (harmless), a loader emitting a bar twice (a defect), and two
symbols merged onto one `asset_id` (a correctness defect). On the counts alone
all three look identical, so these tests fabricate each shape exactly and
assert the verdict flips. That is the negative control for the whole
classification, and for the acceptance gate that now blocks on it.

The classification is deliberately not a judgement about intent: it is
arithmetic on ingestion timestamps and on the values the copies carry.

Written in `scratch/scratch_backfill_forensics.py` and moved here when the gate
started reporting verdicts rather than shapes — a gate and a diagnostic
disagreeing about what a duplicate means would be the Phase 5.9 defect in
miniature.
"""

from datetime import datetime, timedelta

import polars as pl
import pytest

from audit.duplicates import (
    classify_duplicates,
    cluster_runs,
    disagreeing_bars,
    find_concurrent_runs,
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


class TestConcurrentInvocations:
    """The blind spot that produced a wrong diagnosis, and its fix.

    `RUN_GAP_MINUTES` merges appends closer than half an hour, so two
    *overlapping* invocations read as one long run and their duplicates read as
    a within-run double-emit. No gap threshold can separate them — the signal
    has to come from somewhere else. It comes from the datasets: one invocation
    loads them in sequence, so clusters that overlap across datasets prove a
    second process was live.
    """

    def test_sequential_datasets_are_not_concurrent(self):
        """What one invocation leaves: klines, then funding, no overlap."""
        klines = bars("BTC", 60, RUN_A, spacing_seconds=10)
        funding = bars("BTC", 60, RUN_A + timedelta(minutes=20), spacing_seconds=10)

        assert not find_concurrent_runs(
            {"ohlcv_daily": klines, "funding_rate": funding}
        )

    def test_a_cluster_starting_inside_another_is_caught(self):
        """The real shape (`DATA.md` §9.3): funding's cluster is stamped 13:49,
        inside ohlcv_daily's 13:39 + 19.7 minutes."""
        klines = bars("BTC", 120, RUN_A, spacing_seconds=10)  # ~20 minutes
        funding = bars("BTC", 120, RUN_A + timedelta(minutes=10), spacing_seconds=10)

        overlaps = find_concurrent_runs(
            {"ohlcv_daily": klines, "funding_rate": funding}
        )

        assert len(overlaps) == 1
        first, _, second, _ = overlaps[0]
        assert {first, second} == {"ohlcv_daily", "funding_rate"}

    def test_a_single_instant_is_not_read_as_concurrency(self):
        """A bulk append stamps every row identically, and a point has no
        interval to overlap with. Inferring concurrency from one would repeat
        the original error in the other direction."""
        frame = on_dates("BTC", list(range(30)), RUN_A)

        assert not find_concurrent_runs({"ohlcv_daily": frame, "funding_rate": frame})

    def test_concurrency_changes_the_verdict_not_the_counts(self):
        """The same bars classified both ways: the numbers are identical and
        the conclusion is not, which is the whole point of passing the flag."""
        frame = pl.concat(
            [bars("BTC", 30, RUN_A), bars("BTC", 30, RUN_A + timedelta(seconds=5))]
        )

        alone, _ = classify_duplicates(frame, "ohlcv_daily")
        overlapping, _ = classify_duplicates(frame, "ohlcv_daily", concurrent=True)

        assert alone.same_run == overlapping.same_run == 30
        assert "loader bug" in alone.verdict
        assert "concurrent invocations" in overlapping.verdict


class TestDisagreeingBars:
    """Two prices for one bar, asked without reference to ingestion timing."""

    def test_identical_copies_are_not_disagreements(self):
        frame = pl.concat([bars("BTC", 20, RUN_A), bars("BTC", 20, RUN_B)])

        assert not len(disagreeing_bars(frame, "ohlcv_daily", tolerance_pct=0.1))

    def test_a_thousandfold_scale_difference_is_reported_with_its_ratio(self):
        first = bars("CAT", 20, RUN_A, close=1.0)
        second = first.with_columns(pl.col("close") * 1000.0)

        found = disagreeing_bars(pl.concat([first, second]), "ohlcv_daily", 0.1)

        assert len(found) == 20
        assert found["ratio"][0] == pytest.approx(1000.0)

    def test_the_tolerance_is_what_separates_a_revision_from_an_instrument(self):
        """40 bps is spot against perp; 1 bp is a revision. One threshold
        decides, so both sides of it are pinned."""
        first = bars("ETH", 20, RUN_A, close=100.0)
        basis = first.with_columns(pl.col("close") * 1.004)
        revision = first.with_columns(pl.col("close") * 1.0001)

        assert len(disagreeing_bars(pl.concat([first, basis]), "ohlcv_daily", 0.1)) == 20
        assert not len(
            disagreeing_bars(pl.concat([first, revision]), "ohlcv_daily", 0.1)
        )

    def test_a_dataset_without_a_value_column_answers_empty(self):
        frame = bars("BTC", 5, RUN_A).drop("close")

        assert not len(disagreeing_bars(frame, "ohlcv_daily", 0.1))
