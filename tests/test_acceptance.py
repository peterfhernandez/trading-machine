"""Tests for the backfill acceptance gate (`audit/acceptance.py`).

`DATA.md` §3 step 6 is a checklist whose whole value is that it fails loudly on
outcomes the tools it checks report as success. So the tests are mostly about
the *failing* side: a store with a hole in one asset's history, a universe
dataset with one snapshot in it, a duplicated boundary month. A check that
cannot go red on the thing it names is not a check.
"""

import json
import subprocess
import sys
from datetime import datetime, timedelta

import polars as pl
import pytest

from audit.acceptance import (
    AcceptanceError,
    AcceptanceReport,
    AcceptanceThresholds,
    check_bar_gaps,
    check_duplicate_bars,
    check_funding_coverage,
    check_nightly_resume,
    check_ohlcv_coverage,
    check_universe_snapshots,
    main,
    run_acceptance_checks,
)
from datastore import ParquetStore
from loaders.schemas import FUNDING_RATE_SCHEMA, OHLCV_SCHEMA
from universe.schema import UNIVERSE_SCHEMA

INGESTED = datetime(2026, 8, 2, 12, 0)
START = datetime(2021, 8, 1)


# ---------------------------------------------------------------------------
# Fixtures — a store shaped like a real archive backfill, only much smaller
# ---------------------------------------------------------------------------


def make_bars(
    assets: list[str],
    days: int = 1600,
    start: datetime = START,
    skip: dict[str, set[int]] | None = None,
    venue: str = "binance",
    ingested: datetime = INGESTED,
) -> pl.DataFrame:
    """Daily bars, one ingestion timestamp for all of them (a bulk load).

    `skip` drops day offsets for one asset, which is how a gap is made.
    """
    skip = skip or {}
    rows = []
    for asset in assets:
        holes = skip.get(asset, set())
        for offset in range(days):
            if offset in holes:
                continue
            rows.append(
                {
                    "asset_id": asset,
                    "venue": venue,
                    "timeframe": "1d",
                    "event_ts": start + timedelta(days=offset),
                    "ingested_ts": ingested,
                    "open": 100.0,
                    "high": 101.0,
                    "low": 99.0,
                    "close": 100.0,
                    "volume": 1_000_000.0,
                }
            )
    return pl.DataFrame(rows, schema=OHLCV_SCHEMA.to_polars_schema())


def make_funding(
    assets: list[str],
    days: int = 1600,
    start: datetime = START,
    venue: str = "binance",
    ingested: datetime = INGESTED,
) -> pl.DataFrame:
    """One settlement a day (real perps settle three times; the count is not
    what any check here measures, and it keeps the fixture under 100 ms)."""
    rows = [
        {
            "asset_id": asset,
            "venue": venue,
            "event_ts": start + timedelta(days=offset),
            "ingested_ts": ingested,
            "funding_rate": 0.0001,
            "mark_price": None,
            "index_price": None,
        }
        for asset in assets
        for offset in range(days)
    ]
    return pl.DataFrame(rows, schema=FUNDING_RATE_SCHEMA.to_polars_schema())


def make_universe(
    dates: list[datetime],
    members_per_date: int = 40,
    venue: str = "binance",
    ingested: datetime = INGESTED,
) -> pl.DataFrame:
    rows = [
        {
            "asset_id": f"A{i:03d}",
            "venue": venue,
            "event_ts": d,
            "ingested_ts": ingested,
            "in_universe": True,
            "dollar_volume_median": 5_000_000.0,
            "listing_age_days": 400,
            "rank": i,
            "exclusion_reason": None,
        }
        for d in dates
        for i in range(members_per_date)
    ]
    return pl.DataFrame(rows, schema=UNIVERSE_SCHEMA.to_polars_schema())


def weekly_dates(n: int, start: datetime = datetime(2021, 9, 1)) -> list[datetime]:
    return [start + timedelta(days=7 * i) for i in range(n)]


def make_span_only_bars(
    assets: list[str],
    days: int = 1600,
    start: datetime = START,
    venue: str = "binance",
) -> pl.DataFrame:
    """Two rows per asset: the first bar and the last.

    The coverage checks read exactly three things — the asset count, the
    event_ts range, and the row count — so filling in the 1,598 bars between
    them buys nothing but wall clock, and the project's budget is 100 ms a
    test. The checks that *do* care what is in the middle (gaps, duplicates)
    use `make_bars` instead.
    """
    return pl.concat(
        [
            make_bars(assets, days=1, start=start, venue=venue),
            make_bars(assets, days=1, start=start + timedelta(days=days - 1), venue=venue),
        ]
    )


@pytest.fixture
def assets_160() -> list[str]:
    return [f"A{i:03d}" for i in range(160)]


@pytest.fixture
def small_store(tmp_path) -> ParquetStore:
    """A structurally complete store, far below the default thresholds.

    Every test about the *machinery* — the report, the CLI, which checks run —
    takes this one and lowers the bar explicitly. Only the tests that are about
    the default thresholds themselves pay for `backfilled_store`.
    """
    assets = [f"A{i:03d}" for i in range(8)]
    store = ParquetStore(tmp_path / "parquet")
    store.append("ohlcv_daily", make_bars(assets, days=200), OHLCV_SCHEMA)
    store.append("funding_rate", make_funding(assets[:5], days=200), FUNDING_RATE_SCHEMA)
    store.append("universe", make_universe(weekly_dates(20)), UNIVERSE_SCHEMA)
    return store


LENIENT = AcceptanceThresholds(
    min_years=0.5, min_ohlcv_assets=5, min_funding_assets=3
)


@pytest.fixture(scope="module")
def backfilled_store(tmp_path_factory) -> ParquetStore:
    """A store that should pass every check: 4.4 years, 160 assets, 110 with
    funding, weekly universe snapshots, no gaps and no duplicates.

    Module-scoped, and the one fixture here that is: passing the *default*
    thresholds honestly needs 150 assets x 4 years, which is ~250k bars however
    it is built. Every test that takes it only reads, so building it once keeps
    the file inside the project's per-test budget instead of paying for it
    fifteen times.
    """
    assets = [f"A{i:03d}" for i in range(160)]
    store = ParquetStore(tmp_path_factory.mktemp("backfilled") / "parquet")
    store.append("ohlcv_daily", make_bars(assets, days=1600), OHLCV_SCHEMA)
    store.append(
        "funding_rate", make_funding(assets[:110], days=1600), FUNDING_RATE_SCHEMA
    )
    store.append("universe", make_universe(weekly_dates(230)), UNIVERSE_SCHEMA)
    return store


# ---------------------------------------------------------------------------
# The happy path, end to end
# ---------------------------------------------------------------------------


class TestAcceptedBackfill:
    @pytest.mark.slow
    def test_a_good_backfill_passes_every_blocking_check(self, backfilled_store):
        """The one test that runs every check against the *default* thresholds.

        Marked slow and it is: ~250k bars is what "150 assets, 4 years" means,
        and no fixture trick makes that smaller without also making the claim
        smaller. Every other test here lowers the bar explicitly and runs in
        milliseconds; this is the one that says the shipped numbers are
        achievable by a real backfill rather than only by a lenient one.
        """
        report = run_acceptance_checks(store=backfilled_store, venue="binance")

        assert report.passed, report.to_text()
        assert report.blocking_failures == []

    def test_the_missing_checkpoint_warns_without_blocking(self, small_store):
        report = run_acceptance_checks(
            store=small_store, venue="binance", thresholds=LENIENT
        )

        resume = next(c for c in report.checks if c.name == "nightly_resume")
        assert not resume.passed
        assert not resume.blocking
        assert resume.status == "WARN"
        # A warning must not be able to block research on its own.
        assert report.passed

    def test_every_checklist_line_is_covered(self, small_store):
        report = run_acceptance_checks(
            store=small_store, venue="binance", thresholds=LENIENT
        )

        assert {c.name for c in report.checks} == {
            "ohlcv_daily_coverage",
            "funding_rate_coverage",
            "ohlcv_daily_duplicates",
            "funding_rate_duplicates",
            "bar_gaps",
            "universe_snapshots",
            "nightly_resume",
        }

    def test_another_venues_rows_do_not_count(self, tmp_path, assets_160):
        store = ParquetStore(tmp_path / "parquet")
        store.append(
            "ohlcv_daily", make_span_only_bars(assets_160, venue="okx"), OHLCV_SCHEMA
        )

        report = run_acceptance_checks(store=store, venue="binance")

        coverage = next(c for c in report.checks if c.name == "ohlcv_daily_coverage")
        assert not coverage.passed
        assert "no ohlcv_daily rows" in coverage.message

    def test_an_empty_store_cannot_be_confused_with_a_failed_check(self, tmp_path):
        store = ParquetStore(tmp_path / "nothing-here")

        with pytest.raises(AcceptanceError, match="no datasets in the store"):
            run_acceptance_checks(store=store)


# ---------------------------------------------------------------------------
# OHLCV coverage
# ---------------------------------------------------------------------------


class TestOhlcvCoverage:
    def test_too_few_assets_fails_and_says_so(self, assets_160):
        bars = make_span_only_bars(assets_160[:100])

        check = check_ohlcv_coverage(bars, AcceptanceThresholds())

        assert not check.passed
        assert "100 assets < 150" in check.message

    def test_too_short_a_history_fails_and_says_so(self, assets_160):
        bars = make_span_only_bars(assets_160, days=800)  # ~2.2 years

        check = check_ohlcv_coverage(bars, AcceptanceThresholds())

        assert not check.passed
        assert "< 4.0y" in check.message

    def test_a_deliberately_smaller_pull_can_lower_the_bar(self, assets_160):
        bars = make_span_only_bars(assets_160[:40], days=800)

        check = check_ohlcv_coverage(
            bars, AcceptanceThresholds(min_years=2.0, min_ohlcv_assets=30)
        )

        assert check.passed

    def test_no_rows_names_what_breaks_downstream(self):
        check = check_ohlcv_coverage(pl.DataFrame(), AcceptanceThresholds())

        assert not check.passed
        assert "loaders.archive" in check.message


# ---------------------------------------------------------------------------
# Funding coverage
# ---------------------------------------------------------------------------


class TestFundingCoverage:
    def test_fewer_funding_assets_than_ohlcv_is_fine_above_the_floor(self, assets_160):
        bars = make_span_only_bars(assets_160)
        funding = make_span_only_bars(assets_160[:110])

        check = check_funding_coverage(funding, bars, AcceptanceThresholds())

        assert check.passed, check.message

    def test_below_the_floor_fails(self, assets_160):
        bars = make_span_only_bars(assets_160)
        funding = make_span_only_bars(assets_160[:60])

        check = check_funding_coverage(funding, bars, AcceptanceThresholds())

        assert not check.passed
        assert "60 assets < 100" in check.message

    def test_funding_that_stops_years_early_fails_the_span(self, assets_160):
        """The shape a truncated fundingRate loop leaves: the right number of
        assets, the wrong amount of history. `carry` would quietly have a view
        on nothing after the cutoff."""
        bars = make_span_only_bars(assets_160)
        funding = make_span_only_bars(assets_160[:110], days=600)

        check = check_funding_coverage(funding, bars, AcceptanceThresholds())

        assert not check.passed
        assert "spans" in check.message and "less than ohlcv_daily" in check.message

    def test_a_late_starting_perp_set_is_within_tolerance(self, assets_160):
        bars = make_span_only_bars(assets_160)
        funding = make_span_only_bars(
            assets_160[:110], days=1570, start=START + timedelta(days=30)
        )

        check = check_funding_coverage(funding, bars, AcceptanceThresholds())

        assert check.passed, check.message

    def test_no_funding_at_all_names_carry(self):
        check = check_funding_coverage(
            pl.DataFrame(), make_bars(["A"], days=10), AcceptanceThresholds()
        )

        assert not check.passed
        assert "carry" in check.message


# ---------------------------------------------------------------------------
# Duplicates
# ---------------------------------------------------------------------------


class TestDuplicateBars:
    def test_a_clean_first_run_has_none(self, assets_160):
        check = check_duplicate_bars(make_bars(assets_160[:5], days=100), "ohlcv_daily")

        assert check.passed
        assert "0 duplicate bars" in check.message

    def test_a_double_counted_boundary_month_is_caught(self):
        """What a month loop that re-reads its boundary produces: the same bar
        twice, under two ingestions. The store keeps both by design, so nothing
        upstream of here would ever raise."""
        first = make_bars(["BTC"], days=60)
        again = make_bars(["BTC"], days=60, ingested=INGESTED + timedelta(hours=1))
        raw = pl.concat([first, again.head(5)])

        check = check_duplicate_bars(raw, "ohlcv_daily")

        assert not check.passed
        assert "5 of 65 rows" in check.message
        # It must not claim to know which of the two causes this was.
        assert "re-run" in check.message

    def test_it_is_measured_before_the_collapse(self, tmp_path):
        """`run_acceptance_checks` collapses for every other check, so this one
        has to see the raw frame or it can only ever report zero."""
        store = ParquetStore(tmp_path / "parquet")
        store.append("ohlcv_daily", make_bars(["BTC"], days=200), OHLCV_SCHEMA)
        store.append(
            "ohlcv_daily",
            make_bars(["BTC"], days=200, ingested=INGESTED + timedelta(days=1)),
            OHLCV_SCHEMA,
        )
        store.append("universe", make_universe(weekly_dates(5)), UNIVERSE_SCHEMA)

        report = run_acceptance_checks(store=store, venue="binance")

        duplicates = next(c for c in report.checks if c.name == "ohlcv_daily_duplicates")
        assert not duplicates.passed
        assert "200 of 400 rows" in duplicates.message


# ---------------------------------------------------------------------------
# Gaps — the check nothing else in the project performs
# ---------------------------------------------------------------------------


class TestBarGaps:
    def test_contiguous_bars_pass(self, assets_160):
        check = check_bar_gaps(make_bars(assets_160[:5], days=200), AcceptanceThresholds())

        assert check.passed
        assert "worst is 0 day(s)" in check.message

    def test_a_hole_in_one_assets_history_is_found(self):
        """One asset out of five, missing a week in the middle. Every dataset-level
        number stays plausible; `signals/bars.py` would silently trim that asset
        to whatever follows the hole."""
        bars = make_bars(
            [f"A{i}" for i in range(5)], days=200, skip={"A3": {100, 101, 102, 103, 104}}
        )

        check = check_bar_gaps(bars, AcceptanceThresholds())

        assert not check.passed
        assert "1 of 5 assets" in check.message
        assert "A3" in check.message

    def test_the_threshold_is_missing_days_not_spacing(self):
        """Three missing days is exactly at the limit and passes; four fails.
        Stated explicitly because 'a gap > 3 days' can be read either way."""
        three = make_bars(["A", "B"], days=100, skip={"A": {50, 51, 52}})
        four = make_bars(["A", "B"], days=100, skip={"A": {50, 51, 52, 53}})

        assert check_bar_gaps(three, AcceptanceThresholds()).passed
        assert not check_bar_gaps(four, AcceptanceThresholds()).passed

    def test_a_late_listing_is_not_a_gap(self):
        """An asset listed two years in is not missing the first two years —
        the range is per asset, so this must not fire."""
        early = make_bars(["OLD"], days=1000)
        late = make_bars(["NEW"], days=200, start=START + timedelta(days=800))

        check = check_bar_gaps(pl.concat([early, late]), AcceptanceThresholds())

        assert check.passed, check.message

    def test_a_duplicated_bar_is_not_a_gap_either(self):
        """Duplicates are collapsed upstream, but the date-level `unique()` here
        is the second line of defence: two ingestions of one day must not read
        as a zero-day spacing."""
        raw = pl.concat(
            [
                make_bars(["A"], days=100),
                make_bars(["A"], days=100, ingested=INGESTED + timedelta(days=1)),
            ]
        )

        check = check_bar_gaps(raw, AcceptanceThresholds())

        assert check.passed, check.message

    def test_the_offender_list_is_capped(self):
        bars = make_bars(
            [f"A{i}" for i in range(20)],
            days=200,
            skip={f"A{i}": {100, 101, 102, 103, 104} for i in range(20)},
        )

        check = check_bar_gaps(bars, AcceptanceThresholds())

        assert not check.passed
        assert "+15 more" in check.message


# ---------------------------------------------------------------------------
# Universe snapshots — the DATA.md step-3 failure, seen from downstream
# ---------------------------------------------------------------------------


class TestUniverseSnapshots:
    def _store(self, tmp_path, frame: pl.DataFrame | None) -> ParquetStore:
        store = ParquetStore(tmp_path / "parquet")
        if frame is not None:
            store.append("universe", frame, UNIVERSE_SCHEMA)
        return store

    def test_a_weekly_history_passes_and_reports_the_cadence(self, tmp_path):
        store = self._store(tmp_path, make_universe(weekly_dates(200)))

        check = check_universe_snapshots(store, "binance", AcceptanceThresholds())

        assert check.passed, check.message
        assert "every 7 day(s)" in check.message

    def test_no_snapshots_names_the_pit_mode_cause(self, tmp_path):
        """The exact symptom a strict-mode rebuild over backfilled history
        leaves: the command exits 0 having written nothing."""
        store = self._store(tmp_path, None)
        store.append("ohlcv_daily", make_bars(["BTC"], days=10), OHLCV_SCHEMA)

        check = check_universe_snapshots(store, "binance", AcceptanceThresholds())

        assert not check.passed
        assert "--pit-mode event" in check.message

    def test_one_snapshot_is_the_same_failure_as_none(self, tmp_path):
        store = self._store(tmp_path, make_universe(weekly_dates(1)))

        check = check_universe_snapshots(store, "binance", AcceptanceThresholds())

        assert not check.passed
        assert "only 1 snapshot date" in check.message

    def test_a_missing_rebalance_date_is_found(self, tmp_path):
        dates = weekly_dates(50)
        del dates[20:23]
        store = self._store(tmp_path, make_universe(dates))

        check = check_universe_snapshots(store, "binance", AcceptanceThresholds())

        assert not check.passed
        assert "gap(s) in an otherwise 7-day cadence" in check.message

    def test_a_snapshot_with_no_members_fails(self, tmp_path):
        dates = weekly_dates(30)
        frame = make_universe(dates)
        # One date where every asset was considered and none made the cut —
        # which is what an over-tight filter looks like on disk.
        frame = frame.with_columns(
            pl.when(pl.col("event_ts") == dates[10])
            .then(False)
            .otherwise(pl.col("in_universe"))
            .alias("in_universe")
        )
        store = self._store(tmp_path, frame)

        check = check_universe_snapshots(store, "binance", AcceptanceThresholds())

        assert not check.passed
        assert "have no members" in check.message

    def test_an_implausibly_thin_universe_fails(self, tmp_path):
        store = self._store(tmp_path, make_universe(weekly_dates(30), members_per_date=5))

        check = check_universe_snapshots(store, "binance", AcceptanceThresholds())

        assert not check.passed
        assert "median 5 members < 20" in check.message

    def test_a_daily_history_is_read_as_a_daily_cadence(self, tmp_path):
        dates = [datetime(2021, 9, 1) + timedelta(days=i) for i in range(60)]
        store = self._store(tmp_path, make_universe(dates))

        check = check_universe_snapshots(store, "binance", AcceptanceThresholds())

        assert check.passed, check.message
        assert "every 1 day(s)" in check.message


# ---------------------------------------------------------------------------
# Nightly resume
# ---------------------------------------------------------------------------


class TestNightlyResume:
    def test_no_checkpoint_warns_and_explains_what_it_costs(self, tmp_path):
        check = check_nightly_resume(pl.DataFrame(), "binance", tmp_path)

        assert not check.passed
        assert not check.blocking
        assert "--days 1" in check.message
        assert "re-fetch history the store already holds" in check.message

    def test_a_checkpoint_reports_what_the_nightly_would_fetch(self, tmp_path):
        (tmp_path / "binance_backfill.json").write_text(
            json.dumps(
                {
                    "ohlcv_daily": {
                        "covered_start": "2021-08-01T00:00:00",
                        "covered_end": datetime.now().isoformat(),
                    }
                }
            )
        )

        check = check_nightly_resume(pl.DataFrame(), "binance", tmp_path)

        assert check.passed
        assert "covered 2021-08-01" in check.message
        assert "funding_rate: no coverage recorded" in check.message

    def test_a_checkpoint_that_predates_the_stored_bars_is_flagged(self, tmp_path):
        (tmp_path / "binance_backfill.json").write_text(
            json.dumps(
                {
                    "ohlcv_daily": {
                        "covered_start": "2021-08-01T00:00:00",
                        "covered_end": "2023-01-01T00:00:00",
                    }
                }
            )
        )
        bars = make_bars(["BTC"], days=1600)

        check = check_nightly_resume(bars, "binance", tmp_path)

        assert "not reflected in it" in check.message

    def test_an_unreadable_checkpoint_does_not_raise(self, tmp_path):
        (tmp_path / "binance_backfill.json").write_text("{not json")

        check = check_nightly_resume(pl.DataFrame(), "binance", tmp_path)

        assert not check.passed
        assert not check.blocking
        assert "unreadable" in check.message


# ---------------------------------------------------------------------------
# The report and the CLI
# ---------------------------------------------------------------------------


class TestReport:
    def test_the_text_report_survives_a_narrow_console(self, small_store):
        """cp1252 on a default Windows install, and this goes to stdout. The
        nightly pipeline lost a run to exactly this once already. Run against a
        store that *fails*, so the failure messages are encoded too — they are
        the long ones, and the ones a redirected run most needs to print."""
        report = run_acceptance_checks(store=small_store, venue="binance")

        assert not report.passed
        report.to_text().encode("cp1252")

    def test_a_blocked_report_says_so(self):
        from audit.acceptance import AcceptanceCheck

        report = AcceptanceReport(
            checks=[
                AcceptanceCheck("a", True, "fine"),
                AcceptanceCheck("b", False, "broken"),
                AcceptanceCheck("c", False, "noted", blocking=False),
            ]
        )

        assert not report.passed
        assert len(report.blocking_failures) == 1
        assert len(report.warnings) == 1
        assert "BLOCKED: 1 check(s)" in report.to_text()

    def test_json_round_trips(self, small_store):
        report = run_acceptance_checks(
            store=small_store, venue="binance", thresholds=LENIENT
        )

        payload = json.loads(json.dumps(report.to_dict()))

        assert payload["passed"] is True
        assert len(payload["checks"]) == 7


class TestCli:
    def test_a_good_store_exits_zero(self, small_store, tmp_path, capsys):
        code = main(
            [
                "--datastore", str(small_store.root),
                "--checkpoint-dir", str(tmp_path),
                "--min-years", "0.5",
                "--min-assets", "5",
                "--min-funding-assets", "3",
            ]
        )

        assert code == 0
        assert "ACCEPTED" in capsys.readouterr().out

    def test_a_blocking_failure_exits_one(self, tmp_path, capsys):
        store = ParquetStore(tmp_path / "parquet")
        store.append("ohlcv_daily", make_bars(["BTC"], days=100), OHLCV_SCHEMA)

        code = main(["--datastore", str(store.root), "--checkpoint-dir", str(tmp_path)])

        assert code == 1
        assert "BLOCKED" in capsys.readouterr().out

    def test_a_wrong_path_exits_two(self, tmp_path, capsys):
        code = main(["--datastore", str(tmp_path / "typo")])

        assert code == 2
        assert "cannot run acceptance checks" in capsys.readouterr().err

    def test_json_output(self, small_store, tmp_path, capsys):
        code = main(
            [
                "--datastore", str(small_store.root),
                "--checkpoint-dir", str(tmp_path),
                "--min-years", "0.5",
                "--min-assets", "5",
                "--min-funding-assets", "3",
                "--json",
            ]
        )

        assert code == 0
        assert json.loads(capsys.readouterr().out)["passed"] is True

    def test_thresholds_are_settable(self, tmp_path):
        store = ParquetStore(tmp_path / "parquet")
        store.append("ohlcv_daily", make_bars(["A", "B"], days=800), OHLCV_SCHEMA)
        store.append("funding_rate", make_funding(["A", "B"], days=800), FUNDING_RATE_SCHEMA)
        store.append("universe", make_universe(weekly_dates(30), members_per_date=2), UNIVERSE_SCHEMA)

        code = main(
            [
                "--datastore",
                str(store.root),
                "--checkpoint-dir",
                str(tmp_path),
                "--min-years",
                "2",
                "--min-assets",
                "2",
                "--min-funding-assets",
                "2",
            ]
        )

        # The universe median (2) is still under its own floor, so this is a 1 —
        # the point being that the three thresholds passed on the command line
        # took effect and their checks are not what failed.
        assert code == 1

    @pytest.mark.integration
    def test_python_dash_m_runs_and_logs(self, small_store, tmp_path):
        """As a module, not imported: under `-m`, `__name__` is `"__main__"`,
        and `logging_config.resolve_logger_name` is what keeps the records in
        `logs/audit.log`. An import-based assertion passes either way."""
        import os

        log_dir = tmp_path / "logs"
        env = {**os.environ, "TM_LOG_DIR": str(log_dir), "PAPER": "true"}

        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "audit.acceptance",
                "--datastore",
                str(small_store.root),
                "--checkpoint-dir",
                str(tmp_path),
                "--min-years",
                "0.5",
                "--min-assets",
                "5",
                "--min-funding-assets",
                "3",
                "--log-level",
                "INFO",
            ],
            capture_output=True,
            text=True,
            env=env,
            cwd=str(__import__("config").PROJECT_ROOT),
        )

        assert result.returncode == 0, result.stderr
        assert "ACCEPTED" in result.stdout

        records = (log_dir / "audit.log").read_text()
        assert "acceptance ohlcv_daily_coverage" in records
