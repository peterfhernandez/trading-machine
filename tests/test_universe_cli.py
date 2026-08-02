"""The universe builder's date-loop CLI (`python -m universe.builder`).

`build_and_store` writes one snapshot and has done since Phase 3; there was no
way to build a *history* of them from a shell, which is the step `DATA.md` §3
needs after a bulk backfill. The universe dataset is an input rather than an
output — `DatastoreUniverse` reads snapshots and the audit's coverage
denominator is the latest one — so a store with none silently hands every
backtest an empty universe.

The subprocess test is the Phase 5.6 lesson repeated deliberately: run as
`python -m universe.builder`, `__name__` is `"__main__"`, and before
`resolve_logger_name` existed that meant a logger under no component and an
empty `logs/universe.log`. Every import-based assertion passes on exactly the
code path that used to be broken, so one test has to actually run the CLI.
"""

import os
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path

import polars as pl
import pytest

from backtest.calendar import build_rebalance_calendar
from datastore import ParquetStore
from loaders.schemas import OHLCV_SCHEMA
from universe import builder as builder_module
from universe.builder import build_history, main, snapshot_dates

PROJECT_ROOT = Path(__file__).parent.parent


@pytest.fixture
def store_with_bars(tmp_path):
    """A store holding 120 daily bars for two liquid assets.

    Every row shares one `ingested_ts`, which is what a bulk backfill produces
    and what this CLI exists to be run after — and incidentally keeps the whole
    fixture in a single partition, so each test reads one parquet file rather
    than 120.
    """
    store = ParquetStore(tmp_path / "store")
    start = datetime(2024, 1, 1)
    ingested_ts = datetime(2024, 1, 1)
    rows = []
    for asset_id, volume in (("BTC", 50_000.0), ("ETH", 40_000.0)):
        for offset in range(120):
            event_ts = start + timedelta(days=offset)
            rows.append(
                (asset_id, "binance", "1d", event_ts, ingested_ts,
                 100.0, 100.0, 100.0, 100.0, volume)
            )
    frame = pl.DataFrame(
        rows,
        schema=[
            "asset_id", "venue", "timeframe", "event_ts", "ingested_ts",
            "open", "high", "low", "close", "volume",
        ],
        orient="row",
    ).with_columns(
        pl.col("event_ts").cast(pl.Datetime("us")),
        pl.col("ingested_ts").cast(pl.Datetime("us")),
    )
    store.append("ohlcv_daily", frame, OHLCV_SCHEMA)
    return store


class TestSnapshotDates:
    def test_daily_is_every_date_inclusive_of_both_ends(self):
        dates = snapshot_dates(datetime(2024, 3, 1), datetime(2024, 3, 5), "daily")

        assert dates == [datetime(2024, 3, d) for d in (1, 2, 3, 4, 5)]

    def test_weekly_is_the_first_date_of_each_iso_week(self):
        dates = snapshot_dates(datetime(2024, 3, 1), datetime(2024, 3, 20), "weekly")

        # 2024-03-01 is a Friday, so the first week is short and its first date
        # is the range's own start, not the Monday before it.
        assert dates == [
            datetime(2024, 3, 1),
            datetime(2024, 3, 4),
            datetime(2024, 3, 11),
            datetime(2024, 3, 18),
        ]

    def test_monthly_is_the_first_date_of_each_calendar_month(self):
        dates = snapshot_dates(datetime(2024, 1, 15), datetime(2024, 4, 2), "monthly")

        assert dates == [
            datetime(2024, 1, 15),
            datetime(2024, 2, 1),
            datetime(2024, 3, 1),
            datetime(2024, 4, 1),
        ]

    def test_a_single_day_range_is_one_date(self):
        assert snapshot_dates(datetime(2024, 3, 1), datetime(2024, 3, 1)) == [datetime(2024, 3, 1)]

    def test_the_time_of_day_is_dropped(self):
        dates = snapshot_dates(datetime(2024, 3, 1, 17, 30), datetime(2024, 3, 2, 4), "daily")

        assert dates == [datetime(2024, 3, 1), datetime(2024, 3, 2)]

    def test_an_inverted_range_is_rejected(self):
        with pytest.raises(ValueError):
            snapshot_dates(datetime(2024, 3, 5), datetime(2024, 3, 1))

    def test_an_unsupported_frequency_is_rejected(self):
        with pytest.raises(ValueError):
            snapshot_dates(datetime(2024, 3, 1), datetime(2024, 3, 5), "hourly")

    @pytest.mark.parametrize("freq", ["daily", "weekly", "monthly"])
    def test_it_agrees_with_the_backtest_rebalance_calendar(self, freq):
        """`universe` must not import `backtest`, so the rule is restated here.

        A test is then the only thing keeping the two implementations in step —
        a universe built weekly on a different definition of "week" from the
        backtester's would misalign every snapshot against every rebalance.
        """
        start, end = datetime(2024, 1, 1), datetime(2024, 6, 30)
        every_day = snapshot_dates(start, end, "daily")

        assert snapshot_dates(start, end, freq) == build_rebalance_calendar(every_day, freq)


class TestBuildHistory:
    def test_a_snapshot_is_written_per_date(self, store_with_bars):
        written, failed = build_history(
            start=datetime(2024, 4, 1),
            end=datetime(2024, 4, 21),
            freq="weekly",
            store=store_with_bars,
        )

        stored = store_with_bars.read("universe")
        # 2024-04-21 is a Sunday, i.e. the same ISO week as 04-15, so a
        # three-week span at weekly frequency is three snapshots, not four.
        assert (written, failed) == (3, 0)
        assert sorted(stored["event_ts"].unique().to_list()) == [
            datetime(2024, 4, 1),
            datetime(2024, 4, 8),
            datetime(2024, 4, 15),
        ]
        assert stored.filter(pl.col("event_ts") == datetime(2024, 4, 1))[
            "in_universe"
        ].to_list() == [True, True]

    def test_each_snapshot_is_point_in_time(self, store_with_bars):
        """A snapshot dated 2024-04-01 must not see bars from 2024-04-02."""
        build_history(
            start=datetime(2024, 4, 1),
            end=datetime(2024, 4, 8),
            freq="weekly",
            store=store_with_bars,
        )

        stored = store_with_bars.read("universe")
        first = stored.filter(pl.col("event_ts") == datetime(2024, 4, 1))
        assert len(first) > 0
        assert first["listing_age_days"].max() == 91  # 2024-01-01 .. 2024-04-01

    def test_a_dry_run_writes_nothing(self, store_with_bars):
        written, failed = build_history(
            start=datetime(2024, 4, 1),
            end=datetime(2024, 4, 21),
            freq="weekly",
            store=store_with_bars,
            dry_run=True,
        )

        assert (written, failed) == (0, 0)
        assert "universe" not in store_with_bars.list_datasets()

    def test_dates_before_any_data_are_empty_not_failures(self, store_with_bars):
        """A store's first bar is 2024-01-01; the days before it have nothing to rank.

        The range has to *reach* the ingestion date, or the preflight (rightly)
        refuses the run before the loop starts — which is a different finding
        from "this particular date is too early".
        """
        written, failed = build_history(
            start=datetime(2023, 12, 28),
            end=datetime(2024, 1, 2),
            freq="daily",
            store=store_with_bars,
        )

        # 2024-01-01 and 01-02 rank; the four December dates see nothing.
        assert (written, failed) == (2, 0)

    def test_one_bad_date_does_not_abandon_the_rest(self, store_with_bars, monkeypatch):
        """259 snapshots must not be lost to one raising date."""
        real_build = builder_module.UniverseBuilder.build
        calls = {"n": 0}

        def flaky(self, asof=None):
            calls["n"] += 1
            if calls["n"] == 2:
                raise RuntimeError("transient")
            return real_build(self, asof=asof)

        monkeypatch.setattr(builder_module.UniverseBuilder, "build", flaky)

        written, failed = build_history(
            start=datetime(2024, 4, 1),
            end=datetime(2024, 4, 21),
            freq="weekly",
            store=store_with_bars,
        )

        assert (written, failed) == (2, 1)

    def test_the_venue_is_carried_into_the_snapshot(self, store_with_bars):
        build_history(
            start=datetime(2024, 4, 1),
            end=datetime(2024, 4, 1),
            store=store_with_bars,
            venue="binance",
        )

        assert store_with_bars.read("universe")["venue"].unique().to_list() == ["binance"]

    def test_an_unknown_venue_is_a_preflight_failure(self, store_with_bars):
        """A venue typo used to be indistinguishable from a successful no-op."""
        with pytest.raises(builder_module.PreflightError, match="kraken"):
            build_history(
                start=datetime(2024, 4, 1),
                end=datetime(2024, 4, 1),
                store=store_with_bars,
                venue="kraken",
            )

    def test_the_snapshots_are_appended_in_batches(self, store_with_bars, monkeypatch):
        """One partition file per batch, not per snapshot.

        A bulk build lands every snapshot in one ingestion partition, and
        `ParquetStore.read` opens every file in a partition — so per-date
        appends would leave a five-year daily build as ~1,800 files that every
        later universe read reopens.
        """
        monkeypatch.setattr(builder_module, "_APPEND_CHUNK_SNAPSHOTS", 3)

        written, _ = build_history(
            start=datetime(2024, 4, 1),
            end=datetime(2024, 4, 7),
            freq="daily",
            store=store_with_bars,
        )

        files = list((store_with_bars.root / "universe").glob("date=*/*.parquet"))
        assert written == 7
        assert len(files) == 3  # 3 + 3 + 1
        assert len(store_with_bars.read("universe")["event_ts"].unique()) == 7


@pytest.fixture
def backfilled_store(tmp_path):
    """What a bulk archive backfill actually leaves behind.

    Bars dated across 2021, every one of them stamped with the `ingested_ts` of
    the day the backfill ran — which is *after* every snapshot date anyone would
    want to build over that history.
    """
    store = ParquetStore(tmp_path / "store")
    start = datetime(2021, 1, 1)
    ingested_ts = datetime(2026, 8, 2, 9, 30)
    rows = []
    for asset_id, volume in (("BTC", 50_000.0), ("ETH", 40_000.0)):
        for offset in range(240):
            rows.append(
                (asset_id, "binance", "1d", start + timedelta(days=offset), ingested_ts,
                 100.0, 100.0, 100.0, 100.0, volume)
            )
    frame = pl.DataFrame(
        rows,
        schema=[
            "asset_id", "venue", "timeframe", "event_ts", "ingested_ts",
            "open", "high", "low", "close", "volume",
        ],
        orient="row",
    ).with_columns(
        pl.col("event_ts").cast(pl.Datetime("us")),
        pl.col("ingested_ts").cast(pl.Datetime("us")),
    )
    store.append("ohlcv_daily", frame, OHLCV_SCHEMA)
    return store


class TestPitMode:
    """The reported defect: a strict build over backfilled history writes nothing.

    `ParquetStore.read(asof=...)` filters `ingested_ts`, and a bulk backfill
    stamps every row with the moment it ran. So a snapshot dated 2021-09-01
    built from history downloaded in 2026 sees an empty store — for every date
    in the range, for over an hour, exiting 0 with nothing written.
    """

    def test_strict_mode_sees_nothing_in_backfilled_history(self, backfilled_store):
        builder = builder_module.UniverseBuilder(backfilled_store)

        assert len(builder.build(asof=datetime(2021, 9, 1))) == 0

    def test_event_mode_sees_it(self, backfilled_store):
        builder = builder_module.UniverseBuilder(backfilled_store, pit_mode="event")

        snapshot = builder.build(asof=datetime(2021, 9, 1))

        assert len(snapshot) == 2
        assert snapshot["in_universe"].to_list() == [True, True]

    def test_event_mode_still_cannot_see_a_later_bar(self, backfilled_store):
        """Weaker than strict, not absent: `event_ts <= asof` still holds."""
        builder = builder_module.UniverseBuilder(backfilled_store, pit_mode="event")

        snapshot = builder.build(asof=datetime(2021, 3, 1))

        # 2021-01-01 .. 2021-03-01, not the full 240 bars in the store.
        assert snapshot["listing_age_days"].max() == 59

    def test_the_history_build_is_refused_in_strict_mode(self, backfilled_store):
        with pytest.raises(builder_module.PreflightError, match="earliest ingested_ts"):
            build_history(
                start=datetime(2021, 9, 1),
                end=datetime(2021, 12, 1),
                freq="weekly",
                store=backfilled_store,
            )

    def test_the_history_build_works_in_event_mode(self, backfilled_store):
        written, failed = build_history(
            start=datetime(2021, 6, 1),
            end=datetime(2021, 6, 28),
            freq="weekly",
            store=backfilled_store,
            pit_mode="event",
        )

        # 2021-06-01 is a Tuesday, so its ISO week is short: 06-01, 07, 14, 21, 28.
        assert (written, failed) == (5, 0)
        assert len(backfilled_store.read("universe")) == 10

    def test_a_dry_run_reports_the_problem_instead_of_raising(self, backfilled_store, caplog):
        written, failed = build_history(
            start=datetime(2021, 9, 1),
            end=datetime(2021, 12, 1),
            store=backfilled_store,
            dry_run=True,
        )

        assert (written, failed) == (0, 0)
        assert any("--pit-mode event" in r.getMessage() for r in caplog.records)

    def test_the_preflight_message_survives_a_narrow_console(self, backfilled_store):
        """It reaches stderr, which is cp1252 on a default Windows install."""
        with pytest.raises(builder_module.PreflightError) as excinfo:
            build_history(
                start=datetime(2021, 9, 1), end=datetime(2021, 9, 30), store=backfilled_store
            )

        str(excinfo.value).encode("cp1252")  # raises UnicodeEncodeError if not

    def test_an_unknown_pit_mode_is_rejected(self, backfilled_store):
        with pytest.raises(ValueError, match="psychic"):
            builder_module.UniverseBuilder(backfilled_store, pit_mode="psychic")

    def test_the_modes_match_the_backtesters(self):
        """`universe` restates them rather than importing `backtest`."""
        from backtest import engine

        assert (builder_module.PIT_INGESTION, builder_module.PIT_EVENT) == (
            engine.PIT_INGESTION,
            engine.PIT_EVENT,
        )
        assert builder_module.PIT_MODES == engine.PIT_MODES


@pytest.fixture
def store_with_revisions(tmp_path):
    """Honest ingestion history, including a revised bar and a late arrival.

    The shape strict mode exists for, and the shape a panel that collapsed
    duplicates before applying the asof filter would get wrong.
    """
    store = ParquetStore(tmp_path / "store")
    rows = []
    for asset_id, volume in (("BTC", 50_000.0), ("ETH", 40_000.0)):
        for offset in range(120):
            event_ts = datetime(2024, 1, 1) + timedelta(days=offset)
            rows.append((asset_id, "binance", "1d", event_ts, event_ts, 100.0, volume))
    # 2024-03-01's BTC bar revised on 03-05, and a bar that arrived four days late.
    rows.append(("BTC", "binance", "1d", datetime(2024, 3, 1), datetime(2024, 3, 5), 100.0, 90_000.0))
    rows.append(("ETH", "binance", "1d", datetime(2024, 4, 1), datetime(2024, 4, 5), 100.0, 40_000.0))
    frame = pl.DataFrame(
        rows,
        schema=["asset_id", "venue", "timeframe", "event_ts", "ingested_ts", "close", "volume"],
        orient="row",
    ).with_columns(
        pl.col("event_ts").cast(pl.Datetime("us")),
        pl.col("ingested_ts").cast(pl.Datetime("us")),
        pl.lit(100.0).alias("open"),
        pl.lit(100.0).alias("high"),
        pl.lit(100.0).alias("low"),
    )
    store.append("ohlcv_daily", frame, OHLCV_SCHEMA)
    return store


class TestThePanelMatchesTheStore:
    """One store read, re-filtered per date — an optimization, not a second reader.

    The panel is what makes a five-year daily build finish: the store cannot
    prune by event date (it partitions by *ingestion* date), so reading per date
    re-reads the whole dataset every time. Nothing but these tests forces the
    fast path to agree with the slow one.
    """

    @pytest.mark.parametrize("pit_mode", ["ingestion", "event"])
    @pytest.mark.parametrize("asof", [datetime(2024, 3, 1), datetime(2024, 3, 4), datetime(2024, 4, 3)])
    def test_the_two_paths_build_identical_snapshots(self, store_with_revisions, pit_mode, asof):
        panel = builder_module.OhlcvPanel(store_with_revisions, pit_mode=pit_mode)
        via_store = builder_module.UniverseBuilder(store_with_revisions, pit_mode=pit_mode)
        via_panel = builder_module.UniverseBuilder(
            store_with_revisions, pit_mode=pit_mode, bars=panel
        )

        # `ingested_ts` is "now" on both, microseconds apart by construction.
        slow = via_store.build(asof=asof).drop("ingested_ts")
        fast = via_panel.build(asof=asof).drop("ingested_ts")

        assert slow.to_dicts() == fast.to_dicts()

    def test_a_revision_does_not_leak_backwards(self, store_with_revisions):
        """The ordering hazard, on the panel: collapse *after* the asof filter.

        BTC's 2024-03-01 bar is revised on 03-05. A panel that collapsed to the
        latest ingestion up front would let the revision win the bar and then
        filter it away, hiding a bar that was knowable on 03-01.
        """
        panel = builder_module.OhlcvPanel(store_with_revisions, pit_mode="ingestion")

        bars = panel.asof(datetime(2024, 3, 1))
        btc = bars.filter(
            (pl.col("asset_id") == "BTC") & (pl.col("event_ts") == datetime(2024, 3, 1))
        )

        assert len(btc) == 1
        assert btc["volume"][0] == 50_000.0  # the original, not the 90,000 revision

    def test_the_revision_is_visible_once_it_has_been_ingested(self, store_with_revisions):
        panel = builder_module.OhlcvPanel(store_with_revisions, pit_mode="ingestion")

        bars = panel.asof(datetime(2024, 3, 5))
        btc = bars.filter(
            (pl.col("asset_id") == "BTC") & (pl.col("event_ts") == datetime(2024, 3, 1))
        )

        assert len(btc) == 1
        assert btc["volume"][0] == 90_000.0

    def test_it_reads_the_store_once_for_the_whole_history(self, store_with_bars, monkeypatch):
        """The point of the change: 2 reads per date became 1 for the whole run."""
        reads = {"n": 0}
        real_read = ParquetStore.read

        def counting_read(self, dataset, *args, **kwargs):
            if dataset == "ohlcv_daily":
                reads["n"] += 1
            return real_read(self, dataset, *args, **kwargs)

        monkeypatch.setattr(ParquetStore, "read", counting_read)

        build_history(
            start=datetime(2024, 4, 1),
            end=datetime(2024, 4, 30),
            freq="daily",
            store=store_with_bars,
        )

        assert reads["n"] == 1

    def test_a_panel_that_disagrees_with_its_builder_is_rejected(self, store_with_bars):
        panel = builder_module.OhlcvPanel(store_with_bars, pit_mode="event")

        with pytest.raises(ValueError, match="must agree"):
            builder_module.UniverseBuilder(store_with_bars, pit_mode="ingestion", bars=panel)

    def test_a_missing_dataset_gives_an_empty_panel(self, tmp_path):
        panel = builder_module.OhlcvPanel(ParquetStore(tmp_path / "empty"))

        assert panel.is_empty
        assert panel.min_ingested_ts is None
        assert len(panel.asof(datetime(2024, 3, 1))) == 0


class TestMain:
    def test_a_dry_run_reports_the_count_and_writes_nothing(
        self, capsys, isolate_production_datastore
    ):
        code = main(["--start", "2024-01-01", "--end", "2024-03-31", "--freq", "weekly", "--dry-run"])

        out = capsys.readouterr().out
        assert code == 0
        assert "13 weekly date(s)" in out
        assert not (isolate_production_datastore / "universe").exists()

    def test_it_writes_into_the_configured_datastore(
        self, isolate_production_datastore, store_with_bars, monkeypatch
    ):
        """The sandbox the autouse fixture installs, never the real data/parquet."""
        monkeypatch.setattr(
            builder_module, "DATASTORE_PATH", store_with_bars.root, raising=False
        )

        code = main(["--start", "2024-04-01", "--end", "2024-04-15", "--freq", "weekly"])

        assert code == 0
        assert len(store_with_bars.read("universe")) > 0

    def test_a_partial_build_exits_non_zero(self, store_with_bars, monkeypatch):
        def always_raises(self, asof=None):
            raise RuntimeError("nope")

        monkeypatch.setattr(builder_module, "DATASTORE_PATH", store_with_bars.root, raising=False)
        monkeypatch.setattr(builder_module.UniverseBuilder, "build", always_raises)

        assert main(["--start", "2024-04-01", "--end", "2024-04-08", "--freq", "weekly"]) == 1

    def test_a_preflight_failure_exits_two(self, store_with_bars, monkeypatch, capsys):
        """2, not 1: "cannot start" is a different answer from "some dates failed"."""
        monkeypatch.setattr(builder_module, "DATASTORE_PATH", store_with_bars.root, raising=False)

        code = main(["--start", "2021-09-01", "--end", "2021-09-30", "--freq", "weekly"])

        assert code == 2
        assert "--pit-mode event" in capsys.readouterr().err

    def test_the_pit_mode_is_reported(self, capsys):
        main(["--start", "2024-04-01", "--end", "2024-04-03", "--pit-mode", "event", "--dry-run"])

        assert "pit_mode=event" in capsys.readouterr().out

    def test_an_unknown_pit_mode_is_a_usage_error(self):
        with pytest.raises(SystemExit):
            main(["--start", "2024-04-01", "--pit-mode", "psychic"])

    def test_an_inverted_range_is_a_usage_error(self):
        with pytest.raises(SystemExit):
            main(["--start", "2024-04-08", "--end", "2024-04-01"])

    def test_an_unsupported_frequency_is_a_usage_error(self):
        with pytest.raises(SystemExit):
            main(["--start", "2024-04-01", "--freq", "hourly"])

    def test_the_default_frequency_comes_from_config(self, capsys):
        from config import UNIVERSE_CONFIG

        main(["--start", "2024-04-01", "--end", "2024-04-03", "--dry-run"])

        assert f"{UNIVERSE_CONFIG.rebalance_freq} date(s)" in capsys.readouterr().out


class TestTheCliAsItIsActuallyRun:
    """A subprocess, because `python -m` is the invocation that used to be silent."""

    def _run(self, args: list[str], tmp_path: Path) -> subprocess.CompletedProcess:
        return subprocess.run(
            [sys.executable, "-m", "universe.builder", *args],
            cwd=PROJECT_ROOT,
            env={
                **os.environ,
                "PAPER": "true",
                "TM_LOG_DIR": str(tmp_path / "logs"),
                "PYTHONIOENCODING": "utf-8",
            },
            capture_output=True,
            encoding="utf-8",
            errors="replace",
            timeout=120,
        )

    @pytest.mark.integration
    def test_a_dry_run_exits_zero_and_prints_its_plan(self, tmp_path):
        result = self._run(
            ["--start", "2024-01-01", "--end", "2024-12-31", "--freq", "monthly", "--dry-run"],
            tmp_path,
        )

        assert result.returncode == 0, result.stderr
        assert "12 monthly date(s)" in result.stdout

    @pytest.mark.integration
    def test_it_writes_to_the_universe_log(self, tmp_path):
        """`get_logger("__main__")` under `-m` used to reach no file handler."""
        result = self._run(
            ["--start", "2024-01-01", "--end", "2024-01-31", "--dry-run", "--log-level", "INFO"],
            tmp_path,
        )

        log_file = tmp_path / "logs" / "universe.log"
        assert result.returncode == 0, result.stderr
        assert log_file.exists(), f"no universe.log; stderr was: {result.stderr}"
        assert "universe snapshot" in log_file.read_text(encoding="utf-8")
