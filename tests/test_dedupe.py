"""Tests for collapsing repeated ingestions of the same bar.

The store is append-only, so a bar fetched twice is stored twice. Every reader
that aggregates rows has to collapse those copies to the latest ingestion, and
must do so *after* any point-in-time filter -- collapsing first would let a
later revision decide which row survives, then have it filtered away.
"""

from datetime import datetime, timedelta

import polars as pl
import pytest

from datastore import ParquetStore, count_duplicate_bars, latest_per_bar
from loaders.schemas import OHLCV_SCHEMA
from universe import UniverseBuilder


def bars(rows: list[tuple[str, datetime, datetime, float]]) -> pl.DataFrame:
    """(asset_id, event_ts, ingested_ts, close) tuples -> frame."""
    return pl.DataFrame(
        {
            "asset_id": [r[0] for r in rows],
            "event_ts": [r[1] for r in rows],
            "ingested_ts": [r[2] for r in rows],
            "close": [r[3] for r in rows],
        },
        schema={
            "asset_id": pl.Utf8,
            "event_ts": pl.Datetime("us"),
            "ingested_ts": pl.Datetime("us"),
            "close": pl.Float64,
        },
    )


D1 = datetime(2024, 1, 1)
D2 = datetime(2024, 1, 2)
IN1 = datetime(2024, 1, 2, 0, 10)
IN2 = datetime(2024, 1, 3, 0, 10)


class TestLatestPerBar:
    """Tests for latest_per_bar."""

    def test_keeps_latest_ingestion(self):
        """Two ingestions of one bar collapse to the later one."""
        df = bars([
            ("BTC", D1, IN1, 100.0),
            ("BTC", D1, IN2, 101.0),  # same bar, re-ingested with a revision
        ])

        out = latest_per_bar(df)

        assert len(out) == 1
        assert out["close"][0] == 101.0
        assert out["ingested_ts"][0] == IN2

    def test_ingestion_order_in_the_frame_does_not_matter(self):
        """The winner is the max ingested_ts, not the last row."""
        df = bars([
            ("BTC", D1, IN2, 101.0),
            ("BTC", D1, IN1, 100.0),
        ])

        assert latest_per_bar(df)["close"][0] == 101.0

    def test_distinct_bars_are_untouched(self):
        df = bars([
            ("BTC", D1, IN1, 100.0),
            ("BTC", D2, IN2, 102.0),
            ("ETH", D1, IN1, 10.0),
        ])

        out = latest_per_bar(df)

        assert len(out) == 3
        assert out["asset_id"].to_list() == ["BTC", "BTC", "ETH"]

    def test_same_event_ts_across_assets_is_not_a_duplicate(self):
        """The key is (asset_id, event_ts), not event_ts alone."""
        df = bars([
            ("BTC", D1, IN1, 100.0),
            ("ETH", D1, IN1, 10.0),
        ])

        assert len(latest_per_bar(df)) == 2

    def test_output_order_is_deterministic(self):
        """polars group_by does not guarantee order; the helper sorts."""
        df = bars([
            ("ETH", D2, IN1, 11.0),
            ("BTC", D2, IN1, 102.0),
            ("ETH", D1, IN1, 10.0),
            ("BTC", D1, IN1, 100.0),
        ])

        out = latest_per_bar(df)
        assert out.select("asset_id", "event_ts").rows() == [
            ("BTC", D1), ("BTC", D2), ("ETH", D1), ("ETH", D2),
        ]

    def test_custom_sort_order(self):
        df = bars([
            ("ETH", D1, IN1, 10.0),
            ("BTC", D2, IN1, 102.0),
            ("BTC", D1, IN1, 100.0),
        ])

        out = latest_per_bar(df, sort_by=["event_ts", "asset_id"])
        assert out.select("asset_id", "event_ts").rows() == [
            ("BTC", D1), ("ETH", D1), ("BTC", D2),
        ]

    def test_empty_frame(self):
        assert len(latest_per_bar(pl.DataFrame())) == 0

    def test_frame_without_key_columns_is_returned_unchanged(self):
        df = pl.DataFrame({"foo": [1, 2, 3]})
        assert latest_per_bar(df).equals(df)

    def test_frame_without_ingested_ts_is_returned_unchanged(self):
        df = pl.DataFrame({"asset_id": ["BTC"], "event_ts": [D1]})
        assert latest_per_bar(df).equals(df)

    def test_all_columns_come_from_the_winning_row(self):
        """Not a per-column max -- the whole surviving row is one ingestion."""
        df = pl.DataFrame(
            {
                "asset_id": ["BTC", "BTC"],
                "event_ts": [D1, D1],
                "ingested_ts": [IN1, IN2],
                "open": [1.0, 2.0],
                "close": [50.0, 40.0],
            },
            schema={
                "asset_id": pl.Utf8,
                "event_ts": pl.Datetime("us"),
                "ingested_ts": pl.Datetime("us"),
                "open": pl.Float64,
                "close": pl.Float64,
            },
        )

        out = latest_per_bar(df)
        assert (out["open"][0], out["close"][0]) == (2.0, 40.0)


class TestCountDuplicateBars:
    """Tests for count_duplicate_bars."""

    def test_counts_rows_that_would_be_dropped(self):
        df = bars([
            ("BTC", D1, IN1, 100.0),
            ("BTC", D1, IN2, 101.0),
            ("BTC", D2, IN2, 102.0),
        ])

        assert count_duplicate_bars(df) == 1
        assert len(df) - count_duplicate_bars(df) == len(latest_per_bar(df))

    def test_zero_when_unique(self):
        df = bars([("BTC", D1, IN1, 100.0), ("ETH", D1, IN1, 10.0)])
        assert count_duplicate_bars(df) == 0

    def test_empty_frame(self):
        assert count_duplicate_bars(pl.DataFrame()) == 0


class TestUniverseBuilderDeduplicates:
    """The universe builder aggregates rows, so duplicates would skew it."""

    @pytest.fixture
    def store(self, tmp_path):
        return ParquetStore(tmp_path)

    def _write(self, store, asset_id, event_ts, ingested_ts, close, volume):
        df = pl.DataFrame(
            {
                "asset_id": [asset_id],
                "venue": ["binance"],
                "timeframe": ["1d"],
                "event_ts": [event_ts],
                "ingested_ts": [ingested_ts],
                "open": [close],
                "high": [close],
                "low": [close],
                "close": [close],
                "volume": [volume],
            },
            schema=OHLCV_SCHEMA.to_polars_schema(),
        )
        store.append("ohlcv_daily", df, OHLCV_SCHEMA)

    def test_median_volume_is_not_skewed_by_re_ingested_bars(self, store):
        """A nightly run with an overlapping window re-ingests recent bars. If
        those count twice the rolling median tilts toward the newest days, which
        is exactly the stretch that gets duplicated.

        30 bars in the volume window: 15 quiet days then 15 busy ones, so the
        median sits halfway between the two levels. Re-ingesting the 15 busy
        days would make them 30 of 45 rows and drag the median all the way up.
        """
        asof = datetime(2024, 2, 1)
        quiet, busy = 1_000.0, 1_000_000.0
        recent = [asof - timedelta(days=29 - i) for i in range(30)]

        for i, event_ts in enumerate(recent):
            volume = quiet if i < 15 else busy
            self._write(store, "BTC", event_ts, event_ts + timedelta(hours=1), 1.0, volume)

        builder = UniverseBuilder(store=store, venue="binance")
        clean = builder._dollar_volume(asof)["dollar_volume_median"][0]
        assert clean == pytest.approx((quiet + busy) / 2)

        # Re-ingest only the busy tail, as an overlapping nightly window does.
        for event_ts in recent[15:]:
            self._write(store, "BTC", event_ts, asof + timedelta(hours=1), 1.0, busy)

        assert builder._dollar_volume(asof)["dollar_volume_median"][0] == pytest.approx(clean)

    def test_revised_bar_uses_the_latest_value(self, store):
        """When a re-ingested bar carries a corrected volume, the correction
        wins -- deduplication keeps the latest ingestion, not the first."""
        asof = datetime(2024, 2, 1)
        event_ts = asof - timedelta(days=1)

        self._write(store, "BTC", event_ts, event_ts + timedelta(hours=1), 1.0, 100.0)
        self._write(store, "BTC", event_ts, asof + timedelta(hours=1), 1.0, 900.0)

        builder = UniverseBuilder(store=store, venue="binance")
        assert builder._dollar_volume(asof)["dollar_volume_median"][0] == 900.0

    def test_listing_age_ignores_re_ingestion(self, store):
        """Listing age is the first *event*, not the first ingestion."""
        asof = datetime(2024, 2, 1)
        first_bar = datetime(2024, 1, 1)

        self._write(store, "BTC", first_bar, first_bar + timedelta(hours=1), 1.0, 100.0)
        self._write(store, "BTC", first_bar, asof + timedelta(hours=1), 1.0, 100.0)

        builder = UniverseBuilder(store=store, venue="binance")
        age = builder._listing_age(asof)["listing_age_days"][0]

        assert age == 31
