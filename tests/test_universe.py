"""Tests for the universe module (Phase 3: liquidity/listing-age/exclusion rules)."""

from datetime import datetime, timedelta

import polars as pl
import pytest

from config import UniverseConfig
from datastore import ParquetStore
from loaders.schemas import OHLCV_SCHEMA
from universe import UniverseBuilder, compute_turnover
from universe.schema import UNIVERSE_SCHEMA


@pytest.fixture
def temp_store(tmp_path):
    """Create a temporary ParquetStore."""
    return ParquetStore(tmp_path)


@pytest.fixture
def universe_config():
    """Small, deterministic universe config for tests."""
    return UniverseConfig(
        target_size=2,
        min_volume_usdt=100_000.0,
        volume_lookback_days=30,
        min_listing_age_days=30,
    )


def _ohlcv_rows(asset_id, venue, start, end, close, volume, ingested_ts=None):
    """Build one OHLCV row per day in [start, end] for a single asset."""
    rows = []
    d = start
    while d <= end:
        ts = ingested_ts if ingested_ts is not None else d
        rows.append((asset_id, venue, "1d", d, ts, close, close, close, close, volume))
        d += timedelta(days=1)
    return rows


def _build_ohlcv_df(rows):
    df = pl.DataFrame(
        rows,
        schema=[
            "asset_id", "venue", "timeframe", "event_ts", "ingested_ts",
            "open", "high", "low", "close", "volume",
        ],
        orient="row",
    )
    return df.with_columns(
        pl.col("event_ts").cast(pl.Datetime("us")),
        pl.col("ingested_ts").cast(pl.Datetime("us")),
    )


def _seed(store, asof, spec):
    """spec: dict of asset_id -> (start, end, close, volume, [ingested_ts])."""
    rows = []
    for asset_id, params in spec.items():
        rows.extend(_ohlcv_rows(asset_id, "binance", *params))
    store.append("ohlcv_daily", _build_ohlcv_df(rows), OHLCV_SCHEMA)


class TestUniverseBuilder:
    def test_empty_store_returns_empty_universe(self, temp_store, universe_config):
        builder = UniverseBuilder(temp_store, config=universe_config)
        result = builder.build(datetime(2024, 1, 1))
        assert len(result) == 0
        assert result.schema == UNIVERSE_SCHEMA.to_polars_schema()

    def test_liquid_long_lived_assets_are_admitted(self, temp_store, universe_config):
        asof = datetime(2024, 3, 1)
        old_start = asof - timedelta(days=400)
        _seed(temp_store, asof, {
            "BTC": (old_start, asof, 100.0, 1_000_000.0),
            "ETH": (old_start, asof, 50.0, 900_000.0),
        })

        builder = UniverseBuilder(temp_store, config=universe_config)
        result = builder.build(asof)

        members = result.filter(pl.col("in_universe"))["asset_id"].to_list()
        assert set(members) == {"BTC", "ETH"}
        assert all(r is None for r in result.filter(pl.col("in_universe"))["exclusion_reason"])

    def test_stablecoin_excluded_regardless_of_volume(self, temp_store, universe_config):
        asof = datetime(2024, 3, 1)
        old_start = asof - timedelta(days=400)
        _seed(temp_store, asof, {
            "BTC": (old_start, asof, 100.0, 1_000_000.0),
            "USDT": (old_start, asof, 1.0, 5_000_000.0),  # huge volume, still excluded
        })

        builder = UniverseBuilder(temp_store, config=universe_config)
        result = builder.build(asof)

        usdt = result.filter(pl.col("asset_id") == "USDT")
        assert len(usdt) == 1
        assert not usdt["in_universe"][0]
        assert usdt["exclusion_reason"][0] == "stablecoin"

    def test_wrapped_asset_excluded(self, temp_store, universe_config):
        asof = datetime(2024, 3, 1)
        old_start = asof - timedelta(days=400)
        _seed(temp_store, asof, {
            "BTC": (old_start, asof, 100.0, 1_000_000.0),
            "WBTC": (old_start, asof, 100.0, 2_000_000.0),
        })

        builder = UniverseBuilder(temp_store, config=universe_config)
        result = builder.build(asof)

        wbtc = result.filter(pl.col("asset_id") == "WBTC")
        assert len(wbtc) == 1
        assert not wbtc["in_universe"][0]
        assert wbtc["exclusion_reason"][0] == "wrapped"

    def test_listing_age_filter(self, temp_store, universe_config):
        asof = datetime(2024, 3, 1)
        old_start = asof - timedelta(days=400)
        _seed(temp_store, asof, {
            "BTC": (old_start, asof, 100.0, 1_000_000.0),
            "NEWCOIN": (asof - timedelta(days=5), asof, 1.0, 2_000_000.0),
        })

        builder = UniverseBuilder(temp_store, config=universe_config)
        result = builder.build(asof)

        newcoin = result.filter(pl.col("asset_id") == "NEWCOIN")
        assert len(newcoin) == 1
        assert not newcoin["in_universe"][0]
        assert newcoin["exclusion_reason"][0] == "listing_age"
        assert newcoin["listing_age_days"][0] == 5

    def test_low_volume_filter(self, temp_store, universe_config):
        asof = datetime(2024, 3, 1)
        old_start = asof - timedelta(days=400)
        _seed(temp_store, asof, {
            "BTC": (old_start, asof, 100.0, 1_000_000.0),
            "LOWVOL": (old_start, asof, 1.0, 10.0),
        })

        builder = UniverseBuilder(temp_store, config=universe_config)
        result = builder.build(asof)

        lowvol = result.filter(pl.col("asset_id") == "LOWVOL")
        assert len(lowvol) == 1
        assert not lowvol["in_universe"][0]
        assert lowvol["exclusion_reason"][0] == "low_volume"

    def test_rank_cutoff_at_target_size(self, temp_store, universe_config):
        """target_size=2: three otherwise-eligible assets -> only top 2 by volume admitted."""
        asof = datetime(2024, 3, 1)
        old_start = asof - timedelta(days=400)
        _seed(temp_store, asof, {
            "BTC": (old_start, asof, 100.0, 3_000_000.0),
            "ETH": (old_start, asof, 50.0, 2_000_000.0),
            "SOL": (old_start, asof, 20.0, 1_000_000.0),
        })

        builder = UniverseBuilder(temp_store, config=universe_config)
        result = builder.build(asof)

        members = set(result.filter(pl.col("in_universe"))["asset_id"].to_list())
        assert members == {"BTC", "ETH"}

        sol = result.filter(pl.col("asset_id") == "SOL")
        assert sol["exclusion_reason"][0] == "rank_cutoff"
        assert sol["rank"][0] == 3

        # Ranks reflect volume order among eligible assets
        btc_rank = result.filter(pl.col("asset_id") == "BTC")["rank"][0]
        eth_rank = result.filter(pl.col("asset_id") == "ETH")["rank"][0]
        assert btc_rank == 1
        assert eth_rank == 2

    def test_point_in_time_excludes_future_ingestion(self, temp_store, universe_config):
        """Data ingested after `asof` must not affect a universe build as of `asof`
        (point-in-time discipline: only ingested_ts <= asof may be read)."""
        asof = datetime(2024, 3, 1)
        old_start = asof - timedelta(days=400)

        _seed(temp_store, asof, {"BTC": (old_start, asof, 100.0, 1_000_000.0)})

        # ETH's data is timestamped with event_ts in the window, but ingested
        # a day after asof -- it should be invisible to a build as-of asof.
        future_ingest = asof + timedelta(days=1)
        _seed(temp_store, asof, {
            "ETH": (old_start, asof, 50.0, 900_000.0, future_ingest),
        })

        builder = UniverseBuilder(temp_store, config=universe_config)
        result = builder.build(asof)

        assert "ETH" not in result["asset_id"].to_list()

        # But a build as-of the day after should see it.
        result_later = builder.build(future_ingest)
        assert "ETH" in result_later["asset_id"].to_list()

    def test_asset_not_yet_listed_is_absent_from_earlier_snapshot(self, temp_store, universe_config):
        """An asset shouldn't appear at all in a universe snapshot dated before
        it was ever observed in OHLCV data."""
        asof = datetime(2024, 3, 1)
        old_start = asof - timedelta(days=400)
        _seed(temp_store, asof, {
            "BTC": (old_start, asof, 100.0, 1_000_000.0),
            "NEWCOIN": (asof - timedelta(days=5), asof, 1.0, 2_000_000.0),
        })

        builder = UniverseBuilder(temp_store, config=universe_config)
        early_result = builder.build(old_start + timedelta(days=1))

        assert "NEWCOIN" not in early_result["asset_id"].to_list()
        assert "BTC" in early_result["asset_id"].to_list()

    def test_build_and_store_appends_to_datastore(self, temp_store, universe_config):
        # Universe rows are partitioned by ingested_ts (when the build ran),
        # not event_ts (the historical asof date) -- read without a
        # date_range and filter by event_ts instead of relying on partition
        # pruning by date_range, same as _read_ohlcv does internally.
        asof = datetime(2024, 3, 1)
        old_start = asof - timedelta(days=400)
        _seed(temp_store, asof, {"BTC": (old_start, asof, 100.0, 1_000_000.0)})

        builder = UniverseBuilder(temp_store, config=universe_config)
        result = builder.build_and_store(asof)

        assert len(result) > 0
        stored = temp_store.read("universe").filter(pl.col("event_ts") == datetime(2024, 3, 1))
        assert len(stored) == len(result)
        assert "BTC" in stored["asset_id"].to_list()

    def test_build_and_store_is_append_only_across_dates(self, temp_store, universe_config):
        """Two builds for different asof dates should both persist (no overwrite)."""
        old_start = datetime(2023, 1, 1)
        asof1 = datetime(2024, 3, 1)
        asof2 = datetime(2024, 3, 2)
        _seed(temp_store, asof2, {"BTC": (old_start, asof2, 100.0, 1_000_000.0)})

        builder = UniverseBuilder(temp_store, config=universe_config)
        builder.build_and_store(asof1)
        builder.build_and_store(asof2)

        stored = temp_store.read("universe")
        dates = sorted(stored["event_ts"].dt.date().unique().to_list())
        assert dates == [asof1.date(), asof2.date()]


class TestComputeTurnover:
    def test_no_change_is_zero_turnover(self):
        before = pl.DataFrame({
            "asset_id": ["BTC", "ETH"],
            "in_universe": [True, True],
        })
        after = pl.DataFrame({
            "asset_id": ["BTC", "ETH"],
            "in_universe": [True, True],
        })
        assert compute_turnover(before, after) == 0.0

    def test_full_replacement_is_full_turnover(self):
        before = pl.DataFrame({
            "asset_id": ["BTC", "ETH"],
            "in_universe": [True, True],
        })
        after = pl.DataFrame({
            "asset_id": ["SOL", "DOGE"],
            "in_universe": [True, True],
        })
        assert compute_turnover(before, after) == 1.0

    def test_partial_turnover(self):
        """BTC stays, ETH drops out, SOL joins -> 2 changed / 3 union = 2/3."""
        before = pl.DataFrame({
            "asset_id": ["BTC", "ETH"],
            "in_universe": [True, True],
        })
        after = pl.DataFrame({
            "asset_id": ["BTC", "SOL"],
            "in_universe": [True, True],
        })
        assert compute_turnover(before, after) == pytest.approx(2 / 3)

    def test_empty_snapshots_are_zero_turnover(self):
        empty = pl.DataFrame({"asset_id": [], "in_universe": []}, schema={"asset_id": pl.Utf8, "in_universe": pl.Boolean})
        assert compute_turnover(empty, empty) == 0.0
