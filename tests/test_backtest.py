"""Tests for the backtest engine (Phase 4: walk-forward, point-in-time, costs).

Includes the golden tests required by CLAUDE.md: hand-computed fixtures the
engine must reproduce exactly, and the buy-and-hold validation that a position
run through the engine matches the raw price series.
"""

from datetime import datetime, timedelta

import polars as pl
import pytest

from backtest import (
    ZERO_COST,
    Backtester,
    CostModel,
    DatastoreUniverse,
    RebalanceContext,
    buy_and_hold,
    equal_weight,
    long_short_from_scores,
)
from config import BacktestConfig
from datastore import ParquetStore
from loaders.schemas import OHLCV_SCHEMA
from universe.schema import UNIVERSE_SCHEMA

D1 = datetime(2024, 1, 1)


def _dates(n: int, start: datetime = D1) -> list[datetime]:
    return [start + timedelta(days=i) for i in range(n)]


def _seed_prices(
    store: ParquetStore,
    opens_by_asset: dict[str, list[float]],
    start: datetime = D1,
    venue: str = "binance",
    ingested_offset_days: int = 0,
) -> None:
    """Seed `ohlcv_daily` with one daily bar per open price.

    `close` is set equal to the *next* day's open so close-to-close and
    open-to-open series differ, which keeps the engine honest about which one
    it uses for accounting.
    """
    rows = []
    for asset_id, opens in opens_by_asset.items():
        for i, open_px in enumerate(opens):
            event_ts = start + timedelta(days=i)
            close_px = opens[i + 1] if i + 1 < len(opens) else open_px
            rows.append(
                (
                    asset_id,
                    venue,
                    "1d",
                    event_ts,
                    event_ts + timedelta(days=ingested_offset_days),
                    open_px,
                    max(open_px, close_px),
                    min(open_px, close_px),
                    close_px,
                    1_000_000.0,
                )
            )

    df = pl.DataFrame(
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
    store.append("ohlcv_daily", df, OHLCV_SCHEMA)


@pytest.fixture
def store(tmp_path):
    return ParquetStore(tmp_path)


@pytest.fixture
def config():
    """Daily rebalance, $100k, 10bps round-trip spread, no impact."""
    return BacktestConfig(
        rebalance_freq="daily",
        backtest_start="2024-01-01",
        backtest_end=None,
        initial_cash=100_000.0,
        spread_bps=10.0,
        impact_bps_per_million=0.0,
        periods_per_year=365.0,
    )


# ---------------------------------------------------------------------------
# Golden test: hand-computed 3-asset fixture
# ---------------------------------------------------------------------------

class TestGoldenThreeAsset:
    """A 3-asset, 5-bar fixture with every number derived by hand.

    Opens (2024-01-01 .. 2024-01-05):
        A: 100, 100, 110, 121, 121
        B: 200, 200, 180, 162, 162
        C:  50,  50,  50,  50,  50

    Fixed target weights A=0.5, B=0.3, C=0.2; daily rebalance; fills at the
    next bar's open; 10bps round-trip spread (5bps per unit of turnover), no
    impact; $100,000 initial cash.

    Rebalances d1..d4 are fillable (d5 has no following bar); the weights set
    on d4 are never exited, so three periods are realized:

        P1: fill d2 -> exit d3   rA=+10%  rB=-10%  rC=0
        P2: fill d3 -> exit d4   rA=+10%  rB=-10%  rC=0
        P3: fill d4 -> exit d5   rA=  0%  rB=  0%  rC=0
    """

    GROSS = 0.5 * 0.10 + 0.3 * -0.10 + 0.2 * 0.0  # = 0.02
    COST_PER_TURNOVER = 10.0 / 2.0 * 1e-4  # 5 bps
    # Positions drift over P1 to 0.55/1.02, 0.27/1.02, 0.20/1.02; rebalancing
    # back to (0.5, 0.3, 0.2) trades (0.04 + 0.036 + 0.004) / 1.02.
    TURNOVER_2 = 0.08 / 1.02

    @pytest.fixture
    def result(self, store, config):
        _seed_prices(
            store,
            {
                "A": [100.0, 100.0, 110.0, 121.0, 121.0],
                "B": [200.0, 200.0, 180.0, 162.0, 162.0],
                "C": [50.0, 50.0, 50.0, 50.0, 50.0],
            },
        )
        bt = Backtester(store, config=config)
        return bt.run(lambda ctx: {"A": 0.5, "B": 0.3, "C": 0.2})

    def test_three_periods_realized(self, result):
        assert len(result.returns) == 3
        assert result.returns["rebalance_ts"].to_list() == _dates(3)
        assert result.returns["fill_ts"].to_list() == _dates(3, D1 + timedelta(days=1))
        assert result.returns["exit_ts"].to_list() == _dates(3, D1 + timedelta(days=2))

    def test_gross_returns(self, result):
        assert result.returns["gross_return"].to_list() == pytest.approx(
            [self.GROSS, self.GROSS, 0.0]
        )

    def test_turnover_measured_against_drifted_book(self, result):
        assert result.returns["turnover"].to_list() == pytest.approx(
            [1.0, self.TURNOVER_2, self.TURNOVER_2]
        )

    def test_costs(self, result):
        expected = [
            1.0 * self.COST_PER_TURNOVER,
            self.TURNOVER_2 * self.COST_PER_TURNOVER,
            self.TURNOVER_2 * self.COST_PER_TURNOVER,
        ]
        assert result.returns["cost"].to_list() == pytest.approx(expected)

    def test_net_returns(self, result):
        c1 = 1.0 * self.COST_PER_TURNOVER
        c2 = self.TURNOVER_2 * self.COST_PER_TURNOVER
        assert result.returns["net_return"].to_list() == pytest.approx(
            [self.GROSS - c1, self.GROSS - c2, 0.0 - c2]
        )

    def test_equity_curve_compounds_net_returns(self, result):
        c1 = 1.0 * self.COST_PER_TURNOVER
        c2 = self.TURNOVER_2 * self.COST_PER_TURNOVER
        e1 = 100_000.0 * (1 + self.GROSS - c1)
        e2 = e1 * (1 + self.GROSS - c2)
        e3 = e2 * (1 - c2)
        assert result.returns["equity"].to_list() == pytest.approx([e1, e2, e3])
        # First period is exactly 100000 * 1.0195 = 101,950
        assert result.returns["equity"][0] == pytest.approx(101_950.0)

    def test_exposures_and_position_count(self, result):
        assert result.returns["gross_exposure"].to_list() == pytest.approx([1.0, 1.0, 1.0])
        assert result.returns["net_exposure"].to_list() == pytest.approx([1.0, 1.0, 1.0])
        assert result.returns["n_positions"].to_list() == [3, 3, 3]

    def test_weights_recorded_for_every_fillable_rebalance(self, result):
        # d1..d4 are fillable (d5 has no next bar) -> 4 rebalances x 3 assets
        assert len(result.weights) == 12
        assert result.weights["rebalance_ts"].unique().sort().to_list() == _dates(4)
        assert result.weights.filter(pl.col("asset_id") == "B")["weight"].to_list() == [0.3] * 4

    def test_metrics_match_hand_computed_series(self, result):
        c1 = 1.0 * self.COST_PER_TURNOVER
        c2 = self.TURNOVER_2 * self.COST_PER_TURNOVER
        nets = [self.GROSS - c1, self.GROSS - c2, -c2]

        growth = (1 + nets[0]) * (1 + nets[1]) * (1 + nets[2])
        assert result.metrics.n_periods == 3
        assert result.metrics.total_return == pytest.approx(growth - 1)
        assert result.metrics.hit_rate == pytest.approx(2 / 3)
        assert result.metrics.total_cost == pytest.approx(c1 + c2 + c2)
        assert result.metrics.avg_turnover == pytest.approx(
            (1.0 + 2 * self.TURNOVER_2) / 3
        )
        # Only the last period is negative, so the drawdown is exactly that period.
        assert result.metrics.max_drawdown == pytest.approx(nets[2])


# ---------------------------------------------------------------------------
# Validation: buy-and-hold through the engine matches the raw data
# ---------------------------------------------------------------------------

class TestBuyAndHoldValidation:
    OPENS = [100.0, 105.0, 103.0, 110.0, 108.0, 120.0]

    @pytest.fixture
    def store_with_btc(self, store):
        _seed_prices(store, {"BTC": self.OPENS})
        return store

    def test_frictionless_buy_and_hold_matches_open_to_open(self, store_with_btc, config):
        bt = Backtester(store_with_btc, config=config, cost_model=ZERO_COST)
        result = bt.run(buy_and_hold("BTC"))

        # Fills run d2..d6; the position is put on at d2's open and exited at d6's open.
        expected_growth = self.OPENS[5] / self.OPENS[1]
        assert result.returns["equity"][-1] == pytest.approx(
            config.initial_cash * expected_growth
        )
        assert result.metrics.total_return == pytest.approx(expected_growth - 1)

    def test_per_period_returns_match_raw_price_series(self, store_with_btc, config):
        bt = Backtester(store_with_btc, config=config, cost_model=ZERO_COST)
        result = bt.run(buy_and_hold("BTC"))

        expected = [
            self.OPENS[i + 1] / self.OPENS[i] - 1 for i in range(1, len(self.OPENS) - 1)
        ]
        assert result.returns["net_return"].to_list() == pytest.approx(expected)

    def test_buy_and_hold_pays_costs_only_once(self, store_with_btc, config):
        """A held position drifts to exactly its target, so it trades only on day one."""
        bt = Backtester(store_with_btc, config=config, cost_model=CostModel(10.0, 0.0))
        result = bt.run(buy_and_hold("BTC"))

        turnover = result.returns["turnover"].to_list()
        assert turnover[0] == pytest.approx(1.0)
        assert turnover[1:] == pytest.approx([0.0] * (len(turnover) - 1))
        assert result.metrics.total_cost == pytest.approx(5e-4)

    def test_engine_uses_open_not_close_prices(self, store_with_btc, config):
        """The fixture's closes differ from its opens; using closes would show up here."""
        bt = Backtester(store_with_btc, config=config, cost_model=ZERO_COST)
        result = bt.run(buy_and_hold("BTC"))

        raw = store_with_btc.read("ohlcv_daily").sort("event_ts")
        close_to_close = raw["close"][2] / raw["close"][1] - 1
        open_to_open = self.OPENS[2] / self.OPENS[1] - 1
        assert close_to_close != pytest.approx(open_to_open)
        assert result.returns["net_return"][0] == pytest.approx(open_to_open)


# ---------------------------------------------------------------------------
# Point-in-time discipline
# ---------------------------------------------------------------------------

class TestPointInTime:
    def test_strategy_never_sees_bars_after_asof(self, store, config):
        _seed_prices(store, {"A": [100.0, 101.0, 102.0, 103.0, 104.0]})
        seen: list[tuple[datetime, datetime]] = []

        def strategy(ctx: RebalanceContext) -> dict:
            bars = ctx.ohlcv()
            if len(bars):
                seen.append((ctx.asof, bars["event_ts"].max()))
            return {"A": 1.0}

        Backtester(store, config=config).run(strategy)

        assert seen, "strategy saw no data at all"
        for asof, max_event_ts in seen:
            assert max_event_ts <= asof

    def test_strategy_never_sees_late_ingested_rows(self, store, config):
        """A bar ingested 10 days after its event date is invisible until then."""
        _seed_prices(store, {"A": [100.0, 101.0, 102.0, 103.0, 104.0]})
        _seed_prices(
            store,
            {"LATE": [10.0, 11.0, 12.0, 13.0, 14.0]},
            ingested_offset_days=10,
        )

        seen_assets: dict[datetime, set[str]] = {}

        def strategy(ctx: RebalanceContext) -> dict:
            bars = ctx.ohlcv()
            seen_assets[ctx.asof] = set(bars["asset_id"].to_list()) if len(bars) else set()
            return {"A": 1.0}

        Backtester(store, config=config).run(strategy)

        assert seen_assets
        assert all("LATE" not in assets for assets in seen_assets.values())

    def test_context_read_honours_lookback(self, store, config):
        _seed_prices(store, {"A": [100.0] * 6})
        lookbacks: list[int] = []

        def strategy(ctx: RebalanceContext) -> dict:
            bars = ctx.ohlcv(lookback_days=2)
            lookbacks.append(len(bars))
            return {"A": 1.0}

        Backtester(store, config=config).run(strategy)

        # At most 3 bars survive a 2-day lookback (asof-2, asof-1, asof)
        assert max(lookbacks) <= 3

    def test_context_read_of_missing_dataset_is_empty(self, store):
        ctx = RebalanceContext(asof=D1, universe=["A"], store=store, venue="binance")
        assert len(ctx.read("funding_rate")) == 0

    def test_event_mode_sees_backfilled_rows_that_strict_mode_hides(self, store, config):
        """The whole point of event mode: a one-shot backfill stamps every row with
        the same late `ingested_ts`, which strict mode hides entirely."""
        _seed_prices(store, {"A": [100.0, 101.0, 102.0, 103.0]}, ingested_offset_days=365)

        def collect(seen):
            def strategy(ctx: RebalanceContext) -> dict:
                seen.append(len(ctx.ohlcv()))
                return {"A": 1.0}
            return strategy

        strict_seen: list[int] = []
        Backtester(store, config=config).run(collect(strict_seen))
        assert strict_seen and all(n == 0 for n in strict_seen)

        event_seen: list[int] = []
        Backtester(store, config=config, pit_mode="event").run(collect(event_seen))
        assert max(event_seen) > 0

    def test_event_mode_still_hides_future_bars(self, store, config):
        """Event mode relaxes ingestion time only — never event time."""
        _seed_prices(store, {"A": [100.0, 101.0, 102.0, 103.0]}, ingested_offset_days=365)
        seen: list[tuple[datetime, datetime]] = []

        def strategy(ctx: RebalanceContext) -> dict:
            bars = ctx.ohlcv()
            if len(bars):
                seen.append((ctx.asof, bars["event_ts"].max()))
            return {"A": 1.0}

        Backtester(store, config=config, pit_mode="event").run(strategy)

        assert seen
        for asof, max_event_ts in seen:
            assert max_event_ts <= asof

    def test_unknown_pit_mode_raises(self, store, config):
        with pytest.raises(ValueError, match="Unknown pit_mode"):
            Backtester(store, config=config, pit_mode="psychic")
        with pytest.raises(ValueError, match="Unknown pit_mode"):
            DatastoreUniverse(store, "binance", pit_mode="psychic")

    def test_price_panel_is_not_filtered_by_ingestion_time(self, store, config):
        """Accounting prices are the final revised series, so a backfill whose rows
        were all ingested later still backtests."""
        _seed_prices(store, {"A": [100.0, 110.0, 121.0, 133.1]}, ingested_offset_days=365)
        panel = Backtester(store, config=config).load_price_panel()
        assert len(panel.dates) == 4


# ---------------------------------------------------------------------------
# Universe, tradability, and weight handling
# ---------------------------------------------------------------------------

class TestUniverseAndWeights:
    def test_default_universe_is_everything_tradable(self, store, config):
        _seed_prices(store, {"A": [100.0] * 4, "B": [50.0] * 4})
        universes: list[list[str]] = []

        Backtester(store, config=config).run(
            lambda ctx: universes.append(ctx.universe) or {}
        )
        assert all(u == ["A", "B"] for u in universes)

    def test_datastore_universe_provider_restricts_membership(self, store, config):
        _seed_prices(store, {"A": [100.0] * 4, "B": [50.0] * 4})
        members = pl.DataFrame(
            {
                "asset_id": ["A", "B"],
                "venue": ["binance", "binance"],
                "event_ts": [D1, D1],
                "ingested_ts": [D1, D1],
                "in_universe": [True, False],
                "dollar_volume_median": [1e9, 1e9],
                "listing_age_days": [400, 400],
                "rank": [1, None],
                "exclusion_reason": [None, "rank_cutoff"],
            },
            schema=UNIVERSE_SCHEMA.to_polars_schema(),
        )
        store.append("universe", members, UNIVERSE_SCHEMA)

        seen: list[list[str]] = []
        bt = Backtester(
            store, config=config, universe_provider=DatastoreUniverse(store, "binance")
        )
        result = bt.run(lambda ctx: seen.append(ctx.universe) or {"A": 0.5, "B": 0.5})

        assert all(u == ["A"] for u in seen)
        # B was requested but is out of universe, so it is never held
        assert set(result.weights["asset_id"].to_list()) == {"A"}

    def test_datastore_universe_in_event_mode_uses_snapshot_event_dates(self, store, config):
        """A snapshot computed today for a 2024 date is usable in event mode; the
        snapshot itself was built point-in-time, so its rules see no future."""
        _seed_prices(store, {"A": [100.0] * 4, "B": [50.0] * 4})
        built_later = datetime(2025, 6, 1)
        members = pl.DataFrame(
            {
                "asset_id": ["A", "B"],
                "venue": ["binance", "binance"],
                "event_ts": [D1, D1],
                "ingested_ts": [built_later, built_later],
                "in_universe": [True, False],
                "dollar_volume_median": [1e9, 1e9],
                "listing_age_days": [400, 400],
                "rank": [1, None],
                "exclusion_reason": [None, "rank_cutoff"],
            },
            schema=UNIVERSE_SCHEMA.to_polars_schema(),
        )
        store.append("universe", members, UNIVERSE_SCHEMA)

        assert DatastoreUniverse(store, "binance")(D1) == []
        assert DatastoreUniverse(store, "binance", pit_mode="event")(D1) == ["A"]

    def test_untradable_asset_is_dropped_not_held(self, store, config):
        """An asset with no bar on the fill date cannot be filled, so it is dropped."""
        _seed_prices(store, {"A": [100.0, 100.0, 110.0, 110.0]})
        _seed_prices(store, {"GAP": [10.0, 10.0]})  # stops trading after bar 2

        result = Backtester(store, config=config, cost_model=ZERO_COST).run(
            lambda ctx: {"A": 0.5, "GAP": 0.5}
        )

        last_fill = result.weights["fill_ts"].max()
        held_at_end = set(
            result.weights.filter(pl.col("fill_ts") == last_fill)["asset_id"].to_list()
        )
        assert held_at_end == {"A"}

    def test_zero_weights_are_not_recorded_as_positions(self, store, config):
        _seed_prices(store, {"A": [100.0] * 4, "B": [50.0] * 4})
        result = Backtester(store, config=config).run(lambda ctx: {"A": 1.0, "B": 0.0})
        assert set(result.weights["asset_id"].to_list()) == {"A"}

    def test_non_finite_weight_raises(self, store, config):
        _seed_prices(store, {"A": [100.0] * 4})
        with pytest.raises(ValueError, match="non-finite weight"):
            Backtester(store, config=config).run(lambda ctx: {"A": float("nan")})

    def test_empty_book_produces_zero_returns(self, store, config):
        _seed_prices(store, {"A": [100.0, 110.0, 120.0, 130.0]})
        result = Backtester(store, config=config).run(lambda ctx: {})
        assert result.returns["net_return"].to_list() == pytest.approx([0.0, 0.0])
        assert result.metrics.total_return == pytest.approx(0.0)

    def test_long_short_book_is_dollar_neutral(self, store, config):
        _seed_prices(store, {"A": [100.0] * 4, "B": [50.0] * 4})
        strategy = long_short_from_scores(lambda ctx: {"A": 1.0, "B": -1.0}, n_per_side=1)
        result = Backtester(store, config=config).run(strategy)

        assert result.returns["net_exposure"].to_list() == pytest.approx([0.0, 0.0])
        assert result.returns["gross_exposure"].to_list() == pytest.approx([1.0, 1.0])

    def test_equal_weight_matches_average_asset_return(self, store, config):
        _seed_prices(store, {"A": [100.0, 100.0, 110.0], "B": [100.0, 100.0, 90.0]})
        result = Backtester(store, config=config, cost_model=ZERO_COST).run(equal_weight())
        assert result.returns["gross_return"][0] == pytest.approx(0.0)


# ---------------------------------------------------------------------------
# Calendar, windows, and error handling
# ---------------------------------------------------------------------------

class TestCalendarAndWindow:
    def test_weekly_rebalance_trades_less_often_than_daily(self, store):
        _seed_prices(store, {"A": [100.0 + i for i in range(15)]})
        weekly = BacktestConfig(rebalance_freq="weekly", backtest_start="2024-01-01",
                                spread_bps=0.0, impact_bps_per_million=0.0)
        daily = BacktestConfig(rebalance_freq="daily", backtest_start="2024-01-01",
                               spread_bps=0.0, impact_bps_per_million=0.0)

        n_weekly = len(Backtester(store, config=weekly).run(equal_weight()).returns)
        n_daily = len(Backtester(store, config=daily).run(equal_weight()).returns)
        assert 0 < n_weekly < n_daily

    def test_annualization_follows_the_holding_period_not_the_bar(self, store):
        """Weekly books are held ~7 daily bars, so they annualize at ~365/7."""
        _seed_prices(store, {"A": [100.0 + i for i in range(22)]})
        weekly = BacktestConfig(rebalance_freq="weekly", backtest_start="2024-01-01",
                                spread_bps=0.0, impact_bps_per_million=0.0,
                                periods_per_year=365.0)
        result = Backtester(store, config=weekly).run(equal_weight())
        assert result.metrics.periods_per_year == pytest.approx(365 / 7)

    def test_daily_rebalance_annualizes_at_the_bar_rate(self, store, config):
        _seed_prices(store, {"A": [100.0 + i for i in range(6)]})
        result = Backtester(store, config=config).run(equal_weight())
        assert result.metrics.periods_per_year == pytest.approx(365.0)

    def test_explicit_window_bounds_the_run(self, store, config):
        _seed_prices(store, {"A": [100.0 + i for i in range(10)]})
        result = Backtester(store, config=config).run(
            buy_and_hold("A"), start="2024-01-01", end="2024-01-05"
        )
        assert result.returns["exit_ts"].max() <= datetime(2024, 1, 5)

    def test_too_few_bars_raises(self, store, config):
        _seed_prices(store, {"A": [100.0, 110.0]})
        with pytest.raises(ValueError, match="at least 3 bars"):
            Backtester(store, config=config).run(buy_and_hold("A"))

    def test_missing_dataset_raises(self, store, config):
        with pytest.raises(ValueError, match="cannot run a backtest"):
            Backtester(store, config=config).run(buy_and_hold("A"))


# ---------------------------------------------------------------------------
# IC series
# ---------------------------------------------------------------------------

class TestSignalIC:
    @pytest.fixture
    def store_three_assets(self, store):
        # Forward (open-to-open) returns over the first realized period:
        # A +10%, B 0%, C -10%
        _seed_prices(
            store,
            {
                "A": [100.0, 100.0, 110.0, 110.0],
                "B": [100.0, 100.0, 100.0, 100.0],
                "C": [100.0, 100.0, 90.0, 90.0],
            },
        )
        return store

    def test_perfect_signal_has_ic_of_one(self, store_three_assets, config):
        result = Backtester(store_three_assets, config=config).run(
            equal_weight(),
            signals={"perfect": lambda ctx: {"A": 3.0, "B": 2.0, "C": 1.0}},
        )
        assert result.ic["ic"][0] == pytest.approx(1.0)

    def test_inverted_signal_has_ic_of_minus_one(self, store_three_assets, config):
        result = Backtester(store_three_assets, config=config).run(
            equal_weight(),
            signals={"inverted": lambda ctx: {"A": 1.0, "B": 2.0, "C": 3.0}},
        )
        assert result.ic["ic"][0] == pytest.approx(-1.0)

    def test_ic_summary_reports_per_signal_statistics(self, store_three_assets, config):
        result = Backtester(store_three_assets, config=config).run(
            equal_weight(),
            signals={
                "perfect": lambda ctx: {"A": 3.0, "B": 2.0, "C": 1.0},
                "inverted": lambda ctx: {"A": 1.0, "B": 2.0, "C": 3.0},
            },
        )
        summary = result.ic_summary()
        assert summary["signal"].to_list() == ["inverted", "perfect"]
        means = dict(zip(summary["signal"], summary["mean_ic"], strict=True))
        assert means["perfect"] == pytest.approx(1.0)
        assert means["inverted"] == pytest.approx(-1.0)

    def test_signals_are_scored_point_in_time(self, store_three_assets, config):
        asofs: list[datetime] = []

        def signal(ctx: RebalanceContext) -> dict:
            bars = ctx.ohlcv()
            if len(bars):
                assert bars["event_ts"].max() <= ctx.asof
            asofs.append(ctx.asof)
            return {"A": 1.0, "B": 2.0}

        Backtester(store_three_assets, config=config).run(
            equal_weight(), signals={"s": signal}
        )
        assert asofs == sorted(asofs)

    def test_no_signals_gives_empty_ic_frame(self, store_three_assets, config):
        result = Backtester(store_three_assets, config=config).run(equal_weight())
        assert len(result.ic) == 0
        assert len(result.ic_summary()) == 0
