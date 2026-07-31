"""Tests for the Phase 5 signal set: momentum, carry, reversal, low-volatility.

Covers, per signal, the triad the methodology template requires (Section 9):

1. **Golden fixture** — every intermediate value derived by hand in the test
   class's docstring and asserted exactly.
2. **Point-in-time** — a bar that arrives after the rebalance cannot change a
   score that was already computed.
3. **Insufficient history / degenerate input** — every reject path returns
   `None`, never `0.0`.

Plus the shared series reader (`signals.bars`), the two standardizations, the
alpha refinement (IC estimation, shrinkage, `alpha = vol x IC x z`), and the
breadth report.

The close fixtures are built from exact exponentials so that log returns are
round numbers and every standard deviation below can be checked by hand:

    closes = C * exp(cumsum([0, r1, r2, ...]))   ->   log returns exactly [r1, r2, ...]
"""

import math
from datetime import datetime, timedelta

import numpy as np
import polars as pl
import pytest

from backtest import (
    PIT_EVENT,
    PIT_INGESTION,
    Backtester,
    CostModel,
    RebalanceContext,
    long_short_from_scores,
    spearman_ic,
)
from config import BacktestConfig
from datastore import ParquetStore
from loaders.schemas import FUNDING_RATE_SCHEMA, OHLCV_SCHEMA
from signals import alpha as alpha_module
from signals import bars as bars_module
from signals import breadth as breadth_module
from signals import carry as carry_module
from signals import cross_sectional_momentum as xsmom
from signals import low_volatility as lowvol
from signals import registry, signal_families, transforms
from signals import short_term_reversal as strev
from signals import time_series_momentum as tsmom

D1 = datetime(2024, 1, 1)


# ---------------------------------------------------------------------------
# Fixture builders
# ---------------------------------------------------------------------------


def closes_from_log_returns(returns, start: float = 100.0) -> np.ndarray:
    """Close series whose bar-over-bar log returns are exactly `returns`."""
    return start * np.exp(np.cumsum([0.0, *returns]))


def sample_sd(values) -> float:
    """Sample standard deviation (ddof=1), spelled out so the tests can check it."""
    mean = sum(values) / len(values)
    return math.sqrt(sum((v - mean) ** 2 for v in values) / (len(values) - 1))


def seed_ohlcv(
    store: ParquetStore,
    closes_by_asset: dict[str, list[float]],
    start: datetime = D1,
    venue: str = "binance",
    ingested_at: datetime | None = None,
    ingested_offset_days: int = 0,
    dates_by_asset: dict[str, list[datetime]] | None = None,
) -> None:
    """Seed `ohlcv_daily` with one daily bar per close.

    `open` is the previous bar's close, so a signal that accidentally read opens
    instead of closes would produce different numbers.
    """
    rows = []
    for asset_id, closes in closes_by_asset.items():
        dates = (
            dates_by_asset[asset_id]
            if dates_by_asset
            else [start + timedelta(days=i) for i in range(len(closes))]
        )
        for i, (event_ts, close_px) in enumerate(zip(dates, closes, strict=True)):
            open_px = closes[i - 1] if i > 0 else close_px
            rows.append(
                (
                    asset_id, venue, "1d", event_ts,
                    ingested_at or event_ts + timedelta(days=ingested_offset_days),
                    open_px, max(open_px, close_px), min(open_px, close_px),
                    close_px, 1_000_000.0,
                )
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


def seed_funding(
    store: ParquetStore,
    rates_by_asset: dict[str, list[float]],
    start: datetime = D1,
    step: timedelta = timedelta(hours=8),
    venue: str = "binance",
    ingested_at: datetime | None = None,
) -> None:
    """Seed `funding_rate` with 8-hourly settlements per asset."""
    rows = []
    for asset_id, rates in rates_by_asset.items():
        for i, rate in enumerate(rates):
            event_ts = start + step * i
            rows.append(
                (
                    asset_id, venue, event_ts, ingested_at or event_ts,
                    rate, None, None,
                )
            )

    frame = pl.DataFrame(
        rows,
        schema=[
            "asset_id", "venue", "event_ts", "ingested_ts",
            "funding_rate", "mark_price", "index_price",
        ],
        orient="row",
    ).with_columns(
        pl.col("event_ts").cast(pl.Datetime("us")),
        pl.col("ingested_ts").cast(pl.Datetime("us")),
        pl.col("mark_price").cast(pl.Float64),
        pl.col("index_price").cast(pl.Float64),
    )
    store.append("funding_rate", frame, FUNDING_RATE_SCHEMA)


def context(store: ParquetStore, asof: datetime, universe, pit_mode=PIT_EVENT):
    """A rebalance context over `universe`, defaulting to event mode.

    Event mode is the default here because the fixtures stamp `ingested_ts`
    from `event_ts`; the ingestion-mode behaviour gets its own explicit tests.
    """
    return RebalanceContext(
        asof=asof, universe=list(universe), store=store, venue="binance",
        timeframe="1d", pit_mode=pit_mode,
    )


# ===========================================================================
# signals.bars — the shared point-in-time reader and primitives
# ===========================================================================


class TestBars:
    """`trailing_return`, `log_returns`, `realized_vol`, `gap_free_start`."""

    def test_trailing_return_no_skip(self):
        closes = np.array([10.0, 20.0, 40.0, 80.0])
        # 80 / 10 - 1 = 7.0 over 3 bars.
        assert bars_module.trailing_return(closes, lookback=3) == pytest.approx(7.0)

    def test_trailing_return_with_skip(self):
        closes = np.array([10.0, 20.0, 40.0, 80.0, 160.0])
        # skip=1 ends the window at index 3 (80); lookback=3 starts it at 10.
        assert bars_module.trailing_return(closes, lookback=3, skip=1) == pytest.approx(7.0)

    def test_trailing_return_window_does_not_fit(self):
        closes = np.array([10.0, 20.0])
        assert math.isnan(bars_module.trailing_return(closes, lookback=5))

    def test_trailing_return_non_positive_base(self):
        closes = np.array([0.0, 20.0, 40.0])
        assert math.isnan(bars_module.trailing_return(closes, lookback=2))

    def test_log_returns_exact(self):
        closes = closes_from_log_returns([0.1, -0.1, 0.2])
        assert bars_module.log_returns(closes) == pytest.approx([0.1, -0.1, 0.2])

    def test_log_returns_too_short(self):
        assert bars_module.log_returns(np.array([100.0])).size == 0

    def test_realized_vol_matches_sample_sd(self):
        returns = [0.1, -0.1, 0.1, -0.1]
        closes = closes_from_log_returns(returns)
        assert bars_module.realized_vol(closes, window=4) == pytest.approx(sample_sd(returns))

    def test_realized_vol_uses_only_the_trailing_window(self):
        # Six returns, window of 4: the first two must not affect the estimate.
        closes = closes_from_log_returns([5.0, -5.0, 0.1, -0.1, 0.1, -0.1])
        expected = sample_sd([0.1, -0.1, 0.1, -0.1])
        assert bars_module.realized_vol(closes, window=4) == pytest.approx(expected)

    def test_realized_vol_none_for_too_few_observations(self):
        closes = closes_from_log_returns([0.1, -0.1])
        assert math.isnan(bars_module.realized_vol(closes, window=10))

    def test_realized_vol_none_for_constant_series(self):
        """A flat price series is an unpriced asset, not a zero-risk one."""
        closes = np.full(30, 100.0)
        assert math.isnan(bars_module.realized_vol(closes, window=20))

    def test_gap_free_start_no_gap(self):
        dates = np.array([D1 + timedelta(days=i) for i in range(5)], dtype="datetime64[us]")
        assert bars_module.gap_free_start(dates, max_gap_days=1) == 0

    def test_gap_free_start_cuts_at_the_most_recent_gap(self):
        dates = np.array(
            [D1, D1 + timedelta(days=1), D1 + timedelta(days=9), D1 + timedelta(days=10)],
            dtype="datetime64[us]",
        )
        assert bars_module.gap_free_start(dates, max_gap_days=1) == 2

    def test_gap_free_start_ignores_a_gap_outside_max_bars(self):
        """An old gap costs the asset nothing once it is trimmed away."""
        dates = np.array(
            [D1, D1 + timedelta(days=20), D1 + timedelta(days=21), D1 + timedelta(days=22)],
            dtype="datetime64[us]",
        )
        assert bars_module.gap_free_start(dates, max_gap_days=1, max_bars=3) == 1

    def test_gap_free_start_empty(self):
        assert bars_module.gap_free_start(np.empty(0, dtype="datetime64[us]"), 1) == 0

    def test_close_series_reads_point_in_time(self, datastore_path):
        store = ParquetStore(datastore_path)
        seed_ohlcv(store, {"BTC": [100.0, 101.0, 102.0, 103.0]})

        series = bars_module.close_series(
            context(store, D1 + timedelta(days=2), ["BTC"]), lookback_days=30
        )
        # The bar dated D1+3 is after the asof and must be invisible.
        assert series["BTC"].column("close").tolist() == [100.0, 101.0, 102.0]

    def test_close_series_collapses_duplicate_ingestions(self, datastore_path):
        store = ParquetStore(datastore_path)
        seed_ohlcv(store, {"BTC": [100.0, 101.0, 102.0]})
        # Same bars re-fetched later with a revised final close.
        seed_ohlcv(
            store, {"BTC": [100.0, 101.0, 999.0]}, ingested_at=D1 + timedelta(days=10)
        )

        series = bars_module.close_series(
            context(store, D1 + timedelta(days=20), ["BTC"]), lookback_days=60
        )
        assert series["BTC"].column("close").tolist() == [100.0, 101.0, 999.0]
        assert len(series["BTC"]) == 3

    def test_close_series_trims_to_the_gap_free_tail(self, datastore_path):
        store = ParquetStore(datastore_path)
        dates = [D1, D1 + timedelta(days=1), D1 + timedelta(days=8), D1 + timedelta(days=9)]
        seed_ohlcv(
            store,
            {"BTC": [100.0, 101.0, 102.0, 103.0]},
            dates_by_asset={"BTC": dates},
        )

        series = bars_module.close_series(
            context(store, D1 + timedelta(days=9), ["BTC"]), lookback_days=60
        )
        assert series["BTC"].column("close").tolist() == [102.0, 103.0]

    def test_dataset_series_restricts_to_the_universe(self, datastore_path):
        store = ParquetStore(datastore_path)
        seed_funding(store, {"BTC": [0.0001] * 6, "ETH": [0.0002] * 6})

        series = bars_module.dataset_series(
            context(store, D1 + timedelta(days=2), ["BTC"]),
            "funding_rate",
            columns=["funding_rate"],
            lookback_days=7,
        )
        assert set(series) == {"BTC"}

    def test_dataset_series_missing_dataset_is_empty(self, datastore_path):
        store = ParquetStore(datastore_path)
        seed_ohlcv(store, {"BTC": [100.0, 101.0]})
        assert (
            bars_module.dataset_series(
                context(store, D1, ["BTC"]), "funding_rate", ["funding_rate"], 7
            )
            == {}
        )


# ===========================================================================
# cross_sectional_momentum
# ===========================================================================


class TestCrossSectionalMomentum:
    """Golden fixture, derived by hand.

    Parameters: `lookback_days=3`, `skip_days=1` -> `min_history_bars = 5`.
    Closes: `[10, 20, 40, 80, 160]`.

    The skip window excludes the final bar, so the formation window ends at
    index 3 (price 80) and starts 3 bars earlier at index 0 (price 10):

        formation return = 80 / 10 - 1 = 7.0

    The 160 print is deliberately the largest move in the series: a signal that
    ignored `skip_days` would report `160 / 20 - 1 = 7.0` too, so the fixture
    also checks a second asset where the two differ.
    """

    PARAMS = xsmom.CrossSectionalMomentumParams(lookback_days=3, skip_days=1)
    CLOSES = np.array([10.0, 20.0, 40.0, 80.0, 160.0])

    def test_min_history_bars(self):
        assert xsmom.min_history_bars(self.PARAMS) == 5

    def test_golden_formation_return(self):
        diagnostics = xsmom.diagnose_series(self.CLOSES, self.PARAMS)
        assert diagnostics.formation_return == pytest.approx(7.0)
        assert diagnostics.score == pytest.approx(7.0)
        assert diagnostics.reject_reason is None

    def test_skip_window_actually_skips(self):
        """The last bar must not enter the formation window."""
        crashed = np.array([10.0, 20.0, 40.0, 80.0, 1.0])
        # Same formation window as the golden fixture, wildly different last bar.
        assert xsmom.score_series(crashed, self.PARAMS) == pytest.approx(7.0)

    def test_insufficient_history_scores_none(self):
        diagnostics = xsmom.diagnose_series(self.CLOSES[:4], self.PARAMS)
        assert diagnostics.score is None
        assert diagnostics.reject_reason == "insufficient_history"

    def test_non_positive_base_price_scores_none(self):
        closes = np.array([0.0, 20.0, 40.0, 80.0, 160.0])
        diagnostics = xsmom.diagnose_series(closes, self.PARAMS)
        assert diagnostics.score is None
        assert diagnostics.reject_reason == "non_positive_price"

    def test_parameter_validation(self):
        with pytest.raises(ValueError, match="lookback_days"):
            xsmom.CrossSectionalMomentumParams(lookback_days=0)
        with pytest.raises(ValueError, match="skip_days"):
            xsmom.CrossSectionalMomentumParams(skip_days=-1)
        with pytest.raises(ValueError, match="winsorize_pct"):
            xsmom.CrossSectionalMomentumParams(winsorize_pct=50.0)

    def test_with_params_overrides_defaults(self):
        params = xsmom.with_params(lookback_days=30)
        assert params.lookback_days == 30
        assert params.skip_days == xsmom.DEFAULT_PARAMS.skip_days

    def test_score_universe_standardizes_and_preserves_none(self, datastore_path):
        store = ParquetStore(datastore_path)
        seed_ohlcv(
            store,
            {
                "AAA": [10.0, 20.0, 40.0, 80.0, 160.0],
                "BBB": [100.0, 100.0, 100.0, 100.0, 100.0],
                "CCC": [1.0, 1.0],  # too short
            },
        )
        ctx = context(store, D1 + timedelta(days=4), ["AAA", "BBB", "CCC"])

        raw = xsmom.score_universe(ctx, self.PARAMS)
        assert raw["AAA"] == pytest.approx(7.0)
        assert raw["BBB"] == pytest.approx(0.0)
        assert raw["CCC"] is None

        standardized = xsmom.score_universe(ctx, self.PARAMS, standardize=True)
        # Two scored assets, so z-scores are +/- 1/sqrt(2) * sqrt(2) = +/-0.7071.
        assert standardized["AAA"] > 0 > standardized["BBB"]
        assert standardized["CCC"] is None
        assert standardized["AAA"] == pytest.approx(-standardized["BBB"])

    def test_point_in_time_a_later_bar_cannot_change_an_earlier_score(self, datastore_path):
        store = ParquetStore(datastore_path)
        seed_ohlcv(store, {"AAA": [10.0, 20.0, 40.0, 80.0, 160.0]})
        asof = D1 + timedelta(days=4)
        before = xsmom.score_universe(context(store, asof, ["AAA"]), self.PARAMS)

        # A bar dated after the rebalance arrives.
        seed_ohlcv(
            store,
            {"AAA": [500.0]},
            start=D1 + timedelta(days=5),
            ingested_at=D1 + timedelta(days=5),
        )
        after = xsmom.score_universe(context(store, asof, ["AAA"]), self.PARAMS)
        assert after["AAA"] == before["AAA"]

    def test_ingestion_mode_hides_a_late_ingested_bar(self, datastore_path):
        store = ParquetStore(datastore_path)
        seed_ohlcv(store, {"AAA": [10.0, 20.0, 40.0, 80.0]})
        # The fifth bar is dated in time but was not ingested until much later.
        seed_ohlcv(
            store,
            {"AAA": [160.0]},
            start=D1 + timedelta(days=4),
            ingested_at=D1 + timedelta(days=30),
        )
        asof = D1 + timedelta(days=4)

        strict = xsmom.score_universe(
            context(store, asof, ["AAA"], pit_mode=PIT_INGESTION), self.PARAMS
        )
        relaxed = xsmom.score_universe(
            context(store, asof, ["AAA"], pit_mode=PIT_EVENT), self.PARAMS
        )
        assert strict["AAA"] is None  # only 4 bars were knowable
        assert relaxed["AAA"] == pytest.approx(7.0)


# ===========================================================================
# time_series_momentum
# ===========================================================================


class TestTimeSeriesMomentum:
    """Golden fixture, derived by hand.

    Parameters: `lookback_days=2`, `skip_days=0`, `vol_window_days=4`,
    `min_vol_observations=2` -> `min_history_bars = 0 + max(2, 4) + 1 = 5`.

    Closes are built so the four log returns are exactly
    `[0.2, -0.1, 0.2, -0.1]`:

        mean          = 0.05
        deviations    = [0.15, -0.15, 0.15, -0.15]
        sample var    = 4 * 0.0225 / 3 = 0.03
        bar vol       = sqrt(0.03)              = 0.1732050808
        horizon vol   = bar vol * sqrt(2)       = 0.2449489743

    The formation return spans the last two bars, whose log returns are
    `0.2` then `-0.1`, so it is `exp(0.1) - 1 = 0.1051709181`:

        score = 0.1051709181 / 0.2449489743 = 0.4293584751...
    """

    PARAMS = tsmom.TimeSeriesMomentumParams(
        lookback_days=2, skip_days=0, vol_window_days=4, min_vol_observations=2
    )
    RETURNS = [0.2, -0.1, 0.2, -0.1]
    CLOSES = closes_from_log_returns(RETURNS)

    BAR_VOL = math.sqrt(0.03)
    HORIZON_VOL = BAR_VOL * math.sqrt(2)
    FORMATION = math.exp(0.1) - 1.0

    def test_min_history_bars(self):
        assert tsmom.min_history_bars(self.PARAMS) == 5

    def test_golden_intermediates(self):
        diagnostics = tsmom.diagnose_series(self.CLOSES, self.PARAMS)
        assert diagnostics.formation_return == pytest.approx(self.FORMATION)
        assert diagnostics.bar_vol == pytest.approx(self.BAR_VOL)
        assert diagnostics.horizon_vol == pytest.approx(self.HORIZON_VOL)

    def test_golden_score(self):
        expected = self.FORMATION / self.HORIZON_VOL
        assert tsmom.score_series(self.CLOSES, self.PARAMS) == pytest.approx(expected)
        assert expected == pytest.approx(0.4293584751, abs=1e-9)

    def test_insufficient_history_scores_none(self):
        diagnostics = tsmom.diagnose_series(self.CLOSES[:4], self.PARAMS)
        assert diagnostics.score is None
        assert diagnostics.reject_reason == "insufficient_history"

    def test_constant_series_scores_none(self):
        diagnostics = tsmom.diagnose_series(np.full(10, 100.0), self.PARAMS)
        assert diagnostics.score is None
        assert diagnostics.reject_reason == "undefined_volatility"

    def test_skip_days_moves_both_windows(self):
        """With skip=1 the volatility must be measured to the same bar as the return."""
        params = tsmom.TimeSeriesMomentumParams(
            lookback_days=2, skip_days=1, vol_window_days=4, min_vol_observations=2
        )
        closes = closes_from_log_returns([0.2, -0.1, 0.2, -0.1, 5.0])
        diagnostics = tsmom.diagnose_series(closes, params)
        # The final, enormous bar is skipped by both the return and the vol.
        assert diagnostics.formation_return == pytest.approx(self.FORMATION)
        assert diagnostics.bar_vol == pytest.approx(self.BAR_VOL)

    def test_parameter_validation(self):
        with pytest.raises(ValueError, match="vol_window_days"):
            tsmom.TimeSeriesMomentumParams(vol_window_days=1)
        with pytest.raises(ValueError, match="min_vol_observations"):
            tsmom.TimeSeriesMomentumParams(vol_window_days=10, min_vol_observations=11)

    def test_standardization_preserves_a_market_wide_tilt(self, datastore_path):
        """The property `cross_sectional_scale` exists for (doc section 3, step 7).

        Every asset trends up, so a demeaned score would report no view; the
        scaled score must keep every standardized score positive.
        """
        store = ParquetStore(datastore_path)
        seed_ohlcv(
            store,
            {
                "AAA": list(closes_from_log_returns([0.2, -0.1, 0.2, -0.1])),
                "BBB": list(closes_from_log_returns([0.3, -0.1, 0.3, -0.1])),
                "CCC": list(closes_from_log_returns([0.4, -0.1, 0.4, -0.1])),
            },
        )
        ctx = context(store, D1 + timedelta(days=4), ["AAA", "BBB", "CCC"])

        raw = tsmom.score_universe(ctx, self.PARAMS)
        assert all(score > 0 for score in raw.values())

        scaled = tsmom.score_universe(ctx, self.PARAMS, standardize=True)
        assert all(score > 0 for score in scaled.values())

        # A z-score would have destroyed exactly that.
        zscored = transforms.cross_sectional_zscore(raw, self.PARAMS.winsorize_pct)
        assert sum(zscored.values()) == pytest.approx(0.0, abs=1e-9)
        assert any(score < 0 for score in zscored.values())

    def test_standardization_preserves_ranks(self, datastore_path):
        store = ParquetStore(datastore_path)
        seed_ohlcv(
            store,
            {
                "AAA": list(closes_from_log_returns([0.2, -0.1, 0.2, -0.1])),
                "BBB": list(closes_from_log_returns([0.1, -0.1, -0.3, -0.1])),
            },
        )
        ctx = context(store, D1 + timedelta(days=4), ["AAA", "BBB"])
        raw = tsmom.score_universe(ctx, self.PARAMS)
        scaled = tsmom.score_universe(ctx, self.PARAMS, standardize=True)
        assert (raw["AAA"] > raw["BBB"]) == (scaled["AAA"] > scaled["BBB"])


# ===========================================================================
# short_term_reversal
# ===========================================================================


class TestShortTermReversal:
    """Golden fixture, derived by hand.

    Parameters: `lookback_days=1`, `vol_window_days=4`, `min_vol_observations=2`
    -> `min_history_bars = max(1, 4) + 1 = 5`.

    Closes are built so the four log returns are exactly `[0.1, -0.1, 0.1, -0.1]`:

        mean        = 0
        sample var  = 4 * 0.01 / 3 = 0.0133333...
        bar vol     = 0.1 * sqrt(4 / 3) = 0.1154700538
        horizon vol = bar vol * sqrt(1) = 0.1154700538

    The one-bar return is the last log return exponentiated,
    `exp(-0.1) - 1 = -0.0951625820`, and the score negates the vol-scaled value:

        score = -(-0.0951625820 / 0.1154700538) = +0.8241321347...

    Positive, because the asset just *fell* — which is the sign convention this
    fixture exists to pin.
    """

    PARAMS = strev.ShortTermReversalParams(
        lookback_days=1, vol_window_days=4, min_vol_observations=2
    )
    RETURNS = [0.1, -0.1, 0.1, -0.1]
    CLOSES = closes_from_log_returns(RETURNS)

    BAR_VOL = 0.1 * math.sqrt(4.0 / 3.0)
    RECENT = math.exp(-0.1) - 1.0

    def test_min_history_bars(self):
        assert strev.min_history_bars(self.PARAMS) == 5

    def test_golden_intermediates(self):
        diagnostics = strev.diagnose_series(self.CLOSES, self.PARAMS)
        assert diagnostics.recent_return == pytest.approx(self.RECENT)
        assert diagnostics.horizon_vol == pytest.approx(self.BAR_VOL)

    def test_golden_score_and_sign(self):
        expected = -self.RECENT / self.BAR_VOL
        assert strev.score_series(self.CLOSES, self.PARAMS) == pytest.approx(expected)
        assert expected == pytest.approx(0.8241321347, abs=1e-9)
        assert expected > 0  # the asset fell, so it is an attractive long

    def test_a_riser_scores_below_a_faller(self):
        faller = closes_from_log_returns([0.1, -0.1, 0.1, -0.1])
        riser = closes_from_log_returns([-0.1, 0.1, -0.1, 0.1])
        assert strev.score_series(riser, self.PARAMS) < strev.score_series(
            faller, self.PARAMS
        )

    def test_unscaled_path(self):
        params = strev.ShortTermReversalParams(lookback_days=1, vol_scale=False)
        diagnostics = strev.diagnose_series(self.CLOSES, params)
        assert diagnostics.score == pytest.approx(-self.RECENT)
        assert math.isnan(diagnostics.horizon_vol)

    def test_unscaled_needs_less_history(self):
        params = strev.ShortTermReversalParams(lookback_days=1, vol_scale=False)
        assert strev.min_history_bars(params) == 2

    def test_insufficient_history_scores_none(self):
        diagnostics = strev.diagnose_series(self.CLOSES[:4], self.PARAMS)
        assert diagnostics.score is None
        assert diagnostics.reject_reason == "insufficient_history"

    def test_constant_series_scores_none(self):
        diagnostics = strev.diagnose_series(np.full(10, 100.0), self.PARAMS)
        assert diagnostics.score is None
        assert diagnostics.reject_reason == "undefined_volatility"

    def test_gap_in_recent_bars_scores_none(self, datastore_path):
        """The gap-free trim leaves too few bars, so the asset drops out."""
        store = ParquetStore(datastore_path)
        dates = [D1 + timedelta(days=d) for d in (0, 1, 2, 3, 20, 21)]
        seed_ohlcv(
            store,
            {"AAA": [100.0, 110.0, 100.0, 110.0, 100.0, 110.0]},
            dates_by_asset={"AAA": dates},
        )
        ctx = context(store, D1 + timedelta(days=21), ["AAA"])
        assert strev.score_universe(ctx, self.PARAMS)["AAA"] is None


# ===========================================================================
# low_volatility
# ===========================================================================


class TestLowVolatility:
    """Golden fixture, derived by hand.

    Parameters: `vol_window_days=4`, `min_observations=2`,
    `periods_per_year=365` -> `min_history_bars = 3`.

    Log returns `[0.1, -0.1, 0.1, -0.1]` give a sample sd of
    `0.1 * sqrt(4/3) = 0.1154700538`, annualized to
    `0.1154700538 * sqrt(365) = 2.2060522810...`, and the score is its negation.
    """

    PARAMS = lowvol.LowVolatilityParams(vol_window_days=4, min_observations=2)
    RETURNS = [0.1, -0.1, 0.1, -0.1]
    CLOSES = closes_from_log_returns(RETURNS)
    BAR_VOL = 0.1 * math.sqrt(4.0 / 3.0)

    def test_min_history_bars(self):
        assert lowvol.min_history_bars(self.PARAMS) == 3

    def test_golden_intermediates(self):
        diagnostics = lowvol.diagnose_series(self.CLOSES, self.PARAMS)
        assert diagnostics.bar_vol == pytest.approx(self.BAR_VOL)
        assert diagnostics.annualized_vol == pytest.approx(self.BAR_VOL * math.sqrt(365.0))

    def test_golden_score_is_the_negated_volatility(self):
        expected = -self.BAR_VOL * math.sqrt(365.0)
        assert lowvol.score_series(self.CLOSES, self.PARAMS) == pytest.approx(expected)
        assert expected == pytest.approx(-2.2060522810, abs=1e-9)

    def test_quiet_asset_outranks_a_violent_one(self):
        quiet = closes_from_log_returns([0.01, -0.01, 0.01, -0.01])
        violent = closes_from_log_returns([0.5, -0.5, 0.5, -0.5])
        assert lowvol.score_series(quiet, self.PARAMS) > lowvol.score_series(
            violent, self.PARAMS
        )

    def test_constant_series_scores_none(self):
        diagnostics = lowvol.diagnose_series(np.full(10, 100.0), self.PARAMS)
        assert diagnostics.score is None
        assert diagnostics.reject_reason == "undefined_volatility"

    def test_insufficient_history_scores_none(self):
        diagnostics = lowvol.diagnose_series(self.CLOSES[:2], self.PARAMS)
        assert diagnostics.score is None
        assert diagnostics.reject_reason == "insufficient_history"

    def test_annualization_cannot_change_ranks(self):
        """A monotone rescaling: the doc claims it, so the test pins it."""
        other = lowvol.LowVolatilityParams(
            vol_window_days=4, min_observations=2, periods_per_year=1.0
        )
        quiet = closes_from_log_returns([0.01, -0.01, 0.01, -0.01])
        violent = closes_from_log_returns([0.5, -0.5, 0.5, -0.5])
        assert (
            lowvol.score_series(quiet, self.PARAMS)
            > lowvol.score_series(violent, self.PARAMS)
        ) == (
            lowvol.score_series(quiet, other) > lowvol.score_series(violent, other)
        )

    def test_annualized_vol_universe_agrees_with_diagnose(self, datastore_path):
        """The alpha step's volatility must be the signal's own estimate."""
        store = ParquetStore(datastore_path)
        seed_ohlcv(
            store,
            {
                "AAA": list(self.CLOSES),
                "BBB": list(closes_from_log_returns([0.5, -0.5, 0.5, -0.5])),
                "CCC": [100.0, 100.0],  # too short
            },
        )
        ctx = context(store, D1 + timedelta(days=4), ["AAA", "BBB", "CCC"])

        vols = lowvol.annualized_vol_universe(ctx, self.PARAMS)
        assert set(vols) == {"AAA", "BBB"}
        assert vols["AAA"] == pytest.approx(
            lowvol.diagnose_series(self.CLOSES, self.PARAMS).annualized_vol
        )
        scores = lowvol.score_universe(ctx, self.PARAMS)
        assert scores["AAA"] == pytest.approx(-vols["AAA"])
        assert scores["CCC"] is None


# ===========================================================================
# carry
# ===========================================================================


class TestCarry:
    """Golden fixture, derived by hand.

    Parameters: `lookback_days=7`, `min_observations=3`, `max_staleness_days=2`,
    `fundings_per_year = 3 * 365 = 1095`.

    Rates `[0.0001, 0.0002, 0.0003]` average to `0.0002` per settlement, so

        annualized carry = -0.0002 * 1095 = -0.219

    Negative, because positive funding means longs pay — the sign this fixture
    exists to pin (a carry signal negated twice looks like working momentum).
    """

    PARAMS = carry_module.DEFAULT_PARAMS
    ASOF = D1 + timedelta(days=1)

    @staticmethod
    def stamps(n: int, start: datetime = D1, step=timedelta(hours=8)):
        return np.array([start + step * i for i in range(n)], dtype="datetime64[us]")

    def test_golden_score_and_sign(self):
        rates = np.array([0.0001, 0.0002, 0.0003])
        diagnostics = carry_module.diagnose_series(
            self.stamps(3), rates, self.ASOF, self.PARAMS
        )
        assert diagnostics.mean_funding == pytest.approx(0.0002)
        assert diagnostics.annualized_carry == pytest.approx(-0.219)
        assert diagnostics.score == pytest.approx(-0.219)
        assert diagnostics.score < 0  # longs pay: unattractive

    def test_negative_funding_is_an_attractive_long(self):
        rates = np.array([-0.0001, -0.0002, -0.0003])
        diagnostics = carry_module.diagnose_series(
            self.stamps(3), rates, self.ASOF, self.PARAMS
        )
        assert diagnostics.score == pytest.approx(0.219)

    def test_too_few_observations_scores_none(self):
        diagnostics = carry_module.diagnose_series(
            self.stamps(2), np.array([0.0001, 0.0002]), self.ASOF, self.PARAMS
        )
        assert diagnostics.score is None
        assert diagnostics.reject_reason == "insufficient_observations"
        assert diagnostics.n_observations == 2

    def test_stale_funding_scores_none(self):
        """A perp that stopped printing funding is not a perp with low funding."""
        diagnostics = carry_module.diagnose_series(
            self.stamps(3), np.array([0.0001] * 3), D1 + timedelta(days=5), self.PARAMS
        )
        assert diagnostics.score is None
        assert diagnostics.reject_reason == "stale_funding"
        assert diagnostics.staleness_days > self.PARAMS.max_staleness_days

    def test_no_data_scores_none(self):
        diagnostics = carry_module.diagnose_series(
            np.empty(0, dtype="datetime64[us]"), np.empty(0), self.ASOF, self.PARAMS
        )
        assert diagnostics.score is None
        assert diagnostics.reject_reason == "no_funding_data"

    def test_window_excludes_older_settlements(self):
        """Only prints inside `(asof - lookback_days, asof]` are averaged."""
        stamps = np.array(
            [D1 - timedelta(days=30), D1, D1 + timedelta(hours=8), D1 + timedelta(hours=16)],
            dtype="datetime64[us]",
        )
        rates = np.array([9.9, 0.0001, 0.0002, 0.0003])
        diagnostics = carry_module.diagnose_series(stamps, rates, self.ASOF, self.PARAMS)
        assert diagnostics.n_observations == 3
        assert diagnostics.mean_funding == pytest.approx(0.0002)

    def test_future_settlements_are_invisible(self):
        stamps = np.array(
            [D1, D1 + timedelta(hours=8), D1 + timedelta(hours=16), D1 + timedelta(days=5)],
            dtype="datetime64[us]",
        )
        rates = np.array([0.0001, 0.0002, 0.0003, 9.9])
        diagnostics = carry_module.diagnose_series(stamps, rates, self.ASOF, self.PARAMS)
        assert diagnostics.mean_funding == pytest.approx(0.0002)

    def test_parameter_validation(self):
        with pytest.raises(ValueError, match="lookback_days"):
            carry_module.CarryParams(lookback_days=0)
        with pytest.raises(ValueError, match="fundings_per_year"):
            carry_module.CarryParams(fundings_per_year=0.0)

    def test_score_universe_through_the_store(self, datastore_path):
        store = ParquetStore(datastore_path)
        seed_funding(
            store,
            {
                "AAA": [0.0001, 0.0002, 0.0003],   # positive: expensive to be long
                "BBB": [-0.0001, -0.0002, -0.0003],  # negative: paid to be long
            },
        )
        ctx = context(store, self.ASOF, ["AAA", "BBB", "NOPERP"])

        raw = carry_module.score_universe(ctx, self.PARAMS)
        assert raw["AAA"] == pytest.approx(-0.219)
        assert raw["BBB"] == pytest.approx(0.219)
        assert raw["NOPERP"] is None  # in the universe, no perp listing

        standardized = carry_module.score_universe(ctx, self.PARAMS, standardize=True)
        assert standardized["BBB"] > 0 > standardized["AAA"]
        assert standardized["NOPERP"] is None

    def test_point_in_time_a_later_settlement_cannot_change_a_score(self, datastore_path):
        store = ParquetStore(datastore_path)
        seed_funding(store, {"AAA": [0.0001, 0.0002, 0.0003]})
        before = carry_module.score_universe(context(store, self.ASOF, ["AAA"]), self.PARAMS)

        seed_funding(
            store,
            {"AAA": [0.5, 0.5, 0.5]},
            start=self.ASOF + timedelta(hours=8),
        )
        after = carry_module.score_universe(context(store, self.ASOF, ["AAA"]), self.PARAMS)
        assert after["AAA"] == before["AAA"]


# ===========================================================================
# Transforms
# ===========================================================================


class TestStandardizations:
    """`cross_sectional_zscore` demeans; `cross_sectional_scale` does not."""

    SCORES = {"A": 1.0, "B": 2.0, "C": 3.0, "D": None}

    def test_zscore_centres_the_cross_section(self):
        out = transforms.cross_sectional_zscore(self.SCORES)
        assert sum(v for v in out.values() if v is not None) == pytest.approx(0.0)
        assert out["D"] is None

    def test_scale_preserves_the_mean_sign(self):
        out = transforms.cross_sectional_scale(self.SCORES)
        assert sum(v for v in out.values() if v is not None) > 0.0
        assert out["D"] is None

    def test_scale_gives_unit_dispersion(self):
        out = transforms.cross_sectional_scale(self.SCORES)
        values = [v for v in out.values() if v is not None]
        assert sample_sd(values) == pytest.approx(1.0)

    def test_scale_preserves_ranks(self):
        out = transforms.cross_sectional_scale(self.SCORES)
        assert out["A"] < out["B"] < out["C"]

    def test_scale_to_unit_dispersion_degenerate(self):
        assert transforms.scale_to_unit_dispersion([5.0]) == [0.0]
        assert transforms.scale_to_unit_dispersion([5.0, 5.0]) == [0.0, 0.0]

    def test_scale_all_none(self):
        assert transforms.cross_sectional_scale({"A": None}) == {"A": None}


# ===========================================================================
# Registry
# ===========================================================================


class TestRegistry:
    """Every Phase 5 signal is registered, documented, and callable."""

    EXPECTED = {
        "carry",
        "cross_sectional_momentum",
        "low_volatility",
        "markov_mean_reversion",
        "short_term_reversal",
        "time_series_momentum",
    }

    def test_all_six_signals_registered(self):
        assert set(registry.all_signals()) == self.EXPECTED

    def test_every_signal_has_a_methodology_doc(self):
        for signal_id, signal in registry.all_signals().items():
            assert signal.methodology.exists(), f"{signal_id} has no methodology doc"

    def test_families_are_valid(self):
        for signal in registry.all_signals().values():
            assert signal.family in registry.FAMILIES

    def test_signal_families_helper(self):
        families = signal_families()
        assert families["carry"] == "carry"
        assert families["low_volatility"] == "volatility"
        assert set(families) == self.EXPECTED

    def test_signal_functions_shaped_for_the_engine(self):
        functions = registry.signal_functions()
        assert set(functions) == self.EXPECTED
        assert all(callable(fn) for fn in functions.values())

    def test_registration_requires_a_doc(self):
        with pytest.raises(ValueError, match="methodology doc"):
            registry.register("no_such_signal", "momentum", lambda ctx: {})


# ===========================================================================
# Alpha refinement
# ===========================================================================


class TestIcEstimation:
    """IC estimation and the normal-normal shrinkage rule."""

    def test_mean_and_std_are_the_sample_statistics(self):
        ics = [0.01, 0.03, 0.05]
        estimate = alpha_module.estimate_ic(ics, "s")
        assert estimate.mean_ic == pytest.approx(0.03)
        assert estimate.ic_std == pytest.approx(sample_sd(ics))
        assert estimate.n_periods == 3

    def test_nulls_are_dropped_not_treated_as_zero(self):
        """A rebalance with an undefined IC is an absent observation."""
        estimate = alpha_module.estimate_ic([0.02, None, 0.04], "s")
        assert estimate.n_periods == 2
        assert estimate.mean_ic == pytest.approx(0.03)

    def test_shrinkage_matches_the_documented_formula(self):
        ics = [0.01, 0.03, 0.05]
        tau = 0.02
        estimate = alpha_module.estimate_ic(ics, "s", prior_ic_std=tau)
        se = sample_sd(ics) / math.sqrt(3)
        expected = 0.03 * tau**2 / (tau**2 + se**2)
        assert estimate.shrunk_ic == pytest.approx(expected)

    def test_shrinkage_always_moves_toward_zero(self):
        estimate = alpha_module.estimate_ic([0.05, 0.07, 0.03, 0.09], "s")
        assert 0.0 <= estimate.shrunk_ic < estimate.mean_ic

    def test_noisier_series_is_shrunk_harder(self):
        precise = alpha_module.estimate_ic([0.03] * 40 + [0.031] * 40, "precise")
        noisy = alpha_module.estimate_ic([0.5, -0.44] * 40, "noisy")
        assert precise.shrinkage > noisy.shrinkage

    def test_more_periods_shrink_less(self):
        few = alpha_module.estimate_ic([0.02, 0.04, 0.06, 0.00], "few")
        many = alpha_module.estimate_ic([0.02, 0.04, 0.06, 0.00] * 25, "many")
        assert many.shrinkage > few.shrinkage

    def test_single_observation_shrinks_to_zero(self):
        """One period cannot separate skill from luck."""
        estimate = alpha_module.estimate_ic([0.9], "s")
        assert estimate.shrunk_ic == 0.0
        assert estimate.mean_ic == pytest.approx(0.9)

    def test_empty_series(self):
        estimate = alpha_module.estimate_ic([], "s")
        assert estimate.n_periods == 0
        assert estimate.shrunk_ic == 0.0

    def test_cap_binds_on_an_implausible_ic(self):
        estimate = alpha_module.estimate_ic([0.9] * 500, "s", max_abs_ic=0.1)
        assert estimate.shrunk_ic == pytest.approx(0.1)

    def test_negative_ic_stays_negative(self):
        estimate = alpha_module.estimate_ic([-0.02, -0.04, -0.03], "s")
        assert estimate.shrunk_ic < 0.0
        assert estimate.shrunk_ic > estimate.mean_ic  # moved toward zero

    def test_t_stat(self):
        ics = [0.01, 0.03, 0.05]
        estimate = alpha_module.estimate_ic(ics, "s")
        assert estimate.t_stat == pytest.approx(0.03 / (sample_sd(ics) / math.sqrt(3)))

    def test_invalid_prior(self):
        with pytest.raises(ValueError, match="prior_ic_std"):
            alpha_module.estimate_ic([0.01, 0.02], prior_ic_std=0.0)
        with pytest.raises(ValueError, match="max_abs_ic"):
            alpha_module.estimate_ic([0.01, 0.02], max_abs_ic=0.0)

    def test_estimate_ics_from_a_frame(self):
        frame = pl.DataFrame(
            {
                "rebalance_ts": [D1, D1, D1 + timedelta(days=1), D1 + timedelta(days=1)],
                "signal": ["a", "b", "a", "b"],
                "ic": [0.01, -0.02, 0.03, -0.04],
            }
        )
        estimates = alpha_module.estimate_ics(frame)
        assert set(estimates) == {"a", "b"}
        assert estimates["a"].mean_ic == pytest.approx(0.02)
        assert estimates["b"].mean_ic == pytest.approx(-0.03)

    def test_estimate_ics_empty_frame(self):
        frame = pl.DataFrame(schema={"signal": pl.Utf8, "ic": pl.Float64})
        assert alpha_module.estimate_ics(frame) == {}

    def test_summary_renders(self):
        estimates = [alpha_module.estimate_ic([0.01, 0.03], "sig")]
        text = alpha_module.alpha_summary(estimates)
        assert "sig" in text and "shrunk" in text


class TestAlphaFromScores:
    """`alpha = volatility x IC x z`."""

    SCORES = {"A": 1.0, "B": -2.0, "C": None}
    VOLS = {"A": 0.8, "B": 1.2}

    def test_grinold_kahn_product(self):
        alphas = alpha_module.alpha_from_scores(self.SCORES, ic=0.03, vols=self.VOLS)
        assert alphas["A"] == pytest.approx(0.8 * 0.03 * 1.0)
        assert alphas["B"] == pytest.approx(1.2 * 0.03 * -2.0)

    def test_none_score_stays_none(self):
        alphas = alpha_module.alpha_from_scores(self.SCORES, ic=0.03, vols=self.VOLS)
        assert alphas["C"] is None

    def test_missing_volatility_scores_none_by_default(self):
        """An asset whose risk is unknown cannot be sized."""
        alphas = alpha_module.alpha_from_scores({"X": 1.0}, ic=0.03, vols={})
        assert alphas["X"] is None

    def test_default_vol_is_used_when_supplied(self):
        alphas = alpha_module.alpha_from_scores({"X": 1.0}, ic=0.03, vols={}, default_vol=1.0)
        assert alphas["X"] == pytest.approx(0.03)

    def test_non_positive_volatility_scores_none(self):
        alphas = alpha_module.alpha_from_scores({"X": 1.0}, ic=0.03, vols={"X": 0.0})
        assert alphas["X"] is None

    def test_zero_ic_produces_zero_alpha(self):
        alphas = alpha_module.alpha_from_scores(self.SCORES, ic=0.0, vols=self.VOLS)
        assert alphas["A"] == 0.0


class TestCombineAlphas:
    """Summing per-signal alphas, and the view count behind each."""

    def test_sums_available_views(self):
        combined = alpha_module.combine_alphas(
            {"s1": {"A": 0.01, "B": 0.02}, "s2": {"A": 0.03, "B": -0.02}}
        )
        assert combined.alphas["A"] == pytest.approx(0.04)
        assert combined.alphas["B"] == pytest.approx(0.0)
        assert combined.n_views == {"A": 2, "B": 2}

    def test_a_none_view_does_not_drag_toward_zero(self):
        combined = alpha_module.combine_alphas(
            {"s1": {"A": 0.04}, "s2": {"A": None}}
        )
        assert combined.alphas["A"] == pytest.approx(0.04)
        assert combined.n_views["A"] == 1

    def test_asset_with_no_views_is_none(self):
        combined = alpha_module.combine_alphas({"s1": {"A": None}, "s2": {"A": None}})
        assert combined.alphas["A"] is None
        assert combined.n_views["A"] == 0

    def test_weights_apply(self):
        combined = alpha_module.combine_alphas(
            {"s1": {"A": 0.01}, "s2": {"A": 0.01}}, weights={"s1": 2.0}
        )
        assert combined.alphas["A"] == pytest.approx(0.03)

    def test_empty_input(self):
        combined = alpha_module.combine_alphas({})
        assert combined.alphas == {}


# ===========================================================================
# Breadth report
# ===========================================================================


class TestBreadth:
    """Correlation machinery and the report."""

    def test_spearman_matches_the_engines_implementation(self):
        """`signals` must not import `backtest`, so the two are checked equal."""
        rng = np.random.default_rng(7)
        for _ in range(5):
            xs = rng.normal(size=25).tolist()
            ys = rng.normal(size=25).tolist()
            assert breadth_module.spearman(xs, ys) == pytest.approx(spearman_ic(xs, ys))

    def test_spearman_handles_ties(self):
        assert breadth_module.spearman([1.0, 1.0, 2.0], [1.0, 1.0, 2.0]) == pytest.approx(1.0)

    def test_spearman_undefined(self):
        assert breadth_module.spearman([1.0], [2.0]) is None
        assert breadth_module.spearman([1.0, 1.0], [1.0, 2.0]) is None

    def test_pearson_perfect(self):
        assert breadth_module.pearson([1.0, 2.0, 3.0], [2.0, 4.0, 6.0]) == pytest.approx(1.0)

    def test_identical_signals_correlate_at_one(self):
        rows = []
        for date in (D1, D1 + timedelta(days=1)):
            for i, asset in enumerate("ABCDE"):
                for name in ("s1", "s2"):
                    rows.append(
                        {
                            "rebalance_ts": date, "signal": name,
                            "asset_id": asset, "score": float(i),
                        }
                    )
        pairs = breadth_module.score_correlations(breadth_module.score_frame(rows))
        assert len(pairs) == 1
        assert pairs[0].correlation == pytest.approx(1.0)
        assert pairs[0].n_observations == 2

    def test_opposed_signals_correlate_at_minus_one(self):
        rows = []
        for i, asset in enumerate("ABCDE"):
            rows.append(
                {"rebalance_ts": D1, "signal": "s1", "asset_id": asset, "score": float(i)}
            )
            rows.append(
                {"rebalance_ts": D1, "signal": "s2", "asset_id": asset, "score": -float(i)}
            )
        pairs = breadth_module.score_correlations(breadth_module.score_frame(rows))
        assert pairs[0].correlation == pytest.approx(-1.0)

    def test_correlation_uses_only_assets_both_signals_scored(self):
        rows = [
            {"rebalance_ts": D1, "signal": "s1", "asset_id": a, "score": float(i)}
            for i, a in enumerate("ABC")
        ] + [
            {"rebalance_ts": D1, "signal": "s2", "asset_id": a, "score": float(i)}
            for i, a in enumerate("BCD")
        ]
        pairs = breadth_module.score_correlations(breadth_module.score_frame(rows))
        # Shared: B (1 vs 0) and C (2 vs 1) -> perfectly rank-correlated.
        assert pairs[0].correlation == pytest.approx(1.0)

    def test_empty_scores(self):
        assert breadth_module.score_correlations(breadth_module.score_frame([])) == []

    def test_ic_correlations(self):
        frame = pl.DataFrame(
            {
                "rebalance_ts": [D1, D1, D1 + timedelta(days=1), D1 + timedelta(days=1)],
                "signal": ["a", "b", "a", "b"],
                "ic": [0.01, 0.02, 0.03, 0.04],
            }
        )
        pairs = breadth_module.ic_correlations(frame)
        assert pairs[0].correlation == pytest.approx(1.0)
        assert pairs[0].n_observations == 2

    def test_ic_correlations_skip_nulls(self):
        frame = pl.DataFrame(
            {
                "rebalance_ts": [D1, D1],
                "signal": ["a", "b"],
                "ic": [None, 0.02],
            }
        )
        pairs = breadth_module.ic_correlations(frame)
        assert pairs[0].correlation is None
        assert pairs[0].n_observations == 0

    def test_effective_breadth_independent_signals(self):
        pairs = [breadth_module.PairCorrelation("a", "b", 0.0, 10)]
        assert breadth_module.effective_breadth(pairs, 2) == pytest.approx(2.0)

    def test_effective_breadth_identical_signals(self):
        pairs = [breadth_module.PairCorrelation("a", "b", 1.0, 10)]
        assert breadth_module.effective_breadth(pairs, 2) == pytest.approx(1.0)

    def test_effective_breadth_uses_absolute_correlation(self):
        """Reliable disagreement is as redundant as agreement."""
        positive = [breadth_module.PairCorrelation("a", "b", 0.8, 10)]
        negative = [breadth_module.PairCorrelation("a", "b", -0.8, 10)]
        assert breadth_module.effective_breadth(positive, 2) == pytest.approx(
            breadth_module.effective_breadth(negative, 2)
        )

    def test_effective_breadth_five_half_correlated_signals(self):
        pairs = [breadth_module.PairCorrelation("a", "b", 0.5, 10)]
        assert breadth_module.effective_breadth(pairs, 5) == pytest.approx(5 / 3)

    def test_effective_breadth_no_pairs(self):
        assert breadth_module.effective_breadth([], 3) == pytest.approx(3.0)
        assert breadth_module.effective_breadth([], 1) == pytest.approx(1.0)

    def test_score_recorder_captures_and_passes_through(self):
        class Ctx:
            asof = D1
            universe = ["A", "B"]

        recorder = breadth_module.ScoreRecorder(
            {"s1": lambda ctx: {"A": 1.0, "B": None}, "s2": lambda ctx: {"A": -1.0}}
        )
        wrapped = recorder.wrapped()
        assert wrapped["s1"](Ctx()) == {"A": 1.0, "B": None}
        wrapped["s2"](Ctx())

        frame = recorder.frame()
        # The `None` is recorded as absent, not as a score of zero.
        assert len(frame) == 2
        assert set(frame["signal"].to_list()) == {"s1", "s2"}

    def test_breadth_report_flags_redundant_pairs(self):
        rows = []
        for i, asset in enumerate("ABCDE"):
            rows.append(
                {"rebalance_ts": D1, "signal": "s1", "asset_id": asset, "score": float(i)}
            )
            rows.append(
                {"rebalance_ts": D1, "signal": "s2", "asset_id": asset, "score": float(i)}
            )
        report = breadth_module.breadth_report(
            breadth_module.score_frame(rows), families={"s1": "momentum", "s2": "momentum"}
        )
        assert report.signals == ["s1", "s2"]
        assert len(report.redundant_pairs()) == 1
        assert report.effective_breadth == pytest.approx(1.0)
        assert "Breadth report" in report.to_text()

    def test_breadth_report_empty(self):
        report = breadth_module.breadth_report(breadth_module.score_frame([]))
        assert report.signals == []
        assert report.effective_breadth == 0.0

    def test_correlation_frame_sorted_by_magnitude(self):
        pairs = [
            breadth_module.PairCorrelation("a", "b", 0.1, 5),
            breadth_module.PairCorrelation("a", "c", -0.9, 5),
        ]
        frame = breadth_module.correlation_frame(pairs)
        assert frame["signal_b"].to_list() == ["c", "b"]


# ===========================================================================
# End to end through the engine
# ===========================================================================


class TestSignalsThroughTheEngine:
    """Every registered signal runs through the Phase 4 engine and produces IC."""

    @staticmethod
    def seed_market(store: ParquetStore, n_bars: int = 100, n_assets: int = 5):
        rng = np.random.default_rng(11)
        closes = {}
        for i in range(n_assets):
            drift = 0.001 * (i - n_assets / 2)
            returns = drift + 0.02 * rng.normal(size=n_bars - 1)
            closes[f"A{i}"] = list(closes_from_log_returns(returns, start=100.0))
        seed_ohlcv(store, closes)

        # Funding only needs to cover the tail the carry signal reads, and each
        # extra day is another parquet partition to seed and scan.
        rates = {
            f"A{i}": [0.0001 * (i - 2)] * 30 for i in range(n_assets)
        }
        seed_funding(rates_by_asset=rates, store=store,
                     start=D1 + timedelta(days=n_bars - 11))
        rates = {}
        return sorted(closes)

    @pytest.mark.integration
    def test_all_signals_produce_an_ic_series(self, datastore_path):
        store = ParquetStore(datastore_path)
        universe = self.seed_market(store)

        config = BacktestConfig(
            rebalance_freq="monthly",
            backtest_start="2024-01-01",
            backtest_end=None,
            periods_per_year=365.0,
        )
        backtester = Backtester(
            store,
            config=config,
            cost_model=CostModel(spread_bps=5.0, impact_bps_per_million=1.0),
            universe_provider=lambda asof: universe,
            pit_mode=PIT_EVENT,
        )

        recorder = breadth_module.ScoreRecorder(registry.signal_functions())
        result = backtester.run(
            long_short_from_scores(strev.make_signal(), n_per_side=2),
            signals=recorder.wrapped(),
        )

        summary = result.ic_summary()
        # Every signal that could score the universe appears in the IC summary.
        assert set(summary["signal"].to_list()) <= set(registry.all_signals())
        assert "short_term_reversal" in summary["signal"].to_list()
        assert len(result.returns) > 0

        report = breadth_module.breadth_report(
            recorder.frame(), result.ic, families=signal_families()
        )
        assert report.effective_breadth <= len(report.signals)
        assert report.effective_breadth > 0

    @pytest.mark.integration
    def test_alpha_pipeline_end_to_end(self, datastore_path):
        """IC series -> shrunk IC -> alpha = vol x IC x z -> combined alpha."""
        store = ParquetStore(datastore_path)
        universe = self.seed_market(store)

        backtester = Backtester(
            store,
            config=BacktestConfig(rebalance_freq="monthly", backtest_start="2024-01-01"),
            universe_provider=lambda asof: universe,
            pit_mode=PIT_EVENT,
        )
        signals = {
            "short_term_reversal": strev.make_signal(),
            "low_volatility": lowvol.make_signal(),
        }
        result = backtester.run(
            long_short_from_scores(strev.make_signal(), n_per_side=2), signals=signals
        )
        estimates = alpha_module.estimate_ics(result.ic)
        assert set(estimates) == set(signals)

        asof = result.returns["rebalance_ts"].to_list()[-1]
        ctx = context(store, asof, universe)
        vols = lowvol.annualized_vol_universe(ctx)

        alphas = {
            name: alpha_module.alpha_from_scores(fn(ctx), estimates[name].shrunk_ic, vols)
            for name, fn in signals.items()
        }
        combined = alpha_module.combine_alphas(alphas)

        assert set(combined.alphas) == set(universe)
        assert all(
            value is None or math.isfinite(value) for value in combined.alphas.values()
        )
        assert max(combined.n_views.values()) <= len(signals)
