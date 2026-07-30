"""Tests for the markov_mean_reversion signal (Phase 5).

Covers the triad the methodology doc requires (Section 9):

1. **Golden fixture** — a 10-bar series whose returns, z-scores, quantile
   cutoffs, states, transition matrix, midpoints and final score are all derived
   by hand in `TestGoldenFixture`'s docstring and asserted exactly.
2. **Point-in-time** — a bar that arrives later cannot change a state, a
   transition matrix, or a score that was already computed; and a bar ingested
   after the rebalance is invisible under `pit_mode="ingestion"`.
3. **Insufficient history** — too few bars, a gap in the window, or too few
   observations of the current state all score `None`, never `0.0`.

Plus the cross-sectional transforms, the registry's methodology-doc rule, and
the equivalence of the cached research panel with the context read path.
"""

import math
from datetime import datetime, timedelta

import numpy as np
import polars as pl
import pytest

from backtest import PIT_EVENT, PIT_INGESTION, Backtester, CostModel, RebalanceContext
from backtest import engine as engine_module
from config import BacktestConfig
from datastore import ParquetStore
from loaders.schemas import OHLCV_SCHEMA
from signals import markov_mean_reversion as mmr
from signals import panel as panel_module
from signals import registry, transforms
from signals.panel import CachedClosePanel

D1 = datetime(2024, 1, 1)

# Fixture parameters small enough that every intermediate value is hand-checkable.
# min_zscore_obs = min(4, max(5, 2)) = 4; min_edge_obs = min(4, max(3, 2)) = 3;
# first_state_index = 1 + 3 + 2 = 6; min_history_bars = 6 + 3 + 1 = 10.
TINY = mmr.MarkovMeanReversionParams(
    n_states=3,
    return_horizon_days=1,
    state_window_days=4,
    matrix_lookback_days=3,
    min_obs_per_state=1,
    winsorize_pct=0.0,
)

# Closes: 100 compounded by 1.1, 0.9, 1.2, 0.8, 1.1, 0.9, 1.2, 0.8, 1.1 so the
# one-day returns are exactly +0.1, -0.1, +0.2, -0.2, +0.1, -0.1, +0.2, -0.2, +0.1.
FIXTURE_FACTORS = [1.1, 0.9, 1.2, 0.8, 1.1, 0.9, 1.2, 0.8, 1.1]


def fixture_closes() -> np.ndarray:
    closes = [100.0]
    for factor in FIXTURE_FACTORS:
        closes.append(closes[-1] * factor)
    return np.array(closes)


# Every trailing 4-return window in the fixture is a permutation of
# {+0.1, -0.1, +0.2, -0.2}: mean 0, sample variance 0.1/3, sd = sqrt(1/30).
# So z is the return times sqrt(30).
SD = math.sqrt(1.0 / 30.0)
Z_LO = 0.1 / SD  # 0.5477225575051661
Z_HI = 0.2 / SD  # 1.0954451150103321


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

    `open` is set to the previous bar's close so the engine's open-to-open
    accounting and the signal's close series are different arrays — a signal that
    accidentally read opens would give different numbers.
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
                    asset_id,
                    venue,
                    "1d",
                    event_ts,
                    ingested_at or event_ts + timedelta(days=ingested_offset_days),
                    open_px,
                    max(open_px, close_px),
                    min(open_px, close_px),
                    close_px,
                    1_000_000.0,
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


@pytest.fixture
def store(tmp_path):
    return ParquetStore(tmp_path)


def make_ctx(store, asof, universe, pit_mode=PIT_INGESTION) -> RebalanceContext:
    return RebalanceContext(
        asof=asof, universe=list(universe), store=store, venue="binance", pit_mode=pit_mode
    )


# ---------------------------------------------------------------------------
# Golden fixture
# ---------------------------------------------------------------------------

class TestGoldenFixture:
    """A 10-bar fixture with every number derived by hand.

    Parameters: `n_states=3`, `return_horizon_days=1`, `state_window_days=4`,
    `matrix_lookback_days=3`, `min_obs_per_state=1`.

    **Closes** (index 0..9), 100 compounded by the factors above::

        100, 110, 99, 118.8, 95.04, 104.544, 94.0896, 112.90752, 90.326016, 99.3586176

    **Returns** (h=1), index 1..9::

        +0.1, -0.1, +0.2, -0.2, +0.1, -0.1, +0.2, -0.2, +0.1

    **z-scores.** Each trailing 4-return window is a permutation of
    {+0.1, -0.1, +0.2, -0.2}: mean 0, variance (0.01+0.01+0.04+0.04)/3 = 0.1/3,
    sd = sqrt(1/30). So z = return * sqrt(30), and z is defined from index 4
    (the first index with 4 returns behind it)::

        idx:  0..3   4      5      6      7      8      9
        z:    NaN   -Z_HI  +Z_LO  -Z_LO  +Z_HI  -Z_HI  +Z_LO

    with Z_LO = 0.1*sqrt(30) = 0.5477225575, Z_HI = 0.2*sqrt(30) = 1.0954451150.

    **States** need 3 non-null z-scores in the trailing 4-bar window, so indices
    4 and 5 (1 and 2 available) are unclassified and index 6 is the first state —
    which is exactly `first_state_index`. Cutoffs are the 1/3 and 2/3 quantiles,
    linearly interpolated:

    - idx 6, window z[3..6] = {-Z_HI, +Z_LO, -Z_LO} sorted {-1.0954, -0.5477,
      +0.5477}: cutoffs at positions 2/3 and 4/3 -> -0.7303 and -0.1826.
      z = -Z_LO = -0.5477 is above the first cutoff only -> **state 1**.
    - idx 7, window {-Z_HI, +Z_LO, -Z_LO, +Z_HI} sorted {-1.0954, -0.5477,
      +0.5477, +1.0954}: cutoffs at positions 1 and 2 -> -0.5477 and +0.5477.
      z = +Z_HI is above both -> **state 2**.
    - idx 8, same four values, cutoffs -0.5477 / +0.5477, z = -Z_HI is below
      both -> **state 0**.
    - idx 9, same cutoffs, z = +Z_LO **equals** the upper cutoff, and a value on
      a cutoff falls in the higher state -> **state 2**.

    **Transition matrix** over the trailing `matrix_lookback_days + 1 = 4` bars,
    states[6..9] = [1, 2, 0, 2], so the transitions are 1->2, 2->0, 0->2::

        row 0: [0, 0, 1]   (1 observation)
        row 1: [0, 0, 1]   (1 observation)
        row 2: [1, 0, 0]   (1 observation)

    **Midpoints** — mean z per state over the same four bars: state 0 sees only
    idx 8 (-Z_HI), state 1 only idx 6 (-Z_LO), state 2 sees idx 7 and 9, so
    (Z_HI + Z_LO)/2 = 0.8215838363.

    **Score.** Current state 2, current z = +Z_LO. Row 2 puts all mass on state
    0, so expected_next_z = -Z_HI. Raw reversion = Z_LO - (-Z_HI) = +1.6431677
    (the model expects a fall), and the score is its negation::

        score = -(Z_LO + Z_HI) = -1.6431676725154982
    """

    def setup_method(self):
        self.diag = mmr.diagnose_series(fixture_closes(), TINY)

    def test_history_requirements(self):
        assert mmr.min_zscore_obs(TINY) == 4
        assert mmr.min_edge_obs(TINY) == 3
        assert mmr.first_state_index(TINY) == 6
        assert mmr.min_history_bars(TINY) == 10

    def test_zscores(self):
        expected = [None, None, None, None, -Z_HI, Z_LO, -Z_LO, Z_HI, -Z_HI, Z_LO]
        assert len(self.diag.z_scores) == 10
        for i, want in enumerate(expected):
            if want is None:
                assert np.isnan(self.diag.z_scores[i]), f"z[{i}] should be undefined"
            else:
                assert self.diag.z_scores[i] == pytest.approx(want, abs=1e-12)

    def test_states(self):
        # Indices 0..5 unclassified; the first state lands on first_state_index.
        assert self.diag.states.tolist() == [-1, -1, -1, -1, -1, -1, 1, 2, 0, 2]

    def test_value_on_a_cutoff_falls_in_the_higher_state(self):
        # idx 9: z == +Z_LO == the upper cutoff of its window.
        assert self.diag.z_scores[9] == pytest.approx(Z_LO, abs=1e-12)
        assert self.diag.states[9] == 2

    def test_transition_matrix(self):
        assert self.diag.matrix.tolist() == [
            [0.0, 0.0, 1.0],
            [0.0, 0.0, 1.0],
            [1.0, 0.0, 0.0],
        ]
        assert self.diag.row_counts.tolist() == [1.0, 1.0, 1.0]

    def test_midpoints_use_the_transition_lookback_window(self):
        assert self.diag.midpoints[0] == pytest.approx(-Z_HI, abs=1e-12)
        assert self.diag.midpoints[1] == pytest.approx(-Z_LO, abs=1e-12)
        assert self.diag.midpoints[2] == pytest.approx((Z_HI + Z_LO) / 2.0, abs=1e-12)

    def test_expected_next_state(self):
        assert self.diag.current_state == 2
        assert self.diag.current_z == pytest.approx(Z_LO, abs=1e-12)
        assert self.diag.expected_next_z == pytest.approx(-Z_HI, abs=1e-12)

    def test_score(self):
        assert self.diag.reject_reason is None
        assert self.diag.score == pytest.approx(-1.6431676725154982, abs=1e-12)
        assert mmr.score_series(fixture_closes(), TINY) == pytest.approx(
            -(Z_LO + Z_HI), abs=1e-12
        )

    def test_sign_convention_is_higher_equals_more_attractive(self):
        # The fixture's last bar is an up-move whose model expectation is a fall,
        # so the score must be negative (a short), not positive.
        assert self.diag.score < 0.0
        assert self.diag.score == pytest.approx(
            -(self.diag.current_z - self.diag.expected_next_z), abs=1e-12
        )

    def test_extra_history_is_discarded(self):
        # Only the last min_history_bars closes are used, so prepending older
        # bars must not move the score by a single bit.
        padded = np.concatenate([np.array([7.0, 13.0, 21.0, 55.0]), fixture_closes()])
        assert mmr.score_series(padded, TINY) == self.diag.score


# ---------------------------------------------------------------------------
# Point-in-time discipline
# ---------------------------------------------------------------------------

class TestPointInTime:
    """The signal cannot see a bar it could not have seen live."""

    def test_a_later_bar_cannot_change_an_earlier_state(self):
        closes = fixture_closes()
        z_before = mmr.rolling_return_zscores(closes, TINY)
        states_before = mmr.state_series(z_before, TINY)

        moved = closes.copy()
        moved[-1] *= 0.5  # only the most recent bar changes

        z_after = mmr.rolling_return_zscores(moved, TINY)
        states_after = mmr.state_series(z_after, TINY)

        np.testing.assert_array_equal(states_before[:-1], states_after[:-1])
        np.testing.assert_allclose(z_before[:-1], z_after[:-1], equal_nan=True)
        # The last bar's own state did change, i.e. the test is not vacuous.
        assert states_before[-1] != states_after[-1]

    def test_transition_matrix_reads_only_trailing_states(self):
        states = np.array([-1, -1, 0, 1, 2, 0, 1, 2, 2, 0])
        truncated, _ = mmr.transition_matrix(states[:7], TINY, as_of_idx=6)
        full, _ = mmr.transition_matrix(states, TINY, as_of_idx=6)
        np.testing.assert_array_equal(np.nan_to_num(truncated), np.nan_to_num(full))

    def test_state_midpoints_read_only_trailing_bars(self):
        z = np.array([np.nan, np.nan, 0.5, -0.5, 1.5, -1.5, 0.25, -0.25, 9.0, -9.0])
        states = np.array([-1, -1, 1, 0, 2, 0, 1, 0, 2, 0])
        trailing = mmr.state_midpoints(z[:7], states[:7], TINY, as_of_idx=6)
        full = mmr.state_midpoints(z, states, TINY, as_of_idx=6)
        np.testing.assert_allclose(trailing, full, equal_nan=True)

    def test_future_bars_in_the_store_do_not_change_a_past_score(self, store):
        """The real look-ahead risk: bars dated after `asof` must be invisible."""
        closes = fixture_closes().tolist()
        seed_ohlcv(store, {"AAA": closes})
        asof = D1 + timedelta(days=len(closes) - 1)

        before = mmr.score_universe(make_ctx(store, asof, ["AAA"]), params=TINY)

        # Append five more bars *after* asof, ingested after asof too.
        future_dates = [asof + timedelta(days=i) for i in range(1, 6)]
        seed_ohlcv(
            store,
            {"AAA": [500.0, 10.0, 900.0, 5.0, 1_000.0]},
            dates_by_asset={"AAA": future_dates},
        )

        after = mmr.score_universe(make_ctx(store, asof, ["AAA"]), params=TINY)
        assert before["AAA"] == pytest.approx(-(Z_LO + Z_HI), abs=1e-12)
        assert after == before

    def test_ingestion_mode_hides_late_arriving_bars(self, store):
        """A bar dated before `asof` but ingested after it is not knowable yet."""
        closes = fixture_closes().tolist()
        # Every bar ingested a year late: strict mode sees nothing.
        seed_ohlcv(store, {"AAA": closes}, ingested_at=D1 + timedelta(days=365))
        asof = D1 + timedelta(days=len(closes) - 1)

        strict = mmr.score_universe(make_ctx(store, asof, ["AAA"], PIT_INGESTION), params=TINY)
        assert strict == {"AAA": None}

        # Event mode is the documented, explicit relaxation for backfilled history.
        relaxed = mmr.score_universe(make_ctx(store, asof, ["AAA"], PIT_EVENT), params=TINY)
        assert relaxed["AAA"] == pytest.approx(-(Z_LO + Z_HI), abs=1e-12)

    def test_signal_reads_closes_not_opens(self, store):
        """`seed_ohlcv` offsets opens by one bar, so the two series differ."""
        closes = fixture_closes().tolist()
        seed_ohlcv(store, {"AAA": closes})
        asof = D1 + timedelta(days=len(closes) - 1)
        ctx = make_ctx(store, asof, ["AAA"])

        panel = mmr.context_close_panel(ctx, TINY)
        np.testing.assert_allclose(panel["AAA"], np.array(closes))


# ---------------------------------------------------------------------------
# Missing scores are None, never 0.0
# ---------------------------------------------------------------------------

class TestUnscorableAssets:
    def test_insufficient_history_scores_none(self):
        short = fixture_closes()[:-1]  # 9 bars, one short of the 10 required
        diag = mmr.diagnose_series(short, TINY)
        assert diag.score is None
        assert diag.reject_reason == "insufficient_history"

    def test_default_params_need_244_bars(self):
        params = mmr.DEFAULT_PARAMS
        assert mmr.min_history_bars(params) == 244
        rng = np.random.default_rng(11)
        walk = 100.0 * np.exp(np.cumsum(rng.normal(0.0, 0.02, 243)))
        assert mmr.score_series(walk, params) is None
        longer = 100.0 * np.exp(np.cumsum(rng.normal(0.0, 0.02, 244)))
        assert mmr.score_series(longer, params) is not None

    def test_sparse_transition_row_scores_none(self):
        # The fixture's current state has exactly one observed transition.
        strict = mmr.MarkovMeanReversionParams(
            n_states=3, return_horizon_days=1, state_window_days=4,
            matrix_lookback_days=3, min_obs_per_state=2,
        )
        diag = mmr.diagnose_series(fixture_closes(), strict)
        assert diag.row_counts[diag.current_state] == 1.0
        assert diag.score is None
        assert diag.reject_reason == "sparse_transition_row"

    def test_flat_series_is_unclassifiable(self):
        # Zero dispersion means no z-score, so no state and no score.
        diag = mmr.diagnose_series(np.full(20, 100.0), TINY)
        assert diag.score is None
        assert diag.reject_reason == "state_unclassified"

    def test_a_gap_inside_the_window_scores_none(self, store):
        closes = fixture_closes().tolist()
        dates = [D1 + timedelta(days=i) for i in range(len(closes))]
        dates[5:] = [d + timedelta(days=4) for d in dates[5:]]  # 5-day hole
        seed_ohlcv(store, {"AAA": closes}, dates_by_asset={"AAA": dates})

        ctx = make_ctx(store, dates[-1], ["AAA"])
        assert mmr.score_universe(ctx, params=TINY) == {"AAA": None}

    def test_a_gap_older_than_the_window_is_harmless(self):
        closes = np.concatenate([np.array([1.0, 2.0]), fixture_closes()])
        dates = [D1 + timedelta(days=i) for i in range(len(closes))]
        dates[2:] = [d + timedelta(days=30) for d in dates[2:]]  # gap before the window
        tail = mmr.contiguous_tail(np.array(dates, dtype="datetime64[us]"), closes, TINY)
        assert mmr.score_series(tail, TINY) == pytest.approx(-(Z_LO + Z_HI), abs=1e-12)

    def test_unknown_asset_scores_none_rather_than_zero(self, store):
        seed_ohlcv(store, {"AAA": fixture_closes().tolist()})
        asof = D1 + timedelta(days=9)
        scores = mmr.score_universe(make_ctx(store, asof, ["AAA", "GHOST"]), params=TINY)
        assert scores["GHOST"] is None
        assert scores["AAA"] is not None

    def test_empty_store_scores_every_asset_none(self, store):
        ctx = make_ctx(store, D1, ["AAA", "BBB"])
        assert mmr.score_universe(ctx, params=TINY) == {"AAA": None, "BBB": None}


# ---------------------------------------------------------------------------
# Parameters
# ---------------------------------------------------------------------------

class TestParams:
    @pytest.mark.parametrize(
        "overrides",
        [
            {"n_states": 1},
            {"return_horizon_days": 0},
            {"state_window_days": 3, "n_states": 5},
            {"matrix_lookback_days": 0},
            {"min_obs_per_state": 0},
            {"max_gap_days": 0},
            {"winsorize_pct": 50.0},
            {"winsorize_pct": -1.0},
        ],
    )
    def test_invalid_params_rejected(self, overrides):
        with pytest.raises(ValueError):
            mmr.MarkovMeanReversionParams(**overrides)

    def test_defaults_match_the_methodology_doc(self):
        params = mmr.DEFAULT_PARAMS
        assert (params.n_states, params.return_horizon_days) == (5, 5)
        assert (params.state_window_days, params.matrix_lookback_days) == (60, 180)
        assert (params.min_obs_per_state, params.max_gap_days) == (10, 1)
        assert params.winsorize_pct == 2.5

    def test_with_params_overrides_defaults(self):
        params = mmr.with_params(n_states=3)
        assert params.n_states == 3
        assert params.matrix_lookback_days == mmr.DEFAULT_PARAMS.matrix_lookback_days

    def test_more_states_needs_no_more_history_but_thinner_rows(self):
        # Sanity check on the history formula: it grows with the windows, not k.
        few = mmr.with_params(n_states=3)
        many = mmr.with_params(n_states=7)
        assert mmr.min_history_bars(few) == mmr.min_history_bars(many)


# ---------------------------------------------------------------------------
# Quantile helper (must match numpy, since the golden cutoffs assume it)
# ---------------------------------------------------------------------------

class TestRowQuantiles:
    def test_matches_numpy_nanquantile(self):
        rng = np.random.default_rng(5)
        for _ in range(50):
            rows, width = 6, 12
            block = rng.normal(size=(rows, width))
            for r in range(rows):
                block[r, : rng.integers(0, 5)] = np.nan
            counts = np.isfinite(block).sum(axis=1)
            keep = counts >= 2
            if not keep.any():
                continue
            quantiles = np.linspace(0.0, 1.0, 6)[1:-1]
            mine = mmr._row_quantiles(block[keep], counts[keep], quantiles)
            reference = np.nanquantile(block[keep], quantiles, axis=1).T
            np.testing.assert_allclose(mine, reference.reshape(mine.shape))

    def test_two_states_produce_a_single_cutoff(self):
        params = mmr.MarkovMeanReversionParams(
            n_states=2, return_horizon_days=1, state_window_days=4,
            matrix_lookback_days=3, min_obs_per_state=1,
        )
        states = mmr.state_series(mmr.rolling_return_zscores(fixture_closes(), params), params)
        assert set(states[states >= 0].tolist()) <= {0, 1}


# ---------------------------------------------------------------------------
# Cross-sectional transforms
# ---------------------------------------------------------------------------

class TestTransforms:
    def test_zscore_is_mean_zero_unit_sd(self):
        out = transforms.zscore([1.0, 2.0, 3.0, 4.0])
        assert sum(out) == pytest.approx(0.0)
        # Sample sd (ddof=1) of [1,2,3,4] is sqrt(5/3); z = (x - 2.5)/sqrt(5/3).
        assert out[0] == pytest.approx(-1.5 / math.sqrt(5.0 / 3.0))

    def test_zscore_of_a_degenerate_cross_section_is_all_zeros(self):
        assert transforms.zscore([3.0, 3.0, 3.0]) == [0.0, 0.0, 0.0]
        assert transforms.zscore([7.0]) == [0.0]
        assert transforms.zscore([]) == []

    def test_winsorize_clips_to_percentile_bounds(self):
        values = [-100.0, 1.0, 2.0, 3.0, 4.0, 5.0, 100.0]
        clipped = transforms.winsorize(values, 25.0)
        # 25th/75th percentiles of the sorted sample are 1.5 and 4.5.
        assert clipped == [1.5, 1.5, 2.0, 3.0, 4.0, 4.5, 4.5]

    def test_winsorize_matches_numpy_percentiles(self):
        rng = np.random.default_rng(9)
        values = rng.normal(size=25).tolist()
        clipped = transforms.winsorize(values, 10.0)
        lo, hi = np.percentile(values, [10.0, 90.0])
        np.testing.assert_allclose(clipped, np.clip(values, lo, hi))

    def test_winsorize_zero_pct_is_identity(self):
        assert transforms.winsorize([1.0, 50.0, -3.0], 0.0) == [1.0, 50.0, -3.0]

    def test_winsorize_rejects_impossible_pct(self):
        with pytest.raises(ValueError):
            transforms.winsorize([1.0, 2.0, 3.0], 50.0)

    def test_cross_sectional_zscore_preserves_none(self):
        out = transforms.cross_sectional_zscore(
            {"A": 1.0, "B": None, "C": 3.0, "D": 5.0}, winsorize_pct=0.0
        )
        assert out["B"] is None
        assert list(out) == ["A", "B", "C", "D"]
        assert out["A"] + out["C"] + out["D"] == pytest.approx(0.0)

    def test_none_does_not_influence_the_statistics(self):
        with_none = transforms.cross_sectional_zscore({"A": 1.0, "B": 2.0, "C": None})
        without = transforms.cross_sectional_zscore({"A": 1.0, "B": 2.0})
        assert with_none["A"] == pytest.approx(without["A"])
        assert with_none["B"] == pytest.approx(without["B"])

    def test_all_none_stays_all_none(self):
        assert transforms.cross_sectional_zscore({"A": None, "B": None}) == {
            "A": None, "B": None
        }

    def test_winsorization_runs_before_standardizing(self):
        """Clipping first keeps the outlier out of the mean and the sd.

        With one blown-up score left in, the standard deviation it inflates
        squashes every other asset into a narrow band; clipping it first leaves
        the rest of the cross-section legible. Standardizing first and clipping
        afterwards could not do that, since the sd would already be inflated.
        """
        # Ten well-behaved scores plus one blown-up one. At 10% of eleven points
        # the upper bound lands exactly on the largest inlier, so the outlier is
        # clipped down to it.
        scores = {f"A{i:02d}": float(i) for i in range(1, 11)} | {"OUT": 10_000.0}
        unclipped = transforms.cross_sectional_zscore(scores, winsorize_pct=0.0)
        clipped = transforms.cross_sectional_zscore(scores, winsorize_pct=10.0)

        assert unclipped["A10"] - unclipped["A01"] < 0.01  # squashed by the outlier's sd
        assert clipped["A10"] - clipped["A01"] > 2.0  # legible again
        # The clipped outlier ties with the largest inlier, and nothing reorders.
        assert clipped["OUT"] == pytest.approx(clipped["A10"])
        inliers = [clipped[f"A{i:02d}"] for i in range(1, 11)]
        assert inliers == sorted(inliers)


# ---------------------------------------------------------------------------
# Universe scoring and standardization
# ---------------------------------------------------------------------------

class TestScoreUniverse:
    @pytest.fixture
    def seeded(self, store):
        """Three assets: one scorable, one whose history is too short, one flat."""
        closes = fixture_closes().tolist()
        seed_ohlcv(
            store,
            {"AAA": closes, "SHORT": closes[:6], "FLAT": [100.0] * len(closes)},
        )
        return store, D1 + timedelta(days=len(closes) - 1)

    def test_raw_scores(self, seeded):
        store, asof = seeded
        scores = mmr.score_universe(
            make_ctx(store, asof, ["AAA", "SHORT", "FLAT"]), params=TINY
        )
        assert scores["AAA"] == pytest.approx(-(Z_LO + Z_HI), abs=1e-12)
        assert scores["SHORT"] is None
        assert scores["FLAT"] is None

    def test_standardize_applies_the_cross_sectional_step(self, store):
        closes = fixture_closes().tolist()
        # A second scorable asset with a different pattern, so the cross-section
        # has two distinct raw scores to standardize.
        other = [50.0]
        for factor in [0.9, 1.1, 0.8, 1.2, 0.9, 1.1, 0.8, 1.2, 0.9]:
            other.append(other[-1] * factor)
        seed_ohlcv(store, {"AAA": closes, "BBB": other})
        asof = D1 + timedelta(days=len(closes) - 1)
        ctx = make_ctx(store, asof, ["AAA", "BBB"])

        raw = mmr.score_universe(ctx, params=TINY, standardize=False)
        standardized = mmr.score_universe(ctx, params=TINY, standardize=True)

        scored = [a for a, s in raw.items() if s is not None]
        assert len(scored) == 2
        # Standardizing is a monotone rescaling: it preserves the ordering.
        assert sorted(scored, key=lambda a: raw[a]) == sorted(
            scored, key=lambda a: standardized[a]
        )
        assert sum(standardized[a] for a in scored) == pytest.approx(0.0)

    def test_scores_cover_the_universe_exactly(self, seeded):
        store, asof = seeded
        universe = ["FLAT", "AAA", "GHOST"]
        assert list(mmr.score_universe(make_ctx(store, asof, universe), params=TINY)) == universe

    def test_duplicate_ingestions_keep_the_latest(self, store):
        """Append-only means a re-backfilled bar exists twice; use the newer copy."""
        closes = fixture_closes().tolist()
        seed_ohlcv(store, {"AAA": closes}, ingested_at=D1)
        wrong = closes.copy()
        wrong[-1] = 1.0  # a bad first copy of the final bar...
        seed_ohlcv(store, {"AAA": wrong[-1:]},
                   dates_by_asset={"AAA": [D1 + timedelta(days=9)]}, ingested_at=D1)
        # ...re-ingested later with the correct value.
        seed_ohlcv(store, {"AAA": closes[-1:]},
                   dates_by_asset={"AAA": [D1 + timedelta(days=9)]},
                   ingested_at=D1 + timedelta(days=1))

        ctx = make_ctx(store, D1 + timedelta(days=9), ["AAA"])
        panel = mmr.context_close_panel(ctx, TINY)
        assert panel["AAA"][-1] == pytest.approx(closes[-1])
        assert len(panel["AAA"]) == 10

    def test_make_signal_standardizes_by_default(self, seeded):
        store, asof = seeded
        ctx = make_ctx(store, asof, ["AAA", "FLAT"])
        signal = mmr.make_signal(params=TINY)
        scores = signal(ctx)
        assert scores["FLAT"] is None
        # A single scored asset standardizes to 0.0 (a degenerate cross-section).
        assert scores["AAA"] == 0.0


# ---------------------------------------------------------------------------
# Cached research panel
# ---------------------------------------------------------------------------

class TestCachedClosePanel:
    def test_pit_mode_constants_match_the_engine(self):
        assert panel_module.PIT_INGESTION == engine_module.PIT_INGESTION
        assert panel_module.PIT_EVENT == engine_module.PIT_EVENT
        assert panel_module.PIT_MODES == engine_module.PIT_MODES

    def test_rejects_unknown_pit_mode(self, store):
        seed_ohlcv(store, {"AAA": fixture_closes().tolist()})
        with pytest.raises(ValueError, match="pit_mode"):
            CachedClosePanel(store, params=TINY, pit_mode="whenever")

    @pytest.mark.parametrize("pit_mode", [PIT_INGESTION, PIT_EVENT])
    def test_matches_the_context_read_path(self, store, pit_mode):
        """The cache is an optimization, so it must agree bar for bar."""
        closes = fixture_closes().tolist()
        seed_ohlcv(
            store,
            {
                "AAA": closes * 2,
                "BBB": [c * 2.0 for c in reversed(closes * 2)],
                "SHORT": closes[:4],
            },
        )

        cached = CachedClosePanel(store, params=TINY, pit_mode=pit_mode)
        universe = ["AAA", "BBB", "SHORT", "GHOST"]

        # Day 13 scores both real assets, days 9 and 16 score a mix of scores and
        # Nones — so the comparison covers agreement on both.
        for day in (9, 13, 16):
            ctx = make_ctx(store, D1 + timedelta(days=day), universe, pit_mode)
            direct = mmr.score_universe(ctx, params=TINY)
            through_cache = mmr.score_universe(ctx, params=TINY, panel_source=cached)
            assert through_cache == direct, f"day {day} diverged"
            assert any(s is not None for s in direct.values()), f"day {day} compared nothing"

    def test_matches_the_context_path_with_late_ingestion(self, store):
        """Half the bars ingested late: both paths must hide the same ones."""
        closes = fixture_closes().tolist()
        seed_ohlcv(store, {"AAA": closes[:5]}, ingested_at=D1)
        seed_ohlcv(
            store,
            {"AAA": closes[5:]},
            dates_by_asset={"AAA": [D1 + timedelta(days=i) for i in range(5, 10)]},
            ingested_at=D1 + timedelta(days=90),
        )

        cached = CachedClosePanel(store, params=TINY, pit_mode=PIT_INGESTION)
        for day in (4, 9, 95):
            ctx = make_ctx(store, D1 + timedelta(days=day), ["AAA"], PIT_INGESTION)
            assert mmr.score_universe(ctx, params=TINY, panel_source=cached) == (
                mmr.score_universe(ctx, params=TINY)
            )

    def test_empty_store_yields_an_empty_cache(self, store):
        cached = CachedClosePanel(store, params=TINY)
        assert cached.bars == {}
        panel = cached(make_ctx(store, D1, ["AAA"]))
        assert list(panel) == ["AAA"]
        assert panel["AAA"].size == 0

    def test_bars_older_than_the_lookback_are_out_of_reach(self, store):
        """Both paths bound the read, so a stale asset is unscorable on both."""
        closes = fixture_closes().tolist()
        seed_ohlcv(store, {"AAA": closes})
        stale_asof = D1 + timedelta(days=200)
        cached = CachedClosePanel(store, params=TINY, pit_mode=PIT_INGESTION)
        ctx = make_ctx(store, stale_asof, ["AAA"], PIT_INGESTION)

        assert mmr.score_universe(ctx, params=TINY) == {"AAA": None}
        assert mmr.score_universe(ctx, params=TINY, panel_source=cached) == {"AAA": None}


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

class TestRegistry:
    def test_the_signal_is_registered(self):
        signal = registry.get(mmr.SIGNAL_ID)
        assert signal.family == "reversal"
        assert signal.methodology.exists()

    def test_methodology_doc_exists_for_every_registered_signal(self):
        for signal_id, signal in registry.all_signals().items():
            assert signal.methodology.exists(), f"{signal_id} has no methodology doc"

    def test_registering_without_a_doc_is_refused(self, tmp_path):
        with pytest.raises(ValueError, match="methodology"):
            registry.register(
                signal_id="not_a_signal",
                family="reversal",
                score_fn=lambda ctx: {},
                methodology=tmp_path / "missing.md",
            )
        assert "not_a_signal" not in registry.all_signals()

    def test_unknown_family_is_refused(self):
        with pytest.raises(ValueError, match="family"):
            registry.register(
                signal_id=mmr.SIGNAL_ID, family="vibes", score_fn=lambda ctx: {}
            )

    def test_signal_functions_are_shaped_for_the_backtester(self):
        functions = registry.signal_functions([mmr.SIGNAL_ID])
        assert callable(functions[mmr.SIGNAL_ID])

    def test_unknown_signal_id_raises(self):
        with pytest.raises(KeyError):
            registry.get("no_such_signal")


# ---------------------------------------------------------------------------
# End-to-end through the engine
# ---------------------------------------------------------------------------

class TestEngineIntegration:
    """The signal must run through the Phase 4 engine without special handling.

    These runs read the signal's closes through `CachedClosePanel` rather than
    per-rebalance context reads. That is purely to keep the tests quick — a
    context read reopens every date partition, and a walk-forward run does one
    per rebalance. `TestCachedClosePanel` is what establishes the two paths agree,
    and `test_cached_panel_reproduces_the_engine_run` re-checks it end to end.
    """

    N_BARS = 20

    @pytest.fixture
    def seeded(self, store):
        rng = np.random.default_rng(4)
        closes = {
            asset_id: (
                100.0 * np.exp(np.cumsum(rng.normal(0.0, 0.05, self.N_BARS)))
            ).tolist()
            for asset_id in ("AAA", "BBB", "CCC")
        }
        closes["FLAT"] = [50.0] * self.N_BARS  # never scorable
        seed_ohlcv(store, closes)  # one append, so one parquet file per date
        return store

    def _backtester(self, store):
        config = BacktestConfig(
            rebalance_freq="daily",
            backtest_start="2024-01-01",
            backtest_end=None,
            initial_cash=100_000.0,
            spread_bps=10.0,
            impact_bps_per_million=0.0,
            periods_per_year=365.0,
        )
        return Backtester(
            store,
            config=config,
            cost_model=CostModel(spread_bps=10.0, impact_bps_per_million=0.0),
        )

    def _signal(self, store, panel_source=None):
        return mmr.make_signal(
            params=TINY,
            standardize=True,
            panel_source=panel_source
            or CachedClosePanel(store, params=TINY, pit_mode=PIT_INGESTION),
        )

    def test_produces_an_ic_series(self, seeded):
        from backtest import long_short_from_scores

        signal = self._signal(seeded)
        result = self._backtester(seeded).run(
            long_short_from_scores(signal, n_per_side=1),
            signals={mmr.SIGNAL_ID: signal},
        )

        assert len(result.returns) > 0
        summary = result.ic_summary()
        assert summary["signal"].to_list() == [mmr.SIGNAL_ID]
        assert -1.0 <= summary["mean_ic"][0] <= 1.0
        assert summary["n_periods"][0] > 0

    def test_none_scores_never_become_positions(self, seeded):
        """An unscorable asset must be absent from the book, not held at zero."""
        from backtest import long_short_from_scores

        result = self._backtester(seeded).run(
            long_short_from_scores(self._signal(seeded), n_per_side=1)
        )
        held = set(result.weights["asset_id"].to_list())
        assert "FLAT" not in held
        assert held  # the book is not empty, so the assertion above means something

    @pytest.mark.integration
    def test_cached_panel_reproduces_the_engine_run(self, seeded):
        """Slow by design: one full run per point-in-time read path."""
        from backtest import long_short_from_scores

        plain = mmr.make_signal(params=TINY, standardize=True)
        cached = self._signal(seeded)

        direct = self._backtester(seeded).run(long_short_from_scores(plain, n_per_side=1))
        through_cache = self._backtester(seeded).run(long_short_from_scores(cached, n_per_side=1))

        assert len(direct.returns) > 0
        assert direct.returns["net_return"].to_list() == pytest.approx(
            through_cache.returns["net_return"].to_list()
        )
