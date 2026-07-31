"""Signal: time_series_momentum (family: momentum).

See `signals/methodology/time_series_momentum.md` for the full spec.

Construction, in order:

1. Trim each asset's bars to the gap-free tail (`signals.bars`).
2. Formation return: simple return over `lookback_days` bars ending `skip_days`
   bars before the last one — the same window as `cross_sectional_momentum`.
3. Divide by the asset's own realized volatility over the same horizon
   (`realized_vol * sqrt(lookback_days)`), giving the trend in units of the
   asset's own noise.
4. Cross-sectionally winsorize then **scale without demeaning**
   (`standardize=True`, via `transforms.cross_sectional_scale`).

**Step 4 is what makes this a different bet from `cross_sectional_momentum`.**
Z-scoring subtracts the cross-sectional mean, which is precisely the
market-wide trend this signal exists to express: if every asset has trended up,
a demeaned score says "no view" while a time-series signal should say "long".
Scaling to unit dispersion keeps the scores comparable in size without deleting
the tilt. Downstream, a book built from these scores is *not* dollar-neutral by
construction, which is the intended difference and is stated in the doc.

**Point-in-time.** All data arrives through `RebalanceContext`. This module
opens no files and makes no network calls.
"""

from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace

import numpy as np

from logging_config import get_logger

from .bars import Series, close_series, realized_vol, trailing_return
from .transforms import cross_sectional_scale

log = get_logger(__name__)

SIGNAL_ID = "time_series_momentum"
FAMILY = "momentum"


@dataclass(frozen=True)
class TimeSeriesMomentumParams:
    """Parameters for the signal. See methodology doc Section 4 for rationale.

    Attributes:
        lookback_days: Length of the formation window, in bars.
        skip_days: Most recent bars excluded from the formation window.
        vol_window_days: Trailing log returns used for the volatility scaling.
        min_vol_observations: Finite returns required before a volatility is
            trusted; fewer means the asset is unscored rather than scored with a
            volatility estimated from a handful of points.
        max_gap_days: Largest permitted gap between consecutive bars.
        winsorize_pct: Percentile clipped from each tail of the cross-section.
        history_buffer_days: Extra calendar days requested from the store.
    """

    lookback_days: int = 90
    skip_days: int = 0
    vol_window_days: int = 60
    min_vol_observations: int = 40
    max_gap_days: int = 1
    winsorize_pct: float = 2.5
    history_buffer_days: int = 30

    def __post_init__(self) -> None:
        if self.lookback_days < 1:
            raise ValueError(f"lookback_days must be >= 1, got {self.lookback_days}")
        if self.skip_days < 0:
            raise ValueError(f"skip_days must be >= 0, got {self.skip_days}")
        if self.vol_window_days < 2:
            raise ValueError(f"vol_window_days must be >= 2, got {self.vol_window_days}")
        if not 2 <= self.min_vol_observations <= self.vol_window_days:
            raise ValueError(
                f"min_vol_observations must be in [2, vol_window_days="
                f"{self.vol_window_days}], got {self.min_vol_observations}"
            )
        if self.max_gap_days < 1:
            raise ValueError(f"max_gap_days must be >= 1, got {self.max_gap_days}")
        if not 0.0 <= self.winsorize_pct < 50.0:
            raise ValueError(f"winsorize_pct must be in [0, 50), got {self.winsorize_pct}")


DEFAULT_PARAMS = TimeSeriesMomentumParams()

PanelSource = Callable[[object], Mapping[str, Series]]


def min_history_bars(params: TimeSeriesMomentumParams = DEFAULT_PARAMS) -> int:
    """Bars required: enough for the formation window *and* the volatility window.

    The volatility is measured over the returns ending where the formation
    window ends, so both windows sit behind the same `skip_days`.
    """
    return params.skip_days + max(params.lookback_days, params.vol_window_days) + 1


@dataclass(frozen=True)
class TimeSeriesMomentumDiagnostics:
    """Everything behind one asset's score.

    Attributes:
        score: The signal score (higher = more attractive long), or None.
        reject_reason: Why `score` is None; None when the asset was scored.
        formation_return: Raw formation-window return (NaN when unscored).
        bar_vol: Per-bar realized volatility (NaN when unscored).
        horizon_vol: `bar_vol * sqrt(lookback_days)` — the scaling denominator.
        n_bars: Bars available after the gap-free trim.
    """

    score: float | None
    reject_reason: str | None
    formation_return: float
    bar_vol: float
    horizon_vol: float
    n_bars: int


def _rejected(reason: str, n_bars: int, **kwargs) -> TimeSeriesMomentumDiagnostics:
    defaults = {
        "formation_return": float("nan"),
        "bar_vol": float("nan"),
        "horizon_vol": float("nan"),
    }
    return TimeSeriesMomentumDiagnostics(
        score=None, reject_reason=reason, n_bars=n_bars, **{**defaults, **kwargs}
    )


def diagnose_series(
    closes: np.ndarray, params: TimeSeriesMomentumParams = DEFAULT_PARAMS
) -> TimeSeriesMomentumDiagnostics:
    """Score one asset's close series and return the intermediate values.

    Reject reasons: `insufficient_history`, `non_positive_price`,
    `undefined_volatility`.
    """
    closes = np.asarray(closes, dtype=float)
    if closes.size < min_history_bars(params):
        return _rejected("insufficient_history", closes.size)

    formation = trailing_return(closes, params.lookback_days, params.skip_days)
    if not np.isfinite(formation):
        return _rejected("non_positive_price", closes.size)

    # Volatility over the same window end as the formation return, so the trend
    # and the noise it is measured against describe one stretch of history.
    vol_closes = closes if params.skip_days == 0 else closes[: closes.size - params.skip_days]
    bar_vol = realized_vol(vol_closes, params.vol_window_days, params.min_vol_observations)
    if not np.isfinite(bar_vol):
        return _rejected(
            "undefined_volatility", closes.size, formation_return=float(formation)
        )

    horizon_vol = float(bar_vol * np.sqrt(params.lookback_days))
    score = float(formation / horizon_vol)

    return TimeSeriesMomentumDiagnostics(
        score=score,
        reject_reason=None,
        formation_return=float(formation),
        bar_vol=float(bar_vol),
        horizon_vol=horizon_vol,
        n_bars=closes.size,
    )


def score_series(
    closes: np.ndarray, params: TimeSeriesMomentumParams = DEFAULT_PARAMS
) -> float | None:
    """Score one asset's close series; None if it cannot be scored."""
    return diagnose_series(closes, params).score


def context_panel(ctx, params: TimeSeriesMomentumParams = DEFAULT_PARAMS) -> dict[str, Series]:
    """Point-in-time close series per universe asset, read through `ctx`."""
    bars = min_history_bars(params)
    return close_series(
        ctx,
        lookback_days=bars + params.history_buffer_days,
        max_gap_days=params.max_gap_days,
        max_bars=bars,
    )


def score_universe(
    ctx,
    params: TimeSeriesMomentumParams = DEFAULT_PARAMS,
    standardize: bool = False,
    panel_source: PanelSource | None = None,
) -> dict[str, float | None]:
    """Score every asset in `ctx.universe` as of `ctx.asof`.

    Args:
        ctx: Point-in-time rebalance context.
        params: Signal parameters.
        standardize: Apply the cross-sectional winsorize + **scale** (no
            demeaning — see the module docstring).
        panel_source: Optional `ctx -> {asset_id: Series}` override.
    """
    panel = (panel_source or (lambda c: context_panel(c, params)))(ctx)

    scores: dict[str, float | None] = {}
    for asset_id in ctx.universe:
        series = panel.get(asset_id)
        if series is None:
            scores[asset_id] = None
            continue
        diagnostics = diagnose_series(series.column("close"), params)
        scores[asset_id] = diagnostics.score
        if diagnostics.reject_reason is not None:
            log.debug(
                "%s: no score for %s at %s (%s)",
                SIGNAL_ID, asset_id, ctx.asof.date(), diagnostics.reject_reason,
            )

    if standardize:
        return cross_sectional_scale(scores, params.winsorize_pct)
    return scores


def make_signal(
    params: TimeSeriesMomentumParams = DEFAULT_PARAMS,
    standardize: bool = True,
    panel_source: PanelSource | None = None,
) -> Callable[[object], dict[str, float | None]]:
    """Build a `ctx -> {asset_id: score}` function for the engine or registry."""

    def signal(ctx) -> dict[str, float | None]:
        return score_universe(
            ctx, params=params, standardize=standardize, panel_source=panel_source
        )

    return signal


def with_params(**overrides) -> TimeSeriesMomentumParams:
    """`DEFAULT_PARAMS` with fields overridden — for parameter sweeps."""
    return replace(DEFAULT_PARAMS, **overrides)
