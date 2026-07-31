"""Signal: low_volatility (family: volatility).

See `signals/methodology/low_volatility.md` for the full spec.

Construction, in order:

1. Trim each asset's bars to the gap-free tail (`signals.bars`).
2. Realized volatility: standard deviation (ddof=1) of the trailing
   `vol_window_days` daily log returns, annualized by
   `sqrt(periods_per_year)`.
3. Negate — low volatility is the attractive side.
4. Cross-sectionally winsorize then z-score (`standardize=True`).

**Why negate rather than rank ascending.** The engine's convention is that a
higher score is a larger long weight, and a signal that predicts with the wrong
sign must be negated *in the signal*, not read backwards downstream. Everything
that consumes scores — the IC series, `long_short_from_scores`, alpha
refinement — assumes the convention holds.

**The winsorization is doing real work here.** Realized volatility has a long
right tail (one liquidation cascade dominates a 60-day window), and clipping
before standardizing keeps a single blown-up asset from setting the scale of
the whole cross-section. Note that the tail is one-sided, so winsorizing a
volatility cross-section is not symmetric in effect even though the parameter
is: the upper clip binds far more often than the lower one.

**Point-in-time.** All data arrives through `RebalanceContext`. This module
opens no files and makes no network calls.
"""

from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace

import numpy as np

from config import BACKTEST_CONFIG
from logging_config import get_logger

from .bars import Series, close_series, realized_vol
from .transforms import cross_sectional_zscore

log = get_logger(__name__)

SIGNAL_ID = "low_volatility"
FAMILY = "volatility"


@dataclass(frozen=True)
class LowVolatilityParams:
    """Parameters for the signal. See methodology doc Section 4 for rationale.

    Attributes:
        vol_window_days: Trailing daily log returns used for the estimate.
        min_observations: Finite returns required before a volatility is
            trusted; an asset with fewer is unscored rather than scored from a
            handful of points.
        periods_per_year: Bars per year used to annualize (365 for daily crypto
            bars, matching `BACKTEST_CONFIG.periods_per_year`). Annualization is
            a monotone rescaling and so cannot change ranks or the standardized
            scores — it exists to make the raw diagnostics readable.
        max_gap_days: Largest permitted gap between consecutive bars.
        winsorize_pct: Percentile clipped from each tail of the cross-section.
        history_buffer_days: Extra calendar days requested from the store.
    """

    vol_window_days: int = 60
    min_observations: int = 40
    periods_per_year: float = BACKTEST_CONFIG.periods_per_year
    max_gap_days: int = 1
    winsorize_pct: float = 2.5
    history_buffer_days: int = 30

    def __post_init__(self) -> None:
        if self.vol_window_days < 2:
            raise ValueError(f"vol_window_days must be >= 2, got {self.vol_window_days}")
        if not 2 <= self.min_observations <= self.vol_window_days:
            raise ValueError(
                f"min_observations must be in [2, vol_window_days="
                f"{self.vol_window_days}], got {self.min_observations}"
            )
        if self.periods_per_year <= 0:
            raise ValueError(f"periods_per_year must be > 0, got {self.periods_per_year}")
        if self.max_gap_days < 1:
            raise ValueError(f"max_gap_days must be >= 1, got {self.max_gap_days}")
        if not 0.0 <= self.winsorize_pct < 50.0:
            raise ValueError(f"winsorize_pct must be in [0, 50), got {self.winsorize_pct}")


DEFAULT_PARAMS = LowVolatilityParams()

PanelSource = Callable[[object], Mapping[str, Series]]


def min_history_bars(params: LowVolatilityParams = DEFAULT_PARAMS) -> int:
    """Bars required: one more than the returns the estimate needs."""
    return params.min_observations + 1


@dataclass(frozen=True)
class LowVolatilityDiagnostics:
    """Everything behind one asset's score.

    Attributes:
        score: The signal score (higher = more attractive long), or None.
        reject_reason: Why `score` is None; None when the asset was scored.
        bar_vol: Per-bar realized volatility (NaN when unscored).
        annualized_vol: `bar_vol * sqrt(periods_per_year)` (NaN when unscored).
        n_bars: Bars available after the gap-free trim.
    """

    score: float | None
    reject_reason: str | None
    bar_vol: float
    annualized_vol: float
    n_bars: int


def diagnose_series(
    closes: np.ndarray, params: LowVolatilityParams = DEFAULT_PARAMS
) -> LowVolatilityDiagnostics:
    """Score one asset's close series and return the intermediate values.

    Reject reasons: `insufficient_history`, `undefined_volatility` (too few
    finite returns, or a constant price series — which is not a zero-risk asset
    but an unpriced one).
    """
    closes = np.asarray(closes, dtype=float)
    if closes.size < min_history_bars(params):
        return LowVolatilityDiagnostics(
            None, "insufficient_history", float("nan"), float("nan"), closes.size
        )

    bar_vol = realized_vol(closes, params.vol_window_days, params.min_observations)
    if not np.isfinite(bar_vol):
        return LowVolatilityDiagnostics(
            None, "undefined_volatility", float("nan"), float("nan"), closes.size
        )

    annualized = float(bar_vol * np.sqrt(params.periods_per_year))
    return LowVolatilityDiagnostics(
        score=float(-annualized),
        reject_reason=None,
        bar_vol=float(bar_vol),
        annualized_vol=annualized,
        n_bars=closes.size,
    )


def score_series(
    closes: np.ndarray, params: LowVolatilityParams = DEFAULT_PARAMS
) -> float | None:
    """Score one asset's close series; None if it cannot be scored."""
    return diagnose_series(closes, params).score


def annualized_vol_universe(
    ctx,
    params: LowVolatilityParams = DEFAULT_PARAMS,
    panel_source: PanelSource | None = None,
) -> dict[str, float]:
    """Annualized realized volatility per scorable asset, as of `ctx.asof`.

    This is the signal's raw estimate before the sign flip and standardization,
    exposed because `signals.alpha` needs exactly it: Grinold-Kahn's
    `alpha = volatility x IC x z` wants a per-asset volatility forecast, and the
    alternative — a second, subtly different volatility estimator living in the
    alpha module — is how two parts of a system quietly disagree about risk.
    """
    panel = (panel_source or (lambda c: context_panel(c, params)))(ctx)

    vols: dict[str, float] = {}
    for asset_id in ctx.universe:
        series = panel.get(asset_id)
        if series is None:
            continue
        diagnostics = diagnose_series(series.column("close"), params)
        if diagnostics.reject_reason is None:
            vols[asset_id] = diagnostics.annualized_vol
    return vols


def context_panel(ctx, params: LowVolatilityParams = DEFAULT_PARAMS) -> dict[str, Series]:
    """Point-in-time close series per universe asset, read through `ctx`."""
    bars = params.vol_window_days + 1
    return close_series(
        ctx,
        lookback_days=bars + params.history_buffer_days,
        max_gap_days=params.max_gap_days,
        max_bars=bars,
    )


def score_universe(
    ctx,
    params: LowVolatilityParams = DEFAULT_PARAMS,
    standardize: bool = False,
    panel_source: PanelSource | None = None,
) -> dict[str, float | None]:
    """Score every asset in `ctx.universe` as of `ctx.asof`."""
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
        return cross_sectional_zscore(scores, params.winsorize_pct)
    return scores


def make_signal(
    params: LowVolatilityParams = DEFAULT_PARAMS,
    standardize: bool = True,
    panel_source: PanelSource | None = None,
) -> Callable[[object], dict[str, float | None]]:
    """Build a `ctx -> {asset_id: score}` function for the engine or registry."""

    def signal(ctx) -> dict[str, float | None]:
        return score_universe(
            ctx, params=params, standardize=standardize, panel_source=panel_source
        )

    return signal


def with_params(**overrides) -> LowVolatilityParams:
    """`DEFAULT_PARAMS` with fields overridden — for parameter sweeps."""
    return replace(DEFAULT_PARAMS, **overrides)
