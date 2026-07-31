"""Signal: short_term_reversal (family: reversal).

See `signals/methodology/short_term_reversal.md` for the full spec.

Construction, in order:

1. Trim each asset's bars to the gap-free tail (`signals.bars`).
2. Recent return: the simple return over the last `lookback_days` bars.
3. Divide by the asset's own volatility over that horizon
   (`realized_vol * sqrt(lookback_days)`) when `vol_scale` is set, so a 10%
   week means something different for a quiet asset than for a violent one.
4. Negate — a large recent gain is an unattractive long.
5. Cross-sectionally winsorize then z-score (`standardize=True`).

**Volatility scaling is on by default and it matters here more than anywhere
else.** Without it the signal is mechanically a short-the-high-vol-assets bet:
the biggest raw weekly moves belong to the most volatile names whatever the
direction of the market, so the extremes of the cross-section would be selected
by volatility rather than by overshoot.

**Relationship to `markov_mean_reversion`.** Both are reversal-family and both
trade overshoot, so they are expected to correlate; the breadth report
(`signals.breadth`) is what decides whether both earn their place. This one is
deliberately the simple, transparent version — no state discretization, no
transition matrix — so a divergence between them is informative rather than
just noise.

**Point-in-time.** All data arrives through `RebalanceContext`. This module
opens no files and makes no network calls.
"""

from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace

import numpy as np

from logging_config import get_logger

from .bars import Series, close_series, realized_vol, trailing_return
from .transforms import cross_sectional_zscore

log = get_logger(__name__)

SIGNAL_ID = "short_term_reversal"
FAMILY = "reversal"


@dataclass(frozen=True)
class ShortTermReversalParams:
    """Parameters for the signal. See methodology doc Section 4 for rationale.

    Attributes:
        lookback_days: Length of the recent-return window, in bars.
        vol_scale: Divide the recent return by the asset's own volatility over
            the same horizon.
        vol_window_days: Trailing log returns used for that volatility.
        min_vol_observations: Finite returns required before a volatility is
            trusted.
        max_gap_days: Largest permitted gap between consecutive bars.
        winsorize_pct: Percentile clipped from each tail of the cross-section.
        history_buffer_days: Extra calendar days requested from the store.
    """

    lookback_days: int = 5
    vol_scale: bool = True
    vol_window_days: int = 30
    min_vol_observations: int = 20
    max_gap_days: int = 1
    winsorize_pct: float = 2.5
    history_buffer_days: int = 30

    def __post_init__(self) -> None:
        if self.lookback_days < 1:
            raise ValueError(f"lookback_days must be >= 1, got {self.lookback_days}")
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


DEFAULT_PARAMS = ShortTermReversalParams()

PanelSource = Callable[[object], Mapping[str, Series]]


def min_history_bars(params: ShortTermReversalParams = DEFAULT_PARAMS) -> int:
    """Bars required: the return window, and the volatility window when scaling."""
    windows = [params.lookback_days]
    if params.vol_scale:
        windows.append(params.vol_window_days)
    return max(windows) + 1


@dataclass(frozen=True)
class ReversalDiagnostics:
    """Everything behind one asset's score.

    Attributes:
        score: The signal score (higher = more attractive long), or None.
        reject_reason: Why `score` is None; None when the asset was scored.
        recent_return: The raw recent return (NaN when unscored).
        horizon_vol: Volatility over the same horizon, or NaN when not scaling.
        n_bars: Bars available after the gap-free trim.
    """

    score: float | None
    reject_reason: str | None
    recent_return: float
    horizon_vol: float
    n_bars: int


def diagnose_series(
    closes: np.ndarray, params: ShortTermReversalParams = DEFAULT_PARAMS
) -> ReversalDiagnostics:
    """Score one asset's close series and return the intermediate values.

    Reject reasons: `insufficient_history`, `non_positive_price`,
    `undefined_volatility`.
    """
    closes = np.asarray(closes, dtype=float)
    if closes.size < min_history_bars(params):
        return ReversalDiagnostics(
            None, "insufficient_history", float("nan"), float("nan"), closes.size
        )

    recent = trailing_return(closes, params.lookback_days)
    if not np.isfinite(recent):
        return ReversalDiagnostics(
            None, "non_positive_price", float("nan"), float("nan"), closes.size
        )

    horizon_vol = float("nan")
    if params.vol_scale:
        bar_vol = realized_vol(closes, params.vol_window_days, params.min_vol_observations)
        if not np.isfinite(bar_vol):
            return ReversalDiagnostics(
                None, "undefined_volatility", float(recent), float("nan"), closes.size
            )
        horizon_vol = float(bar_vol * np.sqrt(params.lookback_days))
        raw = recent / horizon_vol
    else:
        raw = recent

    return ReversalDiagnostics(
        score=float(-raw),
        reject_reason=None,
        recent_return=float(recent),
        horizon_vol=horizon_vol,
        n_bars=closes.size,
    )


def score_series(
    closes: np.ndarray, params: ShortTermReversalParams = DEFAULT_PARAMS
) -> float | None:
    """Score one asset's close series; None if it cannot be scored."""
    return diagnose_series(closes, params).score


def context_panel(ctx, params: ShortTermReversalParams = DEFAULT_PARAMS) -> dict[str, Series]:
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
    params: ShortTermReversalParams = DEFAULT_PARAMS,
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
    params: ShortTermReversalParams = DEFAULT_PARAMS,
    standardize: bool = True,
    panel_source: PanelSource | None = None,
) -> Callable[[object], dict[str, float | None]]:
    """Build a `ctx -> {asset_id: score}` function for the engine or registry."""

    def signal(ctx) -> dict[str, float | None]:
        return score_universe(
            ctx, params=params, standardize=standardize, panel_source=panel_source
        )

    return signal


def with_params(**overrides) -> ShortTermReversalParams:
    """`DEFAULT_PARAMS` with fields overridden — for parameter sweeps."""
    return replace(DEFAULT_PARAMS, **overrides)
