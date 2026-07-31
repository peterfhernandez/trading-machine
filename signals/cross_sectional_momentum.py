"""Signal: cross_sectional_momentum (family: momentum).

See `signals/methodology/cross_sectional_momentum.md` for the full spec. The doc
is the spec; this module follows it.

Construction, in order:

1. Trim each asset's bars to the gap-free tail (`signals.bars`).
2. Formation return: the simple return over `lookback_days` bars ending
   `skip_days` bars before the last one.
3. Cross-sectionally winsorize then z-score (`standardize=True`).

**The skip window is the whole point of the "cross-sectional" qualifier.** The
most recent week of a price series is dominated by short-horizon reversal (see
`short_term_reversal`, which trades exactly that). Including it in a momentum
formation window mixes two signals with opposite signs into one number and
cancels part of both, which is why the last `skip_days` bars are excluded.

**Point-in-time.** All data arrives through `RebalanceContext`. This module
opens no files and makes no network calls.

**A missing score is not a neutral view.** Insufficient history, a gap in the
recent bars, or a non-positive base price all return `None`.
"""

from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace

import numpy as np

from logging_config import get_logger

from .bars import Series, close_series, trailing_return
from .transforms import cross_sectional_zscore

log = get_logger(__name__)

SIGNAL_ID = "cross_sectional_momentum"
FAMILY = "momentum"


@dataclass(frozen=True)
class CrossSectionalMomentumParams:
    """Parameters for the signal. See methodology doc Section 4 for rationale.

    Attributes:
        lookback_days: Length of the formation window, in bars.
        skip_days: Most recent bars excluded from the formation window.
        max_gap_days: Largest permitted gap between consecutive bars in the
            estimation window; a wider hole makes the "90-day return" span more
            than 90 days, so the asset is dropped instead.
        winsorize_pct: Percentile clipped from each tail of the cross-section
            before z-scoring.
        history_buffer_days: Extra calendar days requested from the store beyond
            the required bar count, so ordinary missing bars do not push an
            otherwise-eligible asset below the minimum.
    """

    lookback_days: int = 90
    skip_days: int = 7
    max_gap_days: int = 1
    winsorize_pct: float = 2.5
    history_buffer_days: int = 30

    def __post_init__(self) -> None:
        if self.lookback_days < 1:
            raise ValueError(f"lookback_days must be >= 1, got {self.lookback_days}")
        if self.skip_days < 0:
            raise ValueError(f"skip_days must be >= 0, got {self.skip_days}")
        if self.max_gap_days < 1:
            raise ValueError(f"max_gap_days must be >= 1, got {self.max_gap_days}")
        if not 0.0 <= self.winsorize_pct < 50.0:
            raise ValueError(f"winsorize_pct must be in [0, 50), got {self.winsorize_pct}")


DEFAULT_PARAMS = CrossSectionalMomentumParams()

PanelSource = Callable[[object], Mapping[str, Series]]
"""`ctx -> {asset_id: Series}` override, for research tooling that caches reads."""


def min_history_bars(params: CrossSectionalMomentumParams = DEFAULT_PARAMS) -> int:
    """Bars required to score an asset: the formation window plus the skip window."""
    return params.lookback_days + params.skip_days + 1


@dataclass(frozen=True)
class MomentumDiagnostics:
    """Everything behind one asset's score, for tests and research inspection.

    Attributes:
        score: The signal score (higher = more attractive long), or None.
        reject_reason: Why `score` is None; None when the asset was scored.
        formation_return: The raw formation-window return (NaN when unscored).
        n_bars: Bars available after the gap-free trim.
    """

    score: float | None
    reject_reason: str | None
    formation_return: float
    n_bars: int


def diagnose_series(
    closes: np.ndarray, params: CrossSectionalMomentumParams = DEFAULT_PARAMS
) -> MomentumDiagnostics:
    """Score one asset's close series and return the intermediate values.

    Reject reasons: `insufficient_history`, `non_positive_price`.
    """
    closes = np.asarray(closes, dtype=float)
    if closes.size < min_history_bars(params):
        return MomentumDiagnostics(None, "insufficient_history", float("nan"), closes.size)

    formation = trailing_return(closes, params.lookback_days, params.skip_days)
    if not np.isfinite(formation):
        return MomentumDiagnostics(None, "non_positive_price", float("nan"), closes.size)

    return MomentumDiagnostics(float(formation), None, float(formation), closes.size)


def score_series(
    closes: np.ndarray, params: CrossSectionalMomentumParams = DEFAULT_PARAMS
) -> float | None:
    """Score one asset's close series; None if it cannot be scored."""
    return diagnose_series(closes, params).score


def context_panel(
    ctx, params: CrossSectionalMomentumParams = DEFAULT_PARAMS
) -> dict[str, Series]:
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
    params: CrossSectionalMomentumParams = DEFAULT_PARAMS,
    standardize: bool = False,
    panel_source: PanelSource | None = None,
) -> dict[str, float | None]:
    """Score every asset in `ctx.universe` as of `ctx.asof`.

    Args:
        ctx: Point-in-time rebalance context.
        params: Signal parameters.
        standardize: Apply the cross-sectional winsorize + z-score. False
            returns raw formation returns, which is what rank IC is computed on
            (ranks are unchanged by standardizing).
        panel_source: Optional `ctx -> {asset_id: Series}` override. A caller
            supplying this takes responsibility for point-in-time filtering.

    Returns:
        `{asset_id: score | None}` for every asset in the universe. `None` means
        "no view" — not a neutral score.
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
        return cross_sectional_zscore(scores, params.winsorize_pct)
    return scores


def make_signal(
    params: CrossSectionalMomentumParams = DEFAULT_PARAMS,
    standardize: bool = True,
    panel_source: PanelSource | None = None,
) -> Callable[[object], dict[str, float | None]]:
    """Build a `ctx -> {asset_id: score}` function for the engine or registry."""

    def signal(ctx) -> dict[str, float | None]:
        return score_universe(
            ctx, params=params, standardize=standardize, panel_source=panel_source
        )

    return signal


def with_params(**overrides) -> CrossSectionalMomentumParams:
    """`DEFAULT_PARAMS` with fields overridden — for parameter sweeps."""
    return replace(DEFAULT_PARAMS, **overrides)
