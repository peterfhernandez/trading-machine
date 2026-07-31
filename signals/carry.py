"""Signal: carry (family: carry).

See `signals/methodology/carry.md` for the full spec.

Construction, in order:

1. Read the trailing `lookback_days` of `funding_rate` observations per asset,
   point-in-time (`signals.bars.dataset_series`).
2. Require `min_observations` of them, and require the most recent one to be no
   older than `max_staleness_days` — a stale funding print is a different
   number from a low one, and the two must not be confused.
3. Mean funding rate over the window, annualized by `fundings_per_year`.
4. Negate: perpetual funding is paid *by* longs when it is positive, so a low
   or negative rate is the attractive side to be long.
5. Cross-sectionally winsorize then z-score (`standardize=True`).

**This is the only signal so far that does not read `ohlcv_daily`,** which is
also its main failure mode: funding exists on perpetuals only, so an asset in
the universe with no perp listing scores `None` forever rather than
occasionally. That is correct behaviour (no view is not a neutral view) but it
means this signal's effective breadth is smaller than the universe size, and
the breadth report should be read with that in mind.

**Sign check worth doing out loud.** Positive funding = longs pay shorts =
being long is expensive = the signal should *dislike* it. Score = `-annualized
mean funding`. A negated-twice carry signal looks like a working momentum
signal for a while, which is exactly why the sign is pinned by a golden test.

**Point-in-time.** All data arrives through `RebalanceContext`. This module
opens no files and makes no network calls.
"""

from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
from datetime import datetime, timedelta

import numpy as np

from logging_config import get_logger

from .bars import Series, dataset_series
from .transforms import cross_sectional_zscore

log = get_logger(__name__)

SIGNAL_ID = "carry"
FAMILY = "carry"

FUNDING_DATASET = "funding_rate"

FUNDINGS_PER_DAY = 3.0
"""Binance and most venues settle perpetual funding every 8 hours."""


@dataclass(frozen=True)
class CarryParams:
    """Parameters for the signal. See methodology doc Section 4 for rationale.

    Attributes:
        lookback_days: Trailing window of funding observations averaged.
        min_observations: Funding prints required in the window before a score
            is emitted.
        max_staleness_days: How old the most recent funding print may be at the
            rebalance date. A perp that stopped printing funding is not a perp
            with low funding.
        fundings_per_year: Settlements per year used to annualize the mean rate.
            Annualization is a monotone rescaling, so it cannot change ranks —
            it exists so the raw score reads as a yield.
        winsorize_pct: Percentile clipped from each tail of the cross-section.
    """

    lookback_days: int = 7
    min_observations: int = 3
    max_staleness_days: int = 2
    fundings_per_year: float = FUNDINGS_PER_DAY * 365.0
    winsorize_pct: float = 2.5

    def __post_init__(self) -> None:
        if self.lookback_days < 1:
            raise ValueError(f"lookback_days must be >= 1, got {self.lookback_days}")
        if self.min_observations < 1:
            raise ValueError(f"min_observations must be >= 1, got {self.min_observations}")
        if self.max_staleness_days < 1:
            raise ValueError(
                f"max_staleness_days must be >= 1, got {self.max_staleness_days}"
            )
        if self.fundings_per_year <= 0:
            raise ValueError(f"fundings_per_year must be > 0, got {self.fundings_per_year}")
        if not 0.0 <= self.winsorize_pct < 50.0:
            raise ValueError(f"winsorize_pct must be in [0, 50), got {self.winsorize_pct}")


DEFAULT_PARAMS = CarryParams()

PanelSource = Callable[[object], Mapping[str, Series]]


@dataclass(frozen=True)
class CarryDiagnostics:
    """Everything behind one asset's score.

    Attributes:
        score: The signal score (higher = more attractive long), or None.
        reject_reason: Why `score` is None; None when the asset was scored.
        mean_funding: Mean funding rate per settlement over the window.
        annualized_carry: `-mean_funding * fundings_per_year` — the score, as a
            yield to the long side.
        n_observations: Funding prints used.
        staleness_days: Age of the most recent print at `asof`, in days.
    """

    score: float | None
    reject_reason: str | None
    mean_funding: float
    annualized_carry: float
    n_observations: int
    staleness_days: float


def _rejected(reason: str, n_observations: int, staleness_days: float) -> CarryDiagnostics:
    return CarryDiagnostics(
        score=None,
        reject_reason=reason,
        mean_funding=float("nan"),
        annualized_carry=float("nan"),
        n_observations=n_observations,
        staleness_days=staleness_days,
    )


def diagnose_series(
    event_ts: np.ndarray,
    funding_rates: np.ndarray,
    asof: datetime,
    params: CarryParams = DEFAULT_PARAMS,
) -> CarryDiagnostics:
    """Score one asset's funding history as of `asof`.

    Args:
        event_ts: Ascending funding settlement timestamps.
        funding_rates: Rates aligned to `event_ts` (per settlement, not annual).
        asof: Rebalance date; the window is `(asof - lookback_days, asof]`.
        params: Signal parameters.

    Reject reasons: `no_funding_data`, `insufficient_observations`,
    `stale_funding`.
    """
    stamps = np.asarray(event_ts, dtype="datetime64[us]")
    rates = np.asarray(funding_rates, dtype=float)
    if stamps.size == 0 or rates.size == 0:
        return _rejected("no_funding_data", 0, float("nan"))

    asof64 = np.datetime64(asof, "us")
    window_start = np.datetime64(asof - timedelta(days=params.lookback_days), "us")
    inside = (stamps > window_start) & (stamps <= asof64) & np.isfinite(rates)
    if not inside.any():
        return _rejected("no_funding_data", 0, float("nan"))

    used = rates[inside]
    latest = stamps[inside].max()
    staleness = float((asof64 - latest) / np.timedelta64(1, "D"))

    if used.size < params.min_observations:
        return _rejected("insufficient_observations", int(used.size), staleness)
    if staleness > params.max_staleness_days:
        return _rejected("stale_funding", int(used.size), staleness)

    mean_funding = float(used.mean())
    annualized = float(-mean_funding * params.fundings_per_year)

    return CarryDiagnostics(
        score=annualized,
        reject_reason=None,
        mean_funding=mean_funding,
        annualized_carry=annualized,
        n_observations=int(used.size),
        staleness_days=staleness,
    )


def context_panel(ctx, params: CarryParams = DEFAULT_PARAMS) -> dict[str, Series]:
    """Point-in-time funding series per universe asset, read through `ctx`.

    `max_gap_days` is deliberately not applied: funding settles on the venue's
    schedule, and a venue that skips a settlement has produced a sparse series,
    not a corrupted one. The staleness and observation-count guards in
    `diagnose_series` are what protect against a series with holes.
    """
    return dataset_series(
        ctx,
        FUNDING_DATASET,
        columns=["funding_rate"],
        lookback_days=params.lookback_days,
        max_gap_days=None,
    )


def score_universe(
    ctx,
    params: CarryParams = DEFAULT_PARAMS,
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
            log.debug(
                "%s: no score for %s at %s (no_funding_data)",
                SIGNAL_ID, asset_id, ctx.asof.date(),
            )
            continue
        diagnostics = diagnose_series(
            series.event_ts, series.column("funding_rate"), ctx.asof, params
        )
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
    params: CarryParams = DEFAULT_PARAMS,
    standardize: bool = True,
    panel_source: PanelSource | None = None,
) -> Callable[[object], dict[str, float | None]]:
    """Build a `ctx -> {asset_id: score}` function for the engine or registry."""

    def signal(ctx) -> dict[str, float | None]:
        return score_universe(
            ctx, params=params, standardize=standardize, panel_source=panel_source
        )

    return signal


def with_params(**overrides) -> CarryParams:
    """`DEFAULT_PARAMS` with fields overridden — for parameter sweeps."""
    return replace(DEFAULT_PARAMS, **overrides)
