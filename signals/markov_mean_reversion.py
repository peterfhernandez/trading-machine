"""Signal: markov_mean_reversion (family: reversal).

See `signals/methodology/markov_mean_reversion.md` for the full spec —
hypothesis, construction rationale, parameter choices, and known failure modes.
The doc is the spec; this module follows it. If they disagree the doc is stale
and must be fixed in the same change.

Construction, in order:

1. Rolling `return_horizon_days` return per asset.
2. Standardize that return against its own trailing `state_window_days` window
   -> a z-score.
3. Discretize the z-score into `n_states` states using quantile cutoffs from the
   same trailing window (so the cutoffs are point-in-time, not fitted on the
   whole sample).
4. Count observed one-step state transitions over the trailing
   `matrix_lookback_days` and normalize rows into probabilities.
5. Expected next-state z = transition-probability-weighted mean of the state
   midpoints, where a midpoint is the mean z observed in that state **over the
   same trailing window the matrix was counted on**.
6. Raw reversion = `current_z - expected_next_z`, negated so higher = more
   attractive long (engine convention).
7. Cross-sectionally winsorize then z-score across the universe
   (`standardize=True`, via `signals.transforms`).

**Fixed estimation window.** Every score is computed from exactly the last
`min_history_bars(params)` bars and no more, so a score does not depend on how
much history an asset happens to have, and the transition matrix and the state
midpoints are estimated over one shared window rather than two different ones.
Assets with fewer bars than that score `None`.

**Point-in-time.** All data arrives through `RebalanceContext`, so
`event_ts <= asof` always holds and `ingested_ts <= asof` holds under the default
`pit_mode="ingestion"`. Within the window every derived series is causal: the
z-score, the quantile cutoffs and the state at bar `i` use only bars `<= i`, so a
bar that arrives later can never change a state already classified. This module
opens no files and makes no network calls.

**A missing score is not a neutral view.** Insufficient history, a gap in the
bars, or too few observations of the current state all return `None`, which drops
the asset from the cross-section rather than parking it at average.
"""

import logging
from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace

import numpy as np
import polars as pl

from .transforms import cross_sectional_zscore

logger = logging.getLogger(__name__)

SIGNAL_ID = "markov_mean_reversion"
FAMILY = "reversal"

UNCLASSIFIED = -1
"""State index for a bar whose state could not be determined (too little trailing data)."""

_EPS = 1e-12


@dataclass(frozen=True)
class MarkovMeanReversionParams:
    """Parameters for the signal. See methodology doc Section 4 for rationale.

    Attributes:
        n_states: Number of z-score states (k). More states express more
            asymmetry but split the same history into thinner per-state samples.
        return_horizon_days: Horizon of the rolling return whose overshoot is
            being measured.
        state_window_days: Trailing window used to standardize the return and to
            set the state quantile cutoffs. Kept distinct from
            `return_horizon_days`: making them equal leaves only
            `state_window_days` heavily-overlapping observations to estimate both
            a mean/variance and `n_states - 1` cutoffs.
        matrix_lookback_days: Trailing window of transitions used for both the
            transition matrix and the state midpoints.
        min_obs_per_state: Minimum transitions observed *out of the current
            state* within the lookback before a score is emitted. Guards the
            failure mode in doc Section 8: rare extreme states are exactly the
            states the signal leans on, and exactly the ones with the fewest
            observations.
        max_gap_days: Largest permitted gap between consecutive bars in the
            estimation window. State classification silently assumes evenly
            spaced bars, so a hole in the history is a correctness problem, not
            a data-volume problem.
        winsorize_pct: Percentile clipped from each tail of the cross-section
            before z-scoring (construction step 7).
        history_buffer_days: Extra calendar days requested from the store beyond
            the required bar count, so that ordinary missing bars do not push an
            otherwise-eligible asset below the minimum.
    """

    n_states: int = 5
    return_horizon_days: int = 5
    state_window_days: int = 60
    matrix_lookback_days: int = 180
    min_obs_per_state: int = 10
    max_gap_days: int = 1
    winsorize_pct: float = 2.5
    history_buffer_days: int = 30

    def __post_init__(self) -> None:
        if self.n_states < 2:
            raise ValueError(f"n_states must be >= 2, got {self.n_states}")
        if self.return_horizon_days < 1:
            raise ValueError(f"return_horizon_days must be >= 1, got {self.return_horizon_days}")
        if self.state_window_days < self.n_states:
            raise ValueError(
                f"state_window_days ({self.state_window_days}) must be >= n_states "
                f"({self.n_states}); fewer observations than states cannot set cutoffs"
            )
        if self.matrix_lookback_days < 1:
            raise ValueError(f"matrix_lookback_days must be >= 1, got {self.matrix_lookback_days}")
        if self.min_obs_per_state < 1:
            raise ValueError(f"min_obs_per_state must be >= 1, got {self.min_obs_per_state}")
        if self.max_gap_days < 1:
            raise ValueError(f"max_gap_days must be >= 1, got {self.max_gap_days}")
        if not 0.0 <= self.winsorize_pct < 50.0:
            raise ValueError(f"winsorize_pct must be in [0, 50), got {self.winsorize_pct}")


DEFAULT_PARAMS = MarkovMeanReversionParams()


# ---------------------------------------------------------------------------
# History requirements
# ---------------------------------------------------------------------------

def min_zscore_obs(params: MarkovMeanReversionParams) -> int:
    """Non-null returns required in the trailing window before a z-score is defined.

    Half the window, floored at 5 so a mean and variance rest on more than a
    couple of points, and capped at the window itself (a short window then
    demands it be completely full).
    """
    return min(params.state_window_days, max(5, params.state_window_days // 2))


def min_edge_obs(params: MarkovMeanReversionParams) -> int:
    """Non-null z-scores required in the trailing window before cutoffs are set.

    At least one observation per state, and otherwise the same half-window rule
    as `min_zscore_obs`.
    """
    return min(params.state_window_days, max(params.n_states, params.state_window_days // 2))


def first_state_index(params: MarkovMeanReversionParams) -> int:
    """Index of the earliest bar in the window that can be assigned a state.

    A return needs `return_horizon_days` bars behind it, a z-score needs
    `min_zscore_obs` returns, and a cutoff needs `min_edge_obs` z-scores.
    """
    return (
        params.return_horizon_days
        + (min_zscore_obs(params) - 1)
        + (min_edge_obs(params) - 1)
    )


def min_history_bars(params: MarkovMeanReversionParams) -> int:
    """Bars required to score an asset.

    Enough that every bar in the transition lookback window is classifiable:
    `first_state_index` warm-up bars, then `matrix_lookback_days` transitions.
    """
    return first_state_index(params) + params.matrix_lookback_days + 1


# ---------------------------------------------------------------------------
# Per-asset series (pure numpy; no I/O, no context)
# ---------------------------------------------------------------------------

def rolling_return_zscores(
    closes: np.ndarray, params: MarkovMeanReversionParams
) -> np.ndarray:
    """Trailing-standardized rolling returns, same length as `closes`.

    `z[i]` standardizes the `return_horizon_days` return ending at bar `i`
    against the mean and sample standard deviation (ddof=1) of the returns over
    the trailing `state_window_days` bars *ending at i*. Entries that cannot be
    computed causally — not enough bars behind them, or a degenerate trailing
    standard deviation — are NaN.
    """
    h = params.return_horizon_days
    w = params.state_window_days
    n = closes.size

    returns = np.full(n, np.nan)
    if n > h:
        prior = closes[:-h]
        with np.errstate(divide="ignore", invalid="ignore"):
            ratio = np.where(prior > 0.0, closes[h:] / prior - 1.0, np.nan)
        returns[h:] = np.where(np.isfinite(ratio), ratio, np.nan)

    z = np.full(n, np.nan)
    if n < w:
        return z

    # windows[j] == returns[j : j + w], i.e. the window ending at bar i = j + w - 1.
    windows = np.lib.stride_tricks.sliding_window_view(returns, w)
    present = np.isfinite(windows)
    counts = present.sum(axis=1)

    usable = counts >= min_zscore_obs(params)
    if not usable.any():
        return z

    filled = np.where(present, windows, 0.0)
    means = np.zeros(windows.shape[0])
    np.divide(filled.sum(axis=1), counts, out=means, where=counts > 0)

    deviations = np.where(present, windows - means[:, None], 0.0)
    variances = np.zeros(windows.shape[0])
    np.divide(
        (deviations**2).sum(axis=1), counts - 1, out=variances, where=counts > 1
    )
    sds = np.sqrt(variances)

    ends = np.arange(w - 1, n)
    valid = usable & (sds > _EPS) & np.isfinite(returns[ends])
    z[ends[valid]] = (returns[ends][valid] - means[valid]) / sds[valid]
    return z


def _row_quantiles(
    windows: np.ndarray, counts: np.ndarray, quantiles: np.ndarray
) -> np.ndarray:
    """Per-row quantiles ignoring NaN, as `numpy.nanquantile(..., axis=1)` would.

    Same linear interpolation between order statistics as numpy's default method,
    computed from one sort instead: `nanquantile` re-sorts and re-masks per row
    and dominates the run time of a parameter sweep by two orders of magnitude.
    NaNs sort to the end of each row, so `counts` (the number of finite entries)
    is exactly how far into the sorted row the real data goes.

    Args:
        windows: Shape `(rows, window)`; every row must hold at least one finite value.
        counts: Finite entries per row, shape `(rows,)`.
        quantiles: Quantiles in [0, 1], shape `(nq,)`.

    Returns:
        Shape `(rows, nq)`.
    """
    ordered = np.sort(windows, axis=1)
    positions = quantiles[None, :] * (counts[:, None] - 1)
    lower = np.floor(positions).astype(np.intp)
    upper = np.ceil(positions).astype(np.intp)
    weight = positions - lower
    rows = np.arange(ordered.shape[0])[:, None]
    return ordered[rows, lower] * (1.0 - weight) + ordered[rows, upper] * weight


def state_series(z_scores: np.ndarray, params: MarkovMeanReversionParams) -> np.ndarray:
    """State index per bar, or `UNCLASSIFIED` where it cannot be determined.

    State cutoffs at bar `i` are the `n_states - 1` interior quantiles of the
    z-scores over the trailing `state_window_days` bars ending at `i`, so they
    move with the asset's own recent distribution and use no future data. A
    z-score exactly on a cutoff falls in the higher state.
    """
    w = params.state_window_days
    k = params.n_states
    n = z_scores.size

    states = np.full(n, UNCLASSIFIED, dtype=int)
    if n < w:
        return states

    windows = np.lib.stride_tricks.sliding_window_view(z_scores, w)
    counts = np.isfinite(windows).sum(axis=1)
    ends = np.arange(w - 1, n)
    z_at_end = z_scores[ends]

    usable = (counts >= min_edge_obs(params)) & np.isfinite(z_at_end)
    if not usable.any():
        return states

    quantiles = np.linspace(0.0, 1.0, k + 1)[1:-1]
    edges = _row_quantiles(windows[usable], counts[usable], quantiles)

    # searchsorted(..., side="right") == "how many cutoffs are <= this value".
    states[ends[usable]] = (edges <= z_at_end[usable][:, None]).sum(axis=1)
    return states


def transition_matrix(
    states: np.ndarray, params: MarkovMeanReversionParams, as_of_idx: int
) -> tuple[np.ndarray, np.ndarray]:
    """Row-normalized transition matrix over the trailing lookback.

    Counts one-step transitions among `states[as_of_idx - matrix_lookback_days :
    as_of_idx + 1]`, i.e. strictly trailing. Transitions touching an
    `UNCLASSIFIED` bar are skipped.

    Returns:
        `(matrix, row_counts)`. `matrix[i]` is NaN for any state never observed
        as an origin; `row_counts[i]` is how many transitions out of state `i`
        the estimate rests on, which is what `min_obs_per_state` is checked
        against.
    """
    k = params.n_states
    start = max(0, as_of_idx - params.matrix_lookback_days)
    window = states[start : as_of_idx + 1]

    counts = np.zeros((k, k))
    if window.size >= 2:
        origin, destination = window[:-1], window[1:]
        observed = (origin >= 0) & (destination >= 0)
        if observed.any():
            flat = np.bincount(
                origin[observed] * k + destination[observed], minlength=k * k
            )
            counts = flat.reshape(k, k).astype(float)

    row_counts = counts.sum(axis=1)
    matrix = np.full((k, k), np.nan)
    np.divide(counts, row_counts[:, None], out=matrix, where=row_counts[:, None] > 0)
    return matrix, row_counts


def state_midpoints(
    z_scores: np.ndarray,
    states: np.ndarray,
    params: MarkovMeanReversionParams,
    as_of_idx: int,
) -> np.ndarray:
    """Representative z-score per state: the mean z observed in that state.

    Estimated over **the same trailing window as `transition_matrix`**, so the
    probabilities and the values they weight describe one period of history. A
    state never observed in the window has a NaN midpoint; by construction any
    state reachable in the matrix was observed in the window, so a reachable
    state always has one.
    """
    start = max(0, as_of_idx - params.matrix_lookback_days)
    window_z = z_scores[start : as_of_idx + 1]
    window_states = states[start : as_of_idx + 1]

    midpoints = np.full(params.n_states, np.nan)
    for state in range(params.n_states):
        values = window_z[(window_states == state) & np.isfinite(window_z)]
        if values.size > 0:
            midpoints[state] = values.mean()
    return midpoints


@dataclass(frozen=True)
class MarkovDiagnostics:
    """Everything behind one asset's score, for tests and research inspection.

    Attributes:
        score: The signal score (higher = more attractive long), or None.
        reject_reason: Why `score` is None; None when the asset was scored.
        z_scores: Trailing-standardized returns over the estimation window.
        states: State index per bar (`UNCLASSIFIED` where undetermined).
        matrix: Transition matrix as of the last bar.
        row_counts: Transitions observed out of each state.
        midpoints: Mean z per state over the transition lookback.
        current_state: State of the last bar, or `UNCLASSIFIED`.
        current_z: z-score of the last bar (NaN if undefined).
        expected_next_z: Probability-weighted expected next-state z (NaN if unscored).
    """

    score: float | None
    reject_reason: str | None
    z_scores: np.ndarray
    states: np.ndarray
    matrix: np.ndarray
    row_counts: np.ndarray
    midpoints: np.ndarray
    current_state: int
    current_z: float
    expected_next_z: float


def _rejected(reason: str, params: MarkovMeanReversionParams, **kwargs) -> MarkovDiagnostics:
    """A diagnostics record for an asset that could not be scored."""
    k = params.n_states
    defaults = {
        "z_scores": np.empty(0),
        "states": np.empty(0, dtype=int),
        "matrix": np.full((k, k), np.nan),
        "row_counts": np.zeros(k),
        "midpoints": np.full(k, np.nan),
        "current_state": UNCLASSIFIED,
        "current_z": float("nan"),
        "expected_next_z": float("nan"),
    }
    return MarkovDiagnostics(score=None, reject_reason=reason, **{**defaults, **kwargs})


def diagnose_series(
    closes: np.ndarray, params: MarkovMeanReversionParams = DEFAULT_PARAMS
) -> MarkovDiagnostics:
    """Score one asset's close series and return the full intermediate state.

    Only the last `min_history_bars(params)` closes are used; anything earlier is
    discarded so the estimate does not depend on the length of the history
    supplied. `closes` must be in ascending bar order.

    Reject reasons: `insufficient_history`, `state_unclassified`,
    `sparse_transition_row`, `no_reachable_midpoint`.
    """
    closes = np.asarray(closes, dtype=float)
    needed = min_history_bars(params)
    if closes.size < needed:
        return _rejected("insufficient_history", params)

    window = closes[-needed:]
    z_scores = rolling_return_zscores(window, params)
    states = state_series(z_scores, params)

    as_of_idx = window.size - 1
    current_state = int(states[as_of_idx])
    current_z = float(z_scores[as_of_idx])
    matrix, row_counts = transition_matrix(states, params, as_of_idx)
    midpoints = state_midpoints(z_scores, states, params, as_of_idx)

    context = {
        "z_scores": z_scores,
        "states": states,
        "matrix": matrix,
        "row_counts": row_counts,
        "midpoints": midpoints,
        "current_state": current_state,
        "current_z": current_z,
    }

    if current_state == UNCLASSIFIED or not np.isfinite(current_z):
        return _rejected("state_unclassified", params, **context)

    if row_counts[current_state] < params.min_obs_per_state:
        # Doc Section 8: too few trailing observations of this state to trust its
        # transition row. Extreme states are the rare ones and the ones the
        # signal leans on hardest, so this guard fires exactly where it matters.
        return _rejected("sparse_transition_row", params, **context)

    probabilities = matrix[current_state]
    reachable = np.isfinite(midpoints) & np.isfinite(probabilities) & (probabilities > 0.0)
    mass = probabilities[reachable].sum() if reachable.any() else 0.0
    if mass <= _EPS:
        return _rejected("no_reachable_midpoint", params, **context)

    # Renormalize over reachable states. Defensive: a state reachable within the
    # lookback was observed within the lookback, so it has a midpoint.
    expected_next_z = float((midpoints[reachable] * probabilities[reachable]).sum() / mass)

    # Raw reversion is most positive when the model expects the asset to fall;
    # negate for the engine's "higher = more attractive long" convention.
    score = float(-(current_z - expected_next_z))

    return MarkovDiagnostics(
        score=score, reject_reason=None, expected_next_z=expected_next_z, **context
    )


def score_series(
    closes: np.ndarray, params: MarkovMeanReversionParams = DEFAULT_PARAMS
) -> float | None:
    """Score one asset's close series; None if it cannot be scored."""
    return diagnose_series(closes, params).score


# ---------------------------------------------------------------------------
# Universe scoring (reads through the point-in-time context)
# ---------------------------------------------------------------------------

PanelSource = Callable[[object], Mapping[str, np.ndarray]]
"""`ctx -> {asset_id: ascending close series}`. See `signals.panel` for a cached one."""

_PANEL_COLUMNS = ["asset_id", "venue", "timeframe", "event_ts", "ingested_ts", "close"]


def contiguous_tail(
    dates: np.ndarray, closes: np.ndarray, params: MarkovMeanReversionParams
) -> np.ndarray:
    """The gap-free tail of a bar series, at most `min_history_bars` long.

    State classification assumes evenly spaced bars, so a hole in the history
    would silently turn a "5-day return" into one spanning more than five days.
    Only the most recent `min_history_bars` bars are ever used, so this trims to
    that many and then cuts at the most recent gap wider than `max_gap_days`
    inside it: an old gap costs the asset nothing, a recent one drops it.

    Args:
        dates: Ascending bar timestamps (`datetime64` or datetimes).
        closes: Closes aligned to `dates`.
        params: Signal parameters.
    """
    if closes.size == 0:
        return closes

    start = max(0, closes.size - min_history_bars(params))
    stamps = np.asarray(dates[start:], dtype="datetime64[s]")
    limit = np.timedelta64(int(params.max_gap_days) * 86_400, "s")
    gaps = np.flatnonzero(np.diff(stamps) > limit)
    if gaps.size:
        start += int(gaps[-1]) + 1
    return closes[start:]


def context_close_panel(
    ctx, params: MarkovMeanReversionParams = DEFAULT_PARAMS
) -> dict[str, np.ndarray]:
    """Point-in-time close series per universe asset, read through `ctx`.

    Reads only as far back as the signal needs (plus `history_buffer_days`),
    keeps the latest ingestion of each `(asset, bar)` — a backfill can append a
    bar more than once and append-only means both copies are still there — and
    returns the longest gap-free tail per asset in ascending bar order.
    """
    lookback = min_history_bars(params) + params.history_buffer_days
    frame = ctx.ohlcv(lookback_days=lookback, columns=_PANEL_COLUMNS)
    if len(frame) == 0:
        return {}

    frame = frame.filter(pl.col("close").is_not_null() & pl.col("close").is_finite())
    if len(frame) == 0:
        return {}

    frame = (
        frame.sort("ingested_ts")
        .group_by(["asset_id", "event_ts"])
        .last()
        .sort(["asset_id", "event_ts"])
    )

    panel: dict[str, np.ndarray] = {}
    for part in frame.partition_by("asset_id", maintain_order=True):
        asset_id = part["asset_id"][0]
        closes = part["close"].to_numpy().astype(float)
        panel[asset_id] = contiguous_tail(part["event_ts"].to_numpy(), closes, params)
    return panel


def score_universe(
    ctx,
    params: MarkovMeanReversionParams = DEFAULT_PARAMS,
    standardize: bool = False,
    panel_source: PanelSource | None = None,
) -> dict[str, float | None]:
    """Score every asset in `ctx.universe` as of `ctx.asof`.

    Args:
        ctx: Point-in-time rebalance context.
        params: Signal parameters.
        standardize: Apply construction steps 6-7 (cross-sectional winsorize
            then z-score). False returns the raw per-asset scores, which is what
            rank IC is computed on (ranks are unchanged by standardizing).
        panel_source: Optional `ctx -> {asset_id: closes}` override, used by
            research tooling to avoid re-reading parquet at every rebalance. A
            caller supplying this takes responsibility for point-in-time
            filtering; the default reads through `ctx` and is always safe.

    Returns:
        `{asset_id: score | None}` for every asset in the universe. `None` means
        "no view" — not a neutral score.
    """
    panel = (panel_source or (lambda c: context_close_panel(c, params)))(ctx)

    scores: dict[str, float | None] = {}
    for asset_id in ctx.universe:
        closes = panel.get(asset_id)
        scores[asset_id] = None if closes is None else score_series(closes, params)

    if standardize:
        return cross_sectional_zscore(scores, params.winsorize_pct)
    return scores


def make_signal(
    params: MarkovMeanReversionParams = DEFAULT_PARAMS,
    standardize: bool = True,
    panel_source: PanelSource | None = None,
) -> Callable[[object], dict[str, float | None]]:
    """Build a `ctx -> {asset_id: score}` function for the engine or registry.

    Args:
        params: Signal parameters.
        standardize: Include the cross-sectional winsorize + z-score (the full
            construction, and what portfolio construction consumes).
        panel_source: See `score_universe`.
    """

    def signal(ctx) -> dict[str, float | None]:
        return score_universe(
            ctx, params=params, standardize=standardize, panel_source=panel_source
        )

    return signal


def with_params(**overrides) -> MarkovMeanReversionParams:
    """`DEFAULT_PARAMS` with fields overridden — for parameter sweeps."""
    return replace(DEFAULT_PARAMS, **overrides)
