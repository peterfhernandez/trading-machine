"""Alpha refinement: IC estimation, shrinkage, and `alpha = volatility x IC x z`.

A signal produces a standardized score. Grinold-Kahn's forecasting rule turns
that score into an expected excess return:

    alpha_i = volatility_i x IC x z_i

where `z_i` is the cross-sectionally standardized score, `IC` is the
correlation between the signal and subsequent returns, and `volatility_i` is
the asset's own return volatility. Only then are the numbers in units the
optimizer can trade against — a z-score of 2 means something different for an
asset with 30% annualized volatility than for one with 150%.

**The IC estimate is where the humility goes.** A raw in-sample mean IC is
optimistic: it is measured on the same data the signal's parameters were chosen
against, and it carries an estimation error that shrinks only as `1/sqrt(n)`.
`estimate_ic` therefore reports the raw mean *and* a shrunk value, and every
consumer here uses the shrunk one.

**Shrinkage rule (normal-normal).** With a prior `IC ~ N(0, tau^2)` and an
observed mean IC whose standard error is `se = ic_std / sqrt(n)`, the posterior
mean is

    shrunk = mean_ic x tau^2 / (tau^2 + se^2)

so a noisy estimate (large `se`) is pulled hard toward zero and a precise one
is left nearly alone. The default `tau = 0.02` is deliberately tight: PLAN.md
§6 says shrink hard, and `signals/methodology/TEMPLATE.md` puts a real crypto
signal at a rank IC of 0.02-0.05, so a prior standard deviation of 0.02 says "a
genuine signal is small" rather than encoding an expectation of skill. The
result is additionally clipped to `max_abs_ic`, which is a guard against a
short, lucky sample rather than part of the Bayesian story.

An estimate with fewer than two IC observations is shrunk to exactly zero: one
period cannot separate skill from noise, and a standard error is undefined.

This module is pure arithmetic over an IC series and a score cross-section — no
store reads, no context, no imports from `backtest/` (signals are consumed by
the backtester, never the other way round).
"""

import math
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass

from logging_config import get_logger

log = get_logger(__name__)

DEFAULT_PRIOR_IC_STD = 0.02
"""Prior standard deviation of a signal's true IC. See the module docstring."""

DEFAULT_MAX_ABS_IC = 0.10
"""Hard cap on a shrunk IC. A larger measured IC is a bug or look-ahead first."""


@dataclass(frozen=True)
class IcEstimate:
    """A signal's information coefficient, raw and shrunk.

    Attributes:
        signal_id: The signal this estimate describes.
        mean_ic: Sample mean of the per-rebalance rank ICs.
        ic_std: Sample standard deviation (ddof=1) of those ICs.
        n_periods: Number of non-null IC observations.
        standard_error: `ic_std / sqrt(n_periods)`.
        t_stat: `mean_ic / standard_error` (0.0 when the error is undefined).
        shrunk_ic: The estimate every consumer should use.
        shrinkage: `shrunk_ic / mean_ic`, i.e. the fraction retained (0.0 when
            `mean_ic` is zero). Reported so the amount of shrinking is visible
            rather than buried in the result.
    """

    signal_id: str
    mean_ic: float
    ic_std: float
    n_periods: int
    standard_error: float
    t_stat: float
    shrunk_ic: float
    shrinkage: float

    def to_dict(self) -> dict[str, float | str | int]:
        """Flat dict, for report frames and logging."""
        return {
            "signal": self.signal_id,
            "mean_ic": self.mean_ic,
            "ic_std": self.ic_std,
            "n_periods": self.n_periods,
            "standard_error": self.standard_error,
            "t_stat": self.t_stat,
            "shrunk_ic": self.shrunk_ic,
            "shrinkage": self.shrinkage,
        }


def estimate_ic(
    ics: Iterable[float | None],
    signal_id: str = "",
    prior_ic_std: float = DEFAULT_PRIOR_IC_STD,
    max_abs_ic: float = DEFAULT_MAX_ABS_IC,
) -> IcEstimate:
    """Estimate a signal's IC from its per-rebalance IC series, with shrinkage.

    Args:
        ics: Per-rebalance rank ICs (e.g. `result.ic` filtered to one signal).
            `None` and non-finite entries are dropped — a rebalance where the
            IC was undefined is an absent observation, not an IC of zero.
        signal_id: Name recorded on the estimate.
        prior_ic_std: `tau` in the shrinkage rule (see the module docstring).
        max_abs_ic: Absolute cap applied after shrinking.

    Returns:
        An `IcEstimate`. With fewer than two observations, `shrunk_ic` is 0.0.

    Raises:
        ValueError: If `prior_ic_std` or `max_abs_ic` is not positive.
    """
    if prior_ic_std <= 0.0:
        raise ValueError(f"prior_ic_std must be > 0, got {prior_ic_std}")
    if max_abs_ic <= 0.0:
        raise ValueError(f"max_abs_ic must be > 0, got {max_abs_ic}")

    values = [float(v) for v in ics if v is not None and math.isfinite(float(v))]
    n = len(values)

    if n == 0:
        return IcEstimate(signal_id, 0.0, 0.0, 0, 0.0, 0.0, 0.0, 0.0)

    mean_ic = sum(values) / n
    if n < 2:
        # One observation gives no standard error, so there is nothing to
        # distinguish skill from luck: shrink all the way to zero.
        log.debug("%s: 1 IC observation; shrunk to zero", signal_id or "signal")
        return IcEstimate(signal_id, mean_ic, 0.0, n, 0.0, 0.0, 0.0, 0.0)

    variance = sum((v - mean_ic) ** 2 for v in values) / (n - 1)
    ic_std = math.sqrt(variance)
    standard_error = ic_std / math.sqrt(n)

    if standard_error <= 0.0:
        # Zero dispersion across many periods is a degenerate series (a constant
        # IC), not infinite precision; trust it no further than the cap.
        t_stat = 0.0
        shrunk = max(-max_abs_ic, min(max_abs_ic, mean_ic))
    else:
        t_stat = mean_ic / standard_error
        factor = prior_ic_std**2 / (prior_ic_std**2 + standard_error**2)
        shrunk = max(-max_abs_ic, min(max_abs_ic, mean_ic * factor))

    shrinkage = shrunk / mean_ic if mean_ic != 0.0 else 0.0
    return IcEstimate(
        signal_id=signal_id,
        mean_ic=mean_ic,
        ic_std=ic_std,
        n_periods=n,
        standard_error=standard_error,
        t_stat=t_stat,
        shrunk_ic=shrunk,
        shrinkage=shrinkage,
    )


def estimate_ics(
    ic_frame,
    prior_ic_std: float = DEFAULT_PRIOR_IC_STD,
    max_abs_ic: float = DEFAULT_MAX_ABS_IC,
) -> dict[str, IcEstimate]:
    """Estimate every signal's IC from a `BacktestResult.ic` frame.

    Args:
        ic_frame: Polars frame with `signal` and `ic` columns, as produced by
            `Backtester.run(signals=...)`.
        prior_ic_std: See `estimate_ic`.
        max_abs_ic: See `estimate_ic`.

    Returns:
        `{signal_id: IcEstimate}`, one per signal present in the frame.
    """
    if len(ic_frame) == 0:
        return {}

    out: dict[str, IcEstimate] = {}
    for signal_id in sorted(set(ic_frame["signal"].to_list())):
        series = ic_frame.filter(ic_frame["signal"] == signal_id)["ic"].to_list()
        out[signal_id] = estimate_ic(series, signal_id, prior_ic_std, max_abs_ic)
    return out


def alpha_from_scores(
    scores: Mapping[str, float | None],
    ic: float,
    vols: Mapping[str, float],
    default_vol: float | None = None,
) -> dict[str, float | None]:
    """`alpha = volatility x IC x z` per asset.

    Args:
        scores: **Standardized** scores (`z`). Pass the output of a signal built
            with `standardize=True`; feeding raw scores in produces alphas in
            whatever units the raw signal happened to use, which is not an error
            this function can detect.
        ic: The signal's IC — pass `IcEstimate.shrunk_ic`, not the raw mean.
        vols: `{asset_id: annualized volatility}` (e.g.
            `low_volatility.annualized_vol_universe`).
        default_vol: Volatility for assets missing from `vols`. `None` (the
            default) scores those assets `None` instead: an asset whose risk is
            unknown cannot be sized, and substituting a cross-sectional average
            would put the largest positions exactly where the risk model is
            blindest.

    Returns:
        `{asset_id: alpha | None}`, same keys as `scores`.
    """
    alphas: dict[str, float | None] = {}
    for asset_id, score in scores.items():
        if score is None:
            alphas[asset_id] = None
            continue
        vol = vols.get(asset_id, default_vol)
        if vol is None or not math.isfinite(vol) or vol <= 0.0:
            alphas[asset_id] = None
            continue
        alphas[asset_id] = float(vol) * float(ic) * float(score)
    return alphas


@dataclass(frozen=True)
class CombinedAlpha:
    """Alphas summed across signals, with the view count behind each.

    Attributes:
        alphas: `{asset_id: combined alpha | None}`.
        n_views: `{asset_id: how many signals had a view}`. An asset carried by
            one signal and an asset all five agree on are different bets, and
            the combined alpha alone does not say which is which.
    """

    alphas: dict[str, float | None]
    n_views: dict[str, int]


def combine_alphas(
    alphas_by_signal: Mapping[str, Mapping[str, float | None]],
    weights: Mapping[str, float] | None = None,
) -> CombinedAlpha:
    """Sum per-signal alphas into one alpha per asset.

    A signal with no view on an asset (`None`) contributes nothing and does
    **not** drag the sum toward zero — that is the whole reason `None` exists
    rather than `0.0`. The consequence is that an asset scored by one signal
    keeps that signal's full alpha, so `n_views` is returned alongside: a
    portfolio that wants to require corroboration has the information to.

    Args:
        alphas_by_signal: `{signal_id: {asset_id: alpha | None}}`.
        weights: Optional per-signal multipliers (default: 1.0 each). Note that
            the IC is already inside each alpha, so these are for deliberate
            over/under-weighting, not for re-expressing confidence.

    Returns:
        A `CombinedAlpha`. An asset no signal had a view on maps to `None`.
    """
    totals: dict[str, float] = {}
    counts: dict[str, int] = {}
    seen: list[str] = []

    for signal_id, alphas in alphas_by_signal.items():
        weight = 1.0 if weights is None else float(weights.get(signal_id, 1.0))
        for asset_id, alpha in alphas.items():
            if asset_id not in counts:
                counts[asset_id] = 0
                seen.append(asset_id)
            if alpha is None or not math.isfinite(float(alpha)):
                continue
            totals[asset_id] = totals.get(asset_id, 0.0) + weight * float(alpha)
            counts[asset_id] += 1

    combined: dict[str, float | None] = {
        asset_id: (totals.get(asset_id) if counts[asset_id] > 0 else None)
        for asset_id in seen
    }
    return CombinedAlpha(alphas=combined, n_views=counts)


def alpha_summary(estimates: Sequence[IcEstimate]) -> str:
    """Human-readable table of IC estimates, for scratch demos and reports."""
    header = (
        f"{'signal':<28} {'n':>5} {'mean IC':>9} {'std err':>9} "
        f"{'t':>7} {'shrunk':>9} {'kept':>6}"
    )
    lines = [header, "-" * len(header)]
    for est in estimates:
        lines.append(
            f"{est.signal_id:<28} {est.n_periods:>5d} {est.mean_ic:>9.4f} "
            f"{est.standard_error:>9.4f} {est.t_stat:>7.2f} "
            f"{est.shrunk_ic:>9.4f} {est.shrinkage * 100:>5.1f}%"
        )
    return "\n".join(lines)
