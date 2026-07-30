"""Cross-sectional transforms shared by every signal.

A signal's construction produces raw per-asset scores in whatever units its
recipe implies. Portfolio construction needs numbers that are comparable across
assets, which means winsorizing the cross-section and then z-scoring it.

**Order matters and is fixed here:** winsorize first, then z-score. Clipping
before computing the mean and standard deviation keeps a single blown-up score
from dragging the whole cross-section's location and scale with it;
z-score-then-clip is a different signal and is not what the methodology docs
specify.

`None` means "no view" and is never invented or filled in: a missing score stays
missing through both transforms, and does not contribute to the mean, the
standard deviation, or the winsorization percentiles. A signal that cannot score
an asset must not be read as scoring it neutrally.
"""

import math
from collections.abc import Mapping, Sequence

Scores = Mapping[str, float | None]


def winsorize(values: Sequence[float], pct: float) -> list[float]:
    """Clip values to the `[pct, 100 - pct]` percentile range of the sample.

    Percentiles are linearly interpolated between order statistics (the numpy
    default), computed on this sample only.

    Args:
        values: Finite sample to clip.
        pct: Percentile clipped from each tail, in percent. 0 disables clipping.

    Returns:
        A new list, same order and length as `values`.

    Raises:
        ValueError: If `pct` is not in [0, 50).
    """
    if not 0.0 <= pct < 50.0:
        raise ValueError(f"winsorize pct must be in [0, 50), got {pct}")
    items = [float(v) for v in values]
    if pct == 0.0 or len(items) < 3:
        # With fewer than three points every value is its own tail; clipping
        # would collapse the cross-section rather than tame it.
        return items

    lo = _percentile(items, pct)
    hi = _percentile(items, 100.0 - pct)
    if lo > hi:
        lo, hi = hi, lo
    return [min(max(v, lo), hi) for v in items]


def _percentile(values: Sequence[float], pct: float) -> float:
    """Linear-interpolation percentile (matches `numpy.percentile` defaults)."""
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    position = (pct / 100.0) * (len(ordered) - 1)
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[int(position)]
    weight = position - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


def zscore(values: Sequence[float]) -> list[float]:
    """Standardize to mean 0, sample standard deviation 1 (ddof=1).

    Returns all zeros for a degenerate cross-section (fewer than two values, or
    zero dispersion): every asset is equally attractive, which is a legitimate
    view, whereas dividing by zero is not.
    """
    items = [float(v) for v in values]
    if len(items) < 2:
        return [0.0] * len(items)
    mean = sum(items) / len(items)
    variance = sum((v - mean) ** 2 for v in items) / (len(items) - 1)
    sd = math.sqrt(variance)
    if sd <= 1e-12:
        return [0.0] * len(items)
    return [(v - mean) / sd for v in items]


def cross_sectional_zscore(scores: Scores, winsorize_pct: float = 0.0) -> dict[str, float | None]:
    """Winsorize then z-score a cross-section of scores, preserving `None`s.

    Args:
        scores: `{asset_id: raw score or None}`.
        winsorize_pct: Percentile clipped from each tail before standardizing.

    Returns:
        `{asset_id: standardized score or None}`, same keys as the input. Assets
        scored `None` stay `None` and are excluded from the statistics.
    """
    scored = {a: float(s) for a, s in scores.items() if s is not None}
    if not scored:
        return dict.fromkeys(scores)

    assets = list(scored)
    standardized = zscore(winsorize([scored[a] for a in assets], winsorize_pct))
    out: dict[str, float | None] = dict(zip(assets, standardized, strict=True))
    for asset in scores:
        out.setdefault(asset, None)
    return {asset: out[asset] for asset in scores}
