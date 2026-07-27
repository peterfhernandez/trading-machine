"""Performance metrics for backtest results.

Pure functions over sequences of per-period returns, plus a `summarize` helper
that packages them into a typed `BacktestMetrics`. Everything is computed in
plain Python so the numbers are trivially hand-checkable in golden tests.

Conventions:
- Returns are simple (not log) per-period returns, net of costs unless stated.
- Annualization uses `periods_per_year` (crypto trades 24/7, so 365 for daily
  bars — see `BacktestConfig.periods_per_year`).
- Information ratio is the risk-free-zero Sharpe of the strategy's own returns:
  annualized mean / annualized volatility. With a market-neutral book (Phase 7)
  these returns are already active returns, so the two coincide.
- Max drawdown is reported as a negative fraction (-0.25 = a 25% peak-to-trough
  decline).
"""

import math
from collections.abc import Sequence
from dataclasses import asdict, dataclass


def _as_list(values: Sequence[float]) -> list[float]:
    """Coerce a Polars Series / list / tuple of returns to a list of floats, dropping nulls."""
    return [float(v) for v in values if v is not None]


def total_return(returns: Sequence[float]) -> float:
    """Compounded return over the whole sample."""
    growth = 1.0
    for r in _as_list(returns):
        growth *= 1.0 + r
    return growth - 1.0


def annualized_return(returns: Sequence[float], periods_per_year: float) -> float:
    """Geometric annualized return; 0.0 for an empty sample."""
    values = _as_list(returns)
    if not values:
        return 0.0
    growth = 1.0 + total_return(values)
    if growth <= 0.0:
        return -1.0
    return growth ** (periods_per_year / len(values)) - 1.0


def annualized_vol(returns: Sequence[float], periods_per_year: float) -> float:
    """Annualized standard deviation of returns (sample stdev, ddof=1)."""
    values = _as_list(returns)
    if len(values) < 2:
        return 0.0
    mean = sum(values) / len(values)
    variance = sum((v - mean) ** 2 for v in values) / (len(values) - 1)
    return math.sqrt(variance) * math.sqrt(periods_per_year)


def information_ratio(returns: Sequence[float], periods_per_year: float) -> float:
    """Annualized mean / annualized volatility (risk-free rate assumed zero)."""
    values = _as_list(returns)
    vol = annualized_vol(values, periods_per_year)
    if vol == 0.0:
        return 0.0
    mean = sum(values) / len(values)
    return (mean * periods_per_year) / vol


def drawdown_series(returns: Sequence[float]) -> list[float]:
    """Drawdown from the running peak of the equity curve, per period (<= 0)."""
    equity = 1.0
    peak = 1.0
    out: list[float] = []
    for r in _as_list(returns):
        equity *= 1.0 + r
        peak = max(peak, equity)
        out.append(equity / peak - 1.0 if peak > 0 else 0.0)
    return out


def max_drawdown(returns: Sequence[float]) -> float:
    """Worst peak-to-trough decline as a negative fraction; 0.0 if never underwater."""
    dd = drawdown_series(returns)
    return min(dd) if dd else 0.0


def hit_rate(returns: Sequence[float]) -> float:
    """Fraction of periods with a strictly positive return."""
    values = _as_list(returns)
    if not values:
        return 0.0
    return sum(1 for v in values if v > 0) / len(values)


def average(values: Sequence[float]) -> float:
    """Arithmetic mean; 0.0 for an empty sequence."""
    items = _as_list(values)
    return sum(items) / len(items) if items else 0.0


def _ranks(values: Sequence[float]) -> list[float]:
    """Ranks with ties averaged (1-based), as used by Spearman correlation."""
    order = sorted(range(len(values)), key=lambda i: values[i])
    ranks = [0.0] * len(values)
    i = 0
    while i < len(order):
        j = i
        while j + 1 < len(order) and values[order[j + 1]] == values[order[i]]:
            j += 1
        avg_rank = (i + j) / 2.0 + 1.0
        for k in range(i, j + 1):
            ranks[order[k]] = avg_rank
        i = j + 1
    return ranks


def pearson(x: Sequence[float], y: Sequence[float]) -> float | None:
    """Pearson correlation; None if undefined (n < 2 or a zero-variance input)."""
    if len(x) != len(y):
        raise ValueError(f"Length mismatch: {len(x)} vs {len(y)}")
    if len(x) < 2:
        return None
    mx = sum(x) / len(x)
    my = sum(y) / len(y)
    cov = sum((a - mx) * (b - my) for a, b in zip(x, y, strict=True))
    vx = sum((a - mx) ** 2 for a in x)
    vy = sum((b - my) ** 2 for b in y)
    if vx <= 0.0 or vy <= 0.0:
        return None
    return cov / math.sqrt(vx * vy)


def spearman_ic(scores: Sequence[float], forward_returns: Sequence[float]) -> float | None:
    """Rank information coefficient between signal scores and forward returns.

    Rank-based (not Pearson on levels) because a signal's job is to order assets,
    and ranks are robust to the fat tails of crypto returns.

    Returns:
        Spearman correlation in [-1, 1], or None if undefined (fewer than two
        assets, or all scores/returns tied).
    """
    if len(scores) != len(forward_returns):
        raise ValueError(f"Length mismatch: {len(scores)} vs {len(forward_returns)}")
    if len(scores) < 2:
        return None
    return pearson(_ranks(list(scores)), _ranks(list(forward_returns)))


@dataclass(frozen=True)
class BacktestMetrics:
    """Headline performance metrics for a backtest run."""

    n_periods: int
    total_return: float
    annualized_return: float
    annualized_vol: float
    information_ratio: float
    max_drawdown: float
    hit_rate: float
    avg_turnover: float
    total_cost: float
    periods_per_year: float

    def to_dict(self) -> dict:
        """Metrics as a plain dict (for logging/reporting)."""
        return asdict(self)


def summarize(
    net_returns: Sequence[float],
    turnover: Sequence[float] = (),
    costs: Sequence[float] = (),
    periods_per_year: float = 365.0,
) -> BacktestMetrics:
    """Package a return/turnover/cost series into `BacktestMetrics`."""
    values = _as_list(net_returns)
    return BacktestMetrics(
        n_periods=len(values),
        total_return=total_return(values),
        annualized_return=annualized_return(values, periods_per_year),
        annualized_vol=annualized_vol(values, periods_per_year),
        information_ratio=information_ratio(values, periods_per_year),
        max_drawdown=max_drawdown(values),
        hit_rate=hit_rate(values),
        avg_turnover=average(turnover),
        total_cost=sum(_as_list(costs)),
        periods_per_year=periods_per_year,
    )
