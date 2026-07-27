"""Backtest module (M5): walk-forward, point-in-time research engine.

Public interface:

```python
from backtest import Backtester, CostModel, DatastoreUniverse

def strategy(ctx):
    bars = ctx.ohlcv(lookback_days=30)          # only data known at ctx.asof
    return {asset_id: 1 / len(ctx.universe) for asset_id in ctx.universe}

result = Backtester(store).run(strategy)
print(result.metrics.to_dict())
```
"""

from .calendar import SUPPORTED_FREQS, build_rebalance_calendar
from .costs import ZERO_COST, CostModel
from .engine import (
    PIT_EVENT,
    PIT_INGESTION,
    PIT_MODES,
    Backtester,
    BacktestResult,
    DatastoreUniverse,
    PricePanel,
    RebalanceContext,
)
from .metrics import (
    BacktestMetrics,
    annualized_return,
    annualized_vol,
    drawdown_series,
    hit_rate,
    information_ratio,
    max_drawdown,
    spearman_ic,
    summarize,
    total_return,
)
from .strategies import buy_and_hold, equal_weight, long_short_from_scores

__all__ = [
    "Backtester",
    "BacktestResult",
    "BacktestMetrics",
    "CostModel",
    "DatastoreUniverse",
    "PIT_EVENT",
    "PIT_INGESTION",
    "PIT_MODES",
    "PricePanel",
    "RebalanceContext",
    "SUPPORTED_FREQS",
    "ZERO_COST",
    "annualized_return",
    "annualized_vol",
    "build_rebalance_calendar",
    "buy_and_hold",
    "drawdown_series",
    "equal_weight",
    "hit_rate",
    "information_ratio",
    "long_short_from_scores",
    "max_drawdown",
    "spearman_ic",
    "summarize",
    "total_return",
]
