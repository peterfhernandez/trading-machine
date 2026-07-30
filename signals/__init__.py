"""Signals module (M6): point-in-time alpha signals.

A signal is a function `RebalanceContext -> {asset_id: score | None}`, where a
higher score means "more attractive long" and `None` means "no view" (never
`0.0`). Signals read only through the context, so point-in-time discipline is
inherited from the backtester rather than re-implemented per signal.

Every signal has a methodology doc in `signals/methodology/` — the doc is the
spec, and `registry.register` refuses to register a signal whose doc is missing.

```python
from backtest import Backtester, long_short_from_scores
from signals import markov_mean_reversion, signal_functions

strategy = long_short_from_scores(markov_mean_reversion.make_signal(), n_per_side=5)
result = Backtester(store).run(strategy, signals=signal_functions())
print(result.ic_summary())
```
"""

from . import markov_mean_reversion
from .markov_mean_reversion import (
    DEFAULT_PARAMS as MARKOV_DEFAULT_PARAMS,
)
from .markov_mean_reversion import (
    MarkovDiagnostics,
    MarkovMeanReversionParams,
)
from .panel import CachedClosePanel
from .registry import (
    FAMILIES,
    Signal,
    all_signals,
    get,
    methodology_path,
    register,
    signal_functions,
)
from .transforms import cross_sectional_zscore, winsorize, zscore

register(
    signal_id=markov_mean_reversion.SIGNAL_ID,
    family=markov_mean_reversion.FAMILY,
    score_fn=markov_mean_reversion.make_signal(),
    params=markov_mean_reversion.DEFAULT_PARAMS,
)

__all__ = [
    "FAMILIES",
    "MARKOV_DEFAULT_PARAMS",
    "CachedClosePanel",
    "MarkovDiagnostics",
    "MarkovMeanReversionParams",
    "Signal",
    "all_signals",
    "cross_sectional_zscore",
    "get",
    "markov_mean_reversion",
    "methodology_path",
    "register",
    "signal_functions",
    "winsorize",
    "zscore",
]
