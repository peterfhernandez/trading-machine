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

Alpha refinement (`signals.alpha`) turns those IC series into shrunk IC
estimates and `alpha = volatility x IC x z`; the breadth report
(`signals.breadth`) answers whether the registered signals are independent bets
or one bet with extra steps.
"""

from . import (
    carry,
    cross_sectional_momentum,
    low_volatility,
    markov_mean_reversion,
    short_term_reversal,
    time_series_momentum,
)
from .alpha import (
    CombinedAlpha,
    IcEstimate,
    alpha_from_scores,
    alpha_summary,
    combine_alphas,
    estimate_ic,
    estimate_ics,
)
from .bars import Series, close_series, dataset_series, realized_vol, trailing_return
from .breadth import (
    BreadthReport,
    PairCorrelation,
    ScoreRecorder,
    breadth_report,
    effective_breadth,
    ic_correlations,
    score_correlations,
)
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
from .transforms import (
    cross_sectional_scale,
    cross_sectional_zscore,
    scale_to_unit_dispersion,
    winsorize,
    zscore,
)

# Registration order is the order `signal_functions()` would return them in
# anyway (it sorts), so this list is grouped by family for readability.
for _module in (
    markov_mean_reversion,
    short_term_reversal,
    cross_sectional_momentum,
    time_series_momentum,
    carry,
    low_volatility,
):
    register(
        signal_id=_module.SIGNAL_ID,
        family=_module.FAMILY,
        score_fn=_module.make_signal(),
        params=_module.DEFAULT_PARAMS,
    )
del _module


def signal_families() -> dict[str, str]:
    """`{signal_id: family}` for every registered signal — shaped for the breadth report."""
    return {signal_id: signal.family for signal_id, signal in all_signals().items()}


__all__ = [
    "FAMILIES",
    "MARKOV_DEFAULT_PARAMS",
    "BreadthReport",
    "CachedClosePanel",
    "CombinedAlpha",
    "IcEstimate",
    "MarkovDiagnostics",
    "MarkovMeanReversionParams",
    "PairCorrelation",
    "ScoreRecorder",
    "Series",
    "Signal",
    "all_signals",
    "alpha_from_scores",
    "alpha_summary",
    "breadth_report",
    "carry",
    "close_series",
    "combine_alphas",
    "cross_sectional_momentum",
    "cross_sectional_scale",
    "cross_sectional_zscore",
    "dataset_series",
    "effective_breadth",
    "estimate_ic",
    "estimate_ics",
    "get",
    "ic_correlations",
    "low_volatility",
    "markov_mean_reversion",
    "methodology_path",
    "realized_vol",
    "register",
    "scale_to_unit_dispersion",
    "score_correlations",
    "short_term_reversal",
    "signal_families",
    "signal_functions",
    "time_series_momentum",
    "trailing_return",
    "winsorize",
    "zscore",
]
