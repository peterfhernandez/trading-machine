"""Cost model for the backtester (spread + linear impact stub).

Costs are charged on traded notional at each rebalance, in return terms
(a fraction of portfolio value), and subtracted from the period's gross
return. Two components:

1. **Half-spread**: crossing the book costs half the quoted spread on every
   traded dollar, in either direction.
2. **Impact (stub)**: a linear term, `impact_bps_per_million` basis points per
   $1M of notional traded in the rebalance. Deliberately crude — Phase 8
   replaces it with measured implementation shortfall — but it is applied from
   day one so no strategy is evaluated cost-free.

Both are pessimistic by design (principle 4 in PLAN.md: subtract as little
value as possible, but never pretend trading is free).
"""

from dataclasses import dataclass

from config import BACKTEST_CONFIG

BPS = 1e-4


@dataclass(frozen=True)
class CostModel:
    """Spread + impact cost model.

    Attributes:
        spread_bps: Round-trip quoted spread in basis points; half is paid per trade.
        impact_bps_per_million: Extra basis points of cost per $1M of notional traded.
    """

    spread_bps: float = BACKTEST_CONFIG.spread_bps
    impact_bps_per_million: float = BACKTEST_CONFIG.impact_bps_per_million

    def cost_bps(self, traded_notional: float) -> float:
        """Cost in basis points of traded notional for a trade of `traded_notional` dollars."""
        return self.spread_bps / 2.0 + self.impact_bps_per_million * (traded_notional / 1_000_000.0)

    def cost(self, turnover: float, portfolio_value: float) -> float:
        """Cost of a rebalance as a fraction of portfolio value.

        Args:
            turnover: Sum of absolute weight changes at this rebalance (1.0 = the
                whole portfolio traded once).
            portfolio_value: Portfolio value before the rebalance, in dollars.

        Returns:
            Cost as a positive fraction of portfolio value (subtract from returns).
        """
        if turnover <= 0.0 or portfolio_value <= 0.0:
            return 0.0
        traded_notional = turnover * portfolio_value
        return turnover * self.cost_bps(traded_notional) * BPS


ZERO_COST = CostModel(spread_bps=0.0, impact_bps_per_million=0.0)
"""Frictionless cost model — for the shadow portfolio and engine validation."""
