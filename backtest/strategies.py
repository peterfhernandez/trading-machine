"""Reference strategies for validating and exercising the engine.

These are not alphas — Phase 5 owns signal research. They exist so the engine
can be validated against arithmetic anyone can check by hand (buy-and-hold must
reproduce the raw price series; equal weight must reproduce the average), and
so later modules have a baseline to beat.
"""

from collections.abc import Callable, Mapping, Sequence

from .engine import RebalanceContext


def buy_and_hold(asset_id: str, weight: float = 1.0) -> Callable[[RebalanceContext], dict]:
    """Hold a constant target weight in one asset.

    Through the engine this must reproduce the asset's own open-to-open return
    series (net of the one-off cost of putting the position on), which is the
    Phase 4 validation check.
    """

    def strategy(ctx: RebalanceContext) -> dict[str, float]:
        return {asset_id: weight}

    return strategy


def equal_weight(gross: float = 1.0) -> Callable[[RebalanceContext], dict]:
    """Split `gross` equally across the universe at each rebalance."""

    def strategy(ctx: RebalanceContext) -> dict[str, float]:
        if not ctx.universe:
            return {}
        w = gross / len(ctx.universe)
        return {asset_id: w for asset_id in ctx.universe}

    return strategy


def long_short_from_scores(
    score_fn: Callable[[RebalanceContext], Mapping[str, float]],
    n_per_side: int = 1,
    gross: float = 1.0,
) -> Callable[[RebalanceContext], dict]:
    """Rank-based dollar-neutral book: long the top scores, short the bottom.

    Each side gets `gross / 2`, split equally within the side, so the book is
    dollar-neutral by construction. Assets outside the top/bottom `n_per_side`
    get no weight. If fewer than `2 * n_per_side` assets are scored, the book
    is empty (no half-formed hedges).

    Args:
        score_fn: Scores assets from a point-in-time context (higher = more attractive).
        n_per_side: Number of assets on each side.
        gross: Total gross exposure (longs + shorts) as a fraction of portfolio value.
    """

    def strategy(ctx: RebalanceContext) -> dict[str, float]:
        scores = {
            asset_id: float(score)
            for asset_id, score in score_fn(ctx).items()
            if score is not None and asset_id in set(ctx.universe)
        }
        if len(scores) < 2 * n_per_side:
            return {}

        ordered: Sequence[str] = sorted(scores, key=lambda a: (-scores[a], a))
        longs = ordered[:n_per_side]
        shorts = ordered[-n_per_side:]

        side_weight = gross / 2.0 / n_per_side
        weights = {asset_id: side_weight for asset_id in longs}
        for asset_id in shorts:
            weights[asset_id] = -side_weight
        return weights

    return strategy
