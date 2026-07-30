#!/usr/bin/env python3
"""Scratch demo: the markov_mean_reversion signal, end to end.

Shows, on a small synthetic store:

1. The per-asset internals — z-scores, states, transition matrix, midpoints —
   so the construction in the methodology doc is visible rather than asserted.
2. Why an asset scores `None`: the reject reasons, one example of each.
3. Cross-sectional standardization (winsorize then z-score) over a universe.
4. The signal driving a long/short book through the Phase 4 engine, with the
   rank IC series the engine produces.
5. That a bar arriving after the rebalance cannot change the score — the
   point-in-time check, run against a real store rather than argued.

Read-only and offline: no exchange connection, no order placement. Runs only
under PAPER mode.

    PAPER=true python scratch/scratch_signal_markov_mean_reversion.py
"""

import logging
import random
import sys
import tempfile
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import polars as pl

sys.path.insert(0, str(Path(__file__).parent.parent))

from backtest import (  # noqa: E402
    Backtester,
    CostModel,
    RebalanceContext,
    long_short_from_scores,
)
from config import PAPER, BacktestConfig  # noqa: E402
from datastore import ParquetStore  # noqa: E402
from loaders.schemas import OHLCV_SCHEMA  # noqa: E402
from signals import markov_mean_reversion as mmr  # noqa: E402
from signals import registry  # noqa: E402

logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(name)s: %(message)s")

START = datetime(2023, 1, 1)
N_BARS = 400

# Small enough to print, big enough to exercise every branch: 10 bars of history
# per score (see the golden fixture in tests/test_signal_markov_mean_reversion.py).
DEMO_PARAMS = mmr.MarkovMeanReversionParams(
    n_states=3,
    return_horizon_days=2,
    state_window_days=10,
    matrix_lookback_days=40,
    min_obs_per_state=3,
    winsorize_pct=2.5,
)


def synthetic_closes(n_bars: int, reverting: bool, seed: int) -> list[float]:
    """A mean-reverting or trending daily close series."""
    rng = random.Random(seed)
    price = level = 100.0
    drift = 0.0 if reverting else rng.choice([-0.001, 0.0015])
    vol = rng.uniform(0.02, 0.04)

    closes = []
    for _ in range(n_bars):
        pull = -0.08 * (price / level - 1.0) if reverting else 0.0
        price = max(price * (1.0 + drift + pull + rng.gauss(0.0, vol)), 1e-6)
        level *= 1.0 + drift
        closes.append(price)
    return closes


def seed_store(store: ParquetStore, closes_by_asset: dict[str, list[float]]) -> None:
    """Write daily bars; `open` is the previous close so opens and closes differ."""
    rows = []
    for asset_id, closes in closes_by_asset.items():
        for i, close_px in enumerate(closes):
            event_ts = START + timedelta(days=i)
            open_px = closes[i - 1] if i > 0 else close_px
            rows.append(
                (
                    asset_id, "binance", "1d", event_ts, event_ts,
                    open_px, max(open_px, close_px), min(open_px, close_px), close_px,
                    5_000_000.0,
                )
            )

    frame = pl.DataFrame(
        rows,
        schema=[
            "asset_id", "venue", "timeframe", "event_ts", "ingested_ts",
            "open", "high", "low", "close", "volume",
        ],
        orient="row",
    ).with_columns(
        pl.col("event_ts").cast(pl.Datetime("us")),
        pl.col("ingested_ts").cast(pl.Datetime("us")),
    )
    store.append("ohlcv_daily", frame, OHLCV_SCHEMA)


def show_internals(closes: list[float]) -> None:
    print("\n" + "=" * 78)
    print("1. Per-asset internals (the construction, step by step)")
    print("=" * 78)

    params = DEMO_PARAMS
    diag = mmr.diagnose_series(np.array(closes), params)
    window = mmr.min_history_bars(params)

    print(f"\n  parameters: {params}")
    print(f"  estimation window: the last {window} bars (earlier history discarded)")
    print(f"  first classifiable bar in the window: index {mmr.first_state_index(params)}")

    print("\n  last 12 bars of the window:")
    print(f"    {'idx':>4} {'close':>10} {'z':>8} {'state':>6}")
    for i in range(len(diag.states) - 12, len(diag.states)):
        z = diag.z_scores[i]
        print(
            f"    {i:>4} {closes[-window:][i]:>10.2f} "
            f"{'nan' if np.isnan(z) else f'{z:>8.3f}':>8} "
            f"{'--' if diag.states[i] < 0 else diag.states[i]:>6}"
        )

    print(f"\n  transition matrix (rows = from state, over {params.matrix_lookback_days} bars):")
    for state in range(params.n_states):
        probabilities = " ".join(
            "  n/a" if np.isnan(p) else f"{p:5.2f}" for p in diag.matrix[state]
        )
        print(f"    from {state}: {probabilities}   ({diag.row_counts[state]:.0f} observations)")

    midpoints = " ".join(
        " n/a" if np.isnan(m) else f"{m:+.3f}" for m in diag.midpoints
    )
    print(f"\n  state midpoints (mean z per state, same window): {midpoints}")
    print(f"  current state {diag.current_state}, current z {diag.current_z:+.4f}")
    print(f"  expected next-state z {diag.expected_next_z:+.4f}")
    print(
        f"  raw reversion {diag.current_z - diag.expected_next_z:+.4f} "
        f"-> score {diag.score:+.4f} (sign flipped: higher = more attractive long)"
    )


def show_reject_reasons(closes: list[float]) -> None:
    print("\n" + "=" * 78)
    print("2. Why an asset scores None (a missing score is not a neutral view)")
    print("=" * 78)

    needed = mmr.min_history_bars(DEMO_PARAMS)
    cases = {
        "insufficient_history": np.array(closes[: needed - 1]),
        "state_unclassified (flat series, no dispersion)": np.full(needed + 5, 100.0),
    }
    for name, series in cases.items():
        diag = mmr.diagnose_series(series, DEMO_PARAMS)
        print(f"  {name:50s} -> score {diag.score}, reason {diag.reject_reason!r}")

    strict = mmr.MarkovMeanReversionParams(
        n_states=7,
        return_horizon_days=2,
        state_window_days=10,
        matrix_lookback_days=40,
        min_obs_per_state=25,  # more than 40 transitions can give any single state
    )
    diag = mmr.diagnose_series(np.array(closes), strict)
    print(
        f"  {'sparse_transition_row (min_obs_per_state=25)':50s} -> "
        f"score {diag.score}, reason {diag.reject_reason!r}"
    )
    print(
        "\n  None drops the asset from the cross-section. Scoring it 0.0 would "
        "instead assert it is exactly average."
    )


def show_cross_section(store: ParquetStore, universe: list[str], asof: datetime) -> None:
    print("\n" + "=" * 78)
    print("3. Cross-section at one rebalance (raw, then winsorized + z-scored)")
    print("=" * 78)

    ctx = RebalanceContext(asof=asof, universe=universe, store=store, venue="binance")
    raw = mmr.score_universe(ctx, params=DEMO_PARAMS, standardize=False)
    standardized = mmr.score_universe(ctx, params=DEMO_PARAMS, standardize=True)

    print(f"\n  asof {asof.date()}, universe of {len(universe)}")
    print(f"    {'asset':10} {'raw':>10} {'standardized':>14}")
    for asset_id in universe:
        raw_value = "None" if raw[asset_id] is None else f"{raw[asset_id]:+.4f}"
        std_value = (
            "None" if standardized[asset_id] is None else f"{standardized[asset_id]:+.4f}"
        )
        print(f"    {asset_id:10} {raw_value:>10} {std_value:>14}")

    scored = [v for v in standardized.values() if v is not None]
    print(f"\n  {len(scored)} of {len(universe)} scored; standardized mean {np.mean(scored):+.2e}")


def show_backtest(store: ParquetStore, dates: list[datetime]) -> None:
    print("\n" + "=" * 78)
    print("4. Through the Phase 4 engine (long/short, weekly, costs charged)")
    print("=" * 78)

    signal = mmr.make_signal(params=DEMO_PARAMS, standardize=True)
    config = BacktestConfig(
        rebalance_freq="weekly",
        backtest_start=dates[0].date().isoformat(),
        backtest_end=dates[-1].date().isoformat(),
        spread_bps=5.0,
        impact_bps_per_million=1.0,
    )
    backtester = Backtester(
        store,
        config=config,
        cost_model=CostModel(spread_bps=5.0, impact_bps_per_million=1.0),
    )
    result = backtester.run(
        long_short_from_scores(signal, n_per_side=2),
        signals={mmr.SIGNAL_ID: signal},
    )

    metrics = result.metrics.to_dict()
    print(f"\n  {result.metrics.n_periods} periods")
    for key in (
        "total_return", "annualized_return", "annualized_vol",
        "information_ratio", "max_drawdown", "avg_turnover", "total_cost",
    ):
        print(f"    {key:20s} {metrics[key]:+.4f}")

    summary = result.ic_summary()
    if len(summary):
        print(
            f"\n  rank IC: mean {summary['mean_ic'][0]:+.4f}  "
            f"vol {summary['ic_std'][0]:.4f}  IC IR {summary['ic_ir'][0]:+.3f}  "
            f"over {summary['n_periods'][0]} periods"
        )
    print(
        "\n  Synthetic data with reversion baked in: this is a smoke test of the "
        "wiring, not evidence. Real evidence comes from "
        "scratch/scratch_markov_param_grid.py against a backfill."
    )


def show_point_in_time(store: ParquetStore, asset_id: str, asof: datetime) -> None:
    print("\n" + "=" * 78)
    print("5. Point-in-time check against the store")
    print("=" * 78)

    ctx = RebalanceContext(asof=asof, universe=[asset_id], store=store, venue="binance")
    before = mmr.score_universe(ctx, params=DEMO_PARAMS)[asset_id]

    # Append wild bars dated *after* asof, ingested after asof.
    rows = []
    for i, close_px in enumerate([1.0, 5_000.0, 2.0, 9_000.0], start=1):
        event_ts = asof + timedelta(days=i)
        rows.append(
            (
                asset_id, "binance", "1d", event_ts, event_ts,
                close_px, close_px, close_px, close_px, 5_000_000.0,
            )
        )
    frame = pl.DataFrame(
        rows,
        schema=[
            "asset_id", "venue", "timeframe", "event_ts", "ingested_ts",
            "open", "high", "low", "close", "volume",
        ],
        orient="row",
    ).with_columns(
        pl.col("event_ts").cast(pl.Datetime("us")),
        pl.col("ingested_ts").cast(pl.Datetime("us")),
    )
    store.append("ohlcv_daily", frame, OHLCV_SCHEMA)

    after = mmr.score_universe(ctx, params=DEMO_PARAMS)[asset_id]
    print(f"\n  score at {asof.date()} before appending 4 later bars: {before:+.6f}")
    print(f"  score at {asof.date()} after  appending 4 later bars: {after:+.6f}")
    print(f"  unchanged: {before == after}")


def main() -> int:
    if not PAPER:
        print("PAPER is False; scratch scripts do not run. Set PAPER=true.")
        return 1

    print("=" * 78)
    print("markov_mean_reversion — signal demo")
    print("=" * 78)

    signal = registry.get(mmr.SIGNAL_ID)
    print(f"\n  registry key: {signal.signal_id} (family: {signal.family})")
    print(f"  methodology:  {signal.methodology.name}")
    print(f"  default params need {mmr.min_history_bars(mmr.DEFAULT_PARAMS)} bars per score")

    store = ParquetStore(Path(tempfile.mkdtemp(prefix="markov_demo_")))
    closes_by_asset = {
        f"REV{i}": synthetic_closes(N_BARS, reverting=True, seed=100 + i) for i in range(3)
    }
    closes_by_asset.update(
        {f"TRD{i}": synthetic_closes(N_BARS, reverting=False, seed=200 + i) for i in range(3)}
    )
    closes_by_asset["FLAT"] = [100.0] * N_BARS
    closes_by_asset["NEW"] = synthetic_closes(20, reverting=True, seed=9)  # too short
    seed_store(store, closes_by_asset)

    dates = [START + timedelta(days=i) for i in range(N_BARS)]
    universe = sorted(closes_by_asset)

    show_internals(closes_by_asset["REV0"])
    show_reject_reasons(closes_by_asset["REV0"])
    show_cross_section(store, universe, dates[-1])
    show_backtest(store, dates)
    show_point_in_time(store, "REV0", dates[-1])

    print("\nDone. No orders were placed; the store was a temp directory.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
