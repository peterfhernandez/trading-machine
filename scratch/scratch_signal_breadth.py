#!/usr/bin/env python3
"""Scratch demo: alpha refinement and the breadth check.

The two Phase 5 pieces that sit *between* the signals and the portfolio:

1. **IC estimation and shrinkage** — the engine's per-rebalance rank IC series
   becomes one shrunk IC per signal, and the demo shows how hard a noisy series
   is pulled toward zero versus a precise one.
2. **`alpha = volatility x IC x z`** — Grinold-Kahn, using the same volatility
   estimator `low_volatility` ranks on.
3. **Combining alphas** — and why a `None` view does not drag the sum to zero.
4. **The breadth report** — score correlation (do they pick the same assets),
   IC correlation (do they work at the same times), and the effective number of
   independent bets.
5. A sample log line from `logs/signals.log`.

The numbers below come from **synthetic data**, so they measure the generator,
not the signals. This script demonstrates the machinery; the real numbers go in
each signal's methodology doc Section 6 once a backfill exists.

Read-only and offline. Runs only under PAPER mode.

    PAPER=true python scratch/scratch_signal_breadth.py
"""

import sys
import tempfile
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import polars as pl

sys.path.insert(0, str(Path(__file__).parent.parent))

from backtest import PIT_EVENT, Backtester, CostModel, long_short_from_scores  # noqa: E402
from config import LOG_CONFIG, PAPER, BacktestConfig  # noqa: E402
from datastore import ParquetStore  # noqa: E402
from loaders.schemas import FUNDING_RATE_SCHEMA, OHLCV_SCHEMA  # noqa: E402
from logging_config import get_logger, get_run_id, new_run_id, set_run_id  # noqa: E402
from signals import alpha as alpha_module  # noqa: E402
from signals import breadth as breadth_module  # noqa: E402
from signals import low_volatility as lowvol  # noqa: E402
from signals import registry, signal_families  # noqa: E402
from signals import short_term_reversal as strev  # noqa: E402

log = get_logger("signals.scratch_breadth")

START = datetime(2023, 1, 1)
N_BARS = 330
N_ASSETS = 10


def banner(title: str) -> None:
    print(f"\n{'=' * 78}\n{title}\n{'=' * 78}")


def build_store(root: Path) -> tuple[ParquetStore, list[str]]:
    """Synthetic market with a spread of trends and volatilities."""
    rng = np.random.default_rng(20260731)
    store = ParquetStore(root)

    rows, funding_rows, assets = [], [], []
    for i in range(N_ASSETS):
        asset_id = f"SYN{i}"
        assets.append(asset_id)
        drift = 0.0012 * (i - N_ASSETS / 2)
        vol = 0.01 + 0.008 * (i % 5)
        returns = drift + vol * rng.normal(size=N_BARS - 1)
        closes = 100.0 * np.exp(np.cumsum([0.0, *returns]))

        for j, close_px in enumerate(closes):
            event_ts = START + timedelta(days=j)
            open_px = float(closes[j - 1]) if j else float(close_px)
            rows.append(
                (
                    asset_id, "binance", "1d", event_ts, event_ts,
                    open_px, max(open_px, float(close_px)),
                    min(open_px, float(close_px)), float(close_px), 5_000_000.0,
                )
            )

        # Funding across the whole history, so `carry` has a view at every
        # rebalance and appears in the breadth report alongside the rest.
        for k in range(3 * N_BARS):
            event_ts = START + timedelta(hours=8 * k)
            funding_rows.append(
                (asset_id, "binance", event_ts, event_ts,
                 0.0001 * (i - N_ASSETS / 2), None, None)
            )

    store.append(
        "ohlcv_daily",
        pl.DataFrame(
            rows,
            schema=[
                "asset_id", "venue", "timeframe", "event_ts", "ingested_ts",
                "open", "high", "low", "close", "volume",
            ],
            orient="row",
        ).with_columns(
            pl.col("event_ts").cast(pl.Datetime("us")),
            pl.col("ingested_ts").cast(pl.Datetime("us")),
        ),
        OHLCV_SCHEMA,
    )
    store.append(
        "funding_rate",
        pl.DataFrame(
            funding_rows,
            schema=[
                "asset_id", "venue", "event_ts", "ingested_ts",
                "funding_rate", "mark_price", "index_price",
            ],
            orient="row",
        ).with_columns(
            pl.col("event_ts").cast(pl.Datetime("us")),
            pl.col("ingested_ts").cast(pl.Datetime("us")),
            pl.col("mark_price").cast(pl.Float64),
            pl.col("index_price").cast(pl.Float64),
        ),
        FUNDING_RATE_SCHEMA,
    )
    return store, sorted(assets)


def demo_shrinkage() -> None:
    banner("1. IC shrinkage — the same mean IC, believed to different degrees")

    rng = np.random.default_rng(3)
    cases = {
        "precise, 200 periods": (0.03 + 0.02 * rng.normal(size=200)).tolist(),
        "noisy, 200 periods": (0.03 + 0.30 * rng.normal(size=200)).tolist(),
        "precise, 12 periods": (0.03 + 0.02 * rng.normal(size=12)).tolist(),
        "one lucky period": [0.60],
    }
    estimates = [alpha_module.estimate_ic(ics, name) for name, ics in cases.items()]
    print(alpha_module.alpha_summary(estimates))
    print(
        "\n  'kept' is the fraction of the raw mean IC that survives shrinking:\n"
        "  prior_ic_std = 0.02 (a real crypto signal is small), and a series with a\n"
        "  large standard error is pulled almost all the way to zero. One period is\n"
        "  shrunk to exactly zero — it cannot separate skill from luck."
    )


def run_backtest(store: ParquetStore, universe: list[str]):
    backtester = Backtester(
        store,
        config=BacktestConfig(
            # Monthly, not weekly: every signal re-reads the store at each
            # rebalance, and a 330-partition scan x 6 signals x 45 weeks is
            # minutes of parquet I/O for a demo. `signals.panel.CachedClosePanel`
            # is the answer when a sweep needs the weekly cadence.
            rebalance_freq="monthly", backtest_start="2023-01-01", periods_per_year=365.0
        ),
        cost_model=CostModel(spread_bps=10.0, impact_bps_per_million=1.0),
        universe_provider=lambda asof: universe,
        pit_mode=PIT_EVENT,  # synthetic history shares one ingested_ts
    )
    recorder = breadth_module.ScoreRecorder(registry.signal_functions())
    result = backtester.run(
        long_short_from_scores(strev.make_signal(), n_per_side=3),
        signals=recorder.wrapped(),
    )
    return result, recorder


def demo_alpha(store: ParquetStore, universe: list[str], result) -> None:
    banner("2-3. alpha = volatility x IC x z, then combined across signals")

    estimates = alpha_module.estimate_ics(result.ic)
    print(alpha_module.alpha_summary([estimates[k] for k in sorted(estimates)]))

    from backtest import RebalanceContext

    asof = result.returns["rebalance_ts"].to_list()[-1]
    ctx = RebalanceContext(
        asof=asof, universe=universe, store=store, venue="binance",
        timeframe="1d", pit_mode=PIT_EVENT,
    )
    vols = lowvol.annualized_vol_universe(ctx)

    alphas = {}
    for signal_id, signal in sorted(registry.all_signals().items()):
        alphas[signal_id] = alpha_module.alpha_from_scores(
            signal.score_fn(ctx), estimates[signal_id].shrunk_ic, vols
        )

    combined = alpha_module.combine_alphas(alphas)
    print(f"\n  Combined alpha as of {asof.date()} (annualized expected excess return):")
    print(f"  {'asset':<8} {'ann. vol':>9} {'views':>6} {'alpha':>10}")
    for asset_id in universe:
        alpha = combined.alphas.get(asset_id)
        vol = vols.get(asset_id)
        print(
            f"  {asset_id:<8} {vol if vol is None else f'{vol:9.2%}':>9} "
            f"{combined.n_views.get(asset_id, 0):>6d} "
            f"{'      n/a' if alpha is None else f'{alpha:10.4f}'}"
        )
    print(
        "\n  'views' is how many signals had an opinion. An asset carried by one\n"
        "  signal and one all six agree on are different bets; the alpha alone\n"
        "  does not say which is which, so the count travels with it."
    )


def demo_breadth(result, recorder) -> None:
    banner("4. Breadth — are these independent bets?")

    report = breadth_module.breadth_report(
        recorder.frame(), result.ic, families=signal_families()
    )
    print(report.to_text())
    print("\n  Families:")
    for signal_id, family in sorted(signal_families().items()):
        print(f"    {signal_id:<28} {family}")
    print(
        "\n  Synthetic data: these correlations describe the generator, not the\n"
        "  signals. The real numbers belong in each doc's Section 6."
    )


def demo_logging() -> None:
    banner("5. A sample log line (Phase 5.5)")

    log.info("breadth demo complete for %d signals", len(registry.all_signals()))
    path = LOG_CONFIG.dir / "signals.log"
    if not path.exists():
        print(f"  No {path} yet.")
        return
    print(f"  {path}, last record:\n")
    print(f"    {path.read_text().splitlines()[-1]}")
    print(f"\n  run_id={get_run_id()}")


def main() -> None:
    if not PAPER:
        print("PAPER is False; scratch scripts do not run in live mode.")
        return

    set_run_id(new_run_id())
    print("Alpha refinement + breadth demo — synthetic store, read-only, no orders.")

    demo_shrinkage()

    with tempfile.TemporaryDirectory() as tmp:
        store, universe = build_store(Path(tmp) / "parquet")
        result, recorder = run_backtest(store, universe)
        demo_alpha(store, universe, result)
        demo_breadth(result, recorder)

    demo_logging()
    print("\nDone.")


if __name__ == "__main__":
    main()
