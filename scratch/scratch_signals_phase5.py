#!/usr/bin/env python3
"""Scratch demo: the Phase 5 signal set, end to end.

Shows, on a small synthetic store:

1. Each signal's per-asset internals — the formation return, the volatility, the
   funding average — so the construction in each methodology doc is visible
   rather than asserted.
2. Why an asset scores `None`: one example of each reject reason.
3. The two standardizations side by side, and why `time_series_momentum` uses
   the one that does not demean.
4. Every registered signal driving the Phase 4 engine, with the rank IC series
   the engine produces.
5. A sample log line from `logs/signals.log` (Phase 5.5).

Read-only and offline: no exchange connection, no order placement. Runs only
under PAPER mode.

    PAPER=true python scratch/scratch_signals_phase5.py
"""

import sys
import tempfile
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import polars as pl

sys.path.insert(0, str(Path(__file__).parent.parent))

from backtest import (  # noqa: E402
    PIT_EVENT,
    Backtester,
    CostModel,
    RebalanceContext,
    long_short_from_scores,
)
from config import PAPER, BacktestConfig  # noqa: E402
from datastore import ParquetStore  # noqa: E402
from loaders.schemas import FUNDING_RATE_SCHEMA, OHLCV_SCHEMA  # noqa: E402
from logging_config import get_logger, get_run_id, new_run_id, set_run_id  # noqa: E402
from signals import carry as carry_module  # noqa: E402
from signals import cross_sectional_momentum as xsmom  # noqa: E402
from signals import low_volatility as lowvol  # noqa: E402
from signals import registry, transforms  # noqa: E402
from signals import short_term_reversal as strev  # noqa: E402
from signals import time_series_momentum as tsmom  # noqa: E402

# A scratch script has no component of its own, so it borrows the one it is
# demonstrating: "tm.signals.scratch_demo" inherits signals.log's handler.
log = get_logger("signals.scratch_demo")

START = datetime(2023, 1, 1)
N_BARS = 260
N_ASSETS = 8

# Small windows so a whole series fits on screen and every number is checkable.
DEMO_XSMOM = xsmom.CrossSectionalMomentumParams(lookback_days=20, skip_days=3)
DEMO_TSMOM = tsmom.TimeSeriesMomentumParams(
    lookback_days=20, skip_days=0, vol_window_days=30, min_vol_observations=20
)
DEMO_STREV = strev.ShortTermReversalParams(
    lookback_days=5, vol_window_days=30, min_vol_observations=20
)
DEMO_LOWVOL = lowvol.LowVolatilityParams(vol_window_days=30, min_observations=20)
DEMO_CARRY = carry_module.DEFAULT_PARAMS


def banner(title: str) -> None:
    print(f"\n{'=' * 78}\n{title}\n{'=' * 78}")


def build_store(root: Path) -> tuple[ParquetStore, list[str]]:
    """A synthetic market: trending, choppy, quiet and violent assets."""
    rng = np.random.default_rng(20260731)
    store = ParquetStore(root)

    rows = []
    funding_rows = []
    assets = []
    for i in range(N_ASSETS):
        asset_id = f"SYN{i}"
        assets.append(asset_id)
        drift = 0.0015 * (i - N_ASSETS / 2)          # trend, spread across assets
        vol = 0.01 + 0.01 * (i % 4)                  # quiet .. violent
        returns = drift + vol * rng.normal(size=N_BARS - 1)
        closes = 100.0 * np.exp(np.cumsum([0.0, *returns]))

        for j, close_px in enumerate(closes):
            event_ts = START + timedelta(days=j)
            open_px = float(closes[j - 1]) if j else float(close_px)
            rows.append(
                (
                    asset_id, "binance", "1d", event_ts, event_ts,
                    open_px, max(open_px, float(close_px)), min(open_px, float(close_px)),
                    float(close_px), 5_000_000.0,
                )
            )

        # Funding roughly tracks the trend: crowded longs pay.
        for k in range(3 * 20):
            event_ts = START + timedelta(days=N_BARS - 21) + timedelta(hours=8 * k)
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


def demo_internals(ctx) -> None:
    banner("1. Per-asset internals — the construction, step by step")

    print("cross_sectional_momentum (formation return, skipping the last 3 bars)")
    panel = xsmom.context_panel(ctx, DEMO_XSMOM)
    for asset_id in ctx.universe[:4]:
        d = xsmom.diagnose_series(panel[asset_id].column("close"), DEMO_XSMOM)
        print(f"  {asset_id}: bars={d.n_bars:3d}  formation={d.formation_return:+.4f}")

    print("\ntime_series_momentum (trend in units of its own noise)")
    panel = tsmom.context_panel(ctx, DEMO_TSMOM)
    for asset_id in ctx.universe[:4]:
        d = tsmom.diagnose_series(panel[asset_id].column("close"), DEMO_TSMOM)
        print(
            f"  {asset_id}: formation={d.formation_return:+.4f}  "
            f"bar_vol={d.bar_vol:.4f}  horizon_vol={d.horizon_vol:.4f}  "
            f"score={d.score:+.3f}"
        )

    print("\nshort_term_reversal (negated 5-day move, vol-scaled)")
    panel = strev.context_panel(ctx, DEMO_STREV)
    for asset_id in ctx.universe[:4]:
        d = strev.diagnose_series(panel[asset_id].column("close"), DEMO_STREV)
        print(
            f"  {asset_id}: recent={d.recent_return:+.4f}  "
            f"horizon_vol={d.horizon_vol:.4f}  score={d.score:+.3f}"
        )

    print("\nlow_volatility (negated annualized realized vol)")
    panel = lowvol.context_panel(ctx, DEMO_LOWVOL)
    for asset_id in ctx.universe[:4]:
        d = lowvol.diagnose_series(panel[asset_id].column("close"), DEMO_LOWVOL)
        print(
            f"  {asset_id}: bar_vol={d.bar_vol:.4f}  "
            f"annualized={d.annualized_vol:.2%}  score={d.score:+.3f}"
        )

    print("\ncarry (annualized funding accruing to the long side)")
    panel = carry_module.context_panel(ctx, DEMO_CARRY)
    for asset_id in ctx.universe[:4]:
        series = panel.get(asset_id)
        if series is None:
            print(f"  {asset_id}: no funding data")
            continue
        d = carry_module.diagnose_series(
            series.event_ts, series.column("funding_rate"), ctx.asof, DEMO_CARRY
        )
        print(
            f"  {asset_id}: n={d.n_observations:2d}  mean_funding={d.mean_funding:+.6f}  "
            f"staleness={d.staleness_days:.2f}d  score={d.score:+.4f}"
        )


def demo_rejects() -> None:
    banner("2. Why an asset scores None — one example of each reject reason")

    short = np.array([100.0, 101.0, 102.0])
    flat = np.full(60, 100.0)
    stamps = np.array(
        [START + timedelta(hours=8 * i) for i in range(3)], dtype="datetime64[us]"
    )

    rows = [
        ("cross_sectional_momentum", "3 bars, needs 24",
         xsmom.diagnose_series(short, DEMO_XSMOM).reject_reason),
        ("time_series_momentum", "constant price series",
         tsmom.diagnose_series(flat, DEMO_TSMOM).reject_reason),
        ("short_term_reversal", "constant price series",
         strev.diagnose_series(flat, DEMO_STREV).reject_reason),
        ("low_volatility", "3 bars, needs 21",
         lowvol.diagnose_series(short, DEMO_LOWVOL).reject_reason),
        ("carry", "last print 5 days old",
         carry_module.diagnose_series(
             stamps, np.array([0.0001] * 3), START + timedelta(days=5), DEMO_CARRY
         ).reject_reason),
        ("carry", "no perp listing at all",
         carry_module.diagnose_series(
             np.empty(0, dtype="datetime64[us]"), np.empty(0), START, DEMO_CARRY
         ).reject_reason),
        ("carry", "only 2 settlements in the window",
         carry_module.diagnose_series(
             stamps[:2], np.array([0.0001] * 2), START + timedelta(hours=9), DEMO_CARRY
         ).reject_reason),
    ]
    for signal_id, situation, reason in rows:
        print(f"  {signal_id:<26} {situation:<28} -> {reason}")
    print("\n  None, never 0.0: a score of zero asserts the asset is exactly average.")


def demo_standardizations(ctx) -> None:
    banner("3. The two standardizations — and why they are not interchangeable")

    raw = tsmom.score_universe(ctx, DEMO_TSMOM)
    scored = {a: s for a, s in raw.items() if s is not None}
    zscored = transforms.cross_sectional_zscore(scored, DEMO_TSMOM.winsorize_pct)
    scaled = transforms.cross_sectional_scale(scored, DEMO_TSMOM.winsorize_pct)

    print(f"  {'asset':<8} {'raw':>9} {'z-scored':>10} {'scaled':>10}")
    for asset_id in sorted(scored):
        print(
            f"  {asset_id:<8} {scored[asset_id]:>9.3f} "
            f"{zscored[asset_id]:>10.3f} {scaled[asset_id]:>10.3f}"
        )
    print(
        f"\n  cross-sectional mean: raw {np.mean(list(scored.values())):+.3f}  "
        f"z-scored {np.mean([v for v in zscored.values()]):+.3f}  "
        f"scaled {np.mean([v for v in scaled.values()]):+.3f}"
    )
    print(
        "  The z-scored mean is zero by construction — which deletes exactly the\n"
        "  market-wide tilt a time-series signal exists to express. Hence\n"
        "  time_series_momentum standardizes with cross_sectional_scale."
    )


def demo_engine(store: ParquetStore, universe: list[str]) -> None:
    banner("4. Every registered signal through the Phase 4 engine")

    backtester = Backtester(
        store,
        config=BacktestConfig(
            rebalance_freq="weekly",
            backtest_start="2023-01-01",
            periods_per_year=365.0,
        ),
        cost_model=CostModel(spread_bps=10.0, impact_bps_per_million=1.0),
        universe_provider=lambda asof: universe,
        pit_mode=PIT_EVENT,  # synthetic history shares one ingested_ts
    )
    result = backtester.run(
        long_short_from_scores(strev.make_signal(DEMO_STREV), n_per_side=2),
        signals=registry.signal_functions(),
    )

    print(f"  Rebalances realized: {len(result.returns)}")
    print("\n  Rank IC by signal (synthetic data — this measures the generator):")
    print(result.ic_summary())
    print("\n  Book metrics (short_term_reversal long/short):")
    for key, value in result.metrics.to_dict().items():
        print(f"    {key:<22} {value:>12.4f}" if isinstance(value, float) else
              f"    {key:<22} {value}")


def demo_logging() -> None:
    banner("5. A sample log line (Phase 5.5)")

    log.info("scratch demo finished; %d signals registered", len(registry.all_signals()))
    from config import LOG_CONFIG

    path = LOG_CONFIG.dir / "signals.log"
    if not path.exists():
        print(f"  No {path} yet.")
        return
    lines = path.read_text().splitlines()
    print(f"  {path} ({len(lines)} records). Last one:\n")
    print(f"    {lines[-1]}")
    print(
        f"\n  Every record carries run_id={get_run_id()}, so this run can be traced\n"
        "  across every component's log file."
    )


def main() -> None:
    if not PAPER:
        print("PAPER is False; scratch scripts do not run in live mode.")
        return

    set_run_id(new_run_id())
    print("Phase 5 signals demo — synthetic store, read-only, no orders.")

    with tempfile.TemporaryDirectory() as tmp:
        store, universe = build_store(Path(tmp) / "parquet")
        asof = START + timedelta(days=N_BARS - 1)
        ctx = RebalanceContext(
            asof=asof, universe=universe, store=store, venue="binance",
            timeframe="1d", pit_mode=PIT_EVENT,
        )

        demo_internals(ctx)
        demo_rejects()
        demo_standardizations(ctx)
        demo_engine(store, universe)
        demo_logging()

    print("\nDone.")


if __name__ == "__main__":
    main()
