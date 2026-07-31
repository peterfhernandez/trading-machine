#!/usr/bin/env python3
"""Scratch script: demo the Phase 4 walk-forward backtester.

Builds synthetic daily OHLCV for a handful of assets, writes point-in-time
universe snapshots with the Phase 3 builder, then runs three strategies through
the engine:

1. buy-and-hold BTC (validation: must reproduce the raw open-to-open series)
2. equal weight across the universe
3. a rank-based long/short book driven by a 20-day momentum score

and reports returns, IR, drawdown, turnover, costs, and the per-signal IC series.

Read-only and offline: no exchange connection and no order placement; the engine
is a research tool. Runs only under PAPER mode, like every other scratch script.
"""

import logging
import random
import sys
import tempfile
from datetime import datetime, timedelta
from pathlib import Path

import polars as pl

sys.path.insert(0, str(Path(__file__).parent.parent))

from log_demo import start_demo_run  # noqa: E402

from backtest import (  # noqa: E402
    ZERO_COST,
    Backtester,
    CostModel,
    DatastoreUniverse,
    buy_and_hold,
    equal_weight,
    long_short_from_scores,
)
from config import PAPER, BacktestConfig, UniverseConfig  # noqa: E402
from datastore import ParquetStore  # noqa: E402
from loaders.schemas import OHLCV_SCHEMA  # noqa: E402
from universe import UniverseBuilder  # noqa: E402

logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

START = datetime(2023, 1, 1)
END = datetime(2023, 12, 31)


def make_synthetic_ohlcv(start: datetime, end: datetime) -> pl.DataFrame:
    """Daily bars with per-asset drift and vol, so momentum has something to find."""
    random.seed(7)

    # asset_id -> (daily drift, daily vol, starting price, dollar volume)
    assets = {
        "BTC": (0.0012, 0.020, 20_000.0, 50_000_000.0),
        "ETH": (0.0009, 0.025, 1_500.0, 20_000_000.0),
        "SOL": (0.0020, 0.045, 12.0, 5_000_000.0),
        "ADA": (-0.0004, 0.030, 0.30, 3_000_000.0),
        "DOGE": (-0.0010, 0.050, 0.08, 2_000_000.0),
        "LINK": (0.0005, 0.035, 6.0, 2_500_000.0),
    }

    rows = []
    for asset_id, (drift, vol, price, dollar_volume) in assets.items():
        d = start
        open_px = price
        while d <= end:
            close_px = open_px * (1 + drift + random.gauss(0, vol))
            rows.append(
                (
                    asset_id, "binance", "1d", d, d,
                    open_px,
                    max(open_px, close_px) * 1.001,
                    min(open_px, close_px) * 0.999,
                    close_px,
                    dollar_volume / open_px,
                )
            )
            open_px = close_px
            d += timedelta(days=1)

    return pl.DataFrame(
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


def momentum_scores(lookback_days: int = 20):
    """Point-in-time 20-day total return per asset (a Phase 5 signal in miniature)."""

    def score(ctx) -> dict[str, float]:
        bars = ctx.ohlcv(lookback_days=lookback_days, columns=None)
        if len(bars) == 0:
            return {}
        ranked = (
            bars.sort("event_ts")
            .group_by("asset_id")
            .agg(pl.col("close").first().alias("first"), pl.col("close").last().alias("last"))
        )
        return {
            row["asset_id"]: row["last"] / row["first"] - 1
            for row in ranked.iter_rows(named=True)
            if row["first"] and row["first"] > 0
        }

    return score


def print_metrics(name: str, result) -> None:
    m = result.metrics
    print(
        f"{name:22s} | {m.n_periods:4d} | {m.total_return:9.2%} | {m.annualized_return:9.2%} "
        f"| {m.annualized_vol:8.2%} | {m.information_ratio:6.2f} | {m.max_drawdown:8.2%} "
        f"| {m.avg_turnover:8.3f} | {m.total_cost:8.4%}"
    )


def main() -> None:
    start_demo_run("backtest")
    if not PAPER:
        logger.error("PAPER mode is False; scratch scripts must not run in production")
        sys.exit(1)

    print("\n" + "=" * 60)
    print("Backtester Demo (Phase 4)")
    print("=" * 60 + "\n")

    with tempfile.TemporaryDirectory() as tmpdir:
        store = ParquetStore(tmpdir)

        print(f"Generating synthetic OHLCV from {START.date()} to {END.date()}...")
        bars = make_synthetic_ohlcv(START, END)
        store.append("ohlcv_daily", bars, OHLCV_SCHEMA)
        print(f"Ingested {len(bars)} rows across {bars['asset_id'].n_unique()} assets\n")

        print("Writing monthly point-in-time universe snapshots...")
        builder = UniverseBuilder(
            store,
            config=UniverseConfig(target_size=5, min_volume_usdt=1_000_000.0,
                                  min_listing_age_days=30),
            venue="binance",
        )
        asof = START + timedelta(days=40)
        while asof <= END:
            builder.build_and_store(asof)
            asof += timedelta(days=30)
        print("Universe snapshots written\n")

        config = BacktestConfig(
            rebalance_freq="weekly",
            backtest_start="2023-02-10",
            backtest_end="2023-12-31",
            initial_cash=100_000.0,
            spread_bps=10.0,
            impact_bps_per_million=1.0,
        )
        # The universe snapshots below were computed in *this* run, so their
        # ingested_ts is "now" -- under strict point-in-time they would be
        # invisible to a 2023 rebalance and every book would be empty. That is
        # exactly the backfilled-history case pit_mode="event" exists for: rules
        # are still applied as of each date, but ingestion time is ignored.
        print("pit_mode='event': researching backfilled history (see backtest/engine.py)\n")
        universe_provider = DatastoreUniverse(store, "binance", pit_mode="event")

        header = (
            f"{'Strategy':22s} | {'N':>4s} | {'Total':>9s} | {'Ann.ret':>9s} | {'Ann.vol':>8s} "
            f"| {'IR':>6s} | {'MaxDD':>8s} | {'Turn':>8s} | {'Costs':>8s}"
        )
        print(header)
        print("-" * len(header))

        # 1. Validation: frictionless buy-and-hold must equal the raw price series.
        bh = Backtester(store, config=config, cost_model=ZERO_COST,
                        universe_provider=universe_provider,
                        pit_mode="event").run(buy_and_hold("BTC"))
        print_metrics("buy & hold BTC (0bps)", bh)

        # 2. Equal weight, with costs.
        ew = Backtester(store, config=config, universe_provider=universe_provider,
                        pit_mode="event").run(equal_weight())
        print_metrics("equal weight", ew)

        # 3. Rank-based long/short on 20-day momentum, with the same signal
        #    scored into an IC series.
        score_fn = momentum_scores(20)
        ls = Backtester(store, config=config, cost_model=CostModel(10.0, 1.0),
                        universe_provider=universe_provider, pit_mode="event").run(
            long_short_from_scores(score_fn, n_per_side=2),
            signals={"momentum_20d": score_fn},
        )
        print_metrics("L/S momentum 20d", ls)

        print("\nBuy-and-hold validation (engine vs. raw open-to-open prices):")
        raw = (
            store.read("ohlcv_daily")
            .filter((pl.col("asset_id") == "BTC") & (pl.col("timeframe") == "1d"))
            .sort("event_ts")
        )
        first_fill = bh.returns["fill_ts"][0]
        last_exit = bh.returns["exit_ts"][-1]
        entry_px = raw.filter(pl.col("event_ts") == first_fill)["open"][0]
        exit_px = raw.filter(pl.col("event_ts") == last_exit)["open"][0]
        raw_return = exit_px / entry_px - 1
        print(f"  raw open({first_fill.date()}) -> open({last_exit.date()}): {raw_return:.6%}")
        print(f"  engine total return:                        {bh.metrics.total_return:.6%}")
        print(f"  difference:                                 {abs(raw_return - bh.metrics.total_return):.2e}")

        print("\nPer-signal IC (momentum_20d vs. next-period returns):")
        print(ls.ic_summary())

        print("\nLast 5 rebalances of the long/short book:")
        print(ls.weights.tail(8))

    print("\n" + "=" * 60)
    print("Backtester Demo Complete")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    main()
