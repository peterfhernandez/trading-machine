#!/usr/bin/env python3
"""Scratch script: demo Universe module (liquidity/listing-age/exclusion rules).

Builds a point-in-time universe over a series of weekly rebalance dates on
synthetic OHLCV data and reports universe size and turnover over history
(sanity check for Phase 3)."""

import logging
import random
import sys
import tempfile
from datetime import datetime, timedelta

import polars as pl

# Imported first: log_demo puts the repository root on sys.path, so the
# project imports below resolve when this script is run directly.
from log_demo import start_demo_run

from config import PAPER, UniverseConfig
from datastore import ParquetStore
from loaders.schemas import OHLCV_SCHEMA
from universe import UniverseBuilder, compute_turnover



logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def make_synthetic_ohlcv(start: datetime, end: datetime) -> pl.DataFrame:
    """Synthetic daily OHLCV for a mix of long-lived, late-listing, low-volume,
    and stablecoin/wrapped assets so every universe filter gets exercised."""
    random.seed(42)

    # asset_id -> (listing_date, base_dollar_volume)
    assets = {
        "BTC": (start, 50_000_000.0),
        "ETH": (start, 20_000_000.0),
        "SOL": (start, 5_000_000.0),
        "ADA": (start, 3_000_000.0),
        "DOGE": (start, 2_000_000.0),
        "LOWVOL": (start, 50_000.0),           # below min_volume_usdt
        "USDT": (start, 30_000_000.0),          # stablecoin, excluded regardless
        "WBTC": (start, 8_000_000.0),           # wrapped, excluded regardless
        "LATECOIN": (datetime(2024, 1, 10), 4_000_000.0),  # listed late; joins once old enough
    }

    rows = []
    for asset_id, (listing_date, base_volume) in assets.items():
        d = max(listing_date, start)
        price = 100.0
        while d <= end:
            price *= 1 + random.uniform(-0.03, 0.03)
            volume = base_volume * random.uniform(0.7, 1.3) / max(price, 1e-6)
            rows.append((asset_id, "binance", "1d", d, d, price, price, price, price, volume))
            d += timedelta(days=1)

    df = pl.DataFrame(
        rows,
        schema=[
            "asset_id", "venue", "timeframe", "event_ts", "ingested_ts",
            "open", "high", "low", "close", "volume",
        ],
        orient="row",
    )
    return df.with_columns(
        pl.col("event_ts").cast(pl.Datetime("us")),
        pl.col("ingested_ts").cast(pl.Datetime("us")),
    )


def main():
    start_demo_run("universe")
    if not PAPER:
        logger.error("PAPER mode is False; scratch scripts must not run in production")
        sys.exit(1)

    print("\n" + "=" * 60)
    print("Universe Module Demo")
    print("=" * 60 + "\n")

    with tempfile.TemporaryDirectory() as tmpdir:
        store = ParquetStore(tmpdir)

        start = datetime(2023, 1, 1)
        end = datetime(2024, 3, 1)

        print(f"Generating synthetic OHLCV data from {start.date()} to {end.date()}...")
        df = make_synthetic_ohlcv(start, end)
        store.append("ohlcv_daily", df, OHLCV_SCHEMA)
        print(f"Ingested {len(df)} rows across {df['asset_id'].n_unique()} assets\n")

        config = UniverseConfig(target_size=5, min_volume_usdt=1_000_000.0)
        builder = UniverseBuilder(store, config=config, venue="binance")

        # Weekly rebalances over the last 90 days of history
        rebalance_dates = []
        d = end - timedelta(days=90)
        while d <= end:
            rebalance_dates.append(d)
            d += timedelta(days=7)

        print("Building weekly universe snapshots...\n")
        print(f"{'Date':12s} | {'Size':>4s} | {'Turnover':>8s} | Members")
        print("-" * 90)

        prev_snapshot = None
        for asof in rebalance_dates:
            snapshot = builder.build_and_store(asof)
            members = sorted(snapshot.filter(pl.col("in_universe"))["asset_id"].to_list())
            turnover = compute_turnover(prev_snapshot, snapshot) if prev_snapshot is not None else 0.0
            print(f"{asof.date()} | {len(members):4d} | {turnover:8.2%} | {', '.join(members)}")
            prev_snapshot = snapshot

        print("\nException reasons on the final snapshot:")
        final = prev_snapshot
        for reason in ["stablecoin", "wrapped", "listing_age", "low_volume", "rank_cutoff"]:
            excluded = final.filter(pl.col("exclusion_reason") == reason)["asset_id"].to_list()
            if excluded:
                print(f"  {reason:12s}: {', '.join(sorted(excluded))}")

    print("\n" + "=" * 60)
    print("Universe Demo Complete")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    main()
