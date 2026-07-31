#!/usr/bin/env python3
"""Scratch script: what the loaders actually see on a real venue.

Demonstrates the two loader fixes against a live (read-only, unauthenticated)
ccxt connection:

1. Spot markets carry no funding rate / open interest, so the perp loaders open
   the venue with `defaultType=LOADER_CONFIG.perp_market_type`.
2. The USDT filter runs before the symbol cap. Slicing first
   (`exchange.symbols[:50]`) and filtering after starves the budget, because a
   venue's symbol list interleaves every quote currency alphabetically.

Read-only: fetches market metadata plus one funding-rate/open-interest sample.
Places no orders.
"""

import logging
import sys

import ccxt

# Imported first: log_demo puts the repository root on sys.path, so the
# project imports below resolve when this script is run directly.
# isort: off
from log_demo import start_demo_run

# isort: on

from config import LOADER_CONFIG, PAPER
from loaders.base import select_usdt_symbols

logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

VENUE = "binance"


def load(market_type: str | None) -> ccxt.Exchange:
    """Open a venue against a specific ccxt market type."""
    exchange_class = getattr(ccxt, VENUE)
    exchange = (
        exchange_class({"options": {"defaultType": market_type}})
        if market_type
        else exchange_class()
    )
    exchange.load_markets()
    return exchange


def main():
    start_demo_run("loaders")
    if not PAPER:
        logger.error("PAPER mode is False; scratch scripts must not run in production")
        sys.exit(1)

    print("\n" + "=" * 70)
    print(f"Perp symbol selection on {VENUE}")
    print("=" * 70)

    try:
        spot = load(None)
        perp = load(LOADER_CONFIG.perp_market_type)
    except Exception as e:
        logger.error(f"Could not reach {VENUE}: {e}")
        sys.exit(1)

    cap = LOADER_CONFIG.max_symbols_per_run

    for label, exchange, prefer_perps in (
        ("spot (ccxt default)", spot, False),
        (f"{LOADER_CONFIG.perp_market_type} (perp loaders)", perp, True),
    ):
        symbols = exchange.symbols or []
        selected = select_usdt_symbols(symbols, max_symbols=cap, prefer_perps=prefer_perps)
        naive = [s for s in symbols[:50] if s.endswith("USDT") or s.endswith(":USDT")]

        print(f"\n--- {label} ---")
        print(f"  venue symbols total       : {len(symbols)}")
        print(f"  selected (filter then cap): {len(selected)}  (cap {cap})")
        print(f"  old behaviour (cap then filter, budget 50): {len(naive)}")
        print(f"  first 5 selected          : {selected[:5]}")

    # Prove the market type matters: the same asset, asked for funding on both.
    print("\n--- funding rate / open interest by market type ---")
    for label, exchange, prefer_perps in (
        ("spot", spot, False),
        (LOADER_CONFIG.perp_market_type, perp, True),
    ):
        selected = select_usdt_symbols(
            exchange.symbols, max_symbols=cap, prefer_perps=prefer_perps
        )
        btc = next((s for s in selected if s.startswith("BTC/")), None)
        if btc is None:
            print(f"  {label:8s} | no BTC/USDT symbol found")
            continue

        try:
            rate = exchange.fetch_funding_rate(btc)
            print(f"  {label:8s} | {btc:18s} funding rate: {rate.get('fundingRate')}")
        except Exception as e:
            print(f"  {label:8s} | {btc:18s} funding rate FAILED: {type(e).__name__}")

        try:
            oi = exchange.fetch_open_interest(btc)
            print(f"  {label:8s} | {btc:18s} open interest: {oi.get('openInterest')}")
        except Exception as e:
            print(f"  {label:8s} | {btc:18s} open interest FAILED: {type(e).__name__}")

    print("\n" + "=" * 70)
    print("Demo complete")
    print("=" * 70 + "\n")


if __name__ == "__main__":
    main()
