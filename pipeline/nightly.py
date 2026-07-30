"""Nightly pipeline: load → audit → report."""

import logging
import sys
from datetime import datetime, timezone
from typing import Optional

from config import DATASTORE_PATH, LOADER_CONFIG
from datastore import ParquetStore, AssetMaster
from loaders.backfill import BackfillRunner
from audit.auditor import DataAudit


logger = logging.getLogger(__name__)


class NightlyPipeline:
    """End-to-end nightly data pipeline."""

    def __init__(self, venue: str = "binance", dry_run: bool = False):
        self.venue = venue
        self.dry_run = dry_run
        self.store = ParquetStore(DATASTORE_PATH)
        self.trading_halted = False

    def run(self, days: int = 1) -> bool:
        """Run the nightly pipeline.

        Args:
            days: How many days of data to fetch (default: 1 = daily)

        Returns:
            True if successful, False if halted due to audit failures
        """
        logger.info("=" * 70)
        logger.info(f"Starting Nightly Pipeline {'(DRY RUN)' if self.dry_run else ''}")
        logger.info(f"Venue: {self.venue}, Days: {days}")
        logger.info("=" * 70)

        try:
            self._load_stage(days)
            self._audit_stage()
            self._report_stage()

            logger.info("=" * 70)
            logger.info("✓ Pipeline complete")
            logger.info("=" * 70)

            return not self.trading_halted
        except Exception as e:
            logger.error(f"Pipeline failed: {e}", exc_info=True)
            return False

    def _venue_markets(self, venue: str) -> dict[str, dict]:
        """Collect USDT markets across every market type the loaders use.

        The OHLCV loader runs against the venue's default (spot) markets while
        the funding-rate and open-interest loaders run against derivatives
        (`LOADER_CONFIG.perp_market_type`). Both symbol namespaces must be in
        the asset master, or the perp loaders resolve every symbol to None and
        write nothing.
        """
        import ccxt

        exchange_class = getattr(ccxt, venue)
        markets: dict[str, dict] = {}

        for options in ({}, {"options": {"defaultType": LOADER_CONFIG.perp_market_type}}):
            try:
                exchange = exchange_class(options) if options else exchange_class()
                exchange.load_markets()
            except Exception as e:
                logger.warning(f"Could not load {venue} markets with {options or 'defaults'}: {e}")
                continue

            for symbol in exchange.symbols or []:
                if "/USDT" not in symbol:
                    continue
                # Map every exact symbol string to its base asset. Loaders
                # iterate exchange.symbols directly and may see spot
                # ("BTC/USDT"), perpetual ("BTC/USDT:USDT"), or quarterly
                # ("BTC/USDT:USDT-260327") notation; resolve_symbol() matches on
                # the literal string, so the mapping must preserve whatever
                # exact form each market uses rather than reconstructing a
                # synthetic "{asset}/USDT" symbol.
                markets.setdefault(symbol, exchange.markets.get(symbol) or {})

        return markets

    def _populate_asset_master(self, venue: str) -> AssetMaster:
        """Populate asset master with venue symbols (spot + perp namespaces)."""
        asset_master_path = DATASTORE_PATH / "asset_master.parquet"
        am = AssetMaster(asset_master_path)

        try:
            markets = self._venue_markets(venue)

            if not markets:
                logger.warning(f"No USDT symbols returned from {venue}")
                return am

            base_date = datetime.now(timezone.utc).replace(tzinfo=None)
            added = 0
            skipped = 0
            for symbol, market in sorted(markets.items()):
                # add_mapping appends unconditionally, so re-adding a symbol on
                # every nightly run would grow the master without bound and make
                # resolve_symbol ambiguous.
                if am.resolve_symbol(symbol, venue, asof=base_date) is not None:
                    skipped += 1
                    continue

                base = market.get("base") or symbol.split("/")[0]
                try:
                    am.add_mapping(base, venue, symbol, base_date)
                    added += 1
                except Exception as e:
                    logger.warning(f"Failed to add mapping for {symbol}: {e}")

            logger.info(
                f"✓ Asset master: {added} new {venue} symbols added, "
                f"{skipped} already mapped ({len(markets)} USDT symbols seen)"
            )
        except Exception as e:
            logger.warning(f"Failed to auto-populate asset master: {e}", exc_info=True)

        return am

    def _load_stage(self, days: int) -> None:
        """Load data via backfill runner."""
        logger.info("\n[LOAD STAGE] Fetching latest data...")

        if self.dry_run:
            logger.info(f"(DRY RUN) Would run backfill for {days} days")
            return

        try:
            # Populate asset master with venue symbols
            asset_master = self._populate_asset_master(self.venue)

            runner = BackfillRunner(self.venue, asset_master=asset_master)
            runner.run(days_back=days)
            logger.info("✓ Load stage complete")
        except Exception as e:
            logger.warning(f"Load stage failed: {e} (will continue to audit)")

    def _audit_stage(self) -> None:
        """Audit data quality."""
        logger.info("\n[AUDIT STAGE] Running data quality checks...")

        datasets = ["ohlcv_daily", "ohlcv_hourly", "funding_rate", "open_interest"]
        today = datetime.now(timezone.utc).replace(tzinfo=None)

        all_passed = True

        for dataset in datasets:
            if self.dry_run:
                logger.info(f"(DRY RUN) Would audit {dataset}")
                continue

            try:
                audit = DataAudit(self.store, venue=self.venue)
                results = audit.audit_dataset(dataset, today)

                passed = all(r.passed for r in results)
                all_passed = all_passed and passed

                status = "✓ PASS" if passed else "✗ FAIL"
                logger.info(f"{status} | {dataset}")

                # Log failures and any check that passed only with a warning
                # (e.g. coverage with no universe snapshot to measure against),
                # so a check that could not be evaluated is never silent.
                for r in results:
                    if not r.passed or r.severity == "warning":
                        logger.warning(f"    └─ {r.check_name}: {r.message}")

                audit.send_alerts()

                if audit.should_halt_trading():
                    self.trading_halted = True
                    logger.error(f"TRADING HALTED: critical audit failure in {dataset}")

            except Exception as e:
                logger.warning(f"Audit failed for {dataset}: {e}")
                all_passed = False

        if not all_passed and not self.dry_run:
            logger.warning("⚠️  Some audit checks failed; review alerts")

    def _report_stage(self) -> None:
        """Generate report."""
        logger.info("\n[REPORT STAGE] Generating report...")

        datasets = ["ohlcv_daily", "ohlcv_hourly", "funding_rate", "open_interest"]

        print("\nDataset Summary:")
        print("-" * 70)

        for dataset in datasets:
            try:
                info = self.store.dataset_info(dataset)
                if not info:
                    print(f"{dataset:20s} | No data")
                else:
                    date_range_str = f"{info['date_range'][0]} to {info['date_range'][1]}" if info['date_range'] else "N/A"
                    print(f"{dataset:20s} | Rows: {info['row_count']:8d} | "
                          f"Dates: {date_range_str}")
            except Exception as e:
                print(f"{dataset:20s} | Error: {e}")

        print("-" * 70)
        print(f"\nTrading Status: {'🛑 HALTED' if self.trading_halted else '✓ ACTIVE'}")
        print()


def run_nightly(venue: str = "binance", dry_run: bool = False, days: int = 1) -> None:
    """Run the nightly pipeline end-to-end.

    Args:
        venue: Exchange name (default: "binance")
        dry_run: If True, simulate without writing (default: False)
        days: How many days of history to fetch (default: 1 = daily update)
    """
    pipeline = NightlyPipeline(venue, dry_run)
    success = pipeline.run(days)

    if not success and not dry_run:
        sys.exit(1)


if __name__ == "__main__":
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    parser = argparse.ArgumentParser(description="Run nightly data pipeline")
    parser.add_argument("--venue", default="binance", help="Exchange name")
    parser.add_argument("--dry-run", action="store_true", help="Simulate without writing")
    parser.add_argument("--days", type=int, default=1, help="Days of history to fetch")

    args = parser.parse_args()

    run_nightly(venue=args.venue, dry_run=args.dry_run, days=args.days)
