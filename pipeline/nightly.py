"""Nightly pipeline: load → audit → report."""

import sys
import time
from datetime import datetime, timedelta, timezone
from typing import Optional

from config import DATASTORE_PATH, LOADER_CONFIG, LOG_CONFIG
from datastore import ParquetStore, AssetMaster
from loaders.backfill import BackfillRunner
from loaders.window import utc_now
from audit.auditor import DataAudit
from logging_config import get_logger, new_run_id, prune_old_logs, set_level, set_run_id


logger = get_logger(__name__)


class NightlyPipeline:
    """End-to-end nightly data pipeline."""

    def __init__(
        self,
        venue: str = "binance",
        dry_run: bool = False,
        ignore_checkpoint: bool = False,
    ):
        self.venue = venue
        self.dry_run = dry_run
        self.ignore_checkpoint = ignore_checkpoint
        self.store = ParquetStore(DATASTORE_PATH)
        self.trading_halted = False

    def run(
        self,
        days: int = 1,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
    ) -> bool:
        """Run the nightly pipeline.

        Sets the `run_id` every component's log records carry, so one night's
        activity can be pulled out of all eleven files in `logs/` by grepping a
        single value. Prunes expired log backups as its last step.

        Args:
            days: Window length in days when `start` is not given (default: 1)
            start: Explicit start of the fetch window (event time, UTC)
            end: Explicit end of the fetch window (default: now)

        Returns:
            True if successful, False if halted due to audit failures
        """
        run_id = new_run_id()
        set_run_id(run_id)

        window_desc = (
            f"{(start or (end or utc_now()) - timedelta(days=days)).date()}"
            f"..{(end or utc_now()).date()}"
        )

        logger.info("=" * 70)
        logger.info(f"Starting Nightly Pipeline {'(DRY RUN)' if self.dry_run else ''}")
        logger.info(f"run_id={run_id}, venue={self.venue}, window={window_desc}")
        logger.info("=" * 70)

        started = time.monotonic()
        try:
            self._stage("load", self._load_stage, days, start=start, end=end)
            self._stage("audit", self._audit_stage)
            self._stage("report", self._report_stage)

            logger.info("=" * 70)
            logger.info(
                f"✓ Pipeline complete in {time.monotonic() - started:.1f}s "
                f"(run_id={run_id}, trading "
                f"{'HALTED' if self.trading_halted else 'active'})"
            )
            logger.info("=" * 70)

            return not self.trading_halted
        except Exception as e:
            # CRITICAL, not ERROR: an unhandled exception here means the run
            # produced no data and no audit, which is a halt in everything but
            # name (LOGGING.md section 3.5).
            logger.critical(f"Pipeline failed after {time.monotonic() - started:.1f}s: {e}",
                            exc_info=True)
            return False
        finally:
            self._prune_logs()

    def _stage(self, name: str, fn, *args, **kwargs) -> None:
        """Run one pipeline stage, logging entry, exit and duration.

        An exception is logged CRITICAL here — with the stage that raised it —
        and re-raised for `run()` to turn into a failed run. The individual
        stages already swallow their own recoverable failures (a loader that
        cannot reach the venue is non-fatal by design); anything reaching this
        handler is not recoverable.
        """
        logger.info(f"[{name.upper()} STAGE] starting")
        started = time.monotonic()
        try:
            fn(*args, **kwargs)
        except Exception:
            logger.critical(
                f"[{name.upper()} STAGE] failed after {time.monotonic() - started:.1f}s",
                exc_info=True,
            )
            raise
        logger.info(
            f"[{name.upper()} STAGE] complete in {time.monotonic() - started:.1f}s"
        )

    @staticmethod
    def _prune_logs() -> None:
        """Delete rotated log backups past the retention window.

        Runs in a `finally`, so retention is enforced even on a failed run —
        a pipeline that fails every night is exactly the one whose logs grow
        fastest. Never allowed to fail the run itself.
        """
        try:
            deleted = prune_old_logs()
            if deleted:
                logger.info(
                    f"Pruned {len(deleted)} log backup(s) older than "
                    f"{LOG_CONFIG.retention_days} days"
                )
        except Exception as e:
            logger.warning(f"Log pruning failed: {e}")

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
                if (
                    am.resolve_symbol(
                        symbol, venue, asof=base_date, warn_if_unresolved=False
                    )
                    is not None
                ):
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

    def _load_stage(
        self,
        days: int,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
    ) -> None:
        """Load data via backfill runner.

        Failures here are deliberately non-fatal — an unreachable venue should
        still let the audit report on what is already in the store — so they are
        logged ERROR (a caught exception that does not halt the run) rather than
        re-raised into `_stage`'s CRITICAL handler.
        """
        if self.dry_run:
            logger.info(f"(DRY RUN) Would run backfill for {days} days")
            return

        try:
            # Populate asset master with venue symbols
            asset_master = self._populate_asset_master(self.venue)

            runner = BackfillRunner(
                self.venue,
                asset_master=asset_master,
                ignore_checkpoint=self.ignore_checkpoint,
            )
            runner.run(start_date=start, end_date=end, days_back=days)
        except Exception as e:
            logger.error(
                f"Load stage failed: {e} (continuing to audit)", exc_info=True
            )

    def _audit_stage(self) -> None:
        """Audit data quality.

        `DataAudit` logs every check's verdict itself, and logs CRITICAL from
        `should_halt_trading()` before the alert is attempted, so this stage
        only records the per-dataset roll-up.
        """
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
                logger.info(f"{'✓ PASS' if passed else '✗ FAIL'} | {dataset}")

                # should_halt_trading() logs CRITICAL itself, before this point
                # and independently of whether the Telegram send below succeeds.
                if audit.should_halt_trading():
                    self.trading_halted = True

                audit.send_alerts()

            except Exception as e:
                logger.error(f"Audit failed for {dataset}: {e}", exc_info=True)
                all_passed = False

        if not all_passed and not self.dry_run:
            logger.warning("⚠️  Some audit checks failed; review alerts")

    def _report_stage(self) -> None:
        """Generate report."""
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


def parse_window_arg(value: Optional[str], label: str) -> Optional[datetime]:
    """Parse a --start/--end CLI value (YYYY-MM-DD or full ISO 8601)."""
    if value is None:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as e:
        raise SystemExit(f"--{label} must be YYYY-MM-DD or ISO 8601, got {value!r}: {e}")
    return parsed.replace(tzinfo=None) if parsed.tzinfo else parsed


def run_nightly(
    venue: str = "binance",
    dry_run: bool = False,
    days: int = 1,
    start: Optional[datetime] = None,
    end: Optional[datetime] = None,
    ignore_checkpoint: bool = False,
) -> None:
    """Run the nightly pipeline end-to-end.

    Args:
        venue: Exchange name (default: "binance")
        dry_run: If True, simulate without writing (default: False)
        days: Window length in days when `start` is not given (default: 1)
        start: Explicit start of the fetch window (event time, UTC)
        end: Explicit end of the fetch window (default: now)
        ignore_checkpoint: Re-fetch the window even where it is already covered
    """
    pipeline = NightlyPipeline(venue, dry_run, ignore_checkpoint=ignore_checkpoint)
    success = pipeline.run(days, start=start, end=end)

    if not success and not dry_run:
        sys.exit(1)


if __name__ == "__main__":
    import argparse

    # No logging.basicConfig() here: it configures the *stdlib root* logger,
    # and the `tm` tree deliberately does not propagate to it (LOGGING.md
    # section 3.6), so it never affected a single record this pipeline emits.
    # Console verbosity is LOG_CONFIG.console_level, changed with --log-level.

    parser = argparse.ArgumentParser(
        description="Run nightly data pipeline",
        epilog=(
            "The fetch window is resumable: each dataset records what it has "
            "covered and only the missing part is fetched, minus a re-fetch "
            "overlap for the incomplete trailing bar. Use --ignore-checkpoint "
            "to force a full re-fetch."
        ),
    )
    parser.add_argument("--venue", default="binance", help="Exchange name")
    parser.add_argument("--dry-run", action="store_true", help="Simulate without writing")
    parser.add_argument(
        "--days", type=int, default=1, help="Window length in days (ignored if --start is given)"
    )
    parser.add_argument("--start", help="Window start, YYYY-MM-DD or ISO 8601 (UTC)")
    parser.add_argument("--end", help="Window end, YYYY-MM-DD or ISO 8601 (UTC); default now")
    parser.add_argument(
        "--ignore-checkpoint",
        action="store_true",
        help="Re-fetch the whole window even where the checkpoint says it is covered",
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help=(
            "Level for logs/*.log for this run (default: TM_LOG_LEVEL, else "
            "INFO). DEBUG adds per-asset signal reject reasons and "
            "per-rebalance backtest detail"
        ),
    )
    parser.add_argument(
        "--console-log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Level for stderr for this run (default: TM_CONSOLE_LOG_LEVEL, else WARNING)",
    )

    args = parser.parse_args()

    if args.log_level or args.console_log_level:
        set_level(args.log_level or LOG_CONFIG.level, args.console_log_level)

    run_nightly(
        venue=args.venue,
        dry_run=args.dry_run,
        days=args.days,
        start=parse_window_arg(args.start, "start"),
        end=parse_window_arg(args.end, "end"),
        ignore_checkpoint=args.ignore_checkpoint,
    )
