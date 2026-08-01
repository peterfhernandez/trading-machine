"""Data audit module: quality checks and alerting."""

import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import polars as pl

from config import ALERT_CONFIG, AUDIT_CONFIG, DATASTORE_PATH
from datastore import (
    DEFAULT_BAR_KEYS,
    ParquetStore,
    count_duplicate_bars,
    latest_per_bar,
)
from logging_config import get_logger

logger = get_logger(__name__)


@dataclass
class AuditResult:
    """Result of a single audit check.

    `severity` is a **data-quality** taxonomy, not an application log level:
    "error" means this check can halt trading, not that an exception occurred.
    The two are deliberately separate concerns (LOGGING.md section 2); the
    mapping used when a result is written to the log is `_LOG_LEVEL_BY_SEVERITY`
    below.
    """

    check_name: str
    passed: bool
    message: str
    severity: str  # "info", "warning", "error"


_LOG_LEVEL_BY_SEVERITY = {
    "info": logging.INFO,
    "warning": logging.WARNING,
    "error": logging.ERROR,
}


class DataAudit:
    """Audit data quality in the datastore."""

    def __init__(
        self,
        store: ParquetStore | None = None,
        venue: str = "binance",
        universe_size: int | None = None,
    ):
        """
        Args:
            store: Datastore to audit (default: the configured store)
            venue: Venue whose universe snapshots define the coverage denominator
            universe_size: Explicit universe size for the coverage check,
                overriding the point-in-time universe snapshot. Use only when
                there is a known expected asset count; leaving it None keeps
                the check honest about not having a reference.
        """
        self.store = store or ParquetStore(DATASTORE_PATH)
        self.venue = venue
        self.universe_size_override = universe_size
        self.results: list[AuditResult] = []

    def audit_dataset(self, dataset: str, date: datetime | None = None) -> list[AuditResult]:
        """Audit a single dataset.

        Args:
            dataset: Dataset name (e.g., "ohlcv_daily")
            date: Date to audit (default: today)

        Returns:
            List of AuditResult objects
        """
        if date is None:
            date = datetime.now(UTC).replace(tzinfo=None)

        self.results = []
        logger.info(f"Auditing {dataset} for {date.date()}")

        # Read a bounded lookback window rather than the exact audit day, so
        # data ingested a day (or more) late shows up as stale rather than
        # being indistinguishable from never having been ingested at all.
        lookback_start = (date - timedelta(days=AUDIT_CONFIG.audit_lookback_days)).date()

        try:
            try:
                df = self.store.read(dataset, date_range=(lookback_start, date.date()))
            except FileNotFoundError:
                # Dataset has never been ingested at all (e.g. first-ever run);
                # this is the same "no data" outcome as an empty read, not a
                # separate execution failure.
                df = pl.DataFrame()

            if len(df) == 0:
                self.results.append(AuditResult(
                    check_name="data_presence",
                    passed=False,
                    message=f"No data found in the last {AUDIT_CONFIG.audit_lookback_days} days (as of {date.date()})",
                    severity="error",
                ))
                self._log_results(dataset)
                return self.results

            # Report duplicates against the raw read, then run every other check
            # against one row per bar. Null rates, price jumps and freshness all
            # aggregate rows, so a bar stored twice would otherwise be counted
            # twice; the duplicate is a fact about ingestion, not about the data.
            self._check_duplicate_bars(df)
            df = latest_per_bar(df)

            members, universe_size, universe_source = self.resolve_universe(date)
            self._check_coverage(df, members, universe_size, universe_source)
            self._check_null_rates(df, dataset)
            self._check_freshness(df, date, AUDIT_CONFIG.freshness_threshold_for(dataset))
            if "close" in df.columns:
                self._check_price_outliers(df)

        except Exception as e:
            logger.error(f"Audit failed for {dataset}: {e}", exc_info=True)
            self.results.append(AuditResult(
                check_name="audit_execution",
                passed=False,
                message=f"Audit execution failed: {e}",
                severity="error",
            ))

        self._log_results(dataset)
        return self.results

    def _log_results(self, dataset: str) -> None:
        """Write every check's verdict to the log at its severity's level.

        Every check, not only the failures: the durable record of a clean run is
        what makes an unattended run reconstructable after the fact, and a check
        that stopped running is only visible by its absence from the log.
        """
        passed = sum(1 for r in self.results if r.passed)
        logger.info(
            f"Audit {dataset}: {passed}/{len(self.results)} checks passed"
        )
        for result in self.results:
            level = _LOG_LEVEL_BY_SEVERITY.get(result.severity, logging.INFO)
            logger.log(
                level,
                f"  {dataset}.{result.check_name}: "
                f"{'PASS' if result.passed else 'FAIL'} — {result.message}",
            )

    def resolve_universe_size(self, date: datetime) -> tuple[int | None, str]:
        """Coverage denominator as of `date`, and where it came from.

        Convenience wrapper over `resolve_universe` for callers that only need
        the size.
        """
        _, size, source = self.resolve_universe(date)
        return size, source

    def resolve_universe(
        self, date: datetime
    ) -> tuple[set[str] | None, int | None, str]:
        """Universe membership as of `date`: (members, size, source).

        The coverage check needs a denominator — "% of *what*". The only
        defensible answer is the point-in-time universe: the latest `universe`
        snapshot with `event_ts <= date`, read through `ingested_ts <= date`, so
        the audit measures itself against the membership that was actually
        known on the audit date.

        Returns (None, None, reason) when no such snapshot exists. A fabricated
        denominator — e.g. a hardcoded target size — makes the check either
        vacuous or permanently red depending on how many assets the loaders
        happen to cover, so the check reports itself unevaluated instead.
        `members` is None when only a size is known (an explicit override).
        """
        if self.universe_size_override is not None:
            return None, self.universe_size_override, "explicit universe_size"

        try:
            df = self.store.read(
                "universe",
                asof=date.date().isoformat(),
                columns=["asset_id", "venue", "event_ts", "ingested_ts", "in_universe"],
            )
        except FileNotFoundError:
            return None, None, "no universe dataset in the store"
        except Exception as e:
            # A malformed/legacy universe partition must not turn into an audit
            # execution failure for an unrelated dataset.
            logger.warning(f"Could not read universe snapshots: {e}")
            return None, None, f"universe dataset unreadable ({e})"

        if len(df) == 0:
            # Either nothing was ever written, or every snapshot was ingested
            # after the audit date and so was not knowable then.
            return None, None, f"no universe snapshot ingested on or before {date.date()}"

        if "venue" in df.columns:
            df = df.filter(pl.col("venue") == self.venue)
        df = df.filter(pl.col("event_ts") <= date)

        if len(df) == 0:
            return (
                None,
                None,
                f"no universe snapshot for {self.venue} at or before {date.date()}",
            )

        latest = df["event_ts"].max()
        member_rows = df.filter((pl.col("event_ts") == latest) & pl.col("in_universe"))
        members = set(member_rows["asset_id"].to_list())

        if not members:
            return None, None, f"universe snapshot {latest.date()} has no members"

        return members, len(members), f"universe snapshot {latest.date()}"

    def _check_coverage(
        self,
        df: pl.DataFrame,
        members: set[str] | None,
        universe_size: int | None,
        universe_source: str,
    ) -> None:
        """Check coverage: how much of the point-in-time universe has data.

        Counts *universe members* present in the dataset, not distinct asset_ids:
        a dataset full of assets that are not in the universe covers none of it.
        """
        if "asset_id" not in df.columns:
            return

        observed = set(df["asset_id"].drop_nulls().to_list())
        asset_count = len(observed & members) if members is not None else len(observed)

        if universe_size is None:
            # Not a pass in the "data looks good" sense — a pass in the "this
            # check has no reference to judge against" sense. Surfaced as a
            # warning so it shows up in logs and alerts without halting
            # trading on a number we made up.
            self.results.append(AuditResult(
                check_name="coverage",
                passed=True,
                message=(
                    f"Coverage: {asset_count} assets; not evaluated "
                    f"({universe_source})"
                ),
                severity="warning",
            ))
            return

        threshold = max(1, int(universe_size * AUDIT_CONFIG.coverage_threshold_pct / 100.0))
        pct = 100.0 * asset_count / universe_size

        passed = asset_count >= threshold
        severity = "error" if not passed else "info"

        self.results.append(AuditResult(
            check_name="coverage",
            passed=passed,
            message=(
                f"Coverage: {asset_count}/{universe_size} universe assets "
                f"({pct:.1f}%) (threshold: {threshold}, from {universe_source})"
            ),
            severity=severity,
        ))

    def _check_duplicate_bars(self, df: pl.DataFrame) -> None:
        """Report bars stored more than once in the lookback window.

        Not an error: the store is append-only, so re-fetching an already-stored
        bar (overlapping loader windows, a retried run, a vendor revision) is
        expected and the older copy is kept on purpose. Readers collapse to the
        latest ingestion. It is worth surfacing because a duplicate rate that
        climbs run over run means the loaders are re-fetching history they
        already have, which costs API budget and disk for nothing.
        """
        if any(k not in df.columns for k in DEFAULT_BAR_KEYS):
            return

        duplicates = count_duplicate_bars(df)
        if duplicates == 0:
            self.results.append(AuditResult(
                check_name="duplicate_bars",
                passed=True,
                message="No duplicate bars in the lookback window",
                severity="info",
            ))
            return

        unique_bars = len(df) - duplicates
        pct = 100.0 * duplicates / len(df)
        self.results.append(AuditResult(
            check_name="duplicate_bars",
            passed=True,
            message=(
                f"{duplicates} of {len(df)} rows ({pct:.1f}%) are repeat ingestions "
                f"of {unique_bars} unique bars; readers use the latest ingestion"
            ),
            severity="warning",
        ))

    def _check_null_rates(self, df: pl.DataFrame, dataset: str = "") -> None:
        """Check null rates per column.

        Some columns are empty by construction rather than by fault: a venue's
        funding-rate *history* carries the rate alone, while mark and index
        price exist only on the current-snapshot endpoint. Those columns are
        listed in `AUDIT_CONFIG.nullable_columns_by_dataset` and reported as
        warnings so they cannot halt trading, while every other column stays
        strict.
        """
        threshold = AUDIT_CONFIG.null_rate_threshold_pct / 100.0
        expected_nullable = AUDIT_CONFIG.nullable_columns_for(dataset)

        for col in df.columns:
            if col in ["event_ts", "ingested_ts", "asset_id", "venue", "symbol"]:
                continue

            null_count = df[col].null_count()
            null_rate = null_count / len(df) if len(df) > 0 else 0.0

            within_threshold = null_rate <= threshold
            optional = col in expected_nullable

            passed = within_threshold or optional
            if within_threshold:
                severity = "info"
            else:
                severity = "warning" if optional else "error"

            if not passed or null_count > 0:
                note = " (not provided by the venue's history endpoint)" if optional else ""
                self.results.append(AuditResult(
                    check_name=f"null_rate_{col}",
                    passed=passed,
                    message=(
                        f"Null rate in {col}: {null_rate*100:.2f}% "
                        f"(threshold: {threshold*100:.2f}%){note}"
                    ),
                    severity=severity,
                ))

    def _check_freshness(
        self, df: pl.DataFrame, audit_date: datetime, threshold_hours: float
    ) -> None:
        """Check data freshness: ingested_ts should be recent."""
        if "ingested_ts" not in df.columns:
            return

        freshness_window = timedelta(hours=threshold_hours)

        latest_ingested = df["ingested_ts"].max()
        if latest_ingested is None:
            self.results.append(AuditResult(
                check_name="freshness",
                passed=False,
                message="No ingested_ts found",
                severity="error",
            ))
            return

        age = audit_date - latest_ingested
        passed = age <= freshness_window
        severity = "error" if not passed else "info"

        self.results.append(AuditResult(
            check_name="freshness",
            passed=passed,
            message=f"Latest data is {age.total_seconds() / 3600:.1f}h old (threshold: {threshold_hours}h)",
            severity=severity,
        ))

    def _check_price_outliers(self, df: pl.DataFrame) -> None:
        """Check for suspicious price jumps within each asset's own price series.

        Consecutive rows in df interleave different assets, so a plain
        row-over-row diff would compare e.g. BTC's close to an unrelated
        asset's close. Compute pct_change per asset_id (sorted by event_ts
        when available) so a jump is only flagged within one asset's series.
        """
        if "close" not in df.columns or len(df) < 2:
            return

        working = df.filter(pl.col("close").is_not_null() & (pl.col("close") > 0))
        if len(working) < 2:
            return

        sort_cols = [c for c in ("asset_id", "event_ts") if c in working.columns]
        if sort_cols:
            working = working.sort(sort_cols)

        pct_change = pl.col("close").pct_change()
        if "asset_id" in working.columns:
            pct_change = pct_change.over("asset_id")
        working = working.with_columns(pct_change.abs().alias("_pct_change"))

        threshold = AUDIT_CONFIG.price_jump_threshold_pct / 100.0
        changes = working["_pct_change"].drop_nulls()
        price_changes = changes.filter(changes > threshold).to_list()

        if price_changes:
            max_change = max(price_changes)
            self.results.append(AuditResult(
                check_name="price_outliers",
                passed=False,
                message=f"Found {len(price_changes)} price jumps > {threshold*100:.1f}%; max: {max_change*100:.2f}%",
                severity="warning",
            ))
        else:
            self.results.append(AuditResult(
                check_name="price_outliers",
                passed=True,
                message=f"No suspicious price jumps (threshold: {threshold*100:.1f}%)",
                severity="info",
            ))

    def send_alerts(self) -> None:
        """Send alerts for failed checks (Telegram).

        The log line comes **before** the send and does not depend on it: a
        Telegram outage is exactly the kind of thing that must still leave a
        durable record (LOGGING.md section 2).
        """
        failed = [r for r in self.results if not r.passed]

        if not ALERT_CONFIG.enabled:
            if failed:
                logger.warning(
                    f"Alerts disabled; {len(failed)} failed check(s) will not be "
                    "notified (they are in this log)"
                )
            else:
                logger.debug("Alerts disabled; skipping")
            return

        if not failed:
            return

        message = "⚠️ Data Audit Alert\n\n"
        for result in failed:
            emoji = "❌" if result.severity == "error" else "⚠️"
            message += f"{emoji} {result.check_name}: {result.message}\n"

        logger.warning(
            f"Alerting on {len(failed)} failed check(s): "
            f"{', '.join(r.check_name for r in failed)}"
        )

        try:
            import requests
            url = f"https://api.telegram.org/bot{ALERT_CONFIG.telegram_bot_token}/sendMessage"
            payload = {
                "chat_id": ALERT_CONFIG.telegram_chat_id,
                "text": message,
            }
            response = requests.post(url, json=payload, timeout=5)
            if response.status_code != 200:
                logger.warning(f"Telegram alert failed: {response.text}")
        except ImportError:
            logger.warning("requests library not available for Telegram alerts")
        except Exception as e:
            logger.error(f"Failed to send Telegram alert: {e}")

    def should_halt_trading(self) -> bool:
        """Determine if trading should halt based on audit results.

        Logs CRITICAL whenever this returns True. The log line lives here rather
        than at the call sites so that *every* path to a halt is recorded —
        including a caller that halts and never sends an alert, which is the
        case LOGGING.md section 2 exists to close.
        """
        critical_failures = [
            r for r in self.results
            if not r.passed and r.severity == "error"
        ]
        if critical_failures:
            logger.critical(
                "TRADING HALT: %d critical audit failure(s) — %s",
                len(critical_failures),
                "; ".join(f"{r.check_name}: {r.message}" for r in critical_failures),
            )
        return len(critical_failures) > 0


def run_audit(dataset: str, date: datetime | None = None) -> AuditResult:
    """Run audit on a dataset and report results.

    Args:
        dataset: Dataset name
        date: Date to audit (default: today)

    Returns:
        Overall audit result
    """
    audit = DataAudit()
    # audit_dataset() logs every check's verdict itself, so this wrapper does
    # not repeat them.
    results = audit.audit_dataset(dataset, date)
    audit.send_alerts()

    overall_passed = all(r.passed for r in results)
    return AuditResult(
        check_name="overall",
        passed=overall_passed,
        message=f"{sum(1 for r in results if r.passed)}/{len(results)} checks passed",
        severity="error" if not overall_passed else "info",
    )
