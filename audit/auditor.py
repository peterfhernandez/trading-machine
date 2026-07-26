"""Data audit module: quality checks and alerting."""

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional

import polars as pl

from config import AUDIT_CONFIG, ALERT_CONFIG, DATASTORE_PATH
from datastore import ParquetStore


logger = logging.getLogger(__name__)


@dataclass
class AuditResult:
    """Result of a single audit check."""

    check_name: str
    passed: bool
    message: str
    severity: str  # "info", "warning", "error"


class DataAudit:
    """Audit data quality in the datastore."""

    def __init__(self, store: Optional[ParquetStore] = None):
        self.store = store or ParquetStore(DATASTORE_PATH)
        self.results: list[AuditResult] = []

    def audit_dataset(self, dataset: str, date: Optional[datetime] = None) -> list[AuditResult]:
        """Audit a single dataset.

        Args:
            dataset: Dataset name (e.g., "ohlcv_daily")
            date: Date to audit (default: today)

        Returns:
            List of AuditResult objects
        """
        if date is None:
            date = datetime.utcnow()

        self.results = []
        logger.info(f"Auditing {dataset} for {date.date()}")

        try:
            df = self.store.read(dataset, date_range=(date.date(), date.date()))

            if len(df) == 0:
                self.results.append(AuditResult(
                    check_name="data_presence",
                    passed=False,
                    message=f"No data found for {date.date()}",
                    severity="error",
                ))
                return self.results

            self._check_coverage(df)
            self._check_null_rates(df)
            self._check_freshness(df, date)
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

        return self.results

    def _check_coverage(self, df: pl.DataFrame) -> None:
        """Check coverage: % of assets with data."""
        if "asset_id" not in df.columns:
            return

        asset_count = df["asset_id"].n_unique()
        threshold = int(150 * AUDIT_CONFIG.coverage_threshold_pct)

        passed = asset_count >= threshold
        severity = "error" if not passed else "info"

        self.results.append(AuditResult(
            check_name="coverage",
            passed=passed,
            message=f"Coverage: {asset_count} assets (threshold: {threshold})",
            severity=severity,
        ))

    def _check_null_rates(self, df: pl.DataFrame) -> None:
        """Check null rates per column."""
        threshold = AUDIT_CONFIG.null_rate_threshold_pct / 100.0

        for col in df.columns:
            if col in ["event_ts", "ingested_ts", "asset_id", "venue", "symbol"]:
                continue

            null_count = df[col].null_count()
            null_rate = null_count / len(df) if len(df) > 0 else 0.0

            passed = null_rate <= threshold
            severity = "error" if not passed else "info"

            if not passed or null_count > 0:
                self.results.append(AuditResult(
                    check_name=f"null_rate_{col}",
                    passed=passed,
                    message=f"Null rate in {col}: {null_rate*100:.2f}% (threshold: {threshold*100:.2f}%)",
                    severity=severity,
                ))

    def _check_freshness(self, df: pl.DataFrame, audit_date: datetime) -> None:
        """Check data freshness: ingested_ts should be recent."""
        if "ingested_ts" not in df.columns:
            return

        freshness_window = timedelta(hours=AUDIT_CONFIG.freshness_threshold_hours)
        cutoff = audit_date - freshness_window

        latest_ingested = df["ingested_ts"].max()
        if latest_ingested is None:
            self.results.append(AuditResult(
                check_name="freshness",
                passed=False,
                message="No ingested_ts found",
                severity="error",
            ))
            return

        age = audit_date - latest_ingested.to_pydatetime()
        passed = age <= freshness_window
        severity = "error" if not passed else "info"

        self.results.append(AuditResult(
            check_name="freshness",
            passed=passed,
            message=f"Latest data is {age.total_seconds() / 3600:.1f}h old (threshold: {AUDIT_CONFIG.freshness_threshold_hours}h)",
            severity=severity,
        ))

    def _check_price_outliers(self, df: pl.DataFrame) -> None:
        """Check for suspicious price jumps (simplified check)."""
        if "close" not in df.columns or len(df) < 2:
            return

        closes = df["close"].drop_nulls()
        if len(closes) < 2:
            return

        threshold = AUDIT_CONFIG.price_jump_threshold_pct / 100.0

        price_changes = []
        for i in range(1, len(closes)):
            prev = closes[i - 1]
            curr = closes[i]
            if prev > 0:
                change = abs((curr - prev) / prev)
                if change > threshold:
                    price_changes.append(change)

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
        """Send alerts for failed checks (Telegram)."""
        if not ALERT_CONFIG.enabled:
            logger.debug("Alerts disabled; skipping")
            return

        failed = [r for r in self.results if not r.passed]
        if not failed:
            return

        message = "⚠️ Data Audit Alert\n\n"
        for result in failed:
            emoji = "❌" if result.severity == "error" else "⚠️"
            message += f"{emoji} {result.check_name}: {result.message}\n"

        logger.info(f"Sending alert to Telegram: {len(failed)} failed checks")

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
        """Determine if trading should halt based on audit results."""
        critical_failures = [
            r for r in self.results
            if not r.passed and r.severity == "error"
        ]
        return len(critical_failures) > 0


def run_audit(dataset: str, date: Optional[datetime] = None) -> AuditResult:
    """Run audit on a dataset and report results.

    Args:
        dataset: Dataset name
        date: Date to audit (default: today)

    Returns:
        Overall audit result
    """
    audit = DataAudit()
    results = audit.audit_dataset(dataset, date)

    logger.info(f"Audit complete: {len(results)} checks, {len([r for r in results if r.passed])} passed")

    for result in results:
        level = logging.WARNING if not result.passed else logging.INFO
        logger.log(level, f"  {result.check_name}: {result.message}")

    audit.send_alerts()

    overall_passed = all(r.passed for r in results)
    return AuditResult(
        check_name="overall",
        passed=overall_passed,
        message=f"{sum(1 for r in results if r.passed)}/{len(results)} checks passed",
        severity="error" if not overall_passed else "info",
    )
