"""Data audit module: quality checks and alerting."""

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
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

    def __init__(
        self,
        store: Optional[ParquetStore] = None,
        venue: str = "binance",
        universe_size: Optional[int] = None,
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

    def audit_dataset(self, dataset: str, date: Optional[datetime] = None) -> list[AuditResult]:
        """Audit a single dataset.

        Args:
            dataset: Dataset name (e.g., "ohlcv_daily")
            date: Date to audit (default: today)

        Returns:
            List of AuditResult objects
        """
        if date is None:
            date = datetime.now(timezone.utc).replace(tzinfo=None)

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
                return self.results

            members, universe_size, universe_source = self.resolve_universe(date)
            self._check_coverage(df, members, universe_size, universe_source)
            self._check_null_rates(df)
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

        return self.results

    def resolve_universe_size(self, date: datetime) -> tuple[Optional[int], str]:
        """Coverage denominator as of `date`, and where it came from.

        Convenience wrapper over `resolve_universe` for callers that only need
        the size.
        """
        _, size, source = self.resolve_universe(date)
        return size, source

    def resolve_universe(
        self, date: datetime
    ) -> tuple[Optional[set[str]], Optional[int], str]:
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
        members: Optional[set[str]],
        universe_size: Optional[int],
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
