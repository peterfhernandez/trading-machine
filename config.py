"""
Configuration for Poor Man's Trading Machine.

All configuration lives here, including:
- File paths (datastore, logs, etc.)
- Venue API keys (via environment variables)
- Universe parameters
- Trading mode (PAPER or LIVE)
- Cost model parameters
- Risk limits
"""

import os
from dataclasses import dataclass, field
from pathlib import Path

# ============================================================================
# Environment & Paths
# ============================================================================

PROJECT_ROOT = Path(__file__).parent
DATASTORE_PATH = PROJECT_ROOT / "data" / "parquet"
LOGS_PATH = PROJECT_ROOT / "logs"
SCRATCH_PATH = PROJECT_ROOT / "scratch"

# Create paths if they don't exist
for path in [DATASTORE_PATH, LOGS_PATH, SCRATCH_PATH]:
    path.mkdir(parents=True, exist_ok=True)


# ============================================================================
# Trading Mode
# ============================================================================

# If PAPER is True, scratch scripts and execution never place real trades.
# If PAPER is False, only prod/live modules execute.
PAPER = os.getenv("PAPER", "true").lower() in ("true", "1", "yes")


# ============================================================================
# Venue API Keys (from environment)
# ============================================================================

# CCXT venues (crypto)
CCXT_VENUES = {
    "binance": {
        "key": os.getenv("BINANCE_API_KEY"),
        "secret": os.getenv("BINANCE_API_SECRET"),
    },
    "deribit": {
        "key": os.getenv("DERIBIT_CLIENT_ID"),
        "secret": os.getenv("DERIBIT_CLIENT_SECRET"),
    },
}

# Deribit-specific (used for options, funding rates, etc.)
DERIBIT_CLIENT_ID = os.getenv("DERIBIT_CLIENT_ID")
DERIBIT_CLIENT_SECRET = os.getenv("DERIBIT_CLIENT_SECRET")


# ============================================================================
# Universe Configuration
# ============================================================================

@dataclass
class UniverseConfig:
    """Parameters for daily universe membership."""

    # Target number of assets in the universe
    target_size: int = 150

    # Minimum rolling median dollar volume (USDT over last 30 days)
    min_volume_usdt: float = 1_000_000.0

    # Lookback window (days) for the rolling median dollar volume calculation
    volume_lookback_days: int = 30

    # Minimum days since listing
    min_listing_age_days: int = 30

    # Exclude stablecoins (USDT, USDC, BUSD, etc.)
    exclude_stablecoins: bool = True

    # Canonical asset_ids treated as stablecoins when exclude_stablecoins=True
    stablecoin_symbols: set[str] = field(default_factory=lambda: {
        "USDT", "USDC", "BUSD", "DAI", "TUSD", "USDP", "FDUSD", "PYUSD", "GUSD", "USDD", "FRAX",
    })

    # Exclude wrapped/bridge assets (wBTC, wETH, etc.)
    exclude_wrapped: bool = True

    # Canonical asset_ids treated as wrapped/bridged duplicates when exclude_wrapped=True
    wrapped_symbols: set[str] = field(default_factory=lambda: {
        "WBTC", "WETH", "WBNB", "WMATIC", "WAVAX", "STETH", "WSTETH", "CBETH", "RETH", "METH",
    })

    # Rebalance frequency: "daily" for daily universe updates
    rebalance_freq: str = "daily"


UNIVERSE_CONFIG = UniverseConfig()


# ============================================================================
# Backtester Configuration
# ============================================================================

@dataclass
class BacktestConfig:
    """Parameters for the walk-forward backtester."""

    # Rebalance frequency: "daily", "weekly", or "monthly"
    rebalance_freq: str = "daily"

    # Start date for backtests (YYYY-MM-DD)
    backtest_start: str = "2021-01-01"

    # End date for backtests (YYYY-MM-DD); None = use all available data
    backtest_end: str | None = None

    # Initial portfolio value (for returns calculation)
    initial_cash: float = 100_000.0

    # Assumed round-trip spread (bps) for cost model; half is paid per trade
    spread_bps: float = 5.0

    # Assumed market impact (bps per $M traded)
    impact_bps_per_million: float = 1.0

    # Venue and bar timeframe the backtester prices against
    venue: str = "binance"
    timeframe: str = "1d"

    # Periods per year for annualization (crypto trades 24/7 -> 365 daily bars)
    periods_per_year: float = 365.0


BACKTEST_CONFIG = BacktestConfig()


# ============================================================================
# Risk Limits
# ============================================================================

@dataclass
class RiskConfig:
    """Risk limits and safety constraints."""

    # Maximum daily loss (% of account) before kill switch
    max_daily_loss_pct: float = 2.0

    # Maximum gross leverage
    max_gross_leverage: float = 3.0

    # Maximum position size (% of portfolio per asset)
    max_position_pct: float = 5.0

    # Target portfolio volatility (annualized, %)
    target_vol_pct: float = 15.0


RISK_CONFIG = RiskConfig()


# ============================================================================
# Data Loading & Audit
# ============================================================================

@dataclass
class LoaderConfig:
    """Parameters for data loaders."""

    # Rate limit (requests per second) for CCXT
    ccxt_rate_limit: float = 2.0

    # Retry on failure: max attempts
    max_retries: int = 3

    # Backfill window: how many days of history to pull on first run
    initial_backfill_days: int = 1825  # ~5 years

    # Maximum symbols a single loader run will fetch. Sits above
    # UNIVERSE_CONFIG.target_size on purpose: the universe builder ranks by
    # liquidity and cuts at target_size, so it needs more candidates than it
    # keeps, and the audit's coverage check compares loaded assets against
    # universe members.
    max_symbols_per_run: int = 200

    # ccxt market type for perpetual-only datasets (funding rate, open
    # interest). Spot markets have neither, so these loaders must not run
    # against the ccxt default ("spot").
    perp_market_type: str = "future"

    # Rows a venue returns per call. Windows longer than this are paged
    # through; `max_pages_per_symbol` bounds how far one run will walk.
    page_limit: int = 1000
    max_pages_per_symbol: int = 50

    # How much of the already-covered window to re-fetch when resuming. The
    # trailing bar of a run is usually incomplete (a daily candle fetched at
    # 00:10 UTC covers ten minutes of the current day) and venues revise recent
    # bars, so the last day covered must be pulled again. Duplicates are
    # expected and collapsed on read (datastore.latest_per_bar).
    refetch_overlap_days: int = 1


@dataclass
class AuditConfig:
    """Thresholds for data audit module."""

    # Alert if coverage below this % of universe
    coverage_threshold_pct: float = 90.0

    # Alert if null rate above this % per column
    null_rate_threshold_pct: float = 1.0

    # Columns a venue legitimately cannot fill for historical rows: Binance's
    # funding rate *history* carries the rate only, while the current-snapshot
    # endpoint also returns mark and index price. Nulls in these are reported
    # as warnings rather than halting trading; every other column stays strict.
    nullable_columns_by_dataset: dict[str, set[str]] = field(default_factory=lambda: {
        "funding_rate": {"mark_price", "index_price"},
    })

    # Alert if price jump > this % vs. second venue (outlier detection)
    price_jump_threshold_pct: float = 10.0

    # Default freshness threshold (hours) for any dataset not listed below
    freshness_threshold_hours: float = 24.0

    # Per-dataset freshness thresholds (hours); a dataset ingested less often
    # than this should not be flagged stale, and one ingested much more often
    # (e.g. hourly candles) should be held to a tighter bar. Falls back to
    # freshness_threshold_hours for any dataset not listed here.
    freshness_threshold_hours_by_dataset: dict[str, float] = field(default_factory=lambda: {
        "ohlcv_daily": 24.0,
        "ohlcv_hourly": 2.0,
        "funding_rate": 24.0,
        "open_interest": 24.0,
    })

    # How many days back the audit looks for *any* data before concluding a
    # dataset is genuinely missing (as opposed to merely stale). Bounds the
    # read regardless of how large freshness_threshold_hours is.
    audit_lookback_days: int = 7

    def freshness_threshold_for(self, dataset: str) -> float:
        """Freshness threshold (hours) for a dataset, falling back to the default."""
        return self.freshness_threshold_hours_by_dataset.get(dataset, self.freshness_threshold_hours)

    def nullable_columns_for(self, dataset: str) -> set[str]:
        """Columns whose nulls are expected (not a halting failure) for a dataset."""
        return self.nullable_columns_by_dataset.get(dataset, set())


LOADER_CONFIG = LoaderConfig()
AUDIT_CONFIG = AuditConfig()


# ============================================================================
# Alerts
# ============================================================================

@dataclass
class AlertConfig:
    """Telegram and other alert settings."""

    # Telegram bot token (from @BotFather)
    telegram_bot_token: str = os.getenv("TELEGRAM_BOT_TOKEN", "")

    # Telegram chat ID (destination for alerts)
    telegram_chat_id: str = os.getenv("TELEGRAM_CHAT_ID", "")

    # Enable alerts (False = no Telegram messages sent)
    enabled: bool = bool(telegram_bot_token and telegram_chat_id)


ALERT_CONFIG = AlertConfig()


# ============================================================================
# Logging
# ============================================================================

@dataclass
class LogConfig:
    """Logging configuration."""

    # Log level: DEBUG, INFO, WARNING, ERROR
    level: str = "INFO"

    # Log file path
    file: Path = LOGS_PATH / "trading_machine.log"


LOG_CONFIG = LogConfig()
