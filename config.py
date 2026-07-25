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
from pathlib import Path
from dataclasses import dataclass


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

    # Minimum days since listing
    min_listing_age_days: int = 30

    # Exclude stablecoins (USDT, USDC, BUSD, etc.)
    exclude_stablecoins: bool = True

    # Exclude wrapped/bridge assets (wBTC, wETH, etc.)
    exclude_wrapped: bool = True

    # Rebalance frequency: "daily" for daily universe updates
    rebalance_freq: str = "daily"


UNIVERSE_CONFIG = UniverseConfig()


# ============================================================================
# Backtester Configuration
# ============================================================================

@dataclass
class BacktestConfig:
    """Parameters for the walk-forward backtester."""

    # Rebalance frequency: e.g., "daily", "weekly"
    rebalance_freq: str = "daily"

    # Start date for backtests (YYYY-MM-DD)
    backtest_start: str = "2021-01-01"

    # End date for backtests (YYYY-MM-DD); None = use all available data
    backtest_end: str = None

    # Initial portfolio value (for returns calculation)
    initial_cash: float = 100_000.0

    # Assumed one-way spread (bps) for cost model
    spread_bps: float = 5.0

    # Assumed market impact (bps per $M traded)
    impact_bps_per_million: float = 1.0


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


@dataclass
class AuditConfig:
    """Thresholds for data audit module."""

    # Alert if coverage below this % of universe
    coverage_threshold_pct: float = 90.0

    # Alert if null rate above this % per column
    null_rate_threshold_pct: float = 1.0

    # Alert if price jump > this % vs. second venue (outlier detection)
    price_jump_threshold_pct: float = 10.0

    # Alert if data is older than this (hours)
    freshness_threshold_hours: float = 24.0


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
