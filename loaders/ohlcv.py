"""OHLCV loader for daily and hourly candles via ccxt."""

import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

import ccxt
import polars as pl

from config import DATASTORE_PATH, LOADER_CONFIG
from datastore import ParquetStore, AssetMaster
from loaders.base import BaseLoader, select_usdt_symbols
from loaders.schemas import OHLCV_SCHEMA


logger = logging.getLogger(__name__)


class OHLCVLoader(BaseLoader):
    """Load OHLCV data from ccxt exchanges (Binance, Deribit, etc.)."""

    def __init__(
        self,
        venue: str,
        lookback_days: int = 365,
        store: Optional[ParquetStore] = None,
        asset_master: Optional[AssetMaster] = None,
        max_symbols: Optional[int] = None,
    ):
        super().__init__(venue, store, asset_master)
        self.lookback_days = lookback_days
        self.max_symbols = (
            LOADER_CONFIG.max_symbols_per_run if max_symbols is None else max_symbols
        )
        self.exchange = self._init_exchange(venue)

    def _init_exchange(self, venue: str):
        """Initialize ccxt exchange instance."""
        exchange_class = getattr(ccxt, venue.lower())
        exchange = exchange_class()
        exchange.load_markets()
        logger.info(f"Initialized {venue} exchange; {len(exchange.symbols)} symbols loaded")
        return exchange

    def fetch(self, timeframe: str = "1d") -> pl.DataFrame:
        """Fetch OHLCV data for top assets.

        Args:
            timeframe: "1d" for daily, "1h" for hourly

        Returns:
            DataFrame with columns: asset_id, venue, timeframe, event_ts, ingested_ts,
                                   open, high, low, close, volume
        """
        logger.info(f"Fetching {timeframe} OHLCV for {self.venue}")

        rows = []
        since = int((datetime.now(timezone.utc) - timedelta(days=self.lookback_days)).timestamp() * 1000)

        usdt_symbols = select_usdt_symbols(
            self.exchange.symbols, max_symbols=self.max_symbols
        )
        logger.info(f"Selected {len(usdt_symbols)} USDT symbols (cap: {self.max_symbols})")

        for symbol in usdt_symbols:
            try:
                candles = self.exchange.fetch_ohlcv(symbol, timeframe, since=since, limit=1000)
                if not candles:
                    logger.debug(f"No data for {symbol}")
                    continue

                for candle in candles:
                    ts = datetime.fromtimestamp(candle[0] / 1000, timezone.utc).replace(tzinfo=None)
                    rows.append({
                        "symbol": symbol,
                        "timeframe": timeframe,
                        "event_ts": ts,
                        "open": float(candle[1]),
                        "high": float(candle[2]),
                        "low": float(candle[3]),
                        "close": float(candle[4]),
                        "volume": float(candle[5]),
                    })
            except (ccxt.ExchangeError, ccxt.NetworkError) as e:
                logger.warning(f"Failed to fetch {symbol}: {e}")
                continue

        if not rows:
            logger.warning(f"No OHLCV data fetched for {self.venue}")
            return pl.DataFrame()

        df = pl.DataFrame(rows)
        logger.info(f"Fetched {len(df)} candles for {df['symbol'].n_unique()} symbols")

        symbols_list = df["symbol"].unique().to_list()
        symbol_to_asset_id = self.resolve_symbols(symbols_list)

        df = df.with_columns(
            pl.col("symbol")
            .map_elements(lambda s: symbol_to_asset_id.get(s), return_dtype=pl.Utf8)
            .alias("asset_id")
        )

        df = df.filter(pl.col("asset_id").is_not_null())

        if len(df) == 0:
            logger.error(f"No symbols resolved for {self.venue}; check asset master")
            return pl.DataFrame()

        df = df.with_columns(pl.lit(self.venue, dtype=pl.Utf8).alias("venue"))

        df = self.add_timestamps(df, event_ts_col="event_ts")

        df = df.select([
            "asset_id",
            "venue",
            "timeframe",
            "event_ts",
            "ingested_ts",
            "open",
            "high",
            "low",
            "close",
            "volume",
        ])

        logger.info(f"Prepared {len(df)} rows; coverage: {df['asset_id'].n_unique()} unique assets")
        return df

    def run_daily(self) -> None:
        """Fetch and append daily OHLCV."""
        df = self.fetch(timeframe="1d")
        if len(df) > 0:
            self.append("ohlcv_daily", df, OHLCV_SCHEMA)

    def run_hourly(self) -> None:
        """Fetch and append hourly OHLCV."""
        df = self.fetch(timeframe="1h")
        if len(df) > 0:
            self.append("ohlcv_hourly", df, OHLCV_SCHEMA)


def load_ohlcv(venue: str = "binance", lookback_days: int = 365) -> None:
    """Load OHLCV data (daily + hourly) from a venue.

    Args:
        venue: Exchange name (default: "binance")
        lookback_days: How many days of history to fetch (default: 365)
    """
    loader = OHLCVLoader(venue, lookback_days)
    loader.run_daily()
    loader.run_hourly()
