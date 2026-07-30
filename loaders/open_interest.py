"""Open interest loader for perpetual swaps."""

import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

import ccxt
import polars as pl

from config import DATASTORE_PATH, LOADER_CONFIG
from datastore import ParquetStore, AssetMaster
from loaders.base import BaseLoader, select_usdt_symbols
from loaders.schemas import OPEN_INTEREST_SCHEMA


logger = logging.getLogger(__name__)


class OpenInterestLoader(BaseLoader):
    """Load open interest data from ccxt exchanges.

    Open interest only exists for derivatives, so the exchange is opened
    against the venue's derivatives markets (`LOADER_CONFIG.perp_market_type`)
    rather than ccxt's default spot markets.
    """

    def __init__(
        self,
        venue: str,
        lookback_days: int = 30,
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
        """Initialize ccxt exchange instance against the perp market type."""
        exchange_class = getattr(ccxt, venue.lower())
        market_type = LOADER_CONFIG.perp_market_type
        try:
            exchange = exchange_class({"options": {"defaultType": market_type}})
        except Exception as e:
            logger.warning(
                f"{venue} rejected defaultType={market_type} ({e}); "
                f"falling back to venue default markets"
            )
            exchange = exchange_class()
        exchange.load_markets()
        logger.info(
            f"Initialized {venue} exchange (market type: {market_type}); "
            f"{len(exchange.symbols)} symbols loaded"
        )
        return exchange

    def fetch(self) -> pl.DataFrame:
        """Fetch open interest for top assets.

        Returns:
            DataFrame with columns: asset_id, venue, event_ts, ingested_ts,
                                   open_interest, open_interest_usd
        """
        logger.info(f"Fetching open interest for {self.venue}")

        rows = []

        symbols = select_usdt_symbols(
            self.exchange.symbols, max_symbols=self.max_symbols, prefer_perps=True
        )
        logger.info(f"Selected {len(symbols)} USDT symbols (cap: {self.max_symbols})")

        for symbol in symbols:
            try:
                if hasattr(self.exchange, 'fetch_open_interest'):
                    oi_data = self.exchange.fetch_open_interest(symbol)
                    if oi_data:
                        ts = (
                            datetime.fromtimestamp(oi_data.get('timestamp', 0) / 1000, timezone.utc).replace(tzinfo=None)
                            if oi_data.get('timestamp')
                            else datetime.now(timezone.utc).replace(tzinfo=None)
                        )
                        rows.append({
                            "symbol": symbol,
                            "event_ts": ts,
                            "open_interest": float(oi_data.get('openInterest', 0.0)),
                            "open_interest_usd": float(oi_data.get('openInterestUsd', 0.0)),
                        })
            except (ccxt.ExchangeError, ccxt.NetworkError, AttributeError) as e:
                logger.debug(f"Could not fetch open interest for {symbol}: {e}")
                continue

        if not rows:
            logger.warning(f"No open interest data fetched for {self.venue}")
            return pl.DataFrame()

        df = pl.DataFrame(rows)
        logger.info(f"Fetched {len(df)} open interest snapshots for {df['symbol'].n_unique()} symbols")

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
            "event_ts",
            "ingested_ts",
            "open_interest",
            "open_interest_usd",
        ])

        logger.info(f"Prepared {len(df)} OI snapshots; coverage: {df['asset_id'].n_unique()} unique assets")
        return df

    def run(self) -> None:
        """Fetch and append open interest."""
        df = self.fetch()
        if len(df) > 0:
            self.append("open_interest", df, OPEN_INTEREST_SCHEMA)


def load_open_interest(venue: str = "binance", lookback_days: int = 30) -> None:
    """Load open interest data from a venue.

    Args:
        venue: Exchange name (default: "binance")
        lookback_days: How many days of history to fetch (default: 30)
    """
    loader = OpenInterestLoader(venue, lookback_days)
    loader.run()
