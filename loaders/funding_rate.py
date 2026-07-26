"""Funding rate loader for perpetual swaps (Binance, Deribit)."""

import logging
from datetime import datetime, timedelta
from typing import Optional

import ccxt
import polars as pl

from config import DATASTORE_PATH
from datastore import ParquetStore, AssetMaster
from loaders.base import BaseLoader
from loaders.schemas import FUNDING_RATE_SCHEMA


logger = logging.getLogger(__name__)


class FundingRateLoader(BaseLoader):
    """Load funding rates from ccxt exchanges (Binance, Deribit, etc.)."""

    def __init__(
        self,
        venue: str,
        lookback_days: int = 30,
        store: Optional[ParquetStore] = None,
        asset_master: Optional[AssetMaster] = None,
    ):
        super().__init__(venue, store, asset_master)
        self.lookback_days = lookback_days
        self.exchange = self._init_exchange(venue)

    def _init_exchange(self, venue: str):
        """Initialize ccxt exchange instance."""
        exchange_class = getattr(ccxt, venue.lower())
        exchange = exchange_class()
        exchange.load_markets()
        logger.info(f"Initialized {venue} exchange; {len(exchange.symbols)} symbols loaded")
        return exchange

    def fetch(self) -> pl.DataFrame:
        """Fetch funding rates for top assets.

        Returns:
            DataFrame with columns: asset_id, venue, event_ts, ingested_ts,
                                   funding_rate, mark_price, index_price
        """
        logger.info(f"Fetching funding rates for {self.venue}")

        rows = []
        since = int((datetime.utcnow() - timedelta(days=self.lookback_days)).timestamp() * 1000)

        for symbol in self.exchange.symbols[:50]:
            if not symbol.endswith(':USDT') and not symbol.endswith('USDT'):
                continue

            try:
                if hasattr(self.exchange, 'fetch_funding_rate'):
                    fr_data = self.exchange.fetch_funding_rate(symbol)
                    if fr_data:
                        ts = datetime.utcfromtimestamp(fr_data.get('timestamp', 0) / 1000) if fr_data.get('timestamp') else datetime.utcnow()
                        rows.append({
                            "symbol": symbol,
                            "event_ts": ts,
                            "funding_rate": float(fr_data.get('fundingRate', 0.0)),
                            "mark_price": float(fr_data.get('markPrice', 0.0)),
                            "index_price": float(fr_data.get('indexPrice', 0.0)),
                        })
                elif hasattr(self.exchange, 'fetch_funding_rates'):
                    all_rates = self.exchange.fetch_funding_rates()
                    if symbol in all_rates:
                        fr_data = all_rates[symbol]
                        ts = datetime.utcfromtimestamp(fr_data.get('timestamp', 0) / 1000) if fr_data.get('timestamp') else datetime.utcnow()
                        rows.append({
                            "symbol": symbol,
                            "event_ts": ts,
                            "funding_rate": float(fr_data.get('fundingRate', 0.0)),
                            "mark_price": float(fr_data.get('markPrice', 0.0)),
                            "index_price": float(fr_data.get('indexPrice', 0.0)),
                        })
            except (ccxt.ExchangeError, ccxt.NetworkError, AttributeError) as e:
                logger.debug(f"Could not fetch funding rate for {symbol}: {e}")
                continue

        if not rows:
            logger.warning(f"No funding rate data fetched for {self.venue}")
            return pl.DataFrame()

        df = pl.DataFrame(rows)
        logger.info(f"Fetched {len(df)} funding rates for {df['symbol'].n_unique()} symbols")

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
            "funding_rate",
            "mark_price",
            "index_price",
        ])

        logger.info(f"Prepared {len(df)} funding rates; coverage: {df['asset_id'].n_unique()} unique assets")
        return df

    def run(self) -> None:
        """Fetch and append funding rates."""
        df = self.fetch()
        if len(df) > 0:
            self.append("funding_rate", df, FUNDING_RATE_SCHEMA)


def load_funding_rates(venue: str = "binance", lookback_days: int = 30) -> None:
    """Load funding rate data from a venue.

    Args:
        venue: Exchange name (default: "binance")
        lookback_days: How many days of history to fetch (default: 30)
    """
    loader = FundingRateLoader(venue, lookback_days)
    loader.run()
