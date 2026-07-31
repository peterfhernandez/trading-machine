"""Funding rate loader for perpetual swaps (Binance, Deribit)."""

import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

import ccxt
import polars as pl

from config import DATASTORE_PATH, LOADER_CONFIG
from datastore import ParquetStore, AssetMaster
from loaders.base import (
    BaseLoader,
    paginate_time_series,
    select_usdt_symbols,
    venue_supports,
)
from loaders.schemas import FUNDING_RATE_SCHEMA
from loaders.window import FetchWindow, utc_now


logger = logging.getLogger(__name__)


class FundingRateLoader(BaseLoader):
    """Load funding rates from ccxt exchanges (Binance, Deribit, etc.).

    Funding is a perpetual-swap concept, so the exchange is opened against the
    venue's derivatives markets (`LOADER_CONFIG.perp_market_type`) rather than
    ccxt's default spot markets — asking a spot symbol for a funding rate just
    raises per symbol and yields an empty dataset.
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

    def _history_rows(self, symbol: str, window: FetchWindow) -> list[dict]:
        """Funding rates over `window` from the venue's history endpoint.

        History entries carry the rate and its timestamp only — mark and index
        price come from the current-snapshot endpoint and are simply not part
        of the historical record, so they are left null rather than invented.
        `AUDIT_CONFIG.nullable_columns_by_dataset` records that expectation.
        """
        entries = paginate_time_series(
            lambda since: self.exchange.fetch_funding_rate_history(
                symbol, since=since, limit=LOADER_CONFIG.page_limit
            ),
            window,
            timestamp_of=lambda entry: entry.get("timestamp"),
        )

        rows = []
        for entry in entries:
            rate = entry.get("fundingRate")
            if rate is None:
                continue
            rows.append({
                "symbol": symbol,
                "event_ts": datetime.fromtimestamp(
                    entry["timestamp"] / 1000, timezone.utc
                ).replace(tzinfo=None),
                "funding_rate": float(rate),
                "mark_price": None,
                "index_price": None,
            })
        return rows

    def _snapshot_row(self, symbol: str, window: FetchWindow) -> list[dict]:
        """The current funding rate, if `window` extends to now.

        A snapshot can only ever answer for the present, so a request for a
        past window returns nothing here rather than stamping today's rate with
        a historical timestamp.
        """
        fr_data = self.exchange.fetch_funding_rate(symbol)
        if not fr_data:
            return []

        ts = (
            datetime.fromtimestamp(fr_data["timestamp"] / 1000, timezone.utc).replace(tzinfo=None)
            if fr_data.get("timestamp")
            else utc_now()
        )
        if not window.contains(ts):
            return []

        return [{
            "symbol": symbol,
            "event_ts": ts,
            "funding_rate": float(fr_data.get("fundingRate") or 0.0),
            "mark_price": float(fr_data.get("markPrice") or 0.0),
            "index_price": float(fr_data.get("indexPrice") or 0.0),
        }]

    def fetch(self, window: Optional[FetchWindow] = None) -> pl.DataFrame:
        """Fetch funding rates for top assets.

        Args:
            window: Event-time interval to fetch (default: the last
                `lookback_days` days, ending now)

        Returns:
            DataFrame with columns: asset_id, venue, event_ts, ingested_ts,
                                   funding_rate, mark_price, index_price
        """
        window = window or FetchWindow.from_lookback(self.lookback_days)
        use_history = venue_supports(self.exchange, "fetchFundingRateHistory")
        logger.info(
            f"Fetching funding rates for {self.venue} over {window} "
            f"({'history' if use_history else 'current snapshot only'})"
        )

        rows = []
        symbols = select_usdt_symbols(
            self.exchange.symbols, max_symbols=self.max_symbols, prefer_perps=True
        )
        logger.info(f"Selected {len(symbols)} USDT symbols (cap: {self.max_symbols})")

        for symbol in symbols:
            try:
                if use_history:
                    rows.extend(self._history_rows(symbol, window))
                else:
                    rows.extend(self._snapshot_row(symbol, window))
            except (ccxt.ExchangeError, ccxt.NetworkError, AttributeError) as e:
                logger.debug(f"Could not fetch funding rate for {symbol}: {e}")
                continue

        if not rows:
            logger.warning(f"No funding rate data fetched for {self.venue}")
            return pl.DataFrame()

        df = pl.DataFrame(
            rows,
            schema_overrides={
                "funding_rate": pl.Float64,
                "mark_price": pl.Float64,
                "index_price": pl.Float64,
            },
        )
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

    def run(self, window: Optional[FetchWindow] = None) -> int:
        """Fetch and append funding rates. Returns the number of rows appended."""
        df = self.fetch(window=window)
        if len(df) == 0:
            return 0
        self.append("funding_rate", df, FUNDING_RATE_SCHEMA)
        return len(df)


def load_funding_rates(venue: str = "binance", lookback_days: int = 30) -> None:
    """Load funding rate data from a venue.

    Args:
        venue: Exchange name (default: "binance")
        lookback_days: How many days of history to fetch (default: 30)
    """
    loader = FundingRateLoader(venue, lookback_days)
    loader.run()
