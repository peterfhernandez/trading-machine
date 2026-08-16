"""OHLCV loader for daily and hourly candles via ccxt."""

from datetime import UTC, datetime

import ccxt
import polars as pl

from config import LOADER_CONFIG
from datastore import AssetMaster, ParquetStore
from loaders.base import BaseLoader, paginate_time_series, select_usdt_symbols
from loaders.schemas import OHLCV_SCHEMA
from loaders.window import FetchWindow
from logging_config import get_logger

logger = get_logger(__name__)


class OHLCVLoader(BaseLoader):
    """Load OHLCV data from ccxt exchanges (Binance, Deribit, etc.).

    **Perpetuals, not spot** (Phase 5.9; `DATA.md` §8 decision 1 and §9.2). This
    loader read the venue's default spot markets from Phase 2 until then, while
    the archive backfill pulled `futures/um` and the funding-rate and
    open-interest loaders read derivatives. Same `venue`, same `asset_id`, two
    instruments, and no column recording which — so the price series switched
    instrument wherever the two overlapped, and `carry` (perp funding) was being
    paired with spot closes.

    It hides where a naming defect does not: spot and perp closes agree to a
    fraction of a percent, so only the *count* of disagreeing bars gave it away
    (~112 assets, ~61 days, exactly the overlap window). The acceptance gate's
    `price_agreement` check now looks for that directly.
    """

    def __init__(
        self,
        venue: str,
        lookback_days: int = 365,
        store: ParquetStore | None = None,
        asset_master: AssetMaster | None = None,
        max_symbols: int | None = None,
    ):
        super().__init__(venue, store, asset_master)
        self.lookback_days = lookback_days
        self.max_symbols = (
            LOADER_CONFIG.max_symbols_per_run if max_symbols is None else max_symbols
        )
        self.exchange = self._init_exchange(venue)

    def _init_exchange(self, venue: str):
        """Initialize ccxt exchange instance against the perp market type.

        Same shape as `FundingRateLoader._init_exchange`, deliberately: one
        market type per venue means all four loaders open the venue the same
        way. A venue that rejects the option is spot-only, which is a
        legitimate configuration rather than a failure.
        """
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

    def fetch(
        self, timeframe: str = "1d", window: FetchWindow | None = None
    ) -> pl.DataFrame:
        """Fetch OHLCV data for top assets.

        Args:
            timeframe: "1d" for daily, "1h" for hourly
            window: Event-time interval to fetch (default: the last
                `lookback_days` days, ending now)

        Returns:
            DataFrame with columns: asset_id, venue, timeframe, event_ts, ingested_ts,
                                   open, high, low, close, volume
        """
        window = window or FetchWindow.from_lookback(self.lookback_days)
        logger.info(f"Fetching {timeframe} OHLCV for {self.venue} over {window}")

        rows = []

        usdt_symbols = select_usdt_symbols(
            self.exchange.symbols, max_symbols=self.max_symbols, prefer_perps=True
        )
        logger.info(f"Selected {len(usdt_symbols)} USDT symbols (cap: {self.max_symbols})")

        for symbol in usdt_symbols:
            try:
                # Paged: one call returns at most LOADER_CONFIG.page_limit bars,
                # so a multi-year window needs to be walked forward.
                candles = paginate_time_series(
                    lambda since, s=symbol: self.exchange.fetch_ohlcv(
                        s, timeframe, since=since, limit=LOADER_CONFIG.page_limit
                    ),
                    window,
                    timestamp_of=lambda candle: candle[0] if candle else None,
                )
                if not candles:
                    logger.debug(f"No data for {symbol}")
                    continue

                for candle in candles:
                    ts = datetime.fromtimestamp(candle[0] / 1000, UTC).replace(tzinfo=None)
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

    def run_daily(self, window: FetchWindow | None = None) -> int:
        """Fetch and append daily OHLCV. Returns the number of rows appended."""
        df = self.fetch(timeframe="1d", window=window)
        if len(df) == 0:
            return 0
        self.append("ohlcv_daily", df, OHLCV_SCHEMA)
        return len(df)

    def run_hourly(self, window: FetchWindow | None = None) -> int:
        """Fetch and append hourly OHLCV. Returns the number of rows appended."""
        df = self.fetch(timeframe="1h", window=window)
        if len(df) == 0:
            return 0
        self.append("ohlcv_hourly", df, OHLCV_SCHEMA)
        return len(df)


def load_ohlcv(venue: str = "binance", lookback_days: int = 365) -> None:
    """Load OHLCV data (daily + hourly) from a venue.

    Args:
        venue: Exchange name (default: "binance")
        lookback_days: How many days of history to fetch (default: 365)
    """
    loader = OHLCVLoader(venue, lookback_days)
    loader.run_daily()
    loader.run_hourly()
