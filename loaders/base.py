"""Base loader class and utilities for all data loaders."""

import logging
from abc import ABC, abstractmethod
from datetime import datetime, timedelta, timezone
from typing import Optional

import polars as pl

from config import DATASTORE_PATH, LOADER_CONFIG
from datastore import ParquetStore, AssetMaster, DatasetSchema


logger = logging.getLogger(__name__)

# Perpetual swaps are quoted with a settle suffix in ccxt's unified notation
# ("BTC/USDT:USDT"); dated futures carry an expiry after it
# ("BTC/USDT:USDT-260327") and are deliberately excluded.
_PERP_SUFFIX = ":USDT"


def select_usdt_symbols(
    symbols: Optional[list[str]],
    max_symbols: Optional[int] = None,
    prefer_perps: bool = False,
) -> list[str]:
    """Pick up to `max_symbols` USDT-quoted symbols from a venue symbol list.

    The USDT filter must be applied *before* the cap. A venue's symbol list
    interleaves every quote currency in alphabetical order, so slicing first
    (`symbols[:50]`) and filtering after yields only the handful of USDT pairs
    that happen to sort early — the reason the funding-rate and open-interest
    loaders covered ~15 assets out of a 50-symbol budget.

    Args:
        symbols: Venue symbol list (typically `exchange.symbols`)
        max_symbols: Cap on returned symbols (default: LOADER_CONFIG.max_symbols_per_run)
        prefer_perps: If True, return perpetual symbols only ("BTC/USDT:USDT"),
            falling back to plain USDT-quoted symbols when the venue exposes no
            perps (e.g. a spot-only venue)

    Returns:
        Sorted list of symbols, capped at `max_symbols`
    """
    if max_symbols is None:
        max_symbols = LOADER_CONFIG.max_symbols_per_run

    if not symbols:
        return []

    perps = [s for s in symbols if s.endswith(_PERP_SUFFIX)]
    if prefer_perps and perps:
        selected = perps
    else:
        selected = [s for s in symbols if s.endswith("USDT")]

    selected = sorted(set(selected))
    if max_symbols is not None and max_symbols > 0:
        selected = selected[:max_symbols]
    return selected


class BaseLoader(ABC):
    """Abstract base class for all data loaders."""

    def __init__(
        self,
        venue: str,
        store: Optional[ParquetStore] = None,
        asset_master: Optional[AssetMaster] = None,
    ):
        self.venue = venue
        self.store = store or ParquetStore(DATASTORE_PATH)
        self.asset_master = asset_master or AssetMaster(DATASTORE_PATH / "asset_master.parquet")

    @abstractmethod
    def fetch(self) -> pl.DataFrame:
        """Fetch data from venue. Must be implemented by subclass."""
        pass

    def check_idempotency(self, dataset: str, date: datetime) -> bool:
        """Check if data for this date already exists in the store.

        Returns True if data exists (should skip), False if should fetch.
        """
        try:
            existing = self.store.read(
                dataset,
                date_range=(date.date(), date.date()),
                columns=["asset_id"],
            )
            if len(existing) > 0:
                logger.info(f"Dataset {dataset} already has data for {date.date()}; skipping")
                return True
        except Exception:
            pass
        return False

    def resolve_symbols(
        self,
        symbols: list[str],
        asof: Optional[datetime] = None,
    ) -> dict[str, str]:
        """Resolve venue symbols to canonical asset_ids.

        Args:
            symbols: List of venue symbols (e.g., ["BTC/USDT", "ETH/USDT"])
            asof: Point-in-time date for resolution (default: now)

        Returns:
            Dict mapping symbol -> asset_id, or symbol -> None if resolution fails
        """
        result = {}
        for symbol in symbols:
            try:
                asset_id = self.asset_master.resolve_symbol(symbol, self.venue, asof=asof)
                result[symbol] = asset_id
                logger.debug(f"Resolved {symbol}@{self.venue} -> {asset_id}")
            except Exception as e:
                logger.warning(f"Failed to resolve {symbol}@{self.venue}: {e}")
                result[symbol] = None
        return result

    def add_timestamps(
        self,
        df: pl.DataFrame,
        event_ts_col: str = "event_ts",
        ingested_ts: Optional[datetime] = None,
    ) -> pl.DataFrame:
        """Ensure DataFrame has event_ts and ingested_ts columns.

        Args:
            df: Input DataFrame
            event_ts_col: Column name containing the event timestamp (default: "event_ts")
            ingested_ts: Timestamp for ingested_ts (default: now)

        Returns:
            DataFrame with both timestamps guaranteed
        """
        if ingested_ts is None:
            ingested_ts = datetime.now(timezone.utc).replace(tzinfo=None)

        df = df.clone()

        if event_ts_col != "event_ts" and event_ts_col in df.columns:
            df = df.rename({event_ts_col: "event_ts"})

        if "event_ts" not in df.columns:
            raise ValueError(f"event_ts column required; {event_ts_col} not found")

        if "ingested_ts" not in df.columns:
            df = df.with_columns(
                pl.lit(ingested_ts, dtype=pl.Datetime("us")).alias("ingested_ts")
            )

        return df

    def append(
        self,
        dataset: str,
        df: pl.DataFrame,
        schema: DatasetSchema,
    ) -> None:
        """Validate and append data to datastore.

        Args:
            dataset: Dataset name (e.g., "ohlcv_daily")
            df: DataFrame with data
            schema: DatasetSchema to validate against
        """
        try:
            self.store.append(dataset, df, schema)
            logger.info(
                f"Appended {len(df)} rows to {dataset}; "
                f"rows have asset_ids from {df['asset_id'].unique()}"
            )
        except Exception as e:
            logger.error(f"Failed to append to {dataset}: {e}")
            raise

    def run(self, schema: DatasetSchema, dataset: str) -> None:
        """Main entry point: fetch, validate, append.

        Args:
            schema: DatasetSchema for validation
            dataset: Dataset name to append to
        """
        logger.info(f"Starting {self.__class__.__name__} for {self.venue}")
        df = self.fetch()
        logger.info(f"Fetched {len(df)} rows")
        self.append(dataset, df, schema)
        logger.info(f"Completed {self.__class__.__name__}")
