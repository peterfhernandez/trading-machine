"""Base loader class and utilities for all data loaders."""

import time
from abc import ABC, abstractmethod
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import polars as pl

from config import DATASTORE_PATH, LOADER_CONFIG
from datastore import ParquetStore, AssetMaster, DatasetSchema
from loaders.window import FetchWindow
from logging_config import get_logger


logger = get_logger(__name__)

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


def venue_supports(exchange: Any, capability: str) -> bool:
    """Whether a ccxt exchange advertises `capability` (e.g. "fetchOHLCV").

    Deliberately conservative: anything that is not a real capability mapping
    (a test double, an exchange that predates the flag) reads as unsupported,
    so callers fall back to the endpoint that is known to work rather than
    calling a method that may not exist.
    """
    has = getattr(exchange, "has", None)
    if not isinstance(has, Mapping):
        return False
    return bool(has.get(capability))


def paginate_time_series(
    fetch_page: Callable[[int], Optional[Sequence[Any]]],
    window: FetchWindow,
    timestamp_of: Callable[[Any], Optional[int]],
    max_pages: Optional[int] = None,
) -> list[Any]:
    """Walk `fetch_page(since_ms)` forward until `window` is covered.

    Venues cap a single response (ccxt's `limit`, 1000 bars on Binance), so a
    window longer than one page silently truncates unless the caller pages
    through it: a five-year daily backfill asked for in one call comes back as
    the first 1000 days, with the rest missing and nothing to say so.

    Stops when a page is empty, when a page adds nothing new (a venue that
    keeps returning the same entries would otherwise loop forever), when the
    window's end is passed, or at `max_pages`.

    Args:
        fetch_page: Called with a millisecond `since`; returns venue entries
        window: Event-time interval to cover
        timestamp_of: Extracts an entry's millisecond timestamp (None to skip it)
        max_pages: Page budget per call (default: LOADER_CONFIG.max_pages_per_symbol)

    Returns:
        Entries inside the window, in the order the venue returned them, with
        each timestamp kept once.
    """
    if max_pages is None:
        max_pages = LOADER_CONFIG.max_pages_per_symbol

    start_ms, end_ms = window.start_ms, window.end_ms
    cursor = start_ms
    collected: list[Any] = []
    seen: set[int] = set()

    for page_num in range(1, max_pages + 1):
        page = fetch_page(cursor)
        if not page:
            break

        newest = cursor
        added = 0
        for entry in page:
            ts = timestamp_of(entry)
            if ts is None or ts < start_ms or ts > end_ms:
                continue
            newest = max(newest, ts)
            if ts in seen:
                continue
            seen.add(ts)
            collected.append(entry)
            added += 1

        # Progress is measured in *new* entries, not in timestamps: a venue
        # that serves one entry per page still advances, while one that keeps
        # replaying the same page stops here.
        if added == 0:
            break

        cursor = newest + 1
        if cursor > end_ms:
            break

        if page_num == max_pages:
            logger.warning(
                f"Stopped paginating at the {max_pages}-page budget with "
                f"{datetime.fromtimestamp(cursor / 1000, timezone.utc).date()} "
                f"of {window.end.date()} reached; raise "
                f"LOADER_CONFIG.max_pages_per_symbol to fetch the rest"
            )

    return collected


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

        unresolved = [s for s, asset_id in result.items() if asset_id is None]
        if unresolved:
            # Every unresolved symbol is an asset whose rows will be dropped, so
            # the count is the difference between what was fetched and what will
            # be stored — the number that explains an audit coverage miss.
            logger.warning(
                f"{len(unresolved)} of {len(symbols)} {self.venue} symbols unresolved; "
                f"their rows will be dropped (first few: {unresolved[:5]})"
            )
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

        The one place a loader's write can fail, so it is where the failure is
        recorded: an ERROR here, and the exception re-raised for the caller
        (`BackfillRunner` treats a per-dataset failure as non-fatal and moves
        on, which is only safe because the failure is in the log).

        Args:
            dataset: Dataset name (e.g., "ohlcv_daily")
            df: DataFrame with data
            schema: DatasetSchema to validate against
        """
        started = time.monotonic()
        try:
            self.store.append(dataset, df, schema)
            logger.info(
                f"Appended {len(df)} rows to {dataset} in "
                f"{time.monotonic() - started:.2f}s; "
                f"{df['asset_id'].n_unique()} unique asset_ids"
            )
        except Exception as e:
            logger.error(f"Failed to append {len(df)} rows to {dataset}: {e}", exc_info=True)
            raise

    def run(self, schema: DatasetSchema, dataset: str) -> int:
        """Main entry point: fetch, validate, append.

        Args:
            schema: DatasetSchema for validation
            dataset: Dataset name to append to

        Returns:
            Number of rows appended
        """
        logger.info(f"Starting {self.__class__.__name__} for {self.venue} -> {dataset}")
        started = time.monotonic()

        df = self.fetch()
        fetched_at = time.monotonic()
        logger.info(f"Fetched {len(df)} rows in {fetched_at - started:.2f}s")

        self.append(dataset, df, schema)
        logger.info(
            f"Completed {self.__class__.__name__} for {dataset}: {len(df)} rows in "
            f"{time.monotonic() - started:.2f}s"
        )
        return len(df)
