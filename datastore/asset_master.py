"""
Asset master: canonical asset IDs and venue symbol mapping.

The asset master solves the security matching problem:
- BTC is "BTC" on Binance, "XBT" on Deribit
- Tickers get renamed, delisted, or re-listed under new symbols
- Multiple assets can trade under the same symbol on different venues

The asset master stores:
- asset_id: canonical internal identifier (e.g., "BTC", "ETH")
- venue_symbol: the symbol used on each venue
- validity_start, validity_end: when the mapping was/is active (point-in-time)
"""

from datetime import datetime, date
from pathlib import Path
from typing import Optional, List, Dict, Tuple
from dataclasses import dataclass, field
import logging

import polars as pl

logger = logging.getLogger(__name__)


@dataclass
class AssetSymbolMapping:
    """A single asset-to-symbol mapping for a venue."""

    asset_id: str
    venue: str
    symbol: str
    validity_start: datetime
    validity_end: Optional[datetime] = None  # None = currently active
    is_primary: bool = True  # Primary symbol for this asset on this venue


class AssetMaster:
    """Canonical asset identifier and venue symbol mapping."""

    def __init__(self, store_path: Path):
        """
        Initialize the asset master.

        Args:
            store_path: Path to asset_master.parquet file
        """
        self.path = Path(store_path)
        self._cache: Optional[pl.DataFrame] = None
        self._load()

    def _load(self) -> None:
        """Load asset master from disk (or create empty if doesn't exist)."""
        if self.path.exists():
            self._cache = pl.read_parquet(self.path)
            logger.info(f"Loaded asset master with {len(self._cache)} mappings")
        else:
            self._cache = pl.DataFrame(
                schema={
                    "asset_id": pl.Utf8,
                    "venue": pl.Utf8,
                    "symbol": pl.Utf8,
                    "validity_start": pl.Datetime("us"),
                    "validity_end": pl.Datetime("us"),
                    "is_primary": pl.Boolean,
                }
            )
            logger.info("Created new (empty) asset master")

    def add_mapping(
        self,
        asset_id: str,
        venue: str,
        symbol: str,
        validity_start: datetime,
        validity_end: Optional[datetime] = None,
        is_primary: bool = True,
    ) -> None:
        """
        Add or update an asset-venue-symbol mapping.

        Args:
            asset_id: Canonical internal asset ID (e.g., "BTC")
            venue: Exchange name (e.g., "binance", "deribit")
            symbol: Symbol as traded on the venue (e.g., "BTC/USDT")
            validity_start: When this mapping became active
            validity_end: When this mapping ended (None = still active)
            is_primary: Whether this is the primary symbol for this asset on this venue
        """
        new_row = pl.DataFrame(
            {
                "asset_id": [asset_id],
                "venue": [venue],
                "symbol": [symbol],
                "validity_start": [validity_start],
                "validity_end": [validity_end],
                "is_primary": [is_primary],
            }
        )
        self._cache = pl.concat([self._cache, new_row], how="diagonal")
        self._save()
        logger.info(
            f"Added mapping: {asset_id} -> {venue}:{symbol} "
            f"(valid {validity_start.date()} to {validity_end.date() if validity_end else 'now'})"
        )

    def resolve_symbol(
        self, symbol: str, venue: str, asof: Optional[datetime] = None
    ) -> Optional[str]:
        """
        Resolve a venue symbol to a canonical asset_id.

        Args:
            symbol: Symbol on the venue (e.g., "BTC/USDT")
            venue: Venue name (e.g., "binance")
            asof: Knowledge date (None = use current time)

        Returns:
            Canonical asset_id, or None if not found
        """
        if asof is None:
            asof = datetime.utcnow()

        matches = self._cache.filter(
            (pl.col("symbol") == symbol)
            & (pl.col("venue") == venue)
            & (pl.col("validity_start") <= asof)
            & (
                (pl.col("validity_end").is_null())
                | (pl.col("validity_end") > asof)
            )
        )

        if len(matches) == 0:
            return None
        if len(matches) > 1:
            logger.warning(
                f"Multiple mappings for {venue}:{symbol} at {asof.date()}; "
                f"returning first (is_primary filter recommended)"
            )

        return matches["asset_id"][0]

    def get_symbol(
        self, asset_id: str, venue: str, asof: Optional[datetime] = None
    ) -> Optional[str]:
        """
        Get the primary symbol for an asset on a venue.

        Args:
            asset_id: Canonical asset ID (e.g., "BTC")
            venue: Venue name
            asof: Knowledge date (None = use current time)

        Returns:
            The primary symbol for this asset on this venue at the given date,
            or None if the mapping doesn't exist
        """
        if asof is None:
            asof = datetime.utcnow()

        matches = self._cache.filter(
            (pl.col("asset_id") == asset_id)
            & (pl.col("venue") == venue)
            & (pl.col("is_primary") == True)
            & (pl.col("validity_start") <= asof)
            & (
                (pl.col("validity_end").is_null())
                | (pl.col("validity_end") > asof)
            )
        )

        if len(matches) == 0:
            return None
        return matches["symbol"][0]

    def list_assets(self) -> List[str]:
        """List all canonical asset IDs."""
        return sorted(self._cache["asset_id"].unique().to_list())

    def list_venues(self) -> List[str]:
        """List all venues in the asset master."""
        return sorted(self._cache["venue"].unique().to_list())

    def asset_info(
        self, asset_id: str, asof: Optional[datetime] = None
    ) -> Dict[str, str]:
        """
        Get current venue symbols for an asset.

        Returns:
            Dict mapping venue -> primary symbol at asof date
        """
        if asof is None:
            asof = datetime.utcnow()

        matches = self._cache.filter(
            (pl.col("asset_id") == asset_id)
            & (pl.col("is_primary") == True)
            & (pl.col("validity_start") <= asof)
            & (
                (pl.col("validity_end").is_null())
                | (pl.col("validity_end") > asof)
            )
        )

        if len(matches) == 0:
            return {}

        return dict(zip(matches["venue"].to_list(), matches["symbol"].to_list()))

    def _save(self) -> None:
        """Save asset master to disk."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._cache.write_parquet(self.path)
        logger.info(f"Saved asset master to {self.path}")
