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

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from datastore.identity import symbol_key
from logging_config import get_logger

logger = get_logger(__name__)

# A mapping with no end date is active forever; comparing against a sentinel is
# simpler than special-casing None at four points in the overlap arithmetic.
_FOREVER = datetime.max


@dataclass
class AssetSymbolMapping:
    """A single asset-to-symbol mapping for a venue."""

    asset_id: str
    venue: str
    symbol: str
    validity_start: datetime
    validity_end: datetime | None = None  # None = currently active
    is_primary: bool = True  # Primary symbol for this asset on this venue


@dataclass(frozen=True)
class AssetIdCollision:
    """Two venue symbols that are not spellings of each other, under one asset_id.

    This is the security-matching failure `DATA.md` §9.1 describes, caught at
    the point of ingest rather than inferred from a price series 711,000x out.
    Notational variants of one listing (`BTCUSDT`, `BTC/USDT`, `BTC/USDT:USDT`)
    are *not* a collision — that is exactly what the asset master is for — so
    the test is `datastore.identity.symbol_key`, not string equality.

    `relation` is the Phase 10a trigger, and the same query answers both cases:

    - **overlap** — both listings were live at once, so they cannot be the same
      thing under two names. Two tokens sharing a ticker; keep them apart.
    - **abut** — one ended where the other began. A genuine redenomination, or
      a reused equity ticker, and the point at which the price-adjustment
      engine (`DATA.md` §10) stops being hypothetical.
    """

    asset_id: str
    venue: str
    symbol: str
    existing_symbol: str
    relation: str  # "overlap" | "abut"

    @property
    def is_redenomination_candidate(self) -> bool:
        return self.relation == "abut"

    def describe(self) -> str:
        if self.relation == "overlap":
            meaning = (
                "both listed at the same time, so they are two different assets "
                "sharing a ticker -- keep them apart"
            )
        else:
            meaning = (
                "one listing ends where the other begins, which is a "
                "redenomination or a reused ticker -- see DATA.md section 10 "
                "(Phase 10a)"
            )
        return (
            f"asset_id {self.asset_id!r} on {self.venue} would map both "
            f"{self.existing_symbol!r} and {self.symbol!r} ({meaning})"
        )


class AssetIdCollisionError(ValueError):
    """Refusing a mapping that would merge two listings into one asset_id."""

    def __init__(self, collision: AssetIdCollision):
        self.collision = collision
        super().__init__(collision.describe())


class AssetMaster:
    """Canonical asset identifier and venue symbol mapping."""

    def __init__(self, store_path: Path):
        """
        Initialize the asset master.

        Args:
            store_path: Path to asset_master.parquet file
        """
        self.path = Path(store_path)
        self._cache: pl.DataFrame | None = None
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

    def check_collision(
        self,
        asset_id: str,
        venue: str,
        symbol: str,
        validity_start: datetime,
        validity_end: datetime | None = None,
    ) -> AssetIdCollision | None:
        """Would this mapping put two different listings under one `asset_id`?

        Returns the collision, or None when the mapping is safe. A symbol whose
        notation this project does not recognise (Deribit's bare `BTC`, say)
        cannot be compared, so it is allowed through rather than guessed at:
        the guard exists to catch one specific known failure, and a guard that
        fires on everything unfamiliar is a guard somebody turns off.
        """
        key = symbol_key(symbol)
        if key is None:
            return None

        existing = self._cache.filter(
            (pl.col("asset_id") == asset_id)
            & (pl.col("venue") == venue)
            & (pl.col("symbol") != symbol)
        )
        if not len(existing):
            return None

        end = validity_end or _FOREVER
        for row in existing.iter_rows(named=True):
            other_key = symbol_key(row["symbol"])
            if other_key is None or other_key == key:
                continue
            other_end = row["validity_end"] or _FOREVER
            overlaps = validity_start < other_end and row["validity_start"] < end
            return AssetIdCollision(
                asset_id=asset_id,
                venue=venue,
                symbol=symbol,
                existing_symbol=row["symbol"],
                relation="overlap" if overlaps else "abut",
            )
        return None

    def find_collisions(self) -> list[AssetIdCollision]:
        """Every collision already recorded in the master.

        `check_collision` stops new ones; this reports what a master built under
        the old canonicalisation is already carrying, which is what the
        acceptance gate needs to ask before any research runs.
        """
        collisions: list[AssetIdCollision] = []

        for (asset_id, venue), group in self._cache.group_by(
            ["asset_id", "venue"], maintain_order=True
        ):
            # One representative row per distinct listing: repeated mappings of
            # the same symbol are the ordinary state of an append-only master.
            listings: dict[tuple[str, str], dict] = {}
            for row in group.sort("validity_start").iter_rows(named=True):
                key = symbol_key(row["symbol"])
                if key is not None:
                    listings.setdefault(key, row)

            rows = list(listings.values())
            for i, row in enumerate(rows):
                for other in rows[i + 1 :]:
                    overlaps = (
                        row["validity_start"] < (other["validity_end"] or _FOREVER)
                        and other["validity_start"] < (row["validity_end"] or _FOREVER)
                    )
                    collisions.append(
                        AssetIdCollision(
                            asset_id=str(asset_id),
                            venue=str(venue),
                            symbol=other["symbol"],
                            existing_symbol=row["symbol"],
                            relation="overlap" if overlaps else "abut",
                        )
                    )
        return collisions

    def add_mapping(
        self,
        asset_id: str,
        venue: str,
        symbol: str,
        validity_start: datetime,
        validity_end: datetime | None = None,
        is_primary: bool = True,
        allow_collision: bool = False,
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
            allow_collision: Record the mapping even though it merges two
                listings under one `asset_id`. For a caller that has looked at
                the collision and decided; never a default.

        Raises:
            AssetIdCollisionError: if this mapping would put a second listing
                under `asset_id` on this venue **while the first is still
                live**. Two contracts trading at once cannot be one asset under
                two names, so the ingest is refused rather than resolved --
                resolving it means choosing between two price series by
                ingestion time, which is the defect this guard exists for
                (`DATA.md` section 9.1).

                Windows that *abut* are recorded with a warning instead: a
                symbol that ends where another begins is a rename, which
                point-in-time validity ranges exist to express. It is also the
                shape of a redenomination and of a reused equity ticker, and
                those need Phase 10a -- so it is surfaced rather than blocked.
        """
        collision = (
            None
            if allow_collision
            else self.check_collision(
                asset_id, venue, symbol, validity_start, validity_end
            )
        )
        if collision is not None:
            if collision.relation == "overlap":
                logger.error(f"Refusing mapping: {collision.describe()}")
                raise AssetIdCollisionError(collision)
            # Abutting windows are a *rename* as far as the store is concerned,
            # and expressing one is what the validity ranges are for — Phase 1
            # shipped that deliberately. So it is recorded, and flagged: the
            # same shape is also a redenomination or a reused equity ticker,
            # and those need the price adjustment `DATA.md` §10 defers to Phase
            # 10a. Nothing here can tell the three apart; a human can, and
            # `find_collisions()` puts it in front of them.
            logger.warning(
                f"Sequential listings under one asset_id: {collision.describe()}"
            )

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
        self,
        symbol: str,
        venue: str,
        asof: datetime | None = None,
        warn_if_unresolved: bool = True,
    ) -> str | None:
        """
        Resolve a venue symbol to a canonical asset_id.

        Args:
            symbol: Symbol on the venue (e.g., "BTC/USDT")
            venue: Venue name (e.g., "binance")
            asof: Knowledge date (None = use current time)
            warn_if_unresolved: Log a WARNING when the symbol has no mapping.
                Set False when absence is the expected answer rather than a
                fault — the nightly pipeline calls this to ask "is this symbol
                already mapped?" before adding it, and on a first run every
                symbol on the venue would otherwise log a warning.

        Returns:
            Canonical asset_id, or None if not found
        """
        if asof is None:
            asof = datetime.now(UTC).replace(tzinfo=None)

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
            # The loaders resolve every venue symbol before writing, and an
            # unresolved one silently drops that asset's rows — so the failure
            # surfaces downstream as an audit coverage miss with no explanation
            # unless it is recorded here.
            if warn_if_unresolved:
                logger.warning(f"Unresolved symbol {venue}:{symbol} at {asof.date()}")
            return None
        if len(matches) > 1:
            candidates = matches["asset_id"].unique().to_list()
            if len(candidates) > 1:
                # A genuine ambiguity: one venue symbol mapping to two different
                # canonical assets is a security-matching failure, and picking
                # the first silently attributes rows to the wrong asset. Prefer
                # the primary mapping, and say so loudly either way.
                primary = matches.filter(pl.col("is_primary"))
                chosen = (primary if len(primary) else matches)["asset_id"][0]
                logger.warning(
                    f"Ambiguous mappings for {venue}:{symbol} at {asof.date()}: "
                    f"{sorted(candidates)}; resolving to {chosen}"
                )
                return chosen

            # Same asset, recorded more than once. Append-only storage plus a
            # nightly job that re-registers the venue's symbols makes this the
            # ordinary state of an old asset master, and it changes nothing
            # about the answer — so it is DEBUG, not a WARNING per symbol per
            # run. (It used to warn, which on a populated master meant hundreds
            # of warnings a night for a non-problem.)
            logger.debug(
                f"{len(matches)} identical mappings for {venue}:{symbol} at "
                f"{asof.date()}; all resolve to {candidates[0]}"
            )

        return matches["asset_id"][0]

    def get_symbol(
        self, asset_id: str, venue: str, asof: datetime | None = None
    ) -> str | None:
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
            asof = datetime.now(UTC).replace(tzinfo=None)

        matches = self._cache.filter(
            (pl.col("asset_id") == asset_id)
            & (pl.col("venue") == venue)
            & (pl.col("is_primary"))
            & (pl.col("validity_start") <= asof)
            & (
                (pl.col("validity_end").is_null())
                | (pl.col("validity_end") > asof)
            )
        )

        if len(matches) == 0:
            return None
        return matches["symbol"][0]

    def list_assets(self) -> list[str]:
        """List all canonical asset IDs."""
        return sorted(self._cache["asset_id"].unique().to_list())

    def list_venues(self) -> list[str]:
        """List all venues in the asset master."""
        return sorted(self._cache["venue"].unique().to_list())

    def asset_info(
        self, asset_id: str, asof: datetime | None = None
    ) -> dict[str, str]:
        """
        Get current venue symbols for an asset.

        Returns:
            Dict mapping venue -> primary symbol at asof date
        """
        if asof is None:
            asof = datetime.now(UTC).replace(tzinfo=None)

        matches = self._cache.filter(
            (pl.col("asset_id") == asset_id)
            & (pl.col("is_primary"))
            & (pl.col("validity_start") <= asof)
            & (
                (pl.col("validity_end").is_null())
                | (pl.col("validity_end") > asof)
            )
        )

        if len(matches) == 0:
            return {}

        return dict(
            zip(
                matches["venue"].to_list(),
                matches["symbol"].to_list(),
                strict=True,
            )
        )

    def _save(self) -> None:
        """Save asset master to disk."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._cache.write_parquet(self.path)
        logger.info(f"Saved asset master to {self.path}")
