"""Canonical asset identity: one rule, used by every loader.

Two loaders answering "what is this symbol called internally?" differently is
how one asset acquires two `asset_id`s and — worse — how two different assets
acquire one. Both happened (`DATA.md` §9.1): `loaders/archive.py` stripped
multiplier prefixes (`1000SHIBUSDT -> SHIB`) while
`pipeline/nightly.py::_populate_asset_master` used ccxt's `market["base"]`,
which keeps them. The store ended up holding `1000SHIB` *and* `SHIB`, and
`register_symbols`' "already mapped?" guard could not notice, because it matches
the literal symbol string and `1000SHIBUSDT` is not `1000SHIB/USDT:USDT`.

So the rule lives here, once, and every loader calls it.

**Keep the venue's own base. Never strip.** The stripping rule rested on a
premise that is false. Returns really are invariant to a constant contract
multiplier, but Binance also uses the prefix to disambiguate **two different
tokens that share a ticker**, and lists both at the same time:
`1000CATUSDT` beside `CATUSDT`, `1000000BOBUSDT` beside `BOBUSDT`, with nine
months of overlap in the `BOB` case. Merging those put prices 711,000x apart
under one `asset_id`, which `latest_per_bar` then resolved by ingestion time —
i.e. arbitrarily.

What the new rule costs is that a *genuine* redenomination would produce two
sequential `asset_id`s rather than one spliced series. That is the better
failure: splicing two price scales without an adjustment factor manufactures a
return that never happened, and a short history does not. Measured against the
bucket on 2026-08-09 there are zero genuine redenominations in 832 supported
symbols, so the cost is theoretical and the merge risk was not. When one does
appear, `AssetMaster`'s collision guard is what says so — see `DATA.md` §10.
"""

from collections.abc import Mapping, Sequence
from dataclasses import dataclass

# Quote currencies we ingest. A second quote for the same asset (`BTCUSDC`,
# `BTCBUSD`) is a separate listing that would double-count rows against one
# `asset_id`, so only one is kept.
QUOTE_ASSETS: tuple[str, ...] = ("USDT",)

# ccxt writes a perpetual as `BASE/QUOTE:SETTLE` and a dated future as
# `BASE/QUOTE:SETTLE-YYMMDD`; the archive writes the latter as `BASEQUOTE_YYMMDD`.
# Dated futures are a different instrument with their own basis and expiry.
_CCXT_SEPARATOR = "/"
_CCXT_SETTLE = ":"
_EXPIRY_MARKERS = ("-", "_")


@dataclass(frozen=True)
class SymbolIdentity:
    """What a venue symbol resolves to, and the parts that decided it."""

    symbol: str
    base: str
    quote: str

    @property
    def asset_id(self) -> str:
        """The canonical internal identifier. It *is* the base, deliberately."""
        return self.base

    @property
    def key(self) -> tuple[str, str]:
        """What makes two symbols notational variants of each other.

        `BTCUSDT` (archive), `BTC/USDT` (ccxt spot) and `BTC/USDT:USDT` (ccxt
        perp) are three spellings of one listing and share a key; `1000CATUSDT`
        and `CATUSDT` are two listings and do not. The collision guard is the
        only consumer, and that distinction is the whole of its job.
        """
        return (self.base, self.quote)


def parse_venue_symbol(
    symbol: str,
    base: str | None = None,
    quotes: Sequence[str] = QUOTE_ASSETS,
) -> SymbolIdentity | None:
    """Split a venue symbol into base and quote, or None if we do not ingest it.

    Accepts both notations this project sees, because the point is that they
    agree:

        parse_venue_symbol("1000CATUSDT")            -> base 1000CAT
        parse_venue_symbol("1000CAT/USDT:USDT")      -> base 1000CAT
        parse_venue_symbol("1000CAT/USDT", base="1000CAT") -> base 1000CAT

    Args:
        symbol: The exact string the venue uses.
        base: The venue's own base asset when the caller already has it —
            ccxt's `market["base"]`. It wins over the parsed base, since the
            venue is the authority on its own naming; the symbol must still
            parse, so a dated future or a second quote currency is rejected
            whatever base comes with it.
        quotes: Quote currencies to accept.

    Returns:
        A `SymbolIdentity`, or None for anything unsupported: a dated future, a
        non-USDT quote, a bare quote asset, an empty string.
    """
    if not symbol:
        return None
    text = symbol.strip().upper()
    if not text:
        return None

    if _CCXT_SEPARATOR in text:
        parsed_base, _, remainder = text.partition(_CCXT_SEPARATOR)
        quote_part, _, settle = remainder.partition(_CCXT_SETTLE)
        # `BTC/USDT:USDT-260327` — the expiry sits on the settle currency.
        if settle and any(marker in settle for marker in _EXPIRY_MARKERS):
            return None
        if any(marker in quote_part for marker in _EXPIRY_MARKERS):
            return None
        quote = quote_part
    else:
        # Archive notation: `BTCUSDT`, and `BTCUSDT_240329` for a dated future.
        if any(marker in text for marker in _EXPIRY_MARKERS):
            return None
        quote = next((q for q in quotes if text.endswith(q)), "")
        if not quote:
            return None
        parsed_base = text[: -len(quote)]

    if quote not in quotes:
        return None

    resolved = (base or "").strip().upper() or parsed_base
    if not resolved:
        return None

    return SymbolIdentity(symbol=symbol, base=resolved, quote=quote)


def canonical_asset_id(
    symbol: str,
    base: str | None = None,
    quotes: Sequence[str] = QUOTE_ASSETS,
) -> str | None:
    """The canonical `asset_id` for a venue symbol, or None if unsupported."""
    identity = parse_venue_symbol(symbol, base=base, quotes=quotes)
    return identity.asset_id if identity else None


def canonical_asset_id_for_market(
    symbol: str,
    market: Mapping | None = None,
    quotes: Sequence[str] = QUOTE_ASSETS,
) -> str | None:
    """`canonical_asset_id` for a ccxt market dict, which may carry the base."""
    base = (market or {}).get("base")
    return canonical_asset_id(symbol, base=base if isinstance(base, str) else None, quotes=quotes)


def is_supported_symbol(symbol: str, quotes: Sequence[str] = QUOTE_ASSETS) -> bool:
    """Whether this symbol is one we ingest at all."""
    return parse_venue_symbol(symbol, quotes=quotes) is not None


def symbol_key(
    symbol: str, base: str | None = None, quotes: Sequence[str] = QUOTE_ASSETS
) -> tuple[str, str] | None:
    """`(base, quote)` — equal for two spellings of one listing, else different."""
    identity = parse_venue_symbol(symbol, base=base, quotes=quotes)
    return identity.key if identity else None
