"""Bulk history from Binance's public data archive (`data.binance.vision`).

Route A of `DATA.md`. The ccxt loaders remain the nightly incremental path;
this module exists because the *history* Phase 5 is blocked on cannot be
fetched through them from a geo-blocked address — `api.binance.com` and
`fapi.binance.com` answer HTTP 451 — while the bulk archive on a different
host answers 200 and serves checksummed monthly zips back to 2020-01.

Three properties make it the preferred route and shape this module:

- **It is the same venue.** Archive rows land under `venue="binance"`, so
  history from here and future rows from the nightly ccxt run join cleanly
  under one `asset_id` namespace.
- **It is a file archive, not an API.** There is no rate limit and no key, but
  also no ccxt: the payload is a zipped CSV per (symbol, month), so parsing and
  timestamp handling are this module's problem rather than a library's. The
  four format details that bite are documented on the parsers below.
- **Enumerate, do not probe.** Symbols and months come from the bucket's S3
  listing. Guessing months and treating 404 as "not listed" wastes half the
  requests and cannot distinguish "the asset did not exist yet" from "the
  archive has a hole" — and the first month a symbol appears is the listing
  date `UNIVERSE_CONFIG.min_listing_age_days` needs.

This module must never import ccxt: being unable to reach the API is the whole
reason it exists.

Point-in-time discipline is unchanged. Every row still carries `event_ts` (the
bar's own time, from the archive) and `ingested_ts` (now, when we learned it),
and the store partitions by the latter — so a bulk backfill lands in one
partition dated the day it ran, which is the honest record of when the data was
knowable. The consequence for research is documented in `DATA.md` §4: backtests
over this history need `pit_mode="event"` and their results are research
indications, not live-fidelity results.
"""

import argparse
import hashlib
import io
import re
import sys
import time
import zipfile
from collections import defaultdict
from collections.abc import Iterable, Iterator, Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any
from urllib.parse import quote
from xml.etree import ElementTree

import polars as pl
import requests

from config import DATASTORE_PATH, LOADER_CONFIG, LOG_CONFIG
from datastore import AssetMaster, DatasetSchema, ParquetStore
from loaders.base import BaseLoader
from loaders.schemas import FUNDING_RATE_SCHEMA, OHLCV_SCHEMA
from loaders.window import FetchWindow, utc_now
from logging_config import get_logger, new_run_id, set_level, set_run_id

logger = get_logger(__name__)

# The archive itself, and the S3 endpoint that lists it. Two different hosts
# for the same bucket: the first serves objects, the second answers the
# `?prefix=&delimiter=` listing queries the object host does not.
ARCHIVE_HOST = "https://data.binance.vision"
LISTING_HOST = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision"

# Not to be confused with `data-api.binance.vision`, one hyphen apart, which is
# a spot-only REST mirror and carries no funding rate.

DEFAULT_WORKERS = 8
DEFAULT_TIMEFRAME = "1d"

KLINES = "klines"
FUNDING_RATE = "fundingRate"

# Kline CSV layout. `volume` (index 5) is *base* volume, which is what
# OHLCV_SCHEMA wants: `universe/builder.py` computes dollar volume as
# `close * volume`. Substituting `quote_volume` would square the price.
KLINE_COLUMNS = (
    "open_time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "close_time",
    "quote_volume",
    "count",
    "taker_buy_volume",
    "taker_buy_quote_volume",
    "ignore",
)

# Funding CSV layout. There is no mark or index price in the archive — those
# are snapshot-only fields on the API — so both stay null, which
# `AUDIT_CONFIG.nullable_columns_by_dataset["funding_rate"]` already permits.
FUNDING_COLUMNS = ("calc_time", "funding_interval_hours", "last_funding_rate")

# Epoch timestamps in the archive are milliseconds, except that some 2025+
# files switched to microseconds mid-stream. Detect by magnitude rather than by
# month: 1e14 ms is the year 5138, and 1e14 µs is 1973, so nothing real is
# ambiguous.
MICROSECOND_THRESHOLD = 100_000_000_000_000

QUOTE_ASSET = "USDT"

# Multiplier contracts (`1000BONKUSDT`, `1MBABYDOGEUSDT`) are the same
# underlying at a scaled contract size, and returns are invariant to a constant
# multiplier — so they map to the underlying base. Two of them under two
# `asset_id`s would be a pair of perfectly correlated "independent" bets, which
# is exactly the lie the breadth report exists to prevent.
#
# The lookahead is what keeps `1INCHUSDT` intact: it starts with "1" but not
# with any multiplier prefix, and a naive strip would score it as "NCH".
_MULTIPLIER_RE = re.compile(r"^(?:1000000|100000|10000|1000|1M)(?=[A-Z]{2,})")

_MONTH_RE = re.compile(r"-(\d{4}-\d{2})\.zip$")


class ArchiveError(Exception):
    """Any failure fetching or reading an archive file."""


class ArchiveNotFoundError(ArchiveError):
    """The archive has no such object (HTTP 404)."""


class ChecksumMismatchError(ArchiveError):
    """A downloaded file does not match its published SHA-256."""


# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------


class ArchiveHttp:
    """GETs against the archive, with one retry and a backoff.

    Deliberately thin and injectable: every test in
    `tests/test_archive_loader.py` passes a fake in here, so the suite stays
    hermetic and fast while the real class stays the only thing that touches
    the network.
    """

    def __init__(
        self,
        timeout: float = 30.0,
        retries: int = 1,
        backoff: float = 1.0,
        session: Any = None,
    ):
        self.timeout = timeout
        self.retries = retries
        self.backoff = backoff
        self.session = session or requests.Session()

    def get(self, url: str) -> bytes:
        """Fetch `url`, retrying once on anything that is not 200 or 404.

        A 404 is a fact about the archive (that month was never published), not
        a transient failure, so it raises immediately rather than after a
        pointless second request.
        """
        last_status = None
        for attempt in range(self.retries + 1):
            try:
                response = self.session.get(url, timeout=self.timeout)
            except Exception as e:  # network-level failure; retry once
                last_status = repr(e)
                if attempt >= self.retries:
                    raise ArchiveError(f"GET {url} failed: {e}") from e
                time.sleep(self.backoff * (2**attempt))
                continue

            status = getattr(response, "status_code", None)
            if status == 200:
                return response.content
            if status == 404:
                raise ArchiveNotFoundError(f"GET {url} -> 404")

            last_status = status
            if attempt < self.retries:
                time.sleep(self.backoff * (2**attempt))

        raise ArchiveError(f"GET {url} -> {last_status}")

    def get_text(self, url: str) -> str:
        return self.get(url).decode("utf-8")


# ---------------------------------------------------------------------------
# S3 listing
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class S3Listing:
    """One page of an S3 bucket listing."""

    prefixes: tuple[str, ...]
    keys: tuple[str, ...]
    is_truncated: bool
    next_marker: str | None


def _local_name(tag: str) -> str:
    """`{http://s3.amazonaws.com/doc/2006-03-01/}Key` -> `Key`."""
    return tag.rpartition("}")[2]


def parse_listing(xml_text: str) -> S3Listing:
    """Parse an S3 `ListBucketResult` page.

    Namespace-insensitive on purpose: the bucket's declared namespace is not
    part of the contract we care about, and matching on it is a silent
    "everything is empty" failure if it ever changes.

    Continuation: S3 returns `NextMarker` when a delimiter is in play, and
    nothing when it is not — in which case the marker is the last key of the
    page. Getting this wrong truncates a listing at 1000 entries with no
    symptom, which is how a backfill quietly covers the alphabet up to "L".
    """
    root = ElementTree.fromstring(xml_text)

    prefixes: list[str] = []
    keys: list[str] = []
    is_truncated = False
    next_marker: str | None = None

    for child in root:
        name = _local_name(child.tag)
        if name == "Contents":
            for field in child:
                if _local_name(field.tag) == "Key" and field.text:
                    keys.append(field.text)
        elif name == "CommonPrefixes":
            for field in child:
                if _local_name(field.tag) == "Prefix" and field.text:
                    prefixes.append(field.text)
        elif name == "IsTruncated":
            is_truncated = (child.text or "").strip().lower() == "true"
        elif name == "NextMarker" and child.text:
            next_marker = child.text

    if is_truncated and next_marker is None:
        if keys:
            next_marker = keys[-1]
        elif prefixes:
            next_marker = prefixes[-1]

    return S3Listing(
        prefixes=tuple(prefixes),
        keys=tuple(keys),
        is_truncated=is_truncated,
        next_marker=next_marker,
    )


def listing_url(prefix: str, delimiter: str = "/", marker: str | None = None) -> str:
    url = f"{LISTING_HOST}?delimiter={quote(delimiter)}&prefix={quote(prefix)}"
    if marker:
        url += f"&marker={quote(marker)}"
    return url


def list_all(
    http: ArchiveHttp,
    prefix: str,
    delimiter: str = "/",
    max_pages: int = 50,
) -> S3Listing:
    """Walk every page of a listing and return the union.

    `max_pages` is a loop guard, not a budget to be reached: a listing that
    hits it is logged as truncated rather than silently returning a partial
    answer.
    """
    prefixes: list[str] = []
    keys: list[str] = []
    marker: str | None = None

    for page in range(1, max_pages + 1):
        listing = parse_listing(http.get_text(listing_url(prefix, delimiter, marker)))
        prefixes.extend(listing.prefixes)
        keys.extend(listing.keys)

        if not listing.is_truncated or not listing.next_marker:
            return S3Listing(tuple(prefixes), tuple(keys), False, None)

        marker = listing.next_marker
        if page == max_pages:
            logger.warning(
                f"Stopped listing {prefix} at the {max_pages}-page guard with "
                f"{len(keys)} key(s) and {len(prefixes)} prefix(es); the listing "
                f"is incomplete"
            )

    return S3Listing(tuple(prefixes), tuple(keys), True, marker)


# ---------------------------------------------------------------------------
# Symbols
# ---------------------------------------------------------------------------


def is_supported_symbol(symbol: str) -> bool:
    """Whether an archive symbol is one we ingest.

    USDT-quoted perpetual/spot listings only. `BTCUSDC` and `BTCBUSD` are
    separate listings of the same asset and would double-count rows against one
    `asset_id`; dated futures (`BTCUSDT_240329`) are a different instrument with
    its own basis and expiry.
    """
    if "_" in symbol:
        return False
    if not symbol.endswith(QUOTE_ASSET):
        return False
    return len(symbol) > len(QUOTE_ASSET)


def asset_id_for(symbol: str) -> str | None:
    """Canonical `asset_id` for an archive symbol, or None if unsupported.

    `1000BONKUSDT -> BONK`, `1MBABYDOGEUSDT -> BABYDOGE`, `1INCHUSDT -> 1INCH`.
    """
    if not is_supported_symbol(symbol):
        return None
    base = symbol[: -len(QUOTE_ASSET)]
    return _MULTIPLIER_RE.sub("", base) or base


# ---------------------------------------------------------------------------
# Payloads
# ---------------------------------------------------------------------------


def parse_checksum(text: str) -> str:
    """Pull the SHA-256 out of a `.CHECKSUM` body (`<sha256>  <filename>`)."""
    parts = text.split()
    if not parts:
        raise ArchiveError("Empty checksum file")
    return parts[0].strip().lower()


def verify_checksum(payload: bytes, checksum_text: str, name: str) -> None:
    """Raise unless `payload` hashes to the published digest.

    Verifying is one extra tiny GET per file and it is the whole reason to
    prefer the archive over scraping: a truncated download that parses is
    otherwise indistinguishable from a month where the asset barely traded.
    """
    expected = parse_checksum(checksum_text)
    actual = hashlib.sha256(payload).hexdigest()
    if actual != expected:
        raise ChecksumMismatchError(
            f"{name}: sha256 {actual} does not match published {expected}"
        )


def unzip_single(payload: bytes) -> bytes:
    """Extract the one member of an archive zip."""
    try:
        with zipfile.ZipFile(io.BytesIO(payload)) as zf:
            names = [n for n in zf.namelist() if not n.endswith("/")]
            if len(names) != 1:
                raise ArchiveError(f"Expected one file in the zip, found {names}")
            return zf.read(names[0])
    except zipfile.BadZipFile as e:
        raise ArchiveError(f"Not a zip file: {e}") from e


def read_archive_csv(payload: bytes, default_columns: Sequence[str]) -> pl.DataFrame:
    """Zipped CSV -> an all-string frame with named columns.

    **Futures klines carry a header row and spot klines do not**, so the first
    line is sniffed rather than assumed either way. Everything is read as text
    and cast explicitly afterwards: inference over a column that is empty for
    one month and populated the next produces two different dtypes for the same
    dataset, which the store's schema check then rejects.
    """
    csv_bytes = unzip_single(payload)
    if not csv_bytes.strip():
        return pl.DataFrame()

    first_line = csv_bytes.split(b"\n", 1)[0].decode("utf-8", errors="replace")
    has_header = default_columns[0] in first_line

    try:
        frame = pl.read_csv(
            io.BytesIO(csv_bytes),
            has_header=False,
            skip_rows=1 if has_header else 0,
            infer_schema_length=0,
            truncate_ragged_lines=True,
        )
    except pl.exceptions.NoDataError:
        # A month published as a header row and nothing else. Empty, not broken.
        return pl.DataFrame()
    if frame.is_empty():
        return frame

    names = list(default_columns[: frame.width])
    names += [f"extra_{i}" for i in range(len(names), frame.width)]
    return frame.rename(dict(zip(frame.columns, names, strict=True)))


def epoch_to_datetime(column: str) -> pl.Expr:
    """Cast an epoch column to naive-UTC `Datetime("us")`, ms or µs.

    The unit is decided per file from the magnitude of the values, because the
    archive switched units partway through 2025 and the month is not a reliable
    predictor of which one a given file uses.
    """
    value = pl.col(column).cast(pl.Int64)
    return (
        pl.when(value > MICROSECOND_THRESHOLD)
        .then(value.cast(pl.Datetime("us")))
        .otherwise(value.cast(pl.Datetime("ms")).dt.cast_time_unit("us"))
        .alias("event_ts")
    )


def parse_klines(payload: bytes) -> pl.DataFrame:
    """A monthly kline zip -> `event_ts, open, high, low, close, volume`."""
    frame = read_archive_csv(payload, KLINE_COLUMNS)
    if frame.is_empty():
        return pl.DataFrame()

    return frame.select(
        epoch_to_datetime("open_time"),
        pl.col("open").cast(pl.Float64),
        pl.col("high").cast(pl.Float64),
        pl.col("low").cast(pl.Float64),
        pl.col("close").cast(pl.Float64),
        pl.col("volume").cast(pl.Float64),
    ).sort("event_ts")


def parse_funding(payload: bytes) -> pl.DataFrame:
    """A monthly funding zip -> `event_ts, funding_rate`."""
    frame = read_archive_csv(payload, FUNDING_COLUMNS)
    if frame.is_empty():
        return pl.DataFrame()

    return frame.select(
        epoch_to_datetime("calc_time"),
        pl.col("last_funding_rate").cast(pl.Float64).alias("funding_rate"),
    ).sort("event_ts")


def months_in(window: FetchWindow) -> list[str]:
    """Every `YYYY-MM` the window touches, inclusive of both ends."""
    months = []
    cursor = window.start.replace(day=1)
    last = window.end.replace(day=1)
    while cursor <= last:
        months.append(f"{cursor.year:04d}-{cursor.month:02d}")
        cursor = (cursor + timedelta(days=32)).replace(day=1)
    return months


def month_start(month: str) -> datetime:
    return datetime.strptime(month, "%Y-%m")


# ---------------------------------------------------------------------------
# Loader
# ---------------------------------------------------------------------------


class BinanceVisionLoader(BaseLoader):
    """Ingest `ohlcv_daily` and `funding_rate` from the Binance public archive.

    Subclasses `BaseLoader` for symbol resolution, timestamp stamping and the
    logged append wrapper — the same contract the ccxt loaders honour, so the
    rows it writes are indistinguishable downstream.

        loader = BinanceVisionLoader(market="um", symbols=["BTCUSDT"])
        loader.run_daily(window=FetchWindow(start, end))
        loader.run_funding(window=FetchWindow(start, end))
    """

    def __init__(
        self,
        market: str = "um",
        symbols: Sequence[str] | None = None,
        venue: str = "binance",
        store: ParquetStore | None = None,
        asset_master: AssetMaster | None = None,
        max_symbols: int | None = None,
        workers: int = DEFAULT_WORKERS,
        http: ArchiveHttp | None = None,
        verify_checksums: bool = True,
        chunk: str = "symbol",
    ):
        """
        Args:
            market: "um" (USDT-margined futures) or "spot". Funding rate exists
                on "um" only.
            symbols: Archive symbols to pull (`["BTCUSDT", ...]`). None
                enumerates the bucket — see `list_symbols` for why an explicit
                list is preferable.
            venue: Store namespace; leave as "binance" so archive history and
                nightly ccxt rows join.
            max_symbols: Cap on enumerated symbols (default:
                `LOADER_CONFIG.max_symbols_per_run`).
            workers: Concurrent downloads.
            http: Injected HTTP client (the tests' seam).
            verify_checksums: Check every file against its published SHA-256.
            chunk: "symbol" (default) or "month" — how much is accumulated
                before an append. See `_append_frames`.
        """
        super().__init__(venue, store, asset_master)
        if market not in ("um", "spot"):
            raise ValueError(f"market must be 'um' or 'spot', got {market!r}")
        if chunk not in ("symbol", "month"):
            raise ValueError(f"chunk must be 'symbol' or 'month', got {chunk!r}")

        self.market = market
        self.symbols = list(symbols) if symbols else None
        self.max_symbols = (
            LOADER_CONFIG.max_symbols_per_run if max_symbols is None else max_symbols
        )
        self.workers = max(1, workers)
        self.http = http or ArchiveHttp()
        self.verify_checksums = verify_checksums
        self.chunk = chunk

    # -- paths --------------------------------------------------------------

    def prefix_for(self, kind: str) -> str:
        """Bucket prefix holding every symbol's directory for a dataset kind."""
        if kind == FUNDING_RATE and self.market != "um":
            raise ValueError("funding rate exists on USDT-margined futures only")
        root = "futures/um" if self.market == "um" else "spot"
        return f"data/{root}/monthly/{kind}/"

    def key_for(self, symbol: str, month: str, kind: str) -> str:
        prefix = self.prefix_for(kind)
        if kind == KLINES:
            return f"{prefix}{symbol}/{DEFAULT_TIMEFRAME}/{symbol}-{DEFAULT_TIMEFRAME}-{month}.zip"
        return f"{prefix}{symbol}/{symbol}-{kind}-{month}.zip"

    def symbol_prefix(self, symbol: str, kind: str) -> str:
        prefix = f"{self.prefix_for(kind)}{symbol}/"
        return f"{prefix}{DEFAULT_TIMEFRAME}/" if kind == KLINES else prefix

    @staticmethod
    def url_for(key: str) -> str:
        return f"{ARCHIVE_HOST}/{key}"

    # -- enumeration --------------------------------------------------------

    def list_symbols(self, kind: str = KLINES) -> list[str]:
        """Every supported symbol the archive publishes for this market.

        The cap is applied after the USDT filter (the Phase 2 lesson: a list
        interleaves quote currencies, so capping first starves the budget), but
        it is still *alphabetical*, and the archive carries no liquidity
        information to rank by. At 938 symbols and a 200 budget that silently
        favours everything beginning with a digit or an early letter, so an
        explicit `symbols=` list — chosen from the venue's volume ranking — is
        the better input whenever you have one.
        """
        listing = list_all(self.http, self.prefix_for(kind))
        symbols = sorted(
            {
                p.rstrip("/").rpartition("/")[2]
                for p in listing.prefixes
                if is_supported_symbol(p.rstrip("/").rpartition("/")[2])
            }
        )
        if self.max_symbols and len(symbols) > self.max_symbols:
            logger.warning(
                f"Archive lists {len(symbols)} supported symbols; capping at "
                f"{self.max_symbols} alphabetically. Pass symbols=[...] to choose "
                f"by liquidity instead."
            )
            symbols = symbols[: self.max_symbols]
        logger.info(f"Enumerated {len(symbols)} symbols under {self.prefix_for(kind)}")
        return symbols

    def list_months(self, symbol: str, kind: str = KLINES) -> list[str]:
        """Every `YYYY-MM` published for a symbol, oldest first.

        The first entry is the symbol's listing month, which is what the
        universe builder's `min_listing_age_days` rule needs and what the asset
        master mapping's validity start is set to.
        """
        listing = list_all(self.http, self.symbol_prefix(symbol, kind), delimiter="")
        months = sorted(
            {
                match.group(1)
                for key in listing.keys
                if not key.endswith(".CHECKSUM")
                for match in [_MONTH_RE.search(key)]
                if match
            }
        )
        logger.debug(f"{symbol}/{kind}: {len(months)} month(s) published")
        return months

    # -- download -----------------------------------------------------------

    def download(self, symbol: str, month: str, kind: str) -> pl.DataFrame:
        """One (symbol, month) file, verified and parsed."""
        key = self.key_for(symbol, month, kind)
        url = self.url_for(key)
        payload = self.http.get(url)

        if self.verify_checksums:
            verify_checksum(payload, self.http.get_text(f"{url}.CHECKSUM"), key)

        frame = parse_klines(payload) if kind == KLINES else parse_funding(payload)
        logger.debug(f"{key}: {len(frame)} row(s)")
        return frame

    # -- preparation --------------------------------------------------------

    def _prepare_ohlcv(self, frame: pl.DataFrame, asset_id: str) -> pl.DataFrame:
        frame = frame.with_columns(
            pl.lit(asset_id, dtype=pl.Utf8).alias("asset_id"),
            pl.lit(self.venue, dtype=pl.Utf8).alias("venue"),
            pl.lit(DEFAULT_TIMEFRAME, dtype=pl.Utf8).alias("timeframe"),
        )
        frame = self.add_timestamps(frame)
        return frame.select(list(OHLCV_SCHEMA.fields))

    def _prepare_funding(self, frame: pl.DataFrame, asset_id: str) -> pl.DataFrame:
        frame = frame.with_columns(
            pl.lit(asset_id, dtype=pl.Utf8).alias("asset_id"),
            pl.lit(self.venue, dtype=pl.Utf8).alias("venue"),
            # Null, not 0.0: the archive does not publish mark and index price,
            # and a fabricated zero is a price a downstream check would believe.
            pl.lit(None, dtype=pl.Float64).alias("mark_price"),
            pl.lit(None, dtype=pl.Float64).alias("index_price"),
        )
        frame = self.add_timestamps(frame)
        return frame.select(list(FUNDING_RATE_SCHEMA.fields))

    # -- asset master -------------------------------------------------------

    def register_symbols(self, first_months: dict[str, str]) -> int:
        """Map archive symbols to canonical `asset_id`s. Returns rows added.

        `NightlyPipeline._populate_asset_master` builds the master from
        `exchange.load_markets()`, which is exactly the call that is
        unreachable here — so the archive route registers its own symbols.

        The mapping is stored under the **exact** archive string (`BTCUSDT`, no
        slash), because `resolve_symbol` matches literally and the ccxt loaders
        will separately register their own notation (`BTC/USDT`,
        `BTC/USDT:USDT`) for the same `asset_id`. Validity starts at the first
        month the archive published for that symbol, which is the closest thing
        to a listing date the archive offers.
        """
        added = 0
        for symbol, month in sorted(first_months.items()):
            asset_id = asset_id_for(symbol)
            if asset_id is None:
                logger.debug(f"Skipping unsupported archive symbol {symbol}")
                continue

            validity_start = month_start(month)
            existing = self.asset_master.resolve_symbol(
                symbol, self.venue, asof=utc_now(), warn_if_unresolved=False
            )
            if existing is not None:
                continue

            self.asset_master.add_mapping(asset_id, self.venue, symbol, validity_start)
            added += 1

        logger.info(
            f"Asset master: {added} new archive symbol(s) registered under "
            f"venue={self.venue} ({len(first_months)} seen)"
        )
        return added

    # -- running ------------------------------------------------------------

    def run_daily(
        self,
        window: FetchWindow | None = None,
        symbols: Sequence[str] | None = None,
    ) -> int:
        """Backfill `ohlcv_daily`. Returns the number of rows appended."""
        return self._run("ohlcv_daily", KLINES, OHLCV_SCHEMA, window, symbols)

    def run_funding(
        self,
        window: FetchWindow | None = None,
        symbols: Sequence[str] | None = None,
    ) -> int:
        """Backfill `funding_rate`. Returns the number of rows appended."""
        return self._run("funding_rate", FUNDING_RATE, FUNDING_RATE_SCHEMA, window, symbols)

    def fetch(
        self,
        kind: str = KLINES,
        window: FetchWindow | None = None,
        symbols: Sequence[str] | None = None,
    ) -> pl.DataFrame:
        """Everything in `window` as one frame, written nowhere.

        A convenience for a small pull (the scratch demo, a look at one symbol)
        and deliberately *not* the production path: it holds the whole result in
        memory, which five years across two hundred symbols should never be
        asked to do. Use `run_daily`/`run_funding`, which append as they go.
        """
        window = window or self._default_window()
        symbols = list(symbols or self.symbols or self.list_symbols(kind))
        frames = [
            prepared
            for _, prepared in self._download_prepared(symbols, kind, window)
            if not prepared.is_empty()
        ]
        return pl.concat(frames, how="vertical") if frames else pl.DataFrame()

    @staticmethod
    def _default_window() -> FetchWindow:
        return FetchWindow.from_lookback(LOADER_CONFIG.initial_backfill_days)

    def _plan(
        self, symbols: Sequence[str], kind: str, window: FetchWindow
    ) -> dict[str, list[str]]:
        """Which months to fetch per symbol, from the listing rather than guesses.

        Registers each symbol in the asset master on the way, using the first
        month the archive publishes — *before* the window trim, because a
        symbol listed in 2021 and fetched from 2024 still has a 2021 listing
        date, and pretending otherwise would let a three-year-old asset fail the
        universe's listing-age rule.
        """
        wanted = set(months_in(window))
        plan: dict[str, list[str]] = {}
        first_months: dict[str, str] = {}

        with ThreadPoolExecutor(max_workers=self.workers) as pool:
            futures = {
                pool.submit(self.list_months, symbol, kind): symbol for symbol in symbols
            }
            for future in as_completed(futures):
                symbol = futures[future]
                try:
                    published = future.result()
                except ArchiveError as e:
                    logger.warning(f"Could not list {symbol}/{kind}: {e}")
                    continue
                if not published:
                    logger.debug(f"{symbol}/{kind}: nothing published")
                    continue
                first_months[symbol] = published[0]
                months = [m for m in published if m in wanted]
                if months:
                    plan[symbol] = months

        self.register_symbols(first_months)
        return plan

    def _download_prepared(
        self, symbols: Sequence[str], kind: str, window: FetchWindow
    ) -> Iterator[tuple[str, pl.DataFrame]]:
        """Yield `(symbol, prepared frame)` per chunk, as downloads complete.

        Downloads are concurrent; the frames are yielded (and therefore
        appended) on the calling thread, because `ParquetStore.append` numbers
        its output file from the count already in the partition and two threads
        would race for the same name.
        """
        plan = self._plan(symbols, kind, window)
        if not plan:
            logger.warning(f"Nothing to fetch for {kind} over {window}")
            return

        resolved = {
            symbol: self.asset_master.resolve_symbol(symbol, self.venue, asof=utc_now())
            for symbol in plan
        }
        total_files = sum(len(months) for months in plan.values())
        logger.info(
            f"Fetching {total_files} {kind} file(s) for {len(plan)} symbol(s) over "
            f"{window} at {self.workers} worker(s)"
        )

        pending: dict[str, int] = {s: len(m) for s, m in plan.items()}
        buffered: dict[str, list[pl.DataFrame]] = defaultdict(list)

        with ThreadPoolExecutor(max_workers=self.workers) as pool:
            futures = {
                pool.submit(self.download, symbol, month, kind): (symbol, month)
                for symbol, months in plan.items()
                for month in months
            }
            for future in as_completed(futures):
                symbol, month = futures[future]
                asset_id = resolved.get(symbol)
                try:
                    frame = future.result()
                except ArchiveError as e:
                    # One missing or corrupt month must not abandon the other
                    # 11,999 files; it is logged as the gap it is.
                    logger.warning(f"Skipping {symbol} {month} {kind}: {e}")
                    frame = pl.DataFrame()

                if asset_id is None:
                    # Registration ran in `_plan`, so this is a symbol the asset
                    # master refused — its rows have nowhere to go.
                    logger.warning(f"{symbol} did not resolve to an asset_id; dropping")
                    frame = pl.DataFrame()
                elif not frame.is_empty():
                    # Files are whole months; the window's ends are not.
                    frame = frame.filter(
                        (pl.col("event_ts") >= window.start)
                        & (pl.col("event_ts") <= window.end)
                    )

                if asset_id is not None and not frame.is_empty():
                    prepared = (
                        self._prepare_ohlcv(frame, asset_id)
                        if kind == KLINES
                        else self._prepare_funding(frame, asset_id)
                    )
                    if self.chunk == "month":
                        yield symbol, prepared
                    else:
                        buffered[symbol].append(prepared)

                pending[symbol] -= 1
                if pending[symbol] == 0 and buffered.get(symbol):
                    frames = buffered.pop(symbol)
                    yield symbol, pl.concat(frames, how="vertical").sort("event_ts")

    def _append_frames(
        self,
        dataset: str,
        schema: DatasetSchema,
        chunks: Iterable[tuple[str, pl.DataFrame]],
    ) -> int:
        """Append each chunk, returning the row total.

        `chunk="symbol"` (the default) accumulates one symbol's months and
        writes them together: five years is ~1,800 rows, so nothing large is
        held, and the store gets ~200 files per dataset instead of ~12,000. That
        matters on the read side — `ParquetStore.read` opens every file in a
        partition, and a bulk backfill lands in a single partition — so a file
        per (symbol, month) would be paid back on every rebalance of every
        backtest. `chunk="month"` writes each file as it arrives, for a machine
        where even a symbol's history is too much to hold.
        """
        total = 0
        for symbol, frame in chunks:
            if frame.is_empty():
                continue
            self.append(dataset, frame, schema)
            total += len(frame)
            logger.debug(f"{symbol}: {len(frame)} row(s) appended to {dataset}")
        return total

    def _run(
        self,
        dataset: str,
        kind: str,
        schema: DatasetSchema,
        window: FetchWindow | None,
        symbols: Sequence[str] | None,
    ) -> int:
        window = window or self._default_window()
        symbols = list(symbols or self.symbols or self.list_symbols(kind))
        if not symbols:
            logger.warning(f"No symbols selected for {dataset}")
            return 0

        started = time.monotonic()
        rows = self._append_frames(
            dataset, schema, self._download_prepared(symbols, kind, window)
        )
        logger.info(
            f"Archive backfill complete: {rows} row(s) -> {dataset} over {window} "
            f"in {time.monotonic() - started:.1f}s"
        )
        return rows


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _parse_date(value: str) -> datetime:
    """`YYYY-MM-DD` or full ISO 8601, as naive UTC."""
    parsed = datetime.fromisoformat(value)
    return parsed.replace(tzinfo=None)


def main(argv: Sequence[str] | None = None) -> int:
    """`python -m loaders.archive --market um --start ... --end ...`"""
    parser = argparse.ArgumentParser(
        description="Backfill ohlcv_daily and funding_rate from data.binance.vision"
    )
    parser.add_argument("--market", default="um", choices=("um", "spot"))
    parser.add_argument("--start", required=True, help="Window start (YYYY-MM-DD, UTC)")
    parser.add_argument("--end", help="Window end (YYYY-MM-DD, UTC; default: now)")
    parser.add_argument(
        "--datasets",
        default="ohlcv_daily,funding_rate",
        help="Comma-separated: ohlcv_daily, funding_rate",
    )
    parser.add_argument("--symbols", help="Comma-separated archive symbols (e.g. BTCUSDT,ETHUSDT)")
    parser.add_argument("--max-symbols", type=int, default=None)
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS)
    parser.add_argument("--chunk", default="symbol", choices=("symbol", "month"))
    parser.add_argument("--no-verify-checksums", action="store_true")
    levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
    parser.add_argument(
        "--log-level",
        choices=levels,
        help="Level for logs/loaders.log for this run (default: TM_LOG_LEVEL, else INFO)",
    )
    parser.add_argument(
        "--console-log-level",
        choices=levels,
        help="Level for stderr for this run (default: TM_CONSOLE_LOG_LEVEL, else WARNING)",
    )
    args = parser.parse_args(argv)

    if args.log_level or args.console_log_level:
        set_level(args.log_level or LOG_CONFIG.level, args.console_log_level)

    run_id = new_run_id()
    set_run_id(run_id)

    window = FetchWindow(
        start=_parse_date(args.start),
        end=_parse_date(args.end) if args.end else utc_now(),
    )
    datasets = [d.strip() for d in args.datasets.split(",") if d.strip()]
    unknown = [d for d in datasets if d not in ("ohlcv_daily", "funding_rate")]
    if unknown:
        parser.error(f"Unknown dataset(s): {unknown}")

    loader = BinanceVisionLoader(
        market=args.market,
        symbols=[s.strip() for s in args.symbols.split(",")] if args.symbols else None,
        store=ParquetStore(DATASTORE_PATH),
        asset_master=AssetMaster(DATASTORE_PATH / "asset_master.parquet"),
        max_symbols=args.max_symbols,
        workers=args.workers,
        verify_checksums=not args.no_verify_checksums,
        chunk=args.chunk,
    )

    logger.info(f"run_id={run_id}, market={args.market}, window={window}, datasets={datasets}")
    print(f"Binance archive backfill: {window}, market={args.market}, run_id={run_id}")

    totals: dict[str, int] = {}
    failed = False
    for dataset in datasets:
        try:
            if dataset == "ohlcv_daily":
                totals[dataset] = loader.run_daily(window=window)
            else:
                totals[dataset] = loader.run_funding(window=window)
        except Exception as e:
            # One dataset failing must not discard the other's rows: they are
            # already appended, and funding is only needed by `carry`.
            logger.error(f"{dataset} backfill failed: {e}", exc_info=True)
            print(f"  {dataset}: FAILED ({e})")
            failed = True
            continue
        print(f"  {dataset}: {totals[dataset]} rows")

    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
