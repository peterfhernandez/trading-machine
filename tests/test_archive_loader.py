"""Tests for the Binance public-archive loader (`loaders/archive.py`).

No network: every test drives `BinanceVisionLoader` through `FakeArchive`, an
in-memory stand-in for `data.binance.vision` that stores zipped payloads by
object key and answers the S3 listing query by computing prefixes and pages
from that map. It is a fake rather than a set of canned responses because the
listing's *continuation* is the part that goes wrong silently — a truncated
listing looks exactly like a symbol that stopped publishing — and only a fake
that actually paginates can exercise it.

The format details being pinned here are the four `DATA.md` calls out, each of
which produces a plausible-looking frame when it is wrong rather than an error:
futures klines carry a header row and spot klines do not; base volume is column
5 and quote volume is column 7; epoch timestamps are milliseconds except where
they are microseconds; and funding files have no mark or index price.
"""

import hashlib
import io
import zipfile
from datetime import datetime
from urllib.parse import parse_qs, urlparse

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from datastore import AssetMaster, ParquetStore, count_duplicate_bars
from loaders.archive import (
    ARCHIVE_HOST,
    FUNDING_RATE,
    KLINES,
    LISTING_HOST,
    ArchiveError,
    ArchiveHttp,
    ArchiveNotFoundError,
    BinanceVisionLoader,
    ChecksumMismatchError,
    asset_id_for,
    is_supported_symbol,
    months_in,
    parse_checksum,
    parse_funding,
    parse_klines,
    parse_listing,
    verify_checksum,
)
from loaders.schemas import FUNDING_RATE_SCHEMA, OHLCV_SCHEMA
from loaders.window import FetchWindow

# ---------------------------------------------------------------------------
# Fixtures and the fake archive
# ---------------------------------------------------------------------------

KLINE_HEADER = (
    "open_time,open,high,low,close,volume,close_time,quote_volume,count,"
    "taker_buy_volume,taker_buy_quote_volume,ignore"
)

# 2021-01-01 and 2021-01-02, 00:00 UTC, in epoch milliseconds.
JAN_1_MS = 1_609_459_200_000
JAN_2_MS = 1_609_545_600_000
# 2025-01-01 00:00 UTC in microseconds — above MICROSECOND_THRESHOLD.
JAN_1_2025_US = 1_735_689_600_000_000


def zipped(name: str, text: str) -> bytes:
    """A zip holding one CSV, the way the archive publishes them."""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(name, text)
    return buffer.getvalue()


def kline_csv(rows: list[str], header: bool) -> str:
    body = "\n".join(rows) + "\n"
    return f"{KLINE_HEADER}\n{body}" if header else body


def kline_row(open_time: int, close: float = 105.0, volume: float = 10.0) -> str:
    return (
        f"{open_time},100.0,110.0,90.0,{close},{volume},"
        f"{open_time + 86_399_999},1000.0,5,5.0,500.0,0"
    )


def funding_csv(rows: list[tuple[int, float]], header: bool = True) -> str:
    body = "".join(f"{ts},8,{rate}\n" for ts, rate in rows)
    return ("calc_time,funding_interval_hours,last_funding_rate\n" + body) if header else body


class FakeArchive:
    """An in-memory `data.binance.vision` plus its S3 listing endpoint."""

    def __init__(self, max_keys: int = 1000):
        self.objects: dict[str, bytes] = {}
        self.max_keys = max_keys
        self.requested: list[str] = []
        self.corrupt_checksums: set[str] = set()

    # -- population ------------------------------------------------------

    def add_object(self, key: str, payload: bytes) -> None:
        self.objects[key] = payload
        digest = hashlib.sha256(payload).hexdigest()
        self.objects[f"{key}.CHECKSUM"] = f"{digest}  {key.rpartition('/')[2]}\n".encode()

    def add_kline_month(
        self,
        symbol: str,
        month: str,
        rows: list[str],
        header: bool = True,
        market: str = "um",
    ) -> str:
        root = "futures/um" if market == "um" else "spot"
        key = f"data/{root}/monthly/klines/{symbol}/1d/{symbol}-1d-{month}.zip"
        self.add_object(key, zipped(f"{symbol}-1d-{month}.csv", kline_csv(rows, header)))
        return key

    def add_funding_month(
        self, symbol: str, month: str, rows: list[tuple[int, float]]
    ) -> str:
        key = (
            f"data/futures/um/monthly/fundingRate/{symbol}/"
            f"{symbol}-fundingRate-{month}.zip"
        )
        self.add_object(key, zipped(f"{symbol}-fundingRate-{month}.csv", funding_csv(rows)))
        return key

    def break_checksum(self, key: str) -> None:
        """Publish a digest that does not match the payload."""
        self.corrupt_checksums.add(key)

    # -- serving ---------------------------------------------------------

    def get(self, url: str) -> bytes:
        self.requested.append(url)
        if url.startswith(LISTING_HOST):
            return self._listing(url)

        key = url[len(ARCHIVE_HOST) + 1 :]
        if key.endswith(".CHECKSUM") and key[: -len(".CHECKSUM")] in self.corrupt_checksums:
            return b"0" * 64 + b"  corrupted\n"
        if key not in self.objects:
            raise ArchiveNotFoundError(f"GET {url} -> 404")
        return self.objects[key]

    def get_text(self, url: str) -> str:
        return self.get(url).decode("utf-8")

    def _listing(self, url: str) -> bytes:
        query = parse_qs(urlparse(url).query, keep_blank_values=True)
        prefix = query.get("prefix", [""])[0]
        delimiter = query.get("delimiter", [""])[0]
        marker = query.get("marker", [None])[0]

        candidates = sorted(k for k in self.objects if k.startswith(prefix))
        if marker:
            candidates = [k for k in candidates if k > marker]

        contents: list[str] = []
        prefixes: list[str] = []
        last_seen = None
        truncated = False

        for key in candidates:
            if len(contents) + len(prefixes) >= self.max_keys:
                truncated = True
                break
            remainder = key[len(prefix) :]
            if delimiter and delimiter in remainder:
                common = prefix + remainder.split(delimiter, 1)[0] + delimiter
                if common not in prefixes:
                    prefixes.append(common)
            else:
                contents.append(key)
            last_seen = key

        body = "".join(f"<Contents><Key>{k}</Key></Contents>" for k in contents)
        body += "".join(f"<CommonPrefixes><Prefix>{p}</Prefix></CommonPrefixes>" for p in prefixes)
        # S3 returns NextMarker only when a delimiter is in play; without one the
        # client must fall back to the last key. Both paths are real, so the fake
        # reproduces both.
        next_marker = (
            f"<NextMarker>{last_seen}</NextMarker>" if truncated and delimiter else ""
        )
        xml = (
            '<?xml version="1.0" encoding="UTF-8"?>'
            '<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
            f"<Name>data.binance.vision</Name><Prefix>{prefix}</Prefix>"
            f"<IsTruncated>{'true' if truncated else 'false'}</IsTruncated>"
            f"{next_marker}{body}</ListBucketResult>"
        )
        return xml.encode("utf-8")


@pytest.fixture
def archive():
    return FakeArchive()


@pytest.fixture
def store(tmp_path):
    return ParquetStore(tmp_path / "store")


@pytest.fixture
def asset_master(tmp_path):
    return AssetMaster(tmp_path / "asset_master.parquet")


@pytest.fixture
def loader(archive, store, asset_master):
    return BinanceVisionLoader(
        market="um",
        symbols=["BTCUSDT"],
        store=store,
        asset_master=asset_master,
        http=archive,
        workers=2,
    )


JANUARY_2021 = FetchWindow(datetime(2021, 1, 1), datetime(2021, 1, 31, 23, 59))


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------


class TestKlineParsing:
    """Golden fixtures: a hand-written frame the parser must reproduce exactly."""

    def test_futures_klines_carry_a_header_row(self):
        payload = zipped(
            "BTCUSDT-1d-2021-01.csv",
            kline_csv(
                [
                    "1609459200000,29000.1,29500.0,28800.0,29350.5,1234.5,"
                    "1609545599999,36000000.0,50000,600.0,17000000.0,0"
                ],
                header=True,
            ),
        )

        expected = pl.DataFrame(
            {
                "event_ts": [datetime(2021, 1, 1)],
                "open": [29000.1],
                "high": [29500.0],
                "low": [28800.0],
                "close": [29350.5],
                "volume": [1234.5],
            },
            schema={
                "event_ts": pl.Datetime("us"),
                "open": pl.Float64,
                "high": pl.Float64,
                "low": pl.Float64,
                "close": pl.Float64,
                "volume": pl.Float64,
            },
        )
        assert_frame_equal(parse_klines(payload), expected)

    def test_spot_klines_do_not(self):
        """The same two bars, headerless — and the header is not eaten as data."""
        payload = zipped(
            "BTCUSDT-1d-2021-01.csv",
            kline_csv([kline_row(JAN_1_MS), kline_row(JAN_2_MS, close=112.0)], header=False),
        )

        frame = parse_klines(payload)

        assert len(frame) == 2
        assert frame["event_ts"].to_list() == [datetime(2021, 1, 1), datetime(2021, 1, 2)]
        assert frame["close"].to_list() == [105.0, 112.0]

    def test_volume_is_base_volume_not_quote_volume(self):
        """`universe/builder.py` computes dollar volume as close * volume."""
        payload = zipped("x.csv", kline_csv([kline_row(JAN_1_MS, volume=10.0)], header=False))

        assert parse_klines(payload)["volume"].to_list() == [10.0]  # not 1000.0

    def test_milliseconds_are_detected(self):
        payload = zipped("x.csv", kline_csv([kline_row(JAN_1_MS)], header=False))

        assert parse_klines(payload)["event_ts"][0] == datetime(2021, 1, 1)

    def test_microseconds_are_detected(self):
        """Some 2025+ files switched units; magnitude decides, not the month."""
        payload = zipped("x.csv", kline_csv([kline_row(JAN_1_2025_US)], header=False))

        assert parse_klines(payload)["event_ts"][0] == datetime(2025, 1, 1)

    def test_a_mixed_file_reads_each_row_in_its_own_unit(self):
        payload = zipped(
            "x.csv",
            kline_csv([kline_row(JAN_1_MS), kline_row(JAN_1_2025_US)], header=False),
        )

        assert parse_klines(payload)["event_ts"].to_list() == [
            datetime(2021, 1, 1),
            datetime(2025, 1, 1),
        ]

    def test_rows_come_back_in_time_order(self):
        payload = zipped(
            "x.csv", kline_csv([kline_row(JAN_2_MS), kline_row(JAN_1_MS)], header=False)
        )

        assert parse_klines(payload)["event_ts"].is_sorted()

    def test_an_empty_file_is_an_empty_frame(self):
        assert parse_klines(zipped("x.csv", "")).is_empty()

    def test_a_header_only_file_is_an_empty_frame(self):
        assert parse_klines(zipped("x.csv", KLINE_HEADER + "\n")).is_empty()

    def test_a_payload_that_is_not_a_zip_raises(self):
        with pytest.raises(ArchiveError):
            parse_klines(b"open_time,open\n1,2\n")

    def test_a_zip_with_two_members_raises(self):
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w") as zf:
            zf.writestr("a.csv", "1\n")
            zf.writestr("b.csv", "2\n")

        with pytest.raises(ArchiveError):
            parse_klines(buffer.getvalue())


class TestFundingParsing:
    def test_golden_funding_month(self):
        payload = zipped(
            "BTCUSDT-fundingRate-2021-01.csv",
            funding_csv([(JAN_1_MS, 0.0001), (JAN_1_MS + 28_800_000, -0.00025)]),
        )

        expected = pl.DataFrame(
            {
                "event_ts": [datetime(2021, 1, 1), datetime(2021, 1, 1, 8)],
                "funding_rate": [0.0001, -0.00025],
            },
            schema={"event_ts": pl.Datetime("us"), "funding_rate": pl.Float64},
        )
        assert_frame_equal(parse_funding(payload), expected)

    def test_a_headerless_funding_file_still_parses(self):
        payload = zipped("x.csv", funding_csv([(JAN_1_MS, 0.0001)], header=False))

        assert parse_funding(payload)["funding_rate"].to_list() == [0.0001]


# ---------------------------------------------------------------------------
# Checksums
# ---------------------------------------------------------------------------


class TestChecksums:
    def test_parse_checksum_takes_the_digest_only(self):
        assert parse_checksum("abc123  BTCUSDT-1d-2021-01.zip\n") == "abc123"

    def test_a_matching_digest_passes(self):
        payload = b"payload"
        digest = hashlib.sha256(payload).hexdigest()

        verify_checksum(payload, f"{digest}  f.zip", "f.zip")  # does not raise

    def test_a_mismatched_digest_raises(self):
        with pytest.raises(ChecksumMismatchError):
            verify_checksum(b"payload", f"{'0' * 64}  f.zip", "f.zip")

    def test_an_empty_checksum_file_raises(self):
        with pytest.raises(ArchiveError):
            parse_checksum("")

    def test_a_corrupt_file_is_not_ingested(self, loader, archive):
        """The failure has to happen before the rows reach the store."""
        key = archive.add_kline_month("BTCUSDT", "2021-01", [kline_row(JAN_1_MS)])
        archive.break_checksum(key)

        with pytest.raises(ChecksumMismatchError):
            loader.download("BTCUSDT", "2021-01", KLINES)

    def test_a_corrupt_month_is_skipped_rather_than_ingested(self, loader, archive, store):
        good = archive.add_kline_month("BTCUSDT", "2021-01", [kline_row(JAN_1_MS)])
        archive.add_kline_month("BTCUSDT", "2021-02", [kline_row(1_612_137_600_000)])
        archive.break_checksum(good)

        rows = loader.run_daily(window=FetchWindow(datetime(2021, 1, 1), datetime(2021, 2, 28)))

        assert rows == 1
        assert store.read("ohlcv_daily")["event_ts"].to_list() == [datetime(2021, 2, 1)]

    def test_verification_can_be_turned_off(self, archive, store, asset_master):
        key = archive.add_kline_month("BTCUSDT", "2021-01", [kline_row(JAN_1_MS)])
        archive.break_checksum(key)
        loader = BinanceVisionLoader(
            symbols=["BTCUSDT"],
            store=store,
            asset_master=asset_master,
            http=archive,
            verify_checksums=False,
        )

        assert len(loader.download("BTCUSDT", "2021-01", KLINES)) == 1


# ---------------------------------------------------------------------------
# The S3 listing
# ---------------------------------------------------------------------------


class TestListingParser:
    def test_common_prefixes_and_keys_are_separated(self):
        xml = (
            '<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
            "<IsTruncated>false</IsTruncated>"
            "<Contents><Key>data/a.zip</Key></Contents>"
            "<CommonPrefixes><Prefix>data/BTCUSDT/</Prefix></CommonPrefixes>"
            "</ListBucketResult>"
        )

        listing = parse_listing(xml)

        assert listing.keys == ("data/a.zip",)
        assert listing.prefixes == ("data/BTCUSDT/",)
        assert listing.is_truncated is False
        assert listing.next_marker is None

    def test_the_namespace_is_not_load_bearing(self):
        """Matching on the declared namespace is a silent 'empty bucket' failure."""
        xml = (
            "<ListBucketResult><IsTruncated>false</IsTruncated>"
            "<Contents><Key>k</Key></Contents></ListBucketResult>"
        )

        assert parse_listing(xml).keys == ("k",)

    def test_next_marker_is_used_when_present(self):
        xml = (
            "<ListBucketResult><IsTruncated>true</IsTruncated>"
            "<NextMarker>data/M/</NextMarker>"
            "<CommonPrefixes><Prefix>data/A/</Prefix></CommonPrefixes>"
            "</ListBucketResult>"
        )

        assert parse_listing(xml).next_marker == "data/M/"

    def test_without_next_marker_the_last_key_is_the_marker(self):
        """S3 omits NextMarker when no delimiter is used."""
        xml = (
            "<ListBucketResult><IsTruncated>true</IsTruncated>"
            "<Contents><Key>k1</Key></Contents>"
            "<Contents><Key>k2</Key></Contents></ListBucketResult>"
        )

        assert parse_listing(xml).next_marker == "k2"

    def test_an_untruncated_page_has_no_marker(self):
        xml = (
            "<ListBucketResult><IsTruncated>false</IsTruncated>"
            "<Contents><Key>k1</Key></Contents></ListBucketResult>"
        )

        assert parse_listing(xml).next_marker is None


class TestListingContinuation:
    def test_symbols_are_enumerated_from_common_prefixes(self, loader, archive):
        for symbol in ("BTCUSDT", "ETHUSDT", "SOLUSDT"):
            archive.add_kline_month(symbol, "2021-01", [kline_row(JAN_1_MS)])

        assert loader.list_symbols() == ["BTCUSDT", "ETHUSDT", "SOLUSDT"]

    def test_a_truncated_listing_is_walked_to_the_end(self, archive, store, asset_master):
        """Two symbols per page: stopping at page one loses the rest of the alphabet."""
        archive.max_keys = 2
        symbols = ["AAAUSDT", "BBBUSDT", "CCCUSDT", "DDDUSDT", "EEEUSDT"]
        for symbol in symbols:
            archive.add_kline_month(symbol, "2021-01", [kline_row(JAN_1_MS)])
        loader = BinanceVisionLoader(store=store, asset_master=asset_master, http=archive)

        assert loader.list_symbols() == symbols

    def test_months_are_listed_oldest_first_and_exclude_checksums(self, loader, archive):
        for month in ("2021-03", "2021-01", "2021-02"):
            archive.add_kline_month("BTCUSDT", month, [kline_row(JAN_1_MS)])

        assert loader.list_months("BTCUSDT") == ["2021-01", "2021-02", "2021-03"]

    def test_a_truncated_month_listing_is_walked_to_the_end(
        self, archive, store, asset_master
    ):
        archive.max_keys = 2  # one month is two objects: the zip and its checksum
        for month in ("2021-01", "2021-02", "2021-03"):
            archive.add_kline_month("BTCUSDT", month, [kline_row(JAN_1_MS)])
        loader = BinanceVisionLoader(store=store, asset_master=asset_master, http=archive)

        assert loader.list_months("BTCUSDT") == ["2021-01", "2021-02", "2021-03"]

    def test_an_unlisted_symbol_has_no_months(self, loader):
        assert loader.list_months("NOPEUSDT") == []


class TestUrlLayout:
    def test_futures_kline_key(self, loader):
        assert loader.key_for("BTCUSDT", "2021-01", KLINES) == (
            "data/futures/um/monthly/klines/BTCUSDT/1d/BTCUSDT-1d-2021-01.zip"
        )

    def test_funding_key(self, loader):
        assert loader.key_for("BTCUSDT", "2021-01", FUNDING_RATE) == (
            "data/futures/um/monthly/fundingRate/BTCUSDT/BTCUSDT-fundingRate-2021-01.zip"
        )

    def test_spot_kline_key(self, store, asset_master, archive):
        spot = BinanceVisionLoader(
            market="spot", store=store, asset_master=asset_master, http=archive
        )

        assert spot.key_for("BTCUSDT", "2021-01", KLINES) == (
            "data/spot/monthly/klines/BTCUSDT/1d/BTCUSDT-1d-2021-01.zip"
        )

    def test_spot_has_no_funding_rate(self, store, asset_master, archive):
        spot = BinanceVisionLoader(
            market="spot", store=store, asset_master=asset_master, http=archive
        )

        with pytest.raises(ValueError):
            spot.prefix_for(FUNDING_RATE)

    def test_an_unknown_market_is_rejected(self, store, asset_master, archive):
        with pytest.raises(ValueError):
            BinanceVisionLoader(
                market="cm", store=store, asset_master=asset_master, http=archive
            )

    def test_the_object_url_is_the_archive_host(self, loader):
        assert loader.url_for("data/x.zip") == f"{ARCHIVE_HOST}/data/x.zip"


# ---------------------------------------------------------------------------
# Symbols
# ---------------------------------------------------------------------------


class TestSymbolMapping:
    @pytest.mark.parametrize(
        ("symbol", "expected"),
        [
            ("BTCUSDT", "BTC"),
            ("ETHUSDT", "ETH"),
            # Multiplier contracts are the same underlying at a scaled contract
            # size, and returns are invariant to a constant multiplier.
            ("1000BONKUSDT", "BONK"),
            ("1000SHIBUSDT", "SHIB"),
            ("1MBABYDOGEUSDT", "BABYDOGE"),
            ("1000000MOGUSDT", "MOG"),
            # ...but a token whose name merely starts with a digit is not one.
            ("1INCHUSDT", "1INCH"),
        ],
    )
    def test_multipliers_map_to_the_underlying(self, symbol, expected):
        assert asset_id_for(symbol) == expected

    @pytest.mark.parametrize(
        "symbol",
        [
            "BTCUSDT_240329",  # dated future: different instrument
            "BTCUSDC",  # same asset, second quote currency
            "BTCBUSD",
            "USDT",  # the quote asset itself
            "ETHBTC",
        ],
    )
    def test_unsupported_symbols_are_excluded(self, symbol):
        assert asset_id_for(symbol) is None
        assert is_supported_symbol(symbol) is False

    def test_a_dated_future_is_skipped_by_enumeration(self, loader, archive):
        archive.add_kline_month("BTCUSDT", "2021-01", [kline_row(JAN_1_MS)])
        archive.add_kline_month("BTCUSDT_240329", "2021-01", [kline_row(JAN_1_MS)])

        assert loader.list_symbols() == ["BTCUSDT"]


class TestMonthEnumeration:
    def test_both_ends_are_included(self):
        assert months_in(FetchWindow(datetime(2024, 11, 15), datetime(2025, 2, 3))) == [
            "2024-11",
            "2024-12",
            "2025-01",
            "2025-02",
        ]

    def test_a_window_inside_one_month_is_one_month(self):
        assert months_in(FetchWindow(datetime(2024, 3, 2), datetime(2024, 3, 29))) == ["2024-03"]


# ---------------------------------------------------------------------------
# Asset master (step 2: registration without ccxt)
# ---------------------------------------------------------------------------


class TestAssetMasterRegistration:
    def test_the_exact_archive_string_is_registered(self, loader, asset_master):
        loader.register_symbols({"BTCUSDT": "2020-01"})

        assert asset_master.resolve_symbol("BTCUSDT", "binance") == "BTC"

    def test_validity_starts_at_the_first_published_month(self, loader, asset_master):
        """The archive's first month is the closest thing to a listing date."""
        loader.register_symbols({"BONKUSDT": "2023-11"})

        assert asset_master.resolve_symbol(
            "BONKUSDT", "binance", asof=datetime(2023, 12, 1)
        ) == "BONK"
        assert (
            asset_master.resolve_symbol("BONKUSDT", "binance", asof=datetime(2023, 10, 1))
            is None
        )

    def test_a_multiplier_symbol_resolves_to_the_underlying(self, loader, asset_master):
        loader.register_symbols({"1000BONKUSDT": "2023-11"})

        assert asset_master.resolve_symbol("1000BONKUSDT", "binance") == "BONK"

    def test_registering_twice_adds_one_mapping(self, loader, asset_master):
        """The master is append-only, so a re-run must not grow it without bound."""
        assert loader.register_symbols({"BTCUSDT": "2020-01"}) == 1
        assert loader.register_symbols({"BTCUSDT": "2020-01"}) == 0
        assert len(asset_master._cache) == 1

    def test_unsupported_symbols_are_not_registered(self, loader, asset_master):
        assert loader.register_symbols({"BTCUSDT_240329": "2023-11"}) == 0
        assert asset_master.list_assets() == []

    def test_a_run_registers_what_it_fetches(self, loader, archive, asset_master):
        archive.add_kline_month("BTCUSDT", "2021-01", [kline_row(JAN_1_MS)])

        loader.run_daily(window=JANUARY_2021)

        assert asset_master.resolve_symbol("BTCUSDT", "binance") == "BTC"

    def test_the_listing_date_predates_the_fetch_window(self, loader, archive, asset_master):
        """A 2021 listing fetched from 2021-03 is still a 2021-01 listing.

        Taking the validity start from the *window* instead would make every
        backfilled asset look newly listed, and `min_listing_age_days` would
        drop the entire universe on the first snapshot.
        """
        for month in ("2021-01", "2021-02", "2021-03"):
            archive.add_kline_month("BTCUSDT", month, [kline_row(JAN_1_MS)])

        loader.run_daily(window=FetchWindow(datetime(2021, 3, 1), datetime(2021, 3, 31)))

        assert (
            asset_master.resolve_symbol("BTCUSDT", "binance", asof=datetime(2021, 1, 15))
            == "BTC"
        )


# ---------------------------------------------------------------------------
# End to end
# ---------------------------------------------------------------------------


class TestRunDaily:
    def test_rows_reach_the_store_under_the_ohlcv_schema(self, loader, archive, store):
        archive.add_kline_month(
            "BTCUSDT", "2021-01", [kline_row(JAN_1_MS), kline_row(JAN_2_MS, close=112.0)]
        )

        rows = loader.run_daily(window=JANUARY_2021)

        stored = store.read("ohlcv_daily")
        assert rows == 2
        assert len(stored) == 2
        assert list(stored.columns) == list(OHLCV_SCHEMA.fields)
        assert stored["asset_id"].unique().to_list() == ["BTC"]
        assert stored["venue"].unique().to_list() == ["binance"]
        assert stored["timeframe"].unique().to_list() == ["1d"]

    def test_every_row_carries_both_timestamps(self, loader, archive, store):
        archive.add_kline_month("BTCUSDT", "2021-01", [kline_row(JAN_1_MS)])

        loader.run_daily(window=JANUARY_2021)

        stored = store.read("ohlcv_daily")
        assert stored["event_ts"][0] == datetime(2021, 1, 1)
        # Ingested now, not when the bar happened: the archive is history we are
        # learning today, and pretending otherwise is look-ahead bias.
        assert stored["ingested_ts"][0] > datetime(2021, 1, 1)

    def test_months_outside_the_window_are_not_fetched(self, loader, archive, store):
        archive.add_kline_month("BTCUSDT", "2021-01", [kline_row(JAN_1_MS)])
        archive.add_kline_month("BTCUSDT", "2021-06", [kline_row(1_622_505_600_000)])

        loader.run_daily(window=JANUARY_2021)

        assert store.read("ohlcv_daily")["event_ts"].to_list() == [datetime(2021, 1, 1)]

    def test_rows_outside_the_window_are_trimmed_inside_a_month(
        self, loader, archive, store
    ):
        """Files are whole months; a window that starts mid-month is not."""
        archive.add_kline_month(
            "BTCUSDT", "2021-01", [kline_row(JAN_1_MS), kline_row(JAN_2_MS)]
        )

        loader.run_daily(window=FetchWindow(datetime(2021, 1, 2), datetime(2021, 1, 31)))

        assert store.read("ohlcv_daily")["event_ts"].to_list() == [datetime(2021, 1, 2)]

    def test_a_first_archive_run_stores_no_duplicate_bars(self, loader, archive, store):
        """The archive has no overlap; a duplicate means the month loop double-counted."""
        archive.add_kline_month("BTCUSDT", "2021-01", [kline_row(JAN_1_MS)])
        archive.add_kline_month("BTCUSDT", "2021-02", [kline_row(1_612_137_600_000)])

        loader.run_daily(window=FetchWindow(datetime(2021, 1, 1), datetime(2021, 2, 28)))

        assert count_duplicate_bars(store.read("ohlcv_daily")) == 0

    def test_several_symbols_land_under_their_own_asset_ids(
        self, archive, store, asset_master
    ):
        for symbol in ("BTCUSDT", "ETHUSDT", "1000BONKUSDT"):
            archive.add_kline_month(symbol, "2021-01", [kline_row(JAN_1_MS)])
        loader = BinanceVisionLoader(
            symbols=["BTCUSDT", "ETHUSDT", "1000BONKUSDT"],
            store=store,
            asset_master=asset_master,
            http=archive,
            workers=3,
        )

        loader.run_daily(window=JANUARY_2021)

        assert sorted(store.read("ohlcv_daily")["asset_id"].unique().to_list()) == [
            "BONK",
            "BTC",
            "ETH",
        ]

    def test_symbols_are_enumerated_when_none_are_given(self, archive, store, asset_master):
        archive.add_kline_month("BTCUSDT", "2021-01", [kline_row(JAN_1_MS)])
        loader = BinanceVisionLoader(store=store, asset_master=asset_master, http=archive)

        assert loader.run_daily(window=JANUARY_2021) == 1

    def test_a_symbol_with_nothing_published_is_skipped(self, loader, archive, store):
        rows = loader.run_daily(window=JANUARY_2021)

        assert rows == 0
        assert archive.requested  # it did look

    def test_chunking_per_symbol_writes_one_file_per_symbol(self, loader, archive, store):
        """~200 files per dataset instead of ~12,000, all in one partition."""
        for month in ("2021-01", "2021-02", "2021-03"):
            archive.add_kline_month("BTCUSDT", month, [kline_row(JAN_1_MS)])

        loader.run_daily(window=FetchWindow(datetime(2021, 1, 1), datetime(2021, 3, 31)))

        files = list((store.root / "ohlcv_daily").glob("date=*/*.parquet"))
        assert len(files) == 1

    def test_chunking_per_month_writes_one_file_per_month(
        self, archive, store, asset_master
    ):
        for month in ("2021-01", "2021-02", "2021-03"):
            archive.add_kline_month("BTCUSDT", month, [kline_row(JAN_1_MS)])
        loader = BinanceVisionLoader(
            symbols=["BTCUSDT"],
            store=store,
            asset_master=asset_master,
            http=archive,
            chunk="month",
            workers=1,
        )

        loader.run_daily(window=FetchWindow(datetime(2021, 1, 1), datetime(2021, 3, 31)))

        files = list((store.root / "ohlcv_daily").glob("date=*/*.parquet"))
        assert len(files) == 3

    def test_an_unknown_chunk_mode_is_rejected(self, store, asset_master, archive):
        with pytest.raises(ValueError):
            BinanceVisionLoader(
                store=store, asset_master=asset_master, http=archive, chunk="year"
            )

    def test_fetch_returns_the_frame_without_writing(self, loader, archive, store):
        archive.add_kline_month("BTCUSDT", "2021-01", [kline_row(JAN_1_MS)])

        frame = loader.fetch(KLINES, window=JANUARY_2021)

        assert len(frame) == 1
        assert not (store.root / "ohlcv_daily").exists()


class TestRunFunding:
    def test_funding_rows_pass_the_schema_with_null_mark_and_index(
        self, loader, archive, store
    ):
        """The archive publishes the rate alone; a fabricated 0.0 would be a price."""
        archive.add_funding_month(
            "BTCUSDT", "2021-01", [(JAN_1_MS, 0.0001), (JAN_1_MS + 28_800_000, -0.00025)]
        )

        rows = loader.run_funding(window=JANUARY_2021)

        stored = store.read("funding_rate")
        assert rows == 2
        assert list(stored.columns) == list(FUNDING_RATE_SCHEMA.fields)
        assert stored["funding_rate"].to_list() == [0.0001, -0.00025]
        assert stored["mark_price"].null_count() == 2
        assert stored["index_price"].null_count() == 2
        assert stored["mark_price"].dtype == pl.Float64

    def test_the_nulls_are_the_ones_the_audit_already_tolerates(self):
        from config import AUDIT_CONFIG

        assert AUDIT_CONFIG.nullable_columns_for("funding_rate") == {
            "mark_price",
            "index_price",
        }

    def test_funding_and_klines_share_one_asset_id(self, loader, archive, store):
        archive.add_kline_month("BTCUSDT", "2021-01", [kline_row(JAN_1_MS)])
        archive.add_funding_month("BTCUSDT", "2021-01", [(JAN_1_MS, 0.0001)])

        loader.run_daily(window=JANUARY_2021)
        loader.run_funding(window=JANUARY_2021)

        assert store.read("ohlcv_daily")["asset_id"].to_list() == ["BTC"]
        assert store.read("funding_rate")["asset_id"].to_list() == ["BTC"]


# ---------------------------------------------------------------------------
# HTTP behaviour
# ---------------------------------------------------------------------------


class _Response:
    def __init__(self, status_code, content=b""):
        self.status_code = status_code
        self.content = content


class _Session:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = 0

    def get(self, url, timeout=None):
        self.calls += 1
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


class TestArchiveHttp:
    def test_a_200_is_returned(self):
        http = ArchiveHttp(session=_Session([_Response(200, b"payload")]))

        assert http.get("https://example/x") == b"payload"

    def test_a_500_is_retried_once_and_then_succeeds(self):
        session = _Session([_Response(503), _Response(200, b"payload")])
        http = ArchiveHttp(session=session, backoff=0)

        assert http.get("https://example/x") == b"payload"
        assert session.calls == 2

    def test_a_persistent_error_raises(self):
        http = ArchiveHttp(session=_Session([_Response(503), _Response(503)]), backoff=0)

        with pytest.raises(ArchiveError):
            http.get("https://example/x")

    def test_a_404_is_not_retried(self):
        """A missing month is a fact about the archive, not a transient fault."""
        session = _Session([_Response(404), _Response(200, b"payload")])
        http = ArchiveHttp(session=session, backoff=0)

        with pytest.raises(ArchiveNotFoundError):
            http.get("https://example/x")
        assert session.calls == 1

    def test_a_network_failure_is_retried(self):
        session = _Session([OSError("connection reset"), _Response(200, b"payload")])
        http = ArchiveHttp(session=session, backoff=0)

        assert http.get("https://example/x") == b"payload"
        assert session.calls == 2


# ---------------------------------------------------------------------------
# Isolation
# ---------------------------------------------------------------------------


class TestIsolation:
    def test_the_module_is_redirected_away_from_the_real_datastore(
        self, isolate_production_datastore
    ):
        """`tests/test_isolation.py` enforces the list; this names the module."""
        import loaders.archive as archive_module

        assert archive_module.DATASTORE_PATH == isolate_production_datastore

    def test_a_loader_built_with_no_paths_stays_in_the_sandbox(
        self, isolate_production_datastore
    ):
        loader = BinanceVisionLoader(http=FakeArchive())

        assert isolate_production_datastore in loader.asset_master.path.parents

    def test_the_module_does_not_import_ccxt(self):
        """Being unable to reach the API is the reason this route exists."""
        import ast
        import pathlib

        import config

        source = (pathlib.Path(config.PROJECT_ROOT) / "loaders" / "archive.py").read_text(
            encoding="utf-8"
        )
        imported = {
            (node.module or "").split(".")[0]
            if isinstance(node, ast.ImportFrom)
            else alias.name.split(".")[0]
            for node in ast.walk(ast.parse(source))
            if isinstance(node, ast.Import | ast.ImportFrom)
            for alias in getattr(node, "names", [None])
        }

        assert "ccxt" not in imported
