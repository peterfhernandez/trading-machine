"""One canonicalisation of `asset_id`, and the guard that enforces it.

`DATA.md` §9.1: two rules for naming an asset lived in one codebase —
`loaders/archive.py` stripped multiplier prefixes, `pipeline/nightly.py` kept
them — and the "already mapped?" guard could not notice, because it matches the
literal symbol string and `1000CATUSDT` is not `1000CAT/USDT:USDT`. The store
ended up holding `1000CAT` *and* `CAT`, and `latest_per_bar` chose between
prices 711,000x apart by ingestion time.

Two things are pinned here. That the rule is *one* rule, over both notations
and over real symbol strings — a fixture is the only thing that can say two
modules agree, since neither calls the other. And that a mapping which would
merge two listings is refused rather than resolved.
"""

from datetime import datetime

import pytest

from datastore import (
    AssetIdCollisionError,
    AssetMaster,
    canonical_asset_id,
    canonical_asset_id_for_market,
    is_supported_symbol,
    parse_venue_symbol,
    symbol_key,
)

JAN = datetime(2024, 1, 1)
JUL = datetime(2024, 7, 1)
NEXT_YEAR = datetime(2025, 1, 1)


class TestTheRule:
    @pytest.mark.parametrize(
        ("symbol", "expected"),
        [
            ("BTCUSDT", "BTC"),
            ("ETHUSDT", "ETH"),
            # The venue's own base, prefix and all. Stripping it assumed a
            # prefix always means one underlying at a scaled contract size.
            ("1000SHIBUSDT", "1000SHIB"),
            ("1000BONKUSDT", "1000BONK"),
            ("1MBABYDOGEUSDT", "1MBABYDOGE"),
            ("1000000MOGUSDT", "1000000MOG"),
            ("1INCHUSDT", "1INCH"),
            # ccxt notation, both forms.
            ("BTC/USDT", "BTC"),
            ("BTC/USDT:USDT", "BTC"),
            ("1000CAT/USDT:USDT", "1000CAT"),
        ],
    )
    def test_the_asset_id_is_the_base(self, symbol, expected):
        assert canonical_asset_id(symbol) == expected

    @pytest.mark.parametrize(
        "symbol",
        [
            "BTCUSDT_240329",  # dated future: different instrument
            "BTC/USDT:USDT-260327",  # the same, in ccxt notation
            "BTCUSDC",  # same asset, second quote currency
            "BTCBUSD",
            "USDT",  # the quote asset itself
            "ETHBTC",
            "ETH/BTC",
            "",
        ],
    )
    def test_unsupported_symbols_resolve_to_nothing(self, symbol):
        assert canonical_asset_id(symbol) is None
        assert is_supported_symbol(symbol) is False

    def test_the_venues_base_wins_over_the_parse(self):
        """ccxt knows its own market; the parse is the fallback for the archive
        notation, which carries no separator to split on."""
        assert (
            canonical_asset_id_for_market("1000CAT/USDT:USDT", {"base": "1000CAT"})
            == "1000CAT"
        )

    def test_a_supplied_base_does_not_rescue_an_unsupported_symbol(self):
        """A dated future is a different instrument whatever its base says."""
        assert (
            canonical_asset_id_for_market("BTC/USDT:USDT-260327", {"base": "BTC"})
            is None
        )

    def test_the_two_loaders_agree_over_real_symbols(self):
        """The assertion the codebase was missing. `loaders/archive.py` and
        `pipeline/nightly.py` never call each other, so nothing but a fixture
        can say they answer this question the same way."""
        for archive_symbol, base, expected in [
            ("BTCUSDT", "BTC", "BTC"),
            ("1000SHIBUSDT", "1000SHIB", "1000SHIB"),
            ("1000CATUSDT", "1000CAT", "1000CAT"),
            ("CATUSDT", "CAT", "CAT"),
            ("1000000BOBUSDT", "1000000BOB", "1000000BOB"),
            ("BOBUSDT", "BOB", "BOB"),
            ("1INCHUSDT", "1INCH", "1INCH"),
        ]:
            ccxt_symbol = f"{base}/USDT:USDT"
            assert canonical_asset_id(archive_symbol) == expected
            assert canonical_asset_id_for_market(ccxt_symbol, {"base": base}) == expected

    @pytest.mark.parametrize(
        ("prefixed", "bare"),
        [("1000CATUSDT", "CATUSDT"), ("1000000BOBUSDT", "BOBUSDT")],
    )
    def test_the_two_pairs_that_made_the_rule_stay_apart(self, prefixed, bare):
        assert canonical_asset_id(prefixed) != canonical_asset_id(bare)


class TestSymbolKey:
    """What makes two symbols spellings of one listing rather than two."""

    def test_every_notation_of_one_listing_shares_a_key(self):
        keys = {
            symbol_key("BTCUSDT"),
            symbol_key("BTC/USDT"),
            symbol_key("BTC/USDT:USDT"),
        }

        assert keys == {("BTC", "USDT")}

    def test_a_prefixed_ticker_has_its_own_key(self):
        assert symbol_key("1000CATUSDT") != symbol_key("CATUSDT")

    def test_an_unsupported_symbol_has_no_key(self):
        assert symbol_key("BTC") is None  # Deribit's bare notation

    def test_the_identity_carries_the_parts_that_decided_it(self):
        identity = parse_venue_symbol("1000CAT/USDT:USDT")

        assert identity is not None
        assert (identity.base, identity.quote, identity.asset_id) == (
            "1000CAT",
            "USDT",
            "1000CAT",
        )


class TestTheCollisionGuard:
    @pytest.fixture
    def master(self, tmp_path) -> AssetMaster:
        return AssetMaster(tmp_path / "asset_master.parquet")

    def test_several_notations_of_one_listing_are_not_a_collision(self, master):
        """The asset master exists to hold exactly this: archive, spot and perp
        spellings of one asset, all resolving to one `asset_id`."""
        master.add_mapping("BTC", "binance", "BTCUSDT", JAN)
        master.add_mapping("BTC", "binance", "BTC/USDT", JAN)
        master.add_mapping("BTC", "binance", "BTC/USDT:USDT", JAN)

        assert master.resolve_symbol("BTCUSDT", "binance") == "BTC"
        assert master.find_collisions() == []

    def test_two_simultaneous_listings_are_refused(self, master):
        """`1000CATUSDT` and `CATUSDT` were published at the same time, so they
        cannot be one asset under two names. Refused rather than resolved:
        resolving means picking a price series by ingestion time."""
        master.add_mapping("CAT", "binance", "1000CATUSDT", JAN)

        with pytest.raises(AssetIdCollisionError) as caught:
            master.add_mapping("CAT", "binance", "CATUSDT", JUL)

        assert caught.value.collision.relation == "overlap"
        assert "1000CATUSDT" in str(caught.value)
        assert "CATUSDT" in str(caught.value)

    def test_the_refused_mapping_is_not_recorded(self, master):
        master.add_mapping("CAT", "binance", "1000CATUSDT", JAN)

        with pytest.raises(AssetIdCollisionError):
            master.add_mapping("CAT", "binance", "CATUSDT", JUL)

        assert master.resolve_symbol("CATUSDT", "binance", asof=NEXT_YEAR) is None

    def test_a_rename_is_recorded_rather_than_refused(self, master):
        """Sequential listings are what point-in-time validity ranges are for,
        and Phase 1 shipped that deliberately. Blocking them would break every
        legitimate ticker change to catch a case that is not this one."""
        master.add_mapping("TOKEN", "binance", "TOKENUSDT", JAN, validity_end=JUL)
        master.add_mapping("TOKEN", "binance", "TOKEN2USDT", JUL)

        assert master.resolve_symbol("TOKENUSDT", "binance", asof=JAN) == "TOKEN"
        assert master.resolve_symbol("TOKEN2USDT", "binance", asof=NEXT_YEAR) == "TOKEN"

    def test_a_rename_is_still_reported_as_the_phase_10a_trigger(self, master):
        """Recorded is not the same as unremarkable: abutting windows are also
        the shape of a redenomination and of a reused equity ticker, both of
        which need price adjustment (`DATA.md` §10)."""
        master.add_mapping("TOKEN", "binance", "TOKENUSDT", JAN, validity_end=JUL)
        master.add_mapping("TOKEN", "binance", "TOKEN2USDT", JUL)

        collisions = master.find_collisions()

        assert len(collisions) == 1
        assert collisions[0].is_redenomination_candidate
        assert "Phase 10a" in collisions[0].describe()

    def test_an_unparseable_symbol_is_allowed_through(self, master):
        """Deribit quotes `BTC`, which has no quote currency to split off. A
        guard that fires on everything it does not recognise is a guard someone
        turns off."""
        master.add_mapping("BTC", "deribit", "BTC", JAN)
        master.add_mapping("BTC", "deribit", "BTC-PERPETUAL", JAN)

        assert master.resolve_symbol("BTC", "deribit") == "BTC"

    def test_re_adding_the_same_symbol_is_never_a_collision(self, master):
        """An append-only master accumulates identical mappings by design."""
        master.add_mapping("BTC", "binance", "BTCUSDT", JAN)
        master.add_mapping("BTC", "binance", "BTCUSDT", JUL)

        assert master.find_collisions() == []

    def test_different_venues_do_not_collide(self, master):
        master.add_mapping("BTC", "binance", "BTCUSDT", JAN)
        master.add_mapping("BTC", "okx", "1000BTCUSDT", JAN)

        assert master.find_collisions() == []

    def test_allow_collision_records_it_anyway(self, master):
        """For a caller that has looked at the collision and decided -- and for
        the tests and fixtures that need to reproduce a pre-5.9 master."""
        master.add_mapping("CAT", "binance", "1000CATUSDT", JAN)
        master.add_mapping("CAT", "binance", "CATUSDT", JUL, allow_collision=True)

        assert len(master.find_collisions()) == 1
        assert master.find_collisions()[0].relation == "overlap"

    def test_check_collision_answers_without_writing(self, master):
        master.add_mapping("CAT", "binance", "1000CATUSDT", JAN)

        collision = master.check_collision("CAT", "binance", "CATUSDT", JUL)

        assert collision is not None and collision.relation == "overlap"
        assert len(master._cache) == 1
