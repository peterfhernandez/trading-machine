"""Tests for loader symbol selection (`select_usdt_symbols`).

The regression these guard: slicing a venue's symbol list before filtering it
to USDT pairs. A venue orders symbols alphabetically across every quote
currency, so `symbols[:50]` followed by a USDT filter yields a fraction of the
intended budget -- which is how the funding-rate and open-interest loaders ended
up covering ~15 assets and failing the audit's coverage check.
"""

from config import LOADER_CONFIG
from loaders.base import select_usdt_symbols


def _interleaved_symbols(n_per_quote: int = 60) -> list[str]:
    """A venue-style symbol list: many quote currencies, alphabetical order."""
    symbols = []
    for i in range(n_per_quote):
        base = f"A{i:03d}"
        symbols.extend([
            f"{base}/BNB",
            f"{base}/BTC",
            f"{base}/ETH",
            f"{base}/USDC",
            f"{base}/USDT",
        ])
    return sorted(symbols)


class TestSelectUsdtSymbols:
    """Tests for select_usdt_symbols."""

    def test_filters_before_capping(self):
        """The cap applies to USDT symbols, not to the raw venue list."""
        symbols = _interleaved_symbols(n_per_quote=60)

        selected = select_usdt_symbols(symbols, max_symbols=50)

        assert len(selected) == 50
        assert all(s.endswith("USDT") for s in selected)

    def test_old_slice_then_filter_would_starve(self):
        """Documents the bug: slicing first yields a fraction of the budget."""
        symbols = _interleaved_symbols(n_per_quote=60)

        naive = [s for s in symbols[:50] if s.endswith("USDT")]
        fixed = select_usdt_symbols(symbols, max_symbols=50)

        assert len(naive) < 15
        assert len(fixed) == 50

    def test_prefers_perpetuals(self):
        """With prefer_perps, only perp symbols ("BTC/USDT:USDT") are returned."""
        symbols = [
            "BTC/USDT",              # spot
            "BTC/USDT:USDT",         # perpetual
            "ETH/USDT",
            "ETH/USDT:USDT",
            "SOL/USDT:USDT",
        ]

        selected = select_usdt_symbols(symbols, max_symbols=10, prefer_perps=True)

        assert selected == ["BTC/USDT:USDT", "ETH/USDT:USDT", "SOL/USDT:USDT"]

    def test_excludes_dated_futures(self):
        """Quarterly futures ("BTC/USDT:USDT-260327") are not perpetuals."""
        symbols = ["BTC/USDT:USDT", "BTC/USDT:USDT-260327", "ETH/USDT:USDT-260327"]

        selected = select_usdt_symbols(symbols, max_symbols=10, prefer_perps=True)

        assert selected == ["BTC/USDT:USDT"]

    def test_perp_preference_falls_back_to_spot(self):
        """A venue exposing no perps still yields its USDT pairs."""
        symbols = ["BTC/USDT", "ETH/USDT", "ETH/BTC"]

        selected = select_usdt_symbols(symbols, max_symbols=10, prefer_perps=True)

        assert selected == ["BTC/USDT", "ETH/USDT"]

    def test_excludes_non_usdt_quotes(self):
        symbols = ["BTC/USDC", "BTC/BUSD", "ETH/BTC", "BTC/USDT"]

        assert select_usdt_symbols(symbols, max_symbols=10) == ["BTC/USDT"]

    def test_deduplicates_and_sorts(self):
        symbols = ["ETH/USDT", "BTC/USDT", "ETH/USDT"]

        assert select_usdt_symbols(symbols, max_symbols=10) == ["BTC/USDT", "ETH/USDT"]

    def test_empty_and_none(self):
        assert select_usdt_symbols([], max_symbols=10) == []
        assert select_usdt_symbols(None, max_symbols=10) == []

    def test_default_cap_comes_from_config(self):
        symbols = [f"A{i:04d}/USDT" for i in range(LOADER_CONFIG.max_symbols_per_run + 25)]

        selected = select_usdt_symbols(symbols)

        assert len(selected) == LOADER_CONFIG.max_symbols_per_run

    def test_cap_above_available_returns_everything(self):
        symbols = ["BTC/USDT", "ETH/USDT"]

        assert len(select_usdt_symbols(symbols, max_symbols=500)) == 2
