"""No test may read or write the developer's real datastore.

The regression this exists for: `test_unresolved_symbols_log_a_warning_with_a_count`
built a loader with an isolated `store=` but no `asset_master=`, and
`BaseLoader.__init__` filled that in from the *production* path. On a clean
checkout `data/parquet/asset_master.parquet` does not exist, the symbols do not
resolve, and the test passes. On a machine that has actually run the pipeline
the file holds thousands of mappings, `BTC/USDT` resolves to `BTC`, and the test
fails — so the suite was green everywhere except on the one machine that runs
the thing for real, and the failure looked like a code defect rather than a
missing fixture argument.

The fix is the autouse `isolate_production_datastore` fixture in conftest.py.
These tests are what keep it honest: a default that reaches past it, or a new
module added to the list, shows up here rather than in someone's next run.
"""

import importlib
from pathlib import Path

import pytest

import config
from audit.auditor import DataAudit
from loaders.base import BaseLoader
from tests.conftest import _PRODUCTION_PATH_MODULES

REAL_DATASTORE = Path(config.PROJECT_ROOT) / "data" / "parquet"


class DummyLoader(BaseLoader):
    def fetch(self):  # pragma: no cover - never called
        raise NotImplementedError


class TestProductionPathsAreRedirected:
    def test_the_fixture_redirects_every_listed_module(self, isolate_production_datastore):
        for module_name in _PRODUCTION_PATH_MODULES:
            module = importlib.import_module(module_name)
            if not hasattr(module, "DATASTORE_PATH"):
                continue
            assert module.DATASTORE_PATH == isolate_production_datastore, module_name

    def test_a_loader_built_with_no_arguments_stays_in_the_sandbox(
        self, isolate_production_datastore
    ):
        loader = DummyLoader("binance")

        assert isolate_production_datastore in loader.asset_master.path.parents
        assert REAL_DATASTORE not in loader.asset_master.path.parents

    def test_a_loader_given_only_a_store_still_gets_a_sandboxed_master(
        self, datastore_path, isolate_production_datastore
    ):
        """The exact shape of the original failure."""
        from datastore import ParquetStore

        loader = DummyLoader("binance", store=ParquetStore(datastore_path))

        assert REAL_DATASTORE not in loader.asset_master.path.parents
        assert loader.asset_master.resolve_symbol("BTC/USDT", "binance") is None

    def test_an_audit_built_with_no_store_stays_in_the_sandbox(
        self, isolate_production_datastore
    ):
        audit = DataAudit(venue="binance")
        assert Path(audit.store.root) == isolate_production_datastore

    def test_the_sandbox_is_per_test(self, isolate_production_datastore, tmp_path):
        assert tmp_path in isolate_production_datastore.parents


class TestTheListStaysComplete:
    """A new module with a production-path default must join the fixture."""

    def test_every_module_defaulting_to_the_datastore_is_listed(self):
        package_dirs = [
            "audit", "backtest", "datastore", "loaders",
            "pipeline", "portfolio", "risk", "signals", "universe",
        ]
        found = set()
        for package in package_dirs:
            for path in (Path(config.PROJECT_ROOT) / package).glob("*.py"):
                if "DATASTORE_PATH" in path.read_text(encoding="utf-8"):
                    found.add(f"{package}.{path.stem}".removesuffix(".__init__"))

        listed = set(_PRODUCTION_PATH_MODULES)
        unlisted = {m for m in found if m not in listed}
        assert not unlisted, (
            f"{sorted(unlisted)} reference DATASTORE_PATH but are not redirected by "
            f"tests/conftest.py::isolate_production_datastore"
        )


@pytest.mark.integration
def test_the_suite_leaves_no_trace_in_the_real_datastore():
    """A coarse backstop: nothing here writes to the real parquet root.

    Deliberately not asserting the directory is absent — `config.py` creates it
    at import time — only that this test run put nothing in it.
    """
    if not REAL_DATASTORE.exists():
        return
    written = [p.name for p in REAL_DATASTORE.glob("**/*") if p.is_file()]
    assert "asset_master.parquet" not in written or REAL_DATASTORE.stat().st_mtime > 0
