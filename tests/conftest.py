"""
Pytest configuration and fixtures for trading-machine tests.
"""

import sys
from pathlib import Path

import pytest

# Add project root to path so imports work
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


# Every module that falls back to the *production* datastore when a caller
# does not pass one. Each does `from config import DATASTORE_PATH`, binding the
# value at import time, so redirecting `config.DATASTORE_PATH` alone would not
# reach them — the name has to be replaced in each module's own namespace.
_PRODUCTION_PATH_MODULES = (
    "config",
    "audit.auditor",
    "backtest.engine",
    "loaders.backfill",
    "loaders.base",
    "loaders.funding_rate",
    "loaders.ohlcv",
    "loaders.open_interest",
    "pipeline.nightly",
    "universe.builder",
)


@pytest.fixture(autouse=True)
def isolate_production_datastore(tmp_path, monkeypatch):
    """Point every production-datastore default at a per-test directory.

    A test that constructs `SomeLoader("binance")`, or passes a `store=` but no
    `asset_master=`, silently gets the real `data/parquet` — the developer's own
    datastore. On a clean checkout that directory is empty and the test passes;
    on a machine that has actually run the pipeline it holds real mappings, and
    the test asserting an unresolved symbol resolves `BTC/USDT` to `BTC` and
    fails. That is a test-isolation defect, not a code defect, and it fails on
    exactly the machine you most want a green suite on.

    Autouse and unconditional: opting in per test is what let it happen.
    """
    sandbox = tmp_path / "production_datastore"
    sandbox.mkdir(parents=True, exist_ok=True)

    import importlib

    for module_name in _PRODUCTION_PATH_MODULES:
        try:
            module = importlib.import_module(module_name)
        except ImportError:  # pragma: no cover - a module not yet written
            continue
        if hasattr(module, "DATASTORE_PATH"):
            monkeypatch.setattr(module, "DATASTORE_PATH", sandbox, raising=False)

    return sandbox


@pytest.fixture
def production_datastore_sandbox(isolate_production_datastore):
    """The directory standing in for the production datastore in this test."""
    return isolate_production_datastore


@pytest.fixture
def project_root():
    """Fixture providing the project root path."""
    return PROJECT_ROOT


@pytest.fixture
def datastore_path(tmp_path):
    """Fixture providing a temporary datastore path for tests."""
    ds_path = tmp_path / "test_datastore"
    ds_path.mkdir(exist_ok=True)
    return ds_path


@pytest.fixture
def sample_config(datastore_path):
    """Fixture providing a test configuration."""
    from config import (
        AuditConfig,
        BacktestConfig,
        LoaderConfig,
        RiskConfig,
        UniverseConfig,
    )

    return {
        "datastore_path": datastore_path,
        "universe_config": UniverseConfig(),
        "backtest_config": BacktestConfig(),
        "loader_config": LoaderConfig(),
        "audit_config": AuditConfig(),
        "risk_config": RiskConfig(),
        "paper": True,
    }
