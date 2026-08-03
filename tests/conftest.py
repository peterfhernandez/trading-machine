"""
Pytest configuration and fixtures for trading-machine tests.
"""

import importlib
import sys
from pathlib import Path

import pytest

# Add project root to path so imports work
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


# ---------------------------------------------------------------------------
# Temp-directory housekeeping must never fail a green run
# ---------------------------------------------------------------------------
#
# Symptom: every test passes, then pytest exits **1** with a traceback ending in
#
#     File "_pytest/pathlib.py", line 357, in cleanup_dead_symlinks
#       left_dir.unlink()
#   PermissionError: [WinError 5] Access is denied:
#       '...\\Temp\\pytest-of-Peter\\pytest-current'
#
# Mechanism, in three steps:
#
# 1. `make_numbered_dir` points `<tmp>/pytest-of-<user>/pytest-current` at the
#    run's numbered directory via `_force_symlink`, which swallows every error.
#    On Windows the `unlink()` it does first fails on an existing *directory*
#    link, so the re-point silently does not happen and the link keeps pointing
#    at an older run.
# 2. That older numbered directory is eventually cleaned up (`keep=3`), leaving
#    `pytest-current` dangling.
# 3. `cleanup_dead_symlinks` finds the dangling link and calls `unlink()` on it
#    — unguarded. On Windows a link to a directory is removed with
#    `RemoveDirectoryW`, not `DeleteFileW`, so `os.unlink` raises
#    PermissionError and it escapes.
#
# It escapes all the way out: `pytest_sessionfinish` is invoked from
# `wrap_session`'s `finally` block, which catches only `exit.Exception`, so the
# error is not converted into an INTERNAL_ERROR status — it propagates through
# `_console_main` and the interpreter exits 1 on a suite that passed. That is
# not cosmetic here: `deploy.yml` gates `git reset --hard` on this exact exit
# code, so the trading machine refuses to adopt a commit whose tests all passed.
#
# The shim below keeps pytest's semantics (remove links whose target is gone)
# and adds two guards: fall back to `rmdir`, which is how a directory link is
# removed on Windows, and never raise. It is installed unconditionally rather
# than under `os.name == "nt"` — on POSIX `unlink` on a dangling link always
# succeeds, so the fallback is dead code there and CI still exercises the rest —
# and it self-heals: once the stale link is removed, the next run creates a live
# one.


def _remove_dangling_link(link: Path) -> None:
    """Delete a symlink whose target is gone, whatever it takes, without raising."""
    try:
        link.unlink()
        return
    except OSError:
        pass
    try:
        link.rmdir()
    except OSError:
        pass


def _tolerant_cleanup_dead_symlinks(root: Path) -> None:
    """`_pytest.pathlib.cleanup_dead_symlinks`, minus the ways it can raise."""
    try:
        entries = list(root.iterdir())
    except OSError:
        return
    for entry in entries:
        try:
            if not entry.is_symlink() or entry.resolve().exists():
                continue
        except OSError:
            continue
        _remove_dangling_link(entry)


def _install_tolerant_symlink_cleanup() -> tuple[str, ...]:
    """Replace the cleanup in every module that holds a reference to it.

    `_pytest.tmpdir` does `from .pathlib import cleanup_dead_symlinks` at import
    time, so patching `_pytest.pathlib` alone leaves the session-finish call site
    — the one in the traceback — still bound to the original.
    """
    patched = []
    for module_name in ("_pytest.pathlib", "_pytest.tmpdir"):
        try:
            module = importlib.import_module(module_name)
        except ImportError:  # pragma: no cover - pytest is a hard dependency
            continue
        if hasattr(module, "cleanup_dead_symlinks"):
            module.cleanup_dead_symlinks = _tolerant_cleanup_dead_symlinks
            patched.append(module_name)
    return tuple(patched)


# Both call sites, or `tests/test_tmpdir_cleanup.py` fails: a silently absent
# shim would look exactly like a fixed pytest until the next Windows run.
_PATCHED_SYMLINK_CLEANUP_MODULES = _install_tolerant_symlink_cleanup()


# Every module that falls back to the *production* datastore when a caller
# does not pass one. Each does `from config import DATASTORE_PATH`, binding the
# value at import time, so redirecting `config.DATASTORE_PATH` alone would not
# reach them — the name has to be replaced in each module's own namespace.
_PRODUCTION_PATH_MODULES = (
    "config",
    "audit.acceptance",
    "audit.auditor",
    "backtest.engine",
    "loaders.archive",
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
