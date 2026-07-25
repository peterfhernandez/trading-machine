"""
Pytest configuration and fixtures for trading-machine tests.
"""

import os
import sys
from pathlib import Path

import pytest

# Add project root to path so imports work
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


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
        UniverseConfig,
        BacktestConfig,
        LoaderConfig,
        AuditConfig,
        RiskConfig,
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
