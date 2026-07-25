"""
Tests for the configuration module.
"""

import pytest

import config


class TestConfigPaths:
    """Test configuration paths."""

    def test_project_root_exists(self):
        """Test that PROJECT_ROOT path exists."""
        assert config.PROJECT_ROOT.exists()

    def test_datastore_path_created(self):
        """Test that DATASTORE_PATH is created."""
        assert config.DATASTORE_PATH.exists()

    def test_logs_path_created(self):
        """Test that LOGS_PATH is created."""
        assert config.LOGS_PATH.exists()

    def test_scratch_path_created(self):
        """Test that SCRATCH_PATH is created."""
        assert config.SCRATCH_PATH.exists()


class TestUniverseConfig:
    """Test universe configuration."""

    def test_default_universe_config(self):
        """Test default universe configuration."""
        cfg = config.UNIVERSE_CONFIG
        assert cfg.target_size == 150
        assert cfg.min_volume_usdt == 1_000_000.0
        assert cfg.min_listing_age_days == 30
        assert cfg.exclude_stablecoins is True
        assert cfg.exclude_wrapped is True


class TestBacktestConfig:
    """Test backtest configuration."""

    def test_default_backtest_config(self):
        """Test default backtest configuration."""
        cfg = config.BACKTEST_CONFIG
        assert cfg.rebalance_freq == "daily"
        assert cfg.spread_bps == 5.0
        assert cfg.impact_bps_per_million == 1.0


class TestRiskConfig:
    """Test risk configuration."""

    def test_default_risk_config(self):
        """Test default risk configuration."""
        cfg = config.RISK_CONFIG
        assert cfg.max_daily_loss_pct == 2.0
        assert cfg.max_gross_leverage == 3.0
        assert cfg.max_position_pct == 5.0
        assert cfg.target_vol_pct == 15.0


class TestPaperMode:
    """Test PAPER mode flag."""

    def test_paper_mode_default(self):
        """Test that PAPER mode defaults to True."""
        # PAPER should be True unless explicitly set to false
        assert config.PAPER is True
