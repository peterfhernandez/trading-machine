"""Tests for backtest metrics, the cost model, and the rebalance calendar.

Every expected value here is hand-computed from the inputs (golden fixtures);
none is taken from a previous run of the code.
"""

import math
from datetime import datetime, timedelta

import pytest

from backtest import (
    ZERO_COST,
    CostModel,
    annualized_return,
    annualized_vol,
    build_rebalance_calendar,
    drawdown_series,
    hit_rate,
    information_ratio,
    max_drawdown,
    spearman_ic,
    summarize,
    total_return,
)
from backtest.metrics import average, pearson


class TestReturnMetrics:
    def test_total_return_compounds(self):
        # 1.10 * 0.90 * 1.05 - 1 = 0.0395
        assert total_return([0.10, -0.10, 0.05]) == pytest.approx(0.0395)

    def test_total_return_of_empty_series_is_zero(self):
        assert total_return([]) == 0.0

    def test_annualized_return_geometric(self):
        # +1% per period for 4 periods, 12 periods/year -> (1.01**4)**3 - 1
        assert annualized_return([0.01] * 4, periods_per_year=12) == pytest.approx(
            1.01 ** 12 - 1
        )

    def test_annualized_return_of_total_wipeout(self):
        assert annualized_return([-1.0, 0.5], periods_per_year=12) == -1.0

    def test_annualized_vol_uses_sample_stdev(self):
        returns = [0.01, -0.01, 0.02, -0.02]
        mean = 0.0
        var = sum((r - mean) ** 2 for r in returns) / (len(returns) - 1)
        assert annualized_vol(returns, periods_per_year=252) == pytest.approx(
            math.sqrt(var) * math.sqrt(252)
        )

    def test_annualized_vol_needs_two_points(self):
        assert annualized_vol([0.01], periods_per_year=252) == 0.0

    def test_information_ratio_is_annualized_mean_over_annualized_vol(self):
        returns = [0.01, -0.01, 0.02, -0.02]
        expected = (average(returns) * 365) / annualized_vol(returns, 365)
        assert information_ratio(returns, 365) == pytest.approx(expected)

    def test_information_ratio_of_constant_returns_is_zero(self):
        assert information_ratio([0.01, 0.01, 0.01], 365) == 0.0

    def test_hit_rate_counts_positive_periods(self):
        assert hit_rate([0.01, -0.01, 0.0, 0.02]) == pytest.approx(0.5)


class TestDrawdown:
    def test_drawdown_series_tracks_running_peak(self):
        # equity: 1.10, 0.99, 1.089
        dd = drawdown_series([0.10, -0.10, 0.10])
        assert dd[0] == pytest.approx(0.0)
        assert dd[1] == pytest.approx(0.99 / 1.10 - 1)
        assert dd[2] == pytest.approx(1.089 / 1.10 - 1)

    def test_max_drawdown_is_worst_trough(self):
        assert max_drawdown([0.10, -0.10, 0.10]) == pytest.approx(0.99 / 1.10 - 1)

    def test_monotonic_gains_have_no_drawdown(self):
        assert max_drawdown([0.01, 0.02, 0.03]) == 0.0

    def test_empty_series_has_no_drawdown(self):
        assert max_drawdown([]) == 0.0


class TestCorrelation:
    def test_pearson_of_perfect_line(self):
        assert pearson([1.0, 2.0, 3.0], [2.0, 4.0, 6.0]) == pytest.approx(1.0)

    def test_pearson_of_constant_input_is_none(self):
        assert pearson([1.0, 1.0, 1.0], [1.0, 2.0, 3.0]) is None

    def test_spearman_is_rank_based_not_level_based(self):
        """A monotone but wildly non-linear relationship still has IC = 1."""
        assert spearman_ic([1.0, 2.0, 3.0], [0.01, 0.02, 100.0]) == pytest.approx(1.0)

    def test_spearman_handles_ties(self):
        # scores tied at the top -> ranks (2.5, 2.5, 1); returns ranks (3, 2, 1)
        ic = spearman_ic([2.0, 2.0, 1.0], [0.03, 0.02, 0.01])
        assert ic == pytest.approx(pearson([2.5, 2.5, 1.0], [3.0, 2.0, 1.0]))

    def test_spearman_needs_two_observations(self):
        assert spearman_ic([1.0], [0.01]) is None

    def test_spearman_of_all_tied_scores_is_none(self):
        assert spearman_ic([1.0, 1.0, 1.0], [0.01, 0.02, 0.03]) is None

    def test_length_mismatch_raises(self):
        with pytest.raises(ValueError, match="Length mismatch"):
            spearman_ic([1.0, 2.0], [0.01])


class TestCostModel:
    def test_half_spread_is_paid_per_trade(self):
        model = CostModel(spread_bps=10.0, impact_bps_per_million=0.0)
        # Trading the whole book once at 10bps round-trip costs 5bps
        assert model.cost(turnover=1.0, portfolio_value=100_000.0) == pytest.approx(5e-4)

    def test_cost_scales_with_turnover(self):
        model = CostModel(spread_bps=10.0, impact_bps_per_million=0.0)
        assert model.cost(0.5, 100_000.0) == pytest.approx(2.5e-4)

    def test_impact_scales_with_traded_notional(self):
        model = CostModel(spread_bps=0.0, impact_bps_per_million=2.0)
        # turnover 1.0 on $2M -> $2M traded -> 4 bps of impact on the whole book
        assert model.cost(1.0, 2_000_000.0) == pytest.approx(4e-4)

    def test_impact_is_quadratic_in_turnover(self):
        """Doubling turnover doubles the rate and the notional -> 4x the cost."""
        model = CostModel(spread_bps=0.0, impact_bps_per_million=1.0)
        small = model.cost(0.5, 1_000_000.0)
        large = model.cost(1.0, 1_000_000.0)
        assert large == pytest.approx(4 * small)

    def test_zero_turnover_is_free(self):
        assert CostModel(10.0, 5.0).cost(0.0, 100_000.0) == 0.0

    def test_zero_cost_model_is_free(self):
        assert ZERO_COST.cost(2.0, 1_000_000.0) == 0.0

    def test_non_positive_portfolio_value_is_free(self):
        assert CostModel(10.0, 1.0).cost(1.0, 0.0) == 0.0


class TestRebalanceCalendar:
    @staticmethod
    def _dates(n, start=datetime(2024, 1, 1)):
        return [start + timedelta(days=i) for i in range(n)]

    def test_daily_uses_every_bar(self):
        dates = self._dates(5)
        assert build_rebalance_calendar(dates, "daily") == dates

    def test_weekly_takes_first_bar_of_each_iso_week(self):
        # 2024-01-01 is a Monday
        dates = self._dates(15)
        weekly = build_rebalance_calendar(dates, "weekly")
        assert weekly == [datetime(2024, 1, 1), datetime(2024, 1, 8), datetime(2024, 1, 15)]

    def test_weekly_handles_a_start_mid_week(self):
        dates = self._dates(10, start=datetime(2024, 1, 3))  # Wednesday
        weekly = build_rebalance_calendar(dates, "weekly")
        assert weekly == [datetime(2024, 1, 3), datetime(2024, 1, 8)]

    def test_monthly_takes_first_bar_of_each_month(self):
        dates = self._dates(45)
        monthly = build_rebalance_calendar(dates, "monthly")
        assert monthly == [datetime(2024, 1, 1), datetime(2024, 2, 1)]

    def test_calendar_is_sorted_and_a_subset_of_bars(self):
        dates = list(reversed(self._dates(10)))
        calendar = build_rebalance_calendar(dates, "weekly")
        assert calendar == sorted(calendar)
        assert set(calendar) <= set(dates)

    def test_unsupported_freq_raises(self):
        with pytest.raises(ValueError, match="Unsupported rebalance_freq"):
            build_rebalance_calendar(self._dates(3), "hourly")


class TestSummarize:
    def test_summary_packages_the_series(self):
        nets = [0.02, -0.01, 0.03]
        metrics = summarize(nets, turnover=[1.0, 0.2, 0.2], costs=[5e-4, 1e-4, 1e-4],
                            periods_per_year=365)

        assert metrics.n_periods == 3
        assert metrics.total_return == pytest.approx(1.02 * 0.99 * 1.03 - 1)
        assert metrics.avg_turnover == pytest.approx(1.4 / 3)
        assert metrics.total_cost == pytest.approx(7e-4)
        assert metrics.hit_rate == pytest.approx(2 / 3)
        assert metrics.periods_per_year == 365

    def test_summary_of_empty_series(self):
        metrics = summarize([])
        assert metrics.n_periods == 0
        assert metrics.total_return == 0.0
        assert metrics.information_ratio == 0.0

    def test_to_dict_round_trips_fields(self):
        metrics = summarize([0.01, 0.02])
        assert metrics.to_dict()["n_periods"] == 2
        assert "information_ratio" in metrics.to_dict()
