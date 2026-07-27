"""Walk-forward backtest engine.

The engine is the module every later module is tested against, so its
semantics are stated explicitly and never bent:

**Point-in-time exposure.** At each rebalance date `t` the strategy is handed a
`RebalanceContext`, whose only view of the world is `store.read(..., asof=t)`
further filtered to `event_ts <= t`. The strategy never receives the engine's
price panel, which is what the engine uses for accounting; there is no code
path by which a strategy can see a bar it could not have seen live.

**Two point-in-time modes (`pit_mode`) — read this before trusting a number.**

- `"ingestion"` (default, strict): reads are filtered to `ingested_ts <= asof`,
  the full look-ahead defence. Correct whenever the store holds honest
  ingestion history, i.e. data collected day by day by the nightly pipeline.
- `"event"` (opt-in, weaker): reads are filtered on `event_ts <= asof` only.
  This exists because a *backfill* stamps every row it pulls with the same
  `ingested_ts` (the moment the backfill ran), so under strict mode a backtest
  over backfilled history sees literally nothing and every book is empty. Event
  mode restores the ability to research that history, and still prevents a
  strategy from seeing a bar dated after the rebalance — but it does **not**
  protect against vendor revisions or against data that genuinely arrived late.
  Results from event mode are research indications; the live-fidelity number is
  the one strict mode produces on data the pipeline collected as it happened.

**Next-bar execution.** Weights decided at `t` are filled at the *open of the
next bar* `f(t)`, never at the close of `t` (that close is the last thing the
strategy saw). A rebalance date with no following bar cannot be executed and is
dropped.

**Holding periods.** The weights set at rebalance `t_k` are held from `f(t_k)`
open to `f(t_(k+1))` open, so each period's asset return is open-to-open and
periods tile the sample without overlap or gaps. The final rebalance has no
following fill, so its weights are recorded but earn no return.

**Drift and turnover.** Between rebalances positions drift with returns:
`w_drift = w * (1 + r) / (1 + r_portfolio)`. Turnover is measured against the
drifted book (`Σ|w_target − w_drift|`), not the previous target, so a
buy-and-hold strategy correctly pays costs once rather than every period.

**Costs.** Charged at the start of each period on the traded notional (see
`backtest.costs.CostModel`) and subtracted from the period's gross return.

**Accounting prices.** The price panel is read once, unfiltered by `asof`: the
engine settles fills and returns against the final revised price series. That is
an accounting choice, not an information channel — the panel is never handed to
a strategy — and it is stated here so it is never mistaken for a point-in-time
read.
"""

import logging
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta

import polars as pl

from config import BACKTEST_CONFIG, DATASTORE_PATH, BacktestConfig
from datastore import ParquetStore

from .calendar import build_rebalance_calendar
from .costs import CostModel
from .metrics import BacktestMetrics, spearman_ic, summarize

logger = logging.getLogger(__name__)

OHLCV_DATASET = "ohlcv_daily"
UNIVERSE_DATASET = "universe"

PIT_INGESTION = "ingestion"
"""Strict point-in-time: only rows with `ingested_ts <= asof` are visible."""

PIT_EVENT = "event"
"""Weaker: rows are filtered by `event_ts <= asof` only (see module docstring)."""

PIT_MODES = (PIT_INGESTION, PIT_EVENT)


def _validate_pit_mode(pit_mode: str) -> str:
    if pit_mode not in PIT_MODES:
        raise ValueError(f"Unknown pit_mode {pit_mode!r}; expected one of {PIT_MODES}")
    return pit_mode


Weights = Mapping[str, float]
Strategy = Callable[["RebalanceContext"], Weights]
SignalFn = Callable[["RebalanceContext"], Mapping[str, float]]
UniverseProvider = Callable[[datetime], Sequence[str]]


def _parse_date(value: str | datetime | None) -> datetime | None:
    """Parse a YYYY-MM-DD string (or pass through a datetime); None stays None."""
    if value is None or isinstance(value, datetime):
        return value
    return datetime.strptime(str(value)[:10], "%Y-%m-%d")


@dataclass(frozen=True)
class RebalanceContext:
    """Point-in-time view of the world handed to a strategy at one rebalance date.

    Every read goes through `asof`, so a strategy physically cannot see data
    dated after the rebalance date — and, under the default
    `pit_mode="ingestion"`, cannot see data that had not yet been ingested
    either. See the module docstring for what `pit_mode="event"` gives up.
    """

    asof: datetime
    universe: list[str]
    store: ParquetStore
    venue: str
    timeframe: str = "1d"
    pit_mode: str = PIT_INGESTION

    def read(
        self,
        dataset: str,
        columns: list[str] | None = None,
        lookback_days: int | None = None,
    ) -> pl.DataFrame:
        """Read a dataset as it was known at `asof`.

        Under `pit_mode="ingestion"` applies the store's `ingested_ts <= asof`
        filter; then, in either mode, drops any row with `event_ts > asof` (a row
        can be ingested on time but describe a future event) and any row from
        another venue.

        Args:
            dataset: Dataset name in the store (e.g. "funding_rate").
            columns: Columns to read; None reads all. `event_ts`/`ingested_ts`
                are always required and must not be pruned away.
            lookback_days: If given, keep only rows with
                `event_ts >= asof - lookback_days`.

        Returns:
            Polars DataFrame, empty if the dataset does not exist yet.
        """
        asof_arg = self.asof.date().isoformat() if self.pit_mode == PIT_INGESTION else None
        try:
            df = self.store.read(dataset, asof=asof_arg, columns=columns)
        except FileNotFoundError:
            return pl.DataFrame()

        if len(df) == 0:
            return df

        if "venue" in df.columns:
            df = df.filter(pl.col("venue") == self.venue)

        df = df.filter(pl.col("event_ts") <= self.asof)

        if lookback_days is not None:
            df = df.filter(pl.col("event_ts") >= self.asof - timedelta(days=lookback_days))

        return df

    def ohlcv(
        self,
        lookback_days: int | None = None,
        columns: list[str] | None = None,
    ) -> pl.DataFrame:
        """Point-in-time daily bars for this venue/timeframe, restricted to the universe."""
        df = self.read(OHLCV_DATASET, columns=columns, lookback_days=lookback_days)
        if len(df) == 0:
            return df
        if "timeframe" in df.columns:
            df = df.filter(pl.col("timeframe") == self.timeframe)
        return df.filter(pl.col("asset_id").is_in(self.universe))


@dataclass(frozen=True)
class PricePanel:
    """Open prices by bar date and asset, used for fills and return accounting."""

    dates: list[datetime]
    opens: dict[datetime, dict[str, float]]
    index: dict[datetime, int] = field(default_factory=dict)

    def price(self, when: datetime, asset_id: str) -> float | None:
        """Open price for `asset_id` on bar `when`, or None if it did not trade."""
        return self.opens.get(when, {}).get(asset_id)

    def assets_on(self, when: datetime) -> list[str]:
        """Assets with a bar on `when`."""
        return sorted(self.opens.get(when, {}))


@dataclass(frozen=True)
class BacktestResult:
    """Outputs of one backtest run.

    Attributes:
        returns: One row per realized holding period (rebalance_ts, fill_ts,
            exit_ts, gross_return, cost, net_return, turnover, equity, ...).
        weights: Target weights actually taken, one row per (rebalance, asset).
        ic: Per-rebalance rank IC per named signal (empty if no signals given).
        metrics: Headline performance metrics over the net return series.
    """

    returns: pl.DataFrame
    weights: pl.DataFrame
    ic: pl.DataFrame
    metrics: BacktestMetrics

    def equity_curve(self) -> pl.DataFrame:
        """Equity by period exit date."""
        if len(self.returns) == 0:
            return pl.DataFrame({"exit_ts": [], "equity": []})
        return self.returns.select("exit_ts", "equity")

    def ic_summary(self) -> pl.DataFrame:
        """Mean IC, IC volatility, and IC information ratio per signal."""
        if len(self.ic) == 0:
            return pl.DataFrame(
                schema={"signal": pl.Utf8, "mean_ic": pl.Float64, "ic_std": pl.Float64,
                        "ic_ir": pl.Float64, "n_periods": pl.UInt32}
            )
        return (
            self.ic.drop_nulls("ic")
            .group_by("signal")
            .agg(
                pl.col("ic").mean().alias("mean_ic"),
                pl.col("ic").std().alias("ic_std"),
                pl.len().alias("n_periods"),
            )
            .with_columns(
                pl.when(pl.col("ic_std") > 0)
                .then(pl.col("mean_ic") / pl.col("ic_std"))
                .otherwise(0.0)
                .alias("ic_ir")
            )
            .select("signal", "mean_ic", "ic_std", "ic_ir", "n_periods")
            .sort("signal")
        )


class DatastoreUniverse:
    """Universe provider backed by the point-in-time `universe` dataset.

    Returns the members of the most recent snapshot with `event_ts <= asof`
    (and, under `pit_mode="ingestion"`, `ingested_ts <= asof`). Returns an empty
    universe if the dataset does not exist or has no snapshot yet.

    Note on `pit_mode="event"`: a Phase 3 snapshot dated `T` was itself built
    only from data with `ingested_ts <= T`, so using it in a backtest as of `T`
    introduces no look-ahead in the *rules*, even when the snapshot was computed
    later in a research run. What event mode does concede is that the snapshot
    may have been built from data revised after `T`.
    """

    def __init__(
        self,
        store: ParquetStore,
        venue: str = "binance",
        pit_mode: str = PIT_INGESTION,
    ):
        self.store = store
        self.venue = venue
        self.pit_mode = _validate_pit_mode(pit_mode)

    def __call__(self, asof: datetime) -> list[str]:
        asof_arg = asof.date().isoformat() if self.pit_mode == PIT_INGESTION else None
        try:
            df = self.store.read(UNIVERSE_DATASET, asof=asof_arg)
        except FileNotFoundError:
            return []

        if len(df) == 0:
            return []

        if "venue" in df.columns:
            df = df.filter(pl.col("venue") == self.venue)
        df = df.filter(pl.col("event_ts") <= asof)
        if len(df) == 0:
            return []

        latest = df["event_ts"].max()
        snapshot = df.filter((pl.col("event_ts") == latest) & pl.col("in_universe"))
        return sorted(snapshot["asset_id"].unique().to_list())


class Backtester:
    """Walk-forward backtester: point-in-time data in, net-of-cost returns out."""

    def __init__(
        self,
        store: ParquetStore | None = None,
        config: BacktestConfig = BACKTEST_CONFIG,
        cost_model: CostModel | None = None,
        universe_provider: UniverseProvider | None = None,
        venue: str | None = None,
        timeframe: str | None = None,
        pit_mode: str = PIT_INGESTION,
    ):
        """
        Args:
            store: Datastore to read from (default: the configured ParquetStore).
            config: Backtest parameters (calendar, cost assumptions, annualization).
            cost_model: Cost model; defaults to one built from `config`.
            universe_provider: `asof -> list[asset_id]`. Default: every asset with
                a bar on the fill date (all tradable assets).
            venue: Venue to price against (default: `config.venue`).
            timeframe: Bar timeframe (default: `config.timeframe`).
            pit_mode: "ingestion" (default, strict `ingested_ts <= asof`) or
                "event" (`event_ts <= asof` only — required to research backfilled
                history, and weaker; see the module docstring).
        """
        self.store = store or ParquetStore(DATASTORE_PATH)
        self.config = config
        self.cost_model = cost_model or CostModel(
            spread_bps=config.spread_bps,
            impact_bps_per_million=config.impact_bps_per_million,
        )
        self.universe_provider = universe_provider
        self.venue = venue or config.venue
        self.timeframe = timeframe or config.timeframe
        self.pit_mode = _validate_pit_mode(pit_mode)

    # ------------------------------------------------------------------
    # Price panel
    # ------------------------------------------------------------------

    def load_price_panel(
        self,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> PricePanel:
        """Load open prices for the backtest window.

        Deliberately *not* filtered by `asof`: this is the engine's accounting
        view (final revised prices, used to settle fills and returns) and is
        never handed to a strategy. Strategy-visible reads go through
        `RebalanceContext`, which is where point-in-time discipline is enforced.
        """
        try:
            df = self.store.read(
                OHLCV_DATASET,
                columns=["asset_id", "venue", "timeframe", "event_ts", "ingested_ts", "open"],
            )
        except FileNotFoundError as exc:
            raise ValueError(
                f"No {OHLCV_DATASET} data in the store; cannot run a backtest"
            ) from exc

        if len(df) == 0:
            return PricePanel(dates=[], opens={}, index={})

        if "venue" in df.columns:
            df = df.filter(pl.col("venue") == self.venue)
        if "timeframe" in df.columns:
            df = df.filter(pl.col("timeframe") == self.timeframe)
        if start is not None:
            df = df.filter(pl.col("event_ts") >= start)
        if end is not None:
            df = df.filter(pl.col("event_ts") <= end)

        df = df.filter(pl.col("open").is_not_null() & pl.col("open").is_finite())
        if len(df) == 0:
            return PricePanel(dates=[], opens={}, index={})

        # A bar can be ingested more than once (backfill overlap); keep the
        # latest ingestion for each (asset, bar) -- append-only means we never
        # delete the earlier copy, we just read past it.
        df = (
            df.sort("ingested_ts")
            .group_by(["asset_id", "event_ts"])
            .last()
            .sort(["event_ts", "asset_id"])
        )

        opens: dict[datetime, dict[str, float]] = {}
        for asset_id, event_ts, open_px in zip(
            df["asset_id"], df["event_ts"], df["open"], strict=True
        ):
            opens.setdefault(event_ts, {})[asset_id] = float(open_px)

        dates = sorted(opens)
        return PricePanel(dates=dates, opens=opens, index={d: i for i, d in enumerate(dates)})

    # ------------------------------------------------------------------
    # Run
    # ------------------------------------------------------------------

    def run(
        self,
        strategy: Strategy,
        signals: Mapping[str, SignalFn] | None = None,
        start: str | datetime | None = None,
        end: str | datetime | None = None,
    ) -> BacktestResult:
        """Run the walk-forward loop.

        Args:
            strategy: `RebalanceContext -> {asset_id: target weight}`. Weights are
                fractions of portfolio value; anything not allocated is cash.
            signals: Optional named signal functions, scored at each rebalance and
                correlated with the following period's returns to produce an IC series.
            start: Backtest start (default `config.backtest_start`).
            end: Backtest end (default `config.backtest_end`; None = all data).

        Returns:
            BacktestResult with per-period returns, taken weights, IC series, metrics.

        Raises:
            ValueError: If there is no price data, or fewer than three bars in the
                window (a walk-forward run needs a rebalance, a fill, and an exit).
        """
        start_dt = _parse_date(start if start is not None else self.config.backtest_start)
        end_dt = _parse_date(end if end is not None else self.config.backtest_end)

        panel = self.load_price_panel(start_dt, end_dt)
        if len(panel.dates) < 3:
            raise ValueError(
                f"Need at least 3 bars to backtest (rebalance, fill, exit); "
                f"got {len(panel.dates)} between {start_dt} and {end_dt}"
            )

        calendar = build_rebalance_calendar(panel.dates, self.config.rebalance_freq)

        # Only rebalances with a following bar can actually be filled.
        fills = [
            (t, panel.dates[panel.index[t] + 1])
            for t in calendar
            if panel.index[t] + 1 < len(panel.dates)
        ]

        equity = float(self.config.initial_cash)
        drifted: dict[str, float] = {}
        return_rows: list[dict] = []
        weight_rows: list[dict] = []
        ic_rows: list[dict] = []
        bars_per_period: list[int] = []

        for k, (rebalance_ts, fill_ts) in enumerate(fills):
            universe = self._universe_for(rebalance_ts, panel, fill_ts)
            ctx = RebalanceContext(
                asof=rebalance_ts,
                universe=universe,
                store=self.store,
                venue=self.venue,
                timeframe=self.timeframe,
                pit_mode=self.pit_mode,
            )

            targets = self._clean_weights(strategy(ctx), universe, panel, fill_ts, rebalance_ts)
            for asset_id, weight in sorted(targets.items()):
                weight_rows.append(
                    {
                        "rebalance_ts": rebalance_ts,
                        "fill_ts": fill_ts,
                        "asset_id": asset_id,
                        "weight": weight,
                    }
                )

            if k + 1 >= len(fills):
                # No following fill: these weights are never exited, so they earn
                # no return and are excluded from the P&L series.
                logger.debug("Final rebalance %s recorded but not realized", rebalance_ts.date())
                break

            exit_ts = fills[k + 1][1]
            bars_per_period.append(panel.index[exit_ts] - panel.index[fill_ts])
            period_returns = self._period_returns(panel, fill_ts, exit_ts)

            gross_return = sum(w * period_returns.get(a, 0.0) for a, w in targets.items())
            turnover = sum(
                abs(targets.get(a, 0.0) - drifted.get(a, 0.0))
                for a in set(targets) | set(drifted)
            )
            cost = self.cost_model.cost(turnover, equity)
            net_return = gross_return - cost
            equity *= 1.0 + net_return

            return_rows.append(
                {
                    "rebalance_ts": rebalance_ts,
                    "fill_ts": fill_ts,
                    "exit_ts": exit_ts,
                    "gross_return": gross_return,
                    "cost": cost,
                    "net_return": net_return,
                    "turnover": turnover,
                    "equity": equity,
                    "n_positions": sum(1 for w in targets.values() if w != 0.0),
                    "gross_exposure": sum(abs(w) for w in targets.values()),
                    "net_exposure": sum(targets.values()),
                }
            )

            if signals:
                ic_rows.extend(
                    self._score_signals(signals, ctx, period_returns, rebalance_ts)
                )

            drifted = self._drift(targets, period_returns, gross_return)

        returns_df = self._returns_frame(return_rows)
        return BacktestResult(
            returns=returns_df,
            weights=self._weights_frame(weight_rows),
            ic=self._ic_frame(ic_rows),
            metrics=summarize(
                returns_df["net_return"].to_list() if len(returns_df) else [],
                turnover=returns_df["turnover"].to_list() if len(returns_df) else [],
                costs=returns_df["cost"].to_list() if len(returns_df) else [],
                periods_per_year=self._periods_per_year(bars_per_period),
            ),
        )

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _periods_per_year(self, bars_per_period: list[int]) -> float:
        """Annualization factor for the *holding periods*, not the bars.

        `config.periods_per_year` counts bars per year (365 for daily crypto
        bars). A weekly rebalance holds each book for ~7 bars, so annualizing its
        returns at 365 would inflate them by an order of magnitude; divide by the
        mean holding period instead.
        """
        if not bars_per_period:
            return float(self.config.periods_per_year)
        mean_bars = sum(bars_per_period) / len(bars_per_period)
        if mean_bars <= 0:
            return float(self.config.periods_per_year)
        return float(self.config.periods_per_year) / mean_bars

    def _universe_for(
        self, rebalance_ts: datetime, panel: PricePanel, fill_ts: datetime
    ) -> list[str]:
        """Universe at `rebalance_ts`; defaults to everything tradable at the fill."""
        if self.universe_provider is None:
            return panel.assets_on(fill_ts)
        return list(self.universe_provider(rebalance_ts))

    def _clean_weights(
        self,
        raw: Weights,
        universe: list[str],
        panel: PricePanel,
        fill_ts: datetime,
        rebalance_ts: datetime,
    ) -> dict[str, float]:
        """Drop weights that cannot be traded, and reject malformed ones.

        A weight survives only if the asset is in the universe and has an open
        price on the fill date. Zero weights are dropped (nothing to hold).

        Raises:
            ValueError: If a weight is not finite.
        """
        universe_set = set(universe)
        cleaned: dict[str, float] = {}
        for asset_id, weight in raw.items():
            value = float(weight)
            if value != value or value in (float("inf"), float("-inf")):
                raise ValueError(
                    f"Strategy returned non-finite weight {weight!r} for {asset_id} "
                    f"at {rebalance_ts.date()}"
                )
            if value == 0.0:
                continue
            if asset_id not in universe_set:
                logger.debug(
                    "Dropping %s at %s: not in universe", asset_id, rebalance_ts.date()
                )
                continue
            if panel.price(fill_ts, asset_id) is None:
                logger.debug(
                    "Dropping %s at %s: no open price on fill date %s",
                    asset_id, rebalance_ts.date(), fill_ts.date(),
                )
                continue
            cleaned[asset_id] = value
        return cleaned

    @staticmethod
    def _period_returns(
        panel: PricePanel, fill_ts: datetime, exit_ts: datetime
    ) -> dict[str, float]:
        """Open-to-open returns between two fill dates, for assets priced at both."""
        entry = panel.opens.get(fill_ts, {})
        exit_prices = panel.opens.get(exit_ts, {})
        out: dict[str, float] = {}
        for asset_id, entry_px in entry.items():
            exit_px = exit_prices.get(asset_id)
            if exit_px is None or entry_px <= 0.0:
                continue
            out[asset_id] = exit_px / entry_px - 1.0
        return out

    @staticmethod
    def _drift(
        weights: Mapping[str, float],
        period_returns: Mapping[str, float],
        gross_return: float,
    ) -> dict[str, float]:
        """Weights after a period of price drift, renormalized by portfolio growth."""
        growth = 1.0 + gross_return
        if abs(growth) < 1e-12:
            return dict(weights)
        return {
            asset_id: w * (1.0 + period_returns.get(asset_id, 0.0)) / growth
            for asset_id, w in weights.items()
        }

    @staticmethod
    def _score_signals(
        signals: Mapping[str, SignalFn],
        ctx: RebalanceContext,
        period_returns: Mapping[str, float],
        rebalance_ts: datetime,
    ) -> list[dict]:
        """Rank IC of each signal's scores against the following period's returns."""
        rows = []
        for name, signal_fn in signals.items():
            scores = signal_fn(ctx)
            aligned = [
                (float(score), period_returns[asset_id])
                for asset_id, score in scores.items()
                if asset_id in period_returns and score is not None
            ]
            ic = (
                spearman_ic([s for s, _ in aligned], [r for _, r in aligned])
                if len(aligned) >= 2
                else None
            )
            rows.append(
                {
                    "rebalance_ts": rebalance_ts,
                    "signal": name,
                    "ic": ic,
                    "n_assets": len(aligned),
                }
            )
        return rows

    @staticmethod
    def _returns_frame(rows: list[dict]) -> pl.DataFrame:
        schema = {
            "rebalance_ts": pl.Datetime("us"),
            "fill_ts": pl.Datetime("us"),
            "exit_ts": pl.Datetime("us"),
            "gross_return": pl.Float64,
            "cost": pl.Float64,
            "net_return": pl.Float64,
            "turnover": pl.Float64,
            "equity": pl.Float64,
            "n_positions": pl.Int64,
            "gross_exposure": pl.Float64,
            "net_exposure": pl.Float64,
        }
        return pl.DataFrame(rows, schema=schema)

    @staticmethod
    def _weights_frame(rows: list[dict]) -> pl.DataFrame:
        schema = {
            "rebalance_ts": pl.Datetime("us"),
            "fill_ts": pl.Datetime("us"),
            "asset_id": pl.Utf8,
            "weight": pl.Float64,
        }
        return pl.DataFrame(rows, schema=schema)

    @staticmethod
    def _ic_frame(rows: list[dict]) -> pl.DataFrame:
        schema = {
            "rebalance_ts": pl.Datetime("us"),
            "signal": pl.Utf8,
            "ic": pl.Float64,
            "n_assets": pl.Int64,
        }
        return pl.DataFrame(rows, schema=schema)
