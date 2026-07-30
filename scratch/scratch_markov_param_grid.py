#!/usr/bin/env python3
"""Walk-forward parameter grid for the markov_mean_reversion signal.

This is the tool that fills in Sections 4 and 5 of
`signals/methodology/markov_mean_reversion.md`. It answers three questions, in
descending order of importance:

1. **Does the signal survive out of sample?** Parameters are selected on data
   the evaluation never sees. The sample is cut into contiguous folds; for each
   fold the best parameters on everything *before* it are chosen, then scored on
   the fold itself, and the fold results are stitched into one out-of-sample
   series. The gap between that number and the best full-sample number is the
   overfitting tax, and it is the single most informative output here.
2. **Is it robust across the grid, or does it work at one setting?** The report
   shows the marginal effect of each parameter — mean IR and mean IC by value —
   so a signal that only works at one `matrix_lookback_days` is visible as such.
   Per the methodology doc, that pattern is evidence of overfitting, not of a
   signal.
3. **Does it survive costs?** The selected parameters are re-run at 2x the
   assumed spread. A signal that only pays at optimistic costs does not pay.

Read-only and offline: no exchange connection, no order placement. Runs only
under PAPER mode like every other scratch script.

Usage
-----
    # No backfill yet? Runs against generated data in a temp store.
    PAPER=true python scratch/scratch_markov_param_grid.py --synthetic

    # Against the real store, quick grid, weekly rebalance
    PAPER=true python scratch/scratch_markov_param_grid.py --grid quick

    # Full grid on backfilled history (see the note about --pit-mode below)
    PAPER=true python scratch/scratch_markov_param_grid.py \
        --grid full --rebalance weekly --folds 4 --pit-mode event \
        --start 2021-01-01 --out scratch/output/markov_grid.csv

A backfill stamps every row with the same `ingested_ts`, so strict
`--pit-mode ingestion` sees nothing and every book comes back empty. The script
detects that case and tells you. Event-mode results are research indications,
not live-fidelity results, and must be labelled that way in the doc.
"""

import argparse
import itertools
import logging
import random
import sys
import tempfile
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import polars as pl

sys.path.insert(0, str(Path(__file__).parent.parent))

from backtest import (  # noqa: E402
    PIT_EVENT,
    PIT_INGESTION,
    Backtester,
    CostModel,
    DatastoreUniverse,
    long_short_from_scores,
)
from config import PAPER, BacktestConfig  # noqa: E402
from datastore import ParquetStore  # noqa: E402
from loaders.schemas import OHLCV_SCHEMA  # noqa: E402
from signals import CachedClosePanel  # noqa: E402
from signals import markov_mean_reversion as mmr  # noqa: E402

logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("markov_grid")

SWEPT_FIELDS = (
    "n_states",
    "return_horizon_days",
    "state_window_days",
    "matrix_lookback_days",
    "min_obs_per_state",
)

GRIDS = {
    # One axis at a time around the defaults: cheap, and enough to see whether
    # the signal is a knife edge before paying for the full product.
    "quick": {
        "n_states": [3, 5, 7],
        "return_horizon_days": [5],
        "state_window_days": [60],
        "matrix_lookback_days": [90, 180, 250],
        "min_obs_per_state": [10],
    },
    "full": {
        "n_states": [3, 5, 7],
        "return_horizon_days": [3, 5, 10],
        "state_window_days": [40, 60, 90],
        "matrix_lookback_days": [90, 180, 250],
        "min_obs_per_state": [5, 10, 20],
    },
}


# ---------------------------------------------------------------------------
# Synthetic data (so the script runs before a backfill exists)
# ---------------------------------------------------------------------------

def make_synthetic_store(start: datetime, end: datetime, n_assets: int = 24) -> ParquetStore:
    """A temp store of daily bars: some mean-reverting assets, some trending.

    Deliberately mixed. A sweep that only ever sees mean-reverting series would
    flatter the signal, and the point of this script is to not be flattered.
    """
    random.seed(11)
    store = ParquetStore(Path(tempfile.mkdtemp(prefix="markov_grid_")))

    rows = []
    for i in range(n_assets):
        reverting = i % 2 == 0
        price, level = 100.0 * (1.0 + i), 100.0 * (1.0 + i)
        drift = 0.0 if reverting else random.choice([-0.0008, 0.0012])
        vol = random.uniform(0.02, 0.05)

        day = start
        while day <= end:
            pull = -0.05 * (price / level - 1.0) if reverting else 0.0
            open_px = price
            price = max(price * (1.0 + drift + pull + random.gauss(0.0, vol)), 1e-6)
            level *= 1.0 + drift
            rows.append(
                (
                    f"SYN{i:02d}", "binance", "1d", day, day,
                    open_px, max(open_px, price), min(open_px, price), price,
                    5_000_000.0,
                )
            )
            day += timedelta(days=1)

    frame = pl.DataFrame(
        rows,
        schema=[
            "asset_id", "venue", "timeframe", "event_ts", "ingested_ts",
            "open", "high", "low", "close", "volume",
        ],
        orient="row",
    ).with_columns(
        pl.col("event_ts").cast(pl.Datetime("us")),
        pl.col("ingested_ts").cast(pl.Datetime("us")),
    )
    store.append("ohlcv_daily", frame, OHLCV_SCHEMA)
    return store


# ---------------------------------------------------------------------------
# One backtest run
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class RunResult:
    """Headline numbers from one (params, window) backtest."""

    params: mmr.MarkovMeanReversionParams
    start: datetime
    end: datetime
    n_periods: int
    mean_ic: float | None
    ic_ir: float | None
    ann_return: float
    ann_vol: float
    information_ratio: float
    max_drawdown: float
    avg_turnover: float
    total_cost: float
    periods_per_year: float
    net_returns: list[float]

    def row(self) -> dict:
        """Flat dict for a results table (parameters expanded into columns)."""
        row = {field: getattr(self.params, field) for field in SWEPT_FIELDS}
        row.update(
            start=self.start.date().isoformat(),
            end=self.end.date().isoformat(),
            n_periods=self.n_periods,
            mean_ic=self.mean_ic,
            ic_ir=self.ic_ir,
            ann_return=self.ann_return,
            ann_vol=self.ann_vol,
            information_ratio=self.information_ratio,
            max_drawdown=self.max_drawdown,
            avg_turnover=self.avg_turnover,
            total_cost=self.total_cost,
        )
        return row

    def objective(self, name: str) -> float:
        """Selection objective; -inf when undefined so it is never selected."""
        value = {
            "ic_ir": self.ic_ir,
            "mean_ic": self.mean_ic,
            "net_ir": self.information_ratio,
        }[name]
        if value is None or not np.isfinite(value):
            return float("-inf")
        return float(value)


class GridRunner:
    """Runs the signal through the engine for a given parameter set and window."""

    def __init__(self, store, args, panel: CachedClosePanel):
        self.store = store
        self.args = args
        self.panel = panel
        self.universe_provider = self._universe_provider()
        self.runs = 0
        self.seconds = 0.0

    def _universe_provider(self):
        """Phase 3 universe if the store has one; otherwise everything tradable."""
        if "universe" not in self.store.list_datasets():
            logger.warning("No universe dataset; using every asset with a bar (engine default)")
            return None
        return DatastoreUniverse(self.store, venue=self.args.venue, pit_mode=self.args.pit_mode)

    def run(
        self,
        params: mmr.MarkovMeanReversionParams,
        start: datetime,
        end: datetime,
        cost_multiple: float = 1.0,
    ) -> RunResult:
        signal = mmr.make_signal(
            params=params, standardize=True, panel_source=self.panel.with_params(params)
        )
        config = BacktestConfig(
            rebalance_freq=self.args.rebalance,
            backtest_start=start.date().isoformat(),
            backtest_end=end.date().isoformat(),
            spread_bps=self.args.spread_bps * cost_multiple,
            impact_bps_per_million=self.args.impact_bps * cost_multiple,
            venue=self.args.venue,
        )
        backtester = Backtester(
            self.store,
            config=config,
            cost_model=CostModel(
                spread_bps=config.spread_bps,
                impact_bps_per_million=config.impact_bps_per_million,
            ),
            universe_provider=self.universe_provider,
            pit_mode=self.args.pit_mode,
        )

        began = time.perf_counter()
        result = backtester.run(
            long_short_from_scores(signal, n_per_side=self.args.n_per_side),
            signals={mmr.SIGNAL_ID: signal},
            start=start,
            end=end,
        )
        self.seconds += time.perf_counter() - began
        self.runs += 1

        summary = result.ic_summary()
        metrics = result.metrics
        return RunResult(
            params=params,
            start=start,
            end=end,
            n_periods=metrics.n_periods,
            mean_ic=summary["mean_ic"][0] if len(summary) else None,
            ic_ir=summary["ic_ir"][0] if len(summary) else None,
            ann_return=metrics.annualized_return,
            ann_vol=metrics.annualized_vol,
            information_ratio=metrics.information_ratio,
            max_drawdown=metrics.max_drawdown,
            avg_turnover=metrics.avg_turnover,
            total_cost=metrics.total_cost,
            periods_per_year=metrics.periods_per_year,
            net_returns=result.returns["net_return"].to_list() if len(result.returns) else [],
        )


# ---------------------------------------------------------------------------
# Grid and folds
# ---------------------------------------------------------------------------

def build_grid(name: str) -> list[mmr.MarkovMeanReversionParams]:
    """Every parameter combination in the named grid, defaults for the rest."""
    axes = GRIDS[name]
    combinations = []
    for values in itertools.product(*(axes[field] for field in SWEPT_FIELDS)):
        combinations.append(mmr.with_params(**dict(zip(SWEPT_FIELDS, values, strict=True))))
    return combinations


def fold_windows(dates: list[datetime], n_folds: int) -> list[tuple[datetime, datetime]]:
    """Split bar dates into `n_folds` contiguous windows of roughly equal length."""
    if n_folds < 2:
        raise ValueError("need at least 2 folds: one to select on, one to evaluate on")
    size = len(dates) // n_folds
    if size < 3:
        raise ValueError(
            f"{len(dates)} bars over {n_folds} folds leaves {size} bars per fold; "
            "a walk-forward fold needs at least a rebalance, a fill and an exit"
        )
    edges = [i * size for i in range(n_folds)] + [len(dates)]
    return [(dates[edges[i]], dates[edges[i + 1] - 1]) for i in range(n_folds)]


def label(params: mmr.MarkovMeanReversionParams) -> str:
    return (
        f"k={params.n_states} h={params.return_horizon_days} "
        f"w={params.state_window_days} L={params.matrix_lookback_days} "
        f"min={params.min_obs_per_state}"
    )


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

def show(value, spec: str = "+.4f") -> str:
    """Format a metric, or "n/a" when it is undefined."""
    if value is None or (isinstance(value, float) and not np.isfinite(value)):
        return "n/a"
    return format(value, spec)


def print_grid_table(results: list[RunResult], objective: str, top: int = 15) -> None:
    ordered = sorted(results, key=lambda r: r.objective(objective), reverse=True)
    print(f"\nFull-sample grid, best {min(top, len(ordered))} of {len(ordered)} by {objective}")
    print("  (in-sample: every cell saw the whole window, so these are NOT evidence)")
    print(
        f"  {'parameters':40s} {'meanIC':>8s} {'ICIR':>7s} {'annRet':>8s} "
        f"{'annVol':>7s} {'IR':>7s} {'maxDD':>8s} {'turn':>6s} {'per':>5s}"
    )
    for result in ordered[:top]:
        print(
            f"  {label(result.params):40s} {show(result.mean_ic):>8s} {show(result.ic_ir, '+.2f'):>7s} "
            f"{show(result.ann_return, '+.2%'):>8s} {show(result.ann_vol, '.2%'):>7s} "
            f"{show(result.information_ratio, '+.2f'):>7s} {show(result.max_drawdown, '+.1%'):>8s} "
            f"{show(result.avg_turnover, '.2f'):>6s} {result.n_periods:>5d}"
        )


def print_sensitivity(results: list[RunResult]) -> None:
    """Marginal effect of each parameter: the robustness question, plainly."""
    print("\nParameter sensitivity (mean over all other settings)")
    print("  A signal that only works at one value of a parameter is an overfit.")
    for field in SWEPT_FIELDS:
        values = sorted({getattr(r.params, field) for r in results})
        if len(values) < 2:
            continue
        print(f"  {field}")
        for value in values:
            subset = [r for r in results if getattr(r.params, field) == value]
            ics = [r.mean_ic for r in subset if r.mean_ic is not None]
            irs = [r.information_ratio for r in subset if np.isfinite(r.information_ratio)]
            positive = sum(1 for ic in ics if ic > 0)
            print(
                f"    {value:>5}: mean IC {show(np.mean(ics) if ics else None)}  "
                f"mean IR {show(np.mean(irs) if irs else None, '+.2f')}  "
                f"IC>0 in {positive}/{len(ics)} cells"
            )


def print_walk_forward(
    selections: list[tuple[tuple[datetime, datetime], RunResult, RunResult]],
    objective: str,
) -> list[float]:
    """Per-fold selection table; returns the stitched out-of-sample return series."""
    print(f"\nWalk-forward: parameters chosen on prior folds only, by {objective}")
    print(
        f"  {'test window':25s} {'selected parameters':40s} "
        f"{'trainObj':>9s} {'testIC':>8s} {'testIR':>8s} {'per':>5s}"
    )
    stitched: list[float] = []
    for (start, end), train, test in selections:
        window = f"{start.date()} → {end.date()}"
        print(
            f"  {window:25s} {label(test.params):40s} "
            f"{show(train.objective(objective), '+.3f'):>9s} {show(test.mean_ic):>8s} "
            f"{show(test.information_ratio, '+.2f'):>8s} {test.n_periods:>5d}"
        )
        stitched.extend(test.net_returns)
    return stitched


def print_verdict(
    stitched: list[float],
    full_sample: list[RunResult],
    default_oos: list[RunResult],
    periods_per_year: float,
    objective: str,
) -> None:
    from backtest import annualized_return, annualized_vol, information_ratio, max_drawdown

    default_stitched = [r for run in default_oos for r in run.net_returns]

    print("\nVerdict")
    if not stitched:
        print("  No out-of-sample periods were realized; nothing to judge.")
        return

    rows = [
        ("walk-forward OOS (selected per fold)", stitched),
        ("out-of-sample at default parameters", default_stitched),
    ]
    for name, series in rows:
        if not series:
            print(f"  {name:38s} n/a")
            continue
        print(
            f"  {name:38s} IR {show(information_ratio(series, periods_per_year), '+.2f')}  "
            f"annRet {show(annualized_return(series, periods_per_year), '+.2%')}  "
            f"annVol {show(annualized_vol(series, periods_per_year), '.2%')}  "
            f"maxDD {show(max_drawdown(series), '+.1%')}  ({len(series)} periods)"
        )
    by_objective = max(full_sample, key=lambda r: r.objective(objective))
    ceiling = max(full_sample, key=lambda r: r.information_ratio)
    print(
        f"  {'best full-sample cell by ' + objective:38s} "
        f"IR {show(by_objective.information_ratio, '+.2f')}  "
        f"meanIC {show(by_objective.mean_ic)}  [{label(by_objective.params)}]"
    )
    print(
        f"  {'best full-sample IR (cherry-picked)':38s} "
        f"IR {show(ceiling.information_ratio, '+.2f')}  "
        f"meanIC {show(ceiling.mean_ic)}  [{label(ceiling.params)}]"
    )

    oos_ir = information_ratio(stitched, periods_per_year)
    print(
        f"\n  Overfitting tax: the cherry-picked full-sample IR "
        f"{show(ceiling.information_ratio, '+.2f')} is what tuning on everything would "
        f"let you quote; the walk-forward OOS IR is {show(oos_ir, '+.2f')}. "
        "The OOS number is the one that goes in Section 5 of the methodology doc."
    )
    if np.isfinite(oos_ir) and oos_ir > ceiling.information_ratio:
        print(
            "  (OOS above the full-sample ceiling means the selected parameters "
            "happened to suit the later folds; with only a handful of folds that is "
            "noise, not evidence of an edge. More folds, or a longer sample.)"
        )
    print(
        "  Prior from the doc is a rank IC of 0.02-0.05. An OOS mean IC far above "
        "that is a bug or look-ahead, not a discovery."
    )


# ---------------------------------------------------------------------------
# Preflight
# ---------------------------------------------------------------------------

def preflight(runner: GridRunner, dates: list[datetime], params) -> bool:
    """Check the signal can score anything at all before burning an hour on a grid."""
    asof = dates[-1]
    universe = (
        list(runner.universe_provider(asof))
        if runner.universe_provider is not None
        else sorted({a for a in runner.panel.bars})
    )
    if not universe:
        print(f"  FAIL: universe is empty at {asof.date()}")
        return False

    panel = runner.panel.with_params(params)
    needed = mmr.min_history_bars(params)
    lengths = [panel.closes_asof(asset, asof).size for asset in universe]
    scorable = sum(1 for n in lengths if n >= needed)
    print(
        f"  universe {len(universe)} assets at {asof.date()}; "
        f"{scorable} have the {needed} bars the default parameters need "
        f"(median available: {int(np.median(lengths)) if lengths else 0})"
    )
    if scorable:
        # A rank-based long/short book refuses to half-form: fewer scored assets
        # than both sides need means an empty book and a flat, meaningless run.
        if scorable < 2 * runner.args.n_per_side:
            print(
                f"  FAIL: only {scorable} scorable assets but --n-per-side "
                f"{runner.args.n_per_side} needs {2 * runner.args.n_per_side}. Every book "
                "would be empty. Lower --n-per-side or widen the universe."
            )
            return False
        return True

    if runner.args.pit_mode == PIT_INGESTION:
        print(
            "  FAIL: nothing is scorable under strict pit_mode='ingestion'. If this "
            "history was backfilled, every row shares one ingested_ts and strict mode "
            "sees nothing until that date. Re-run with --pit-mode event and label the "
            "results as research indications."
        )
    else:
        print("  FAIL: not enough history per asset. Backfill more bars or shrink the grid.")
    return False


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--grid", choices=sorted(GRIDS), default="quick")
    parser.add_argument("--folds", type=int, default=4, help="walk-forward folds (>= 2)")
    parser.add_argument("--rebalance", choices=["daily", "weekly", "monthly"], default="weekly")
    parser.add_argument("--objective", choices=["ic_ir", "mean_ic", "net_ir"], default="ic_ir")
    parser.add_argument("--pit-mode", choices=[PIT_INGESTION, PIT_EVENT], default=PIT_INGESTION)
    parser.add_argument("--venue", default="binance")
    parser.add_argument("--start", default="2021-01-01")
    parser.add_argument("--end", default=None)
    parser.add_argument("--n-per-side", type=int, default=5)
    parser.add_argument("--spread-bps", type=float, default=5.0)
    parser.add_argument("--impact-bps", type=float, default=1.0)
    parser.add_argument("--max-assets", type=int, default=None, help="cap the universe (speed)")
    parser.add_argument("--synthetic", action="store_true", help="generate data in a temp store")
    parser.add_argument("--out", default=None, help="write the full grid table to this CSV")
    return parser.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)

    if not PAPER:
        print("PAPER is False; scratch scripts do not run. Set PAPER=true.")
        return 1

    start = datetime.strptime(args.start, "%Y-%m-%d")
    end = datetime.strptime(args.end, "%Y-%m-%d") if args.end else datetime(2100, 1, 1)

    print("=" * 78)
    print("markov_mean_reversion — walk-forward parameter grid")
    print("=" * 78)

    if args.synthetic:
        synthetic_end = min(end, start + timedelta(days=1_200))
        print(f"\nGenerating synthetic bars {start.date()} → {synthetic_end.date()}")
        store = make_synthetic_store(start, synthetic_end)
    else:
        from config import DATASTORE_PATH

        print(f"\nStore: {DATASTORE_PATH}")
        store = ParquetStore(DATASTORE_PATH)

    grid = build_grid(args.grid)
    print(
        f"Grid '{args.grid}': {len(grid)} cells · rebalance {args.rebalance} · "
        f"pit_mode {args.pit_mode} · objective {args.objective} · {args.folds} folds"
    )
    if args.pit_mode == PIT_EVENT:
        print(
            "  NOTE: pit_mode='event' filters event_ts only. No defence against "
            "revisions or late data — research indications, not live-fidelity results."
        )

    print("\nLoading close panel (one store read for the whole sweep)...")
    began = time.perf_counter()
    panel = CachedClosePanel(
        store,
        params=mmr.DEFAULT_PARAMS,
        venue=args.venue,
        timeframe="1d",
        pit_mode=args.pit_mode,
    )
    if args.max_assets is not None and len(panel.bars) > args.max_assets:
        keep = sorted(panel.bars)[: args.max_assets]
        panel.bars = {asset: panel.bars[asset] for asset in keep}
    print(f"  {len(panel.bars)} assets in {time.perf_counter() - began:.1f}s")
    if not panel.bars:
        print("  FAIL: no bars in the store. Run the loaders first, or pass --synthetic.")
        return 1

    runner = GridRunner(store, args, panel)
    dates = Backtester(store, venue=args.venue).load_price_panel(start, end).dates
    if len(dates) < 3:
        print(f"  FAIL: only {len(dates)} bars between {start.date()} and {end.date()}")
        return 1
    print(f"  {len(dates)} bars {dates[0].date()} → {dates[-1].date()}")

    print("\nPreflight")
    if not preflight(runner, dates, mmr.DEFAULT_PARAMS):
        return 1

    try:
        folds = fold_windows(dates, args.folds)
    except ValueError as exc:
        print(f"  FAIL: {exc}")
        return 1

    # --- full-sample grid (in-sample; for sensitivity, never for evidence) ---
    print(f"\nRunning {len(grid)} cells over the full sample...")
    full_sample: list[RunResult] = []
    for i, params in enumerate(grid, start=1):
        result = runner.run(params, dates[0], dates[-1])
        full_sample.append(result)
        print(
            f"  [{i:>3}/{len(grid)}] {label(params):40s} "
            f"meanIC {show(result.mean_ic):>8s}  IR {show(result.information_ratio, '+.2f'):>6s}  "
            f"({runner.seconds:.0f}s elapsed)"
        )

    print_grid_table(full_sample, args.objective)
    print_sensitivity(full_sample)

    # --- walk-forward: select on prior folds, evaluate on the next -----------
    print(f"\nRunning walk-forward over {len(folds)} folds...")
    selections = []
    default_oos = []
    for i in range(1, len(folds)):
        train_start, train_end = folds[0][0], folds[i - 1][1]
        test_start, test_end = folds[i]

        trained = [runner.run(params, train_start, train_end) for params in grid]
        best_train = max(trained, key=lambda r: r.objective(args.objective))
        if best_train.objective(args.objective) == float("-inf"):
            print(f"  fold {i}: no cell produced a usable objective on the training window")
            continue

        tested = runner.run(best_train.params, test_start, test_end)
        selections.append(((test_start, test_end), best_train, tested))
        default_oos.append(runner.run(mmr.DEFAULT_PARAMS, test_start, test_end))
        print(f"  fold {i}/{len(folds) - 1} done ({runner.seconds:.0f}s elapsed)")

    stitched = print_walk_forward(selections, args.objective)
    # Annualize the stitched series at the engine's own factor (holding periods
    # per year, not bars per year), averaged over the folds it came from.
    realized = [s[2] for s in selections] or full_sample
    periods_per_year = float(np.mean([r.periods_per_year for r in realized]))
    print_verdict(stitched, full_sample, default_oos, periods_per_year, args.objective)

    # --- cost sensitivity ---------------------------------------------------
    print("\nCost sensitivity at the most-selected parameters")
    if selections:
        chosen = max(
            {label(s[2].params) for s in selections},
            key=lambda name: sum(1 for s in selections if label(s[2].params) == name),
        )
        params = next(s[2].params for s in selections if label(s[2].params) == chosen)
        for multiple in (1.0, 2.0):
            result = runner.run(params, dates[0], dates[-1], cost_multiple=multiple)
            print(
                f"  {multiple:.0f}x costs: IR {show(result.information_ratio, '+.2f')}  "
                f"annRet {show(result.ann_return, '+.2%')}  "
                f"total cost {show(result.total_cost, '.4f')}"
            )
        print("  A signal that only survives at 1x is not a signal that survives.")

    if args.out:
        out_path = Path(args.out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        table = pl.DataFrame([r.row() for r in full_sample])
        for (test_start, test_end), _, tested in selections:
            row = tested.row()
            row["window"] = f"oos {test_start.date()}→{test_end.date()}"
            table = pl.concat([table, pl.DataFrame([row])], how="diagonal")
        table.write_csv(out_path)
        print(f"\nWrote {len(table)} rows to {out_path}")

    if args.synthetic:
        print(
            "\n  REMINDER: --synthetic bakes mean reversion into half the assets by "
            "construction. These numbers show the machinery works end to end; they are "
            "not evidence about the signal. Nothing here belongs in the methodology doc."
        )

    print(
        f"\n{runner.runs} backtests in {runner.seconds:.0f}s. "
        "Record the run config and the OOS numbers in "
        "signals/methodology/markov_mean_reversion.md Sections 4 and 5 — "
        "and if the OOS IR is not there, say so in the doc rather than retuning "
        "until it is."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
