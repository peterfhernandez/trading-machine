# Poor Man's Trading Machine

A single-person, low-cost implementation of a multifactor crypto trading system.

**Status: Phase 5 (Signals → alphas) — In progress · Phase 5.5 (Logging retrofit) — designed, not yet applied**

## Quick Start

### Prerequisites

- Python 3.12+
- pip/poetry for dependency management

### Installation

```bash
# Clone the repo
git clone <repo-url>
cd trading-machine

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -e ".[dev,optimizer]"
```

### Configuration

All configuration lives in `config.py`. Key settings:

- **Paths**: `DATASTORE_PATH`, `LOGS_PATH`, `SCRATCH_PATH`
- **Trading Mode**: `PAPER` flag (True = paper trading, False = disabled for scratch)
- **Venue Keys**: Set via environment variables
  - `BINANCE_API_KEY`, `BINANCE_API_SECRET`
  - `DERIBIT_CLIENT_ID`, `DERIBIT_CLIENT_SECRET`
- **Alerts**: `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`
- **Logging**: `LOG_CONFIG` — the existing `level`/`file` stub in `config.py`,
  extended with `console_level`, `dir` (replaces the single `file` path),
  `max_bytes` (10 MB), `retention_days` (365), and `components`. See
  [Logging](#logging) below and `LOGGING.md`.

Example:

```bash
export DERIBIT_CLIENT_ID="your-id"
export DERIBIT_CLIENT_SECRET="your-secret"
export TELEGRAM_BOT_TOKEN="your-token"
export TELEGRAM_CHAT_ID="your-chat-id"
export PAPER=true
```

## Architecture

```text
datastore/     — Parquet store + asset master
loaders/       — Data loaders (OHLCV, funding rates, etc.)
audit/         — Data quality checks
universe/      — Universe membership rules
backtest/      — Walk-forward backtester
signals/       — Alpha signals (momentum, carry, etc.)
risk/          — Factor risk model
portfolio/     — Portfolio construction & optimizer
execution/     — Paper broker + exchange testnet
attribution/   — Performance & PnL attribution
pipeline/      — Daily scheduled job orchestration

tests/         — pytest test suite
scratch/       — Research notebooks & demo scripts
config.py      — Central configuration
logging_config.py — Logging setup (get_logger, run_id, retention pruning); see Logging below
logs/          — Rotating, retained per-component log files (git-ignored)
```

## Project Structure

### Phases

This project follows a strict phased build order defined in `TODO.md`. Each phase:

- Produces something runnable and tested
- Has narrowly-scoped public interfaces
- Communicates only through the datastore and typed dataclasses

### Completed: Phase 3 (Universe)

- [x] Liquidity metrics (rolling median dollar volume), listing-age filter, stablecoin/wrapped exclusions
- [x] Daily universe membership written point-in-time to the store
- [x] Sanity scratch: universe size and turnover over history

### Completed: Phase 4 (Backtester)

- [x] Walk-forward engine: rebalance calendar, point-in-time data exposure, next-bar execution
- [x] Cost model: half-spread + linear impact stub, charged on traded notional
- [x] Metrics: returns, vol, IR, drawdown, turnover, per-signal rank IC series
- [x] Golden tests: hand-computed 3-asset fixture reproduced exactly
- [x] Validated: buy-and-hold BTC through the engine matches the raw price series

### Current Phase: Phase 5 (Signals → alphas)

- [x] Signal interface + registry (registration requires a methodology doc)
- [x] Cross-sectional transforms: winsorize then z-score, `None` preserved
- [x] `markov_mean_reversion` signal + walk-forward parameter grid script
- [ ] Backtest evidence for `markov_mean_reversion` against a real backfill
- [ ] Momentum, carry, short-term reversal, low-volatility signals
- [ ] Alpha refinement (IC estimation + shrinkage) and the breadth report

### Next: Phase 5.5 (Logging & Observability Retrofit)

- [x] Logging architecture designed (`LOGGING.md`) and reference implementation
      written (`logging_config.py`)
- [ ] Retrofit into Phases 1-4 and current Phase 5 source (see `TODO.md`)

## Testing

```bash
# Run all tests
pytest

# Run specific test file
pytest tests/test_datastore.py

# Run with coverage
pytest --cov=datastore --cov=loaders tests/

# Run only unit tests
pytest -m unit

# Run with verbose output
pytest -v
```

## Code Quality

```bash
# Lint with ruff
ruff check .

# Format with ruff
ruff format .

# Type checking (mypy)
mypy datastore/ loaders/ audit/ universe/ backtest/ signals/
```

## Logging

Every module gets a logger via `logging_config.get_logger(__name__)`. Each
pipeline component (`pipeline`, `datastore`, `loaders`, `audit`, `universe`,
`backtest`, `signals`, and — from Phase 6 on — `risk`, `portfolio`,
`execution`, `attribution`) writes structured JSON to its own file under
`logs/`, e.g. `logs/loaders.log`. Files rotate at 10 MB; rotated backups are
timestamped and pruned once older than 12 months
(`logging_config.prune_old_logs`, run as the last step of the nightly
pipeline). A `run_id` is attached to every record so one pipeline run can be
traced across every component's log file.

```python
from logging_config import get_logger

log = get_logger(__name__)
log.info("loaded %d symbols", len(symbols))
```

Logs are distinct from Telegram alerts: logs are the always-written technical
record of what happened in every run; alerts fire only on audit threshold
breaches, execution drift, and the kill switch, and are meant for a human to
see immediately. A halt or alert always has a matching CRITICAL log line, so
the durable record doesn't depend on the Telegram send succeeding.

Full design rationale — why rotation and retention are decoupled, log levels,
the per-module retrofit map — is in `LOGGING.md`.

## Development

### Adding a new module

1. Create package directory with `__init__.py`
2. Add public interface (typed dataclasses, main functions)
3. Write tests in `tests/test_<module>.py`
4. Create scratch script in `scratch/scratch_<module>.py` (demo only)
5. Wire logging via `get_logger(__name__)` (see Logging above and `LOGGING.md`)
6. Update `README.md` progress

### Datastore Access

All modules access data through the datastore API:

```python
from datastore import ParquetStore, AssetMaster

store = ParquetStore(config.DATASTORE_PATH)

# Write
store.append("ohlcv", df, schema=my_schema)

# Read (point-in-time)
df = store.read("ohlcv", date_range=("2024-01-01", "2024-12-31"), asof="2024-12-31")
```

### Duplicate bars

The store is append-only, so a bar fetched twice is **stored** twice: overlapping
loader windows, a retried run, and genuine vendor revisions all look the same on
disk. That is deliberate — keeping the older copy is what lets a backtest ask
what a bar looked like before it was revised — so duplicates are a *read-side*
concern, not something the writer should prevent.

Every reader that aggregates rows collapses each bar to its latest ingestion via
`datastore.latest_per_bar`:

```python
from datastore import latest_per_bar, count_duplicate_bars

df = store.read("ohlcv_daily", asof="2026-07-30")   # point-in-time filter first
count_duplicate_bars(df)                             # 12 -> rows that repeat a bar
df = latest_per_bar(df)                              # one row per (asset_id, event_ts)
```

Order matters: collapse **after** any `ingested_ts <= asof` filter. Collapsing
first would let a later revision win the bar and then be filtered away, hiding a
bar that was knowable at `asof`.

The nightly audit reports the duplicate rate as a `duplicate_bars` warning (never
a halt — re-ingestion is expected); a rate that climbs run over run means the
loaders are re-fetching history the store already has.

```bash
PAPER=true python scratch/scratch_duplicate_bars.py   # demo
```

### Loaders

Three data loaders fetch from Binance/Deribit via ccxt and ingest to the datastore:

```python
from loaders import OHLCVLoader, FundingRateLoader, OpenInterestLoader

# Fetch last 7 days of daily and hourly OHLCV
loader = OHLCVLoader("binance", lookback_days=7)
rows = loader.run_daily()      # fetches, appends, returns rows appended
loader.run_hourly()

# Fetch funding rates and open interest
fr_loader = FundingRateLoader("binance", lookback_days=30)
fr_loader.run()

oi_loader = OpenInterestLoader("binance", lookback_days=30)
oi_loader.run()
```

Each `run*()` fetches once and returns the number of rows appended (0 if the
venue returned nothing). Callers must not call `fetch()` first to test for
emptiness — that pulls every symbol from the venue a second time.

**Market types.** Funding rate and open interest exist only on derivatives, so
those two loaders open the venue with `defaultType=LOADER_CONFIG.perp_market_type`
(`"future"`) and select perpetual symbols (`BTC/USDT:USDT`, excluding dated
futures). The OHLCV loader stays on the venue's default (spot) markets. Both
symbol namespaces are registered in the asset master by the nightly pipeline, so
either notation resolves to the same canonical `asset_id`.

**Symbol budget.** Each loader fetches up to `LOADER_CONFIG.max_symbols_per_run`
(200) USDT-quoted symbols. The USDT filter is applied *before* the cap — a
venue's symbol list interleaves every quote currency alphabetically, so capping
first and filtering after silently starves the budget.

**Historical data.** Funding rate and open interest have two endpoints per
venue: a *snapshot* of the current value, and a *history* over a window. The
loaders prefer history when the venue advertises it (`fetchFundingRateHistory`,
`fetchOpenInterestHistory`) and fall back to the snapshot otherwise. A snapshot
can only answer for now, so with the fallback a request for a past window
returns nothing rather than stamping today's value with a historical timestamp.
Binance's funding history carries the rate alone — `mark_price` and
`index_price` are snapshot-only fields and stay null on historical rows, which
`AUDIT_CONFIG.nullable_columns_by_dataset` records so the audit warns instead of
halting.

**Backfill runner** orchestrates all loaders over an explicit, resumable window:

```python
from loaders import BackfillRunner
from datetime import datetime

runner = BackfillRunner("binance")
runner.run(start_date=datetime(2024, 3, 1), end_date=datetime(2024, 3, 8))
runner.run(days_back=30)                          # or a lookback from now
```

Each dataset records the interval it has covered in
`data/checkpoints/<venue>_backfill.json`, and a later run fetches only what is
missing:

```text
run 1  2024-03-01..2024-03-08   -> fetches 7 days
run 2  2024-03-01..2024-03-08   -> fetches 2024-03-07..2024-03-08 (overlap only)
run 3  2024-03-01..2024-03-15   -> fetches 2024-03-07..2024-03-15
run 4  2023-06-01..2023-07-01   -> fetches all of it (older than what is covered)
```

Three properties worth knowing:

- **The overlap is deliberate.** Resuming re-fetches
  `LOADER_CONFIG.refetch_overlap_days` (1) before the covered end, because the
  trailing bar of a run is usually incomplete — a daily candle fetched at 00:10
  UTC covers ten minutes of the current day — and venues revise recent bars.
  Those duplicates are collapsed on read (see duplicate bars above).
- **Coverage is an interval, not a high-water mark.** A request reaching back
  before the covered range is fetched in full: a single "last date" cannot prove
  older history is present.
- **Long windows are paged.** A venue caps one response
  (`LOADER_CONFIG.page_limit`, 1000 rows); windows longer than that are walked
  forward, bounded by `LOADER_CONFIG.max_pages_per_symbol`. Without this a
  five-year daily request came back as the first 1000 days with nothing to say
  the rest was missing.

Pass `ignore_checkpoint=True` (or `--ignore-checkpoint`) to re-fetch a window
that is already covered.

```bash
PAPER=true python scratch/scratch_windowed_fetch.py   # demo
```

### Audit

Data quality audit with 6 checks: coverage (% of universe), null rates per column,
price outliers, freshness (data age), data presence, and duplicate bars. Sends
Telegram alerts on breaches. Every check except `duplicate_bars` runs against one
row per bar, so a re-ingested bar is never counted twice.

```python
from audit import DataAudit

audit = DataAudit(venue="binance")
results = audit.audit_dataset("ohlcv_daily")
audit.send_alerts()

if audit.should_halt_trading():
    print("Critical failure; trading halted")
```

**Coverage denominator.** "% of universe" needs a universe. The denominator is
the number of members in the latest point-in-time `universe` snapshot with
`event_ts <= asof`, read through `ingested_ts <= asof` — never a hardcoded
target size, which makes the check either vacuous or permanently red depending
on how many assets the loaders happen to cover. With no snapshot available the
check reports itself **not evaluated** (a warning carrying the observed asset
count) instead of halting trading on a fabricated threshold:

```python
DataAudit(store, venue="binance").resolve_universe_size(asof)
# (150, "universe snapshot 2026-07-29")   -> threshold = 90% of 150
# (None, "no universe dataset in the store") -> coverage not evaluated
```

The numerator is universe **members** present in the dataset, so rows for assets
outside the universe cannot inflate coverage. Pass
`DataAudit(store, universe_size=N)` to set the denominator explicitly when the
expected asset count is known from elsewhere.

### Universe

Point-in-time daily universe membership from `ohlcv_daily`: rolling median dollar
volume, minimum listing age, and stablecoin/wrapped exclusions, ranked by liquidity
and capped at `UNIVERSE_CONFIG.target_size`. Writes one row per asset ever
considered (not just members), with `exclusion_reason` explaining any asset left
out (`stablecoin`, `wrapped`, `listing_age`, `low_volume`, or `rank_cutoff`).

```python
from universe import UniverseBuilder, compute_turnover

builder = UniverseBuilder(venue="binance")
snapshot = builder.build_and_store(asof=datetime(2024, 3, 1))

members = snapshot.filter(snapshot["in_universe"])["asset_id"].to_list()

# Turnover between two snapshots (for monitoring universe stability)
turnover = compute_turnover(previous_snapshot, snapshot)
```

### Nightly Pipeline

Run the complete data pipeline end-to-end: load → audit → report.

```bash
# Dry-run mode (simulate without writing)
python -m pipeline.nightly --dry-run --days 1

# Live run: the last day, resuming from the checkpoint
python -m pipeline.nightly --venue binance --days 1

# A specific stretch of history
python -m pipeline.nightly --start 2024-03-01 --end 2024-03-08

# Re-fetch a window the checkpoint already covers
python -m pipeline.nightly --start 2024-03-01 --end 2024-03-08 --ignore-checkpoint
```

`--start`/`--end` take `YYYY-MM-DD` or full ISO 8601 and are UTC; `--days` is a
lookback from now and is ignored when `--start` is given. Datasets produced:
`ohlcv_daily`, `ohlcv_hourly`, `funding_rate`, `open_interest`.

### Backtester

Walk-forward research engine. At each rebalance date the strategy sees only
point-in-time data; weights fill at the **next bar's open**, are held until the
next fill, and pay spread + impact costs on traded notional.

```python
from backtest import Backtester, CostModel, DatastoreUniverse

def momentum(ctx):
    bars = ctx.ohlcv(lookback_days=30)          # only data known at ctx.asof
    return {asset_id: 1 / len(ctx.universe) for asset_id in ctx.universe}

bt = Backtester(
    store,
    cost_model=CostModel(spread_bps=10.0, impact_bps_per_million=1.0),
    universe_provider=DatastoreUniverse(store, "binance"),
)
result = bt.run(momentum, signals={"momentum_30d": score_fn})

result.metrics.to_dict()   # total/annualized return, vol, IR, max drawdown, turnover, costs
result.returns             # per-period gross/cost/net return, turnover, equity, exposures
result.weights             # target weights actually taken, per rebalance
result.ic_summary()        # mean IC, IC volatility, IC IR per signal
```

Engine semantics (fully documented in `backtest/engine.py`):

- **Next-bar execution** — the close of the decision bar is the last thing the
  strategy saw, so fills happen at the following bar's open.
- **Drift-aware turnover** — positions drift with returns between rebalances, so
  a buy-and-hold book pays costs once, not every period.
- **Annualization follows the holding period** — weekly rebalances annualize at
  ~365/7, not 365.
- **`pit_mode`** — `"ingestion"` (default) filters `ingested_ts <= asof`, the
  full look-ahead defence. `"event"` filters `event_ts <= asof` only; it exists
  because a backfill stamps every row with one ingestion timestamp, which would
  otherwise make backfilled history invisible. Event mode is weaker (no defence
  against revisions) and must be chosen explicitly.

```bash
PAPER=true python scratch/scratch_backtest.py   # demo: buy & hold, equal weight, L/S momentum
```

### Signals

A signal is a function `RebalanceContext -> {asset_id: score | None}`, where a
higher score means "more attractive long" and `None` means **no view** — never
`0.0`, which would assert the asset is exactly average.

```python
from backtest import Backtester, long_short_from_scores
from signals import markov_mean_reversion as mmr
from signals import signal_functions

signal = mmr.make_signal()                    # ctx -> standardized scores
result = Backtester(store).run(
    long_short_from_scores(signal, n_per_side=5),
    signals=signal_functions(),               # every registered signal's IC series
)
print(result.ic_summary())
```

Per-asset internals are inspectable, which is what the golden tests assert
against:

```python
diag = mmr.diagnose_series(closes)            # numpy in, no I/O
diag.states, diag.matrix, diag.midpoints      # the construction, step by step
diag.score, diag.reject_reason                # None + why, when unscorable
```

Shared pieces: `signals/transforms.py` (winsorize then z-score a cross-section,
preserving `None`) and `signals/panel.py` (`CachedClosePanel` — a point-in-time
close panel that reads the store once, for parameter sweeps).

**Registered signals**

| Signal | Family | Status |
| --- | --- | --- |
| `markov_mean_reversion` | reversal | implemented; backtest evidence pending |

`markov_mean_reversion` discretizes each asset's standardized rolling return into
`n_states` quantile states, estimates a rolling transition matrix, and scores the
gap between the current state and the probability-weighted expected next state.
It needs 244 bars per score at the default parameters and returns `None` for any
asset with less, with a gap in its recent bars, or with too few observed
transitions out of its current state.

**Every signal gets a methodology doc** in `signals/methodology/` (hypothesis,
data inputs and their point-in-time contract, construction, parameters, backtest
evidence, breadth check, failure modes) — copy `TEMPLATE.md`. The doc is the spec
and comes first: `signals.register` raises if a signal's doc does not exist.

```bash
PAPER=true python scratch/scratch_signal_markov_mean_reversion.py   # demo
```

### Parameter sweeps

`scratch/scratch_markov_param_grid.py` walks a parameter grid forward through the
sample: for each fold it selects parameters using only the folds before it, then
evaluates on the fold itself, and stitches the fold results into one
out-of-sample series. It reports the gap against the best full-sample cell (the
overfitting tax), the marginal effect of each parameter (a signal that works at
exactly one setting is an overfit), and performance at 2x assumed costs.

```bash
# Runs against generated data if you have no backfill yet
PAPER=true python scratch/scratch_markov_param_grid.py --synthetic

# Against the store: quick grid (9 cells), weekly rebalance, 4 folds
PAPER=true python scratch/scratch_markov_param_grid.py --grid quick --folds 4

# Full grid (243 cells) on backfilled history — see the pit_mode note below
PAPER=true python scratch/scratch_markov_param_grid.py \
    --grid full --pit-mode event --out scratch/output/markov_grid.csv
```

A backfill stamps every row with one `ingested_ts`, so strict
`--pit-mode ingestion` sees nothing until that date and every book comes back
empty. The script's preflight detects this and says so. Event-mode numbers are
research indications, not live-fidelity results, and the methodology doc requires
them to be labelled that way.

## Important Principles

1. **Point-in-time discipline**: Every dataset row carries `event_ts` and `ingested_ts`.
2. **Append-only**: Never overwrite history in the datastore.
3. **No look-ahead bias**: Backtests only read data with `ingested_ts <= asof`
   (relaxable to `event_ts <= asof` for backfilled history — explicitly, via
   `pit_mode="event"`, never by default).
4. **Risk model is not optional**: Half the value is understanding volatility costs.
5. **Breadth over hero bets**: Many independent bets beat big single bets.
6. **Measure costs pessimistically**: Model spreads, impact, slippage from day one.
7. **Observability by default**: every module logs to `logs/<component>.log`;
   alerts are for a human's immediate attention, logs are the durable record
   an unattended run can be reconstructed from. See `LOGGING.md`.

## Progress

See `TODO.md` for detailed phase breakdown and progress log.

## References

- **Source**: "What Nobody Tells You About Being a Quant" (The Quant Insider)
- **Architecture**: See `PLAN.md` for detailed design rationale
- **Build Order**: See `TODO.md` for phased implementation plan
- **Logging**: See `LOGGING.md` for the observability design

---

**Next Phase**: finish Phase 5 (remaining signals, alpha refinement), then
Phase 5.5 — retrofit logging into Phases 1-5 (see `TODO.md` and `LOGGING.md`)
