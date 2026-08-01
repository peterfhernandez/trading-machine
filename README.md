# Poor Man's Trading Machine

A single-person, low-cost implementation of a multifactor crypto trading system.

**Status: Phase 5 (Signals → alphas) — code complete, backtest evidence pending a real backfill · Phase 5.5 (Logging & observability) — applied and audited · Phase 5.6 (CI, test isolation, observability fixes) — applied**

## Quick Start

### Prerequisites

- Python 3.11 or 3.12 (both are covered by CI; `requires-python = ">=3.11"`)
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
  `max_bytes` (10 MB), `retention_days` (365), and `components`. Levels and the
  log directory are overridable without editing the file: `TM_LOG_LEVEL`,
  `TM_CONSOLE_LOG_LEVEL`, `TM_LOG_DIR`. See [Logging](#logging) below and
  `LOGGING.md`.

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
.github/workflows/ — CI (pre-merge) and deploy (test-then-pull); see Testing and CI
config.py      — Central configuration
logging_config.py — Logging setup (get_logger, run_id, retention pruning); see Logging below
pipeline/prune_logs.py — Standalone log retention (`python -m pipeline.prune_logs`)
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
- [x] Cross-sectional transforms: winsorize then z-score (and a scale-only
      variant that keeps the cross-sectional mean), `None` preserved
- [x] Shared point-in-time series reader and primitives (`signals/bars.py`)
- [x] Six signals: `markov_mean_reversion`, `short_term_reversal`,
      `cross_sectional_momentum`, `time_series_momentum`, `carry`,
      `low_volatility` — each with a methodology doc written first
- [x] Alpha refinement: IC estimation with shrinkage, `alpha = vol × IC × z`
- [x] Breadth report: score correlation, IC correlation, effective bet count
- [ ] **Backtest evidence** — every methodology doc's Section 5 is empty and
      every signal is `draft`. This needs a multi-year backfill; a synthetic
      backtest measures the generator, not the signal. See
      [What is not done](#what-is-not-done).

### Completed: Phase 5.5 (Logging & Observability)

- [x] Architecture designed (`LOGGING.md`) and implemented (`logging_config.py`)
- [x] Retrofitted into Phases 1-5 source — every module logs through
      `get_logger(__name__)`, a halt always leaves a CRITICAL record before
      the alert is attempted, and `prune_old_logs()` runs as the nightly
      pipeline's last step

### Completed: Phase 5.6 (CI, test isolation, observability fixes)

Auditing the applied retrofit asked a different question from building it —
*does it work?* — and found four defects in the mechanism:

- [x] `python -m pipeline.nightly` wrote **nothing** to `logs/pipeline.log`
      (its logger was `tm.__main__`, under no component); fixed for every
      future `python -m` entry point, and tested by running the CLI rather
      than importing it
- [x] DEBUG was unreachable without editing `config.py` — `--log-level` and
      `TM_LOG_LEVEL` now exist
- [x] Rotation destroyed a backup when two rotations landed in the same
      second, and cost ~320 ms of syscalls per rotation at the configured
      `backupCount`
- [x] Log files were created for components that do not exist yet
- [x] Test isolation: an autouse fixture keeps every test out of the real
      datastore (the one regression that only failed on a machine with data)
- [x] Methodology docs are parsed and checked against the code; `Status` is a
      queryable field
- [x] Pre-merge CI, and a deploy workflow that tests before it pulls

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
# Lint with ruff — clean, and enforced by CI
ruff check .

# Format with ruff — available, deliberately NOT enforced (see below)
ruff format .

# Type checking (mypy) — advisory in CI
mypy datastore/ loaders/ audit/ universe/ backtest/ signals/
```

`ruff format` is not a gate and the codebase is not formatter-clean: adopting it
would reformat 62 of 87 files in one commit, which is a large diff for no
correctness gain. Run it on a file you are already rewriting if you like, but
don't reformat the repo in a change that does anything else. Line length is
consequently unenforced — `E501` is in the ignore list in `pyproject.toml`,
with the reasoning written down there.

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

A log file appears when its component first logs something, not at import: an
empty `risk.log` would claim a Phase 6 that does not exist yet.

```python
from logging_config import get_logger

log = get_logger(__name__)
log.info("loaded %d symbols", len(symbols))
```

**Turning up the detail.** DEBUG carries the per-asset signal reject reasons,
the backtester's per-rebalance detail, and the duplicate-bar collapse count. It
is off by default and does not require editing `config.py`:

```bash
python -m pipeline.nightly --log-level DEBUG --console-log-level INFO
TM_LOG_LEVEL=DEBUG python -m pipeline.prune_logs      # any entry point
TM_LOG_DIR=/tmp/tm-logs python -m pipeline.nightly    # write elsewhere
```

Logs are distinct from Telegram alerts: logs are the always-written technical
record of what happened in every run; alerts fire only on audit threshold
breaches, execution drift, and the kill switch, and are meant for a human to
see immediately. A halt or alert always has a matching CRITICAL log line, so
the durable record doesn't depend on the Telegram send succeeding.

Retention is a separate mechanism from rotation, and runs separately too:

```bash
python -m pipeline.prune_logs --dry-run   # what would be deleted
python -m pipeline.prune_logs             # delete expired backups
```

```bash
PAPER=true python scratch/scratch_observability.py   # all of the above, demonstrated
```

`NightlyPipeline.run()` already calls it as its final step — in a `finally`, so
a failed run still prunes. The standalone command is for a research box that
runs backtests and sweeps (writing to `backtest.log` and `signals.log`) without
ever running the pipeline.

**A halt always leaves a record.** `DataAudit.should_halt_trading()` logs
CRITICAL from inside the method — so every route to a halt is covered, not just
the ones that remembered to log — and it does so before the Telegram send is
attempted. Telegram being down is exactly the kind of thing that should still
appear in the log.

Full design rationale — why rotation and retention are decoupled, log levels,
the per-module map and the places the retrofit deviated from it — is in
`LOGGING.md`.

## Testing and CI

```bash
pytest                     # 594 tests, ~35s, no network
ruff check .               # clean; CI fails on any finding
```

Two workflows in `.github/workflows/`:

- **`ci.yml`** — on every pull request and every push to `main`, on
  GitHub-hosted runners across Python 3.11 and 3.12: ruff (a hard gate), the
  full suite, and mypy (advisory). Make it a required check on `main` and a
  red suite stops
  being mergeable. It also fails a PR whose commit messages carry tool
  attribution, per `CLAUDE.md` — using the same rule as the three `PreToolUse`
  hooks in `.claude/settings.json`, so a message the hooks accept cannot be
  rejected by CI or vice versa. The rule strips the two repo paths
  (`CLAUDE.md`, `.claude/`) before matching, because naming a file you are
  changing is not attribution, and it skips merge commits, whose subjects
  GitHub generates from `claude/<topic>` branch names.
- **`deploy.yml`** — when a PR merges into `main`, on the self-hosted
  `trading-machine` runner. It **tests before it pulls**: `git fetch` (which
  touches nothing), check the incoming commit out into a throwaway worktree,
  run the suite there, and only then `git reset --hard` the live working copy.
  A failure leaves the machine on the last known-good commit. There is
  deliberately no `git clean` — `data/` and `logs/` are git-ignored and hold
  the datastore and the durable run record.

Three things about the suite worth knowing:

- **pytest ≥ 8.4 is a floor, not a preference.** The `tm` logger tree does not
  propagate to the root logger, and only 8.4+ attaches `caplog` to
  non-propagating loggers. On anything older the assertions that a halt leaves
  a CRITICAL record stop working — the negative ones silently. A canary test
  fails loudly if that ever changes.
- **No test may touch the real datastore.** An autouse fixture
  (`tests/conftest.py::isolate_production_datastore`) redirects every module's
  `DATASTORE_PATH` default to a per-test directory. Without it a loader built
  without an explicit `asset_master=` reads the developer's own
  `data/parquet/asset_master.parquet` — green on a clean checkout, failing on
  the machine that has actually run the pipeline.
- **A green run must exit 0, and on Windows it did not.** Every test passed and
  then pytest exited **1** with `PermissionError: [WinError 5]` from
  `cleanup_dead_symlinks` — its own temp-directory housekeeping tripping over
  the stale `pytest-current` link it keeps in `%TEMP%\pytest-of-<user>\`. The
  removal is unguarded and runs in `pytest_sessionfinish`, which is called from
  a `finally` that catches only `exit.Exception`, so it escapes as a raw
  traceback. That is a deployment gate, not a cosmetic annoyance: `deploy.yml`
  keys `git reset --hard` off this exit code, so the trading machine refused
  commits whose tests had all passed. `tests/conftest.py` replaces the cleanup
  with one that falls back to `rmdir` (how a directory link is removed on
  Windows) and cannot raise; `tests/test_tmpdir_cleanup.py` pins it, exit code
  included.

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

**Registered signals** — all six are implemented and unit-tested; none has
backtest evidence yet (see [What is not done](#what-is-not-done)).

| Signal | Family | Construction | Min. history |
| --- | --- | --- | --- |
| `cross_sectional_momentum` | momentum | 90-day return ending 7 bars back | 98 bars |
| `time_series_momentum` | momentum | same window ÷ the asset's own volatility | 91 bars |
| `carry` | carry | negated annualized mean funding rate | 3 settlements |
| `short_term_reversal` | reversal | negated vol-scaled 5-day return | 31 bars |
| `markov_mean_reversion` | reversal | expected next-state gap from a rolling transition matrix | 244 bars |
| `low_volatility` | volatility | negated annualized realized volatility | 41 bars |

Two of these are worth a sentence beyond the table:

- **`time_series_momentum` standardizes without demeaning.** Every other signal
  ends in a cross-sectional z-score. This one uses `cross_sectional_scale`
  (winsorize, then divide by the cross-sectional standard deviation) because
  subtracting the mean deletes exactly the market-wide tilt a time-series
  signal exists to express. A book weighted by its score *levels* is therefore
  not dollar-neutral, deliberately.
- **`carry` is the only signal that does not read prices.** It reads
  `funding_rate`, which exists on perpetuals only — so a universe member with
  no perp listing scores `None` at every rebalance, and this signal's effective
  breadth is smaller than the universe size.

`signals/bars.py` is the shared point-in-time reader every price signal uses:
read through the context, collapse each `(asset, bar)` to its latest ingestion,
trim to the most recent gap-free stretch. Writing that five times would be five
chances to get the point-in-time boundary subtly wrong.

### Alphas and breadth

The engine's per-rebalance rank IC series is the input to alpha refinement:

```python
from signals import alpha, breadth, low_volatility, signal_functions, signal_families
from signals.breadth import ScoreRecorder

recorder = ScoreRecorder(signal_functions())
result = Backtester(store).run(strategy, signals=recorder.wrapped())

estimates = alpha.estimate_ics(result.ic)          # shrunk IC per signal
print(alpha.alpha_summary(sorted(estimates.values(), key=lambda e: e.signal_id)))

vols = low_volatility.annualized_vol_universe(ctx)  # one volatility estimator
alphas = alpha.alpha_from_scores(scores, estimates["carry"].shrunk_ic, vols)
combined = alpha.combine_alphas({"carry": alphas, ...})

print(breadth.breadth_report(recorder.frame(), result.ic, signal_families()).to_text())
```

**IC shrinkage is where the humility goes.** A raw in-sample mean IC is
optimistic. `estimate_ic` reports it *and* a shrunk value, and every consumer
uses the shrunk one:

```text
shrunk = mean_ic × τ² / (τ² + se²)      τ = 0.02, se = ic_std / √n
```

τ encodes "a genuine crypto signal is small" rather than an expectation of
skill; the result is capped at ±0.10, and an estimate resting on fewer than two
observations is shrunk to exactly zero (one period cannot separate skill from
luck). `combine_alphas` sums across signals, where a `None` view contributes
nothing rather than dragging the sum toward zero — and returns the per-asset
**view count**, because an asset one signal likes and an asset all six like are
different bets.

**The breadth report answers two questions, not one.** *Score correlation* (per
rebalance, averaged) asks whether two signals pick the same assets; *IC
correlation* asks whether they work at the same times. Two signals can pick
entirely different assets and still be one bet if they fail in the same regime.
`effective_breadth` turns the mean absolute pairwise correlation into
`n / (1 + (n−1)ρ̄)` independent bets.

```bash
PAPER=true python scratch/scratch_signals_phase5.py    # the six signals
PAPER=true python scratch/scratch_signal_breadth.py    # alphas + breadth
```

**Every signal gets a methodology doc** in `signals/methodology/` (hypothesis,
data inputs and their point-in-time contract, construction, parameters, backtest
evidence, breadth check, failure modes) — copy `TEMPLATE.md`. The doc is the spec
and comes first: `signals.register` raises if a signal's doc does not exist.

Existence is not the whole contract — the doc has to describe *this* signal.
Registration parses the header table and the §4 parameter table, and refuses a
signal whose doc declares a different `Signal ID` or `Family`, or that runs a
parameter the doc never mentions. (Three of the six did: `max_gap_days` and
`history_buffer_days` were live parameters documented nowhere.) The reverse is
allowed — `markov_mean_reversion` documents `pooling` as considered and not
implemented, which is worth being able to write down.

`Status` is therefore data rather than prose:

```python
from signals import signal_statuses, get

signal_statuses()          # {'carry': 'draft', 'cross_sectional_momentum': 'draft', ...}
get("carry").is_evidenced  # False — nothing leaves `draft` without §5 numbers
```

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

## What is not done

Worth stating plainly, because the code being finished and the research being
finished are different things:

**No signal has backtest evidence.** All six are implemented, unit-tested
against hand-computed golden fixtures, and wired into the engine — and Section
5 of every methodology doc is empty, with every signal marked `draft`. There is
no multi-year backfill in this repository, and a backtest on synthetic data
measures the generator rather than the signal. The breadth demo makes the point
concretely: on synthetic bars `cross_sectional_momentum` and
`time_series_momentum` score-correlate at 0.86, which is a plausible finding
about two signals sharing a formation window and still not evidence of
anything.

Filling those sections needs, in order:

1. `python -m pipeline.nightly --start <5y ago> --end <today>` (hours,
   resumable — it checkpoints per dataset).
2. A walk-forward parameter grid per signal, selecting on prior folds only
   (`scratch/scratch_markov_param_grid.py` is the pattern to copy).
3. `--pit-mode event` on backfilled history, labelled as research indications
   rather than live-fidelity results, since a backfill stamps every row with
   one `ingested_ts`.
4. `scratch/scratch_signal_breadth.py` against that store, to fill Section 6
   with measured correlations rather than expectations.

**Where the backfill has to run.** Binance geo-blocks by egress IP: from a US
address `api.binance.com` and `fapi.binance.com` answer **HTTP 451**
("restricted location") for every endpoint, including public market data. ccxt
surfaces that as a bare `NetworkError` with the status code and the reason
stripped out, so the loaders see an unreachable venue, write nothing, and the
run ends as an audit `data_presence` halt — a misleading symptom for a network
policy. Reachable from a blocked address, if that is where you are:
`data-api.binance.vision` (public **spot** market data only — no `fapi`, so no
funding rate and no open interest, which means no `carry`), `api.binance.us`
(separate entity, no perps), and Deribit, Kraken, Coinbase, Gate, KuCoin,
Bitget and MEXC in full. The straightforward answer is to run the backfill on
the machine that runs the pipeline.

**Also outstanding:** the 10 MB rotation size and 12-month retention window in
`LOGGING.md` were specified, not measured — no component's real log volume is
known until a backfill has actually run.

## Progress

See `TODO.md` for detailed phase breakdown and progress log.

## References

- **Source**: "What Nobody Tells You About Being a Quant" (The Quant Insider)
- **Architecture**: See `PLAN.md` for detailed design rationale
- **Build Order**: See `TODO.md` for phased implementation plan
- **Logging**: See `LOGGING.md` for the observability design

---

**Next Phase**: collect backtest evidence for the six signals against a real
backfill (see [What is not done](#what-is-not-done)), then Phase 6 — the factor
risk model (see `TODO.md`)
