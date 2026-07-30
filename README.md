# Poor Man's Trading Machine

A single-person, low-cost implementation of a multifactor crypto trading system.

**Status: Phase 4 (Backtester) — Complete**

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

### Current Phase: Phase 4 (Backtester)

- [x] Walk-forward engine: rebalance calendar, point-in-time data exposure, next-bar execution
- [x] Cost model: half-spread + linear impact stub, charged on traded notional
- [x] Metrics: returns, vol, IR, drawdown, turnover, per-signal rank IC series
- [x] Golden tests: hand-computed 3-asset fixture reproduced exactly
- [x] Validated: buy-and-hold BTC through the engine matches the raw price series
- [ ] Ready for Phase 5 (Signals → alphas)

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
mypy datastore/ loaders/ audit/ universe/
```

## Development

### Adding a new module

1. Create package directory with `__init__.py`
2. Add public interface (typed dataclasses, main functions)
3. Write tests in `tests/test_<module>.py`
4. Create scratch script in `scratch/scratch_<module>.py` (demo only)
5. Update `README.md` progress

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

### Loaders

Three data loaders fetch from Binance/Deribit via ccxt and ingest to the datastore:

```python
from loaders import OHLCVLoader, FundingRateLoader, OpenInterestLoader

# Fetch last 7 days of daily and hourly OHLCV
loader = OHLCVLoader("binance", lookback_days=7)
loader.run_daily()
loader.run_hourly()

# Fetch funding rates and open interest
fr_loader = FundingRateLoader("binance", lookback_days=30)
fr_loader.run()

oi_loader = OpenInterestLoader("binance", lookback_days=30)
oi_loader.run()
```

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

**Backfill runner** orchestrates all loaders with resumable checkpoint tracking:

```python
from loaders import BackfillRunner

runner = BackfillRunner("binance")
runner.run(days_back=30)  # Fetches 30 days; resumes from checkpoint on retry
```

### Audit

Data quality audit with 5 checks: coverage (% of universe), null rates per column,
price outliers, freshness (data age), and data presence. Sends Telegram alerts on
breaches.

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

# Live run
python -m pipeline.nightly --venue binance --days 1
```

Datasets produced: `ohlcv_daily`, `ohlcv_hourly`, `funding_rate`, `open_interest`.

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

### Methodology & Signals

Every signal gets a `METHODOLOGY.md` documenting:

- Hypothesis
- Construction rules
- Parameters
- Known failure modes

The doc is the spec; code follows the doc.

## Important Principles

1. **Point-in-time discipline**: Every dataset row carries `event_ts` and `ingested_ts`.
2. **Append-only**: Never overwrite history in the datastore.
3. **No look-ahead bias**: Backtests only read data with `ingested_ts <= asof`
   (relaxable to `event_ts <= asof` for backfilled history — explicitly, via
   `pit_mode="event"`, never by default).
4. **Risk model is not optional**: Half the value is understanding volatility costs.
5. **Breadth over hero bets**: Many independent bets beat big single bets.
6. **Measure costs pessimistically**: Model spreads, impact, slippage from day one.

## Progress

See `TODO.md` for detailed phase breakdown and progress log.

## References

- **Source**: "What Nobody Tells You About Being a Quant" (The Quant Insider)
- **Architecture**: See `PLAN.md` for detailed design rationale
- **Build Order**: See `TODO.md` for phased implementation plan

---

**Next Phase**: Phase 5 — Signals → alphas
