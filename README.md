# Poor Man's Trading Machine

A single-person, low-cost implementation of a multifactor crypto trading system.

**Status: Phase 3 (Universe) — Complete**

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

### Current Phase: Phase 3 (Universe)

- [x] OHLCV loader (daily + hourly) for top ~150 USDT/USD perps via ccxt
- [x] Funding-rate loader; open-interest loader
- [x] Backfill runner (resumable, checkpoint-tracked) — pull 3–5 years history
- [x] Audit module: coverage %, null rates, outlier price jumps, freshness checks; Telegram alerts
- [x] Nightly pipeline: end-to-end load → audit → report orchestration
- [x] Liquidity metrics (rolling median dollar volume), listing-age filter, stablecoin/wrapped exclusions
- [x] Daily universe membership written point-in-time to the store
- [ ] Ready for Phase 4 (Backtester)

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

**Backfill runner** orchestrates all loaders with resumable checkpoint tracking:

```python
from loaders import BackfillRunner

runner = BackfillRunner("binance")
runner.run(days_back=30)  # Fetches 30 days; resumes from checkpoint on retry
```

### Audit

Data quality audit with 5 checks: coverage (% of universe), null rates per column,
price outliers, freshness (data age), and row count. Sends Telegram alerts on breaches.

```python
from audit import DataAudit

audit = DataAudit()
results = audit.audit_dataset("ohlcv_daily")
audit.send_alerts()

if audit.should_halt_trading():
    print("Critical failure; trading halted")
```

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
3. **No look-ahead bias**: Backtests only read data with `ingested_ts <= asof`.
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

**Next Phase**: Phase 4 — Backtester
