# Poor Man's Trading Machine

A single-person, low-cost implementation of a multifactor crypto trading system.

**Status: Phase 0 (Scaffold) — In Progress**

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

### Current Phase: Phase 0 (Scaffold)

- [x] Repo skeleton with all packages
- [x] `config.py` with environment configuration
- [x] pytest + ruff configured
- [x] CLAUDE.md in repo root
- [ ] Ready for Phase 1 (Datastore & asset master)

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
mypy datastore/ loaders/ audit/
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

**Next Phase**: Phase 1 — Datastore & asset master
