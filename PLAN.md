# Poor Man's Trading Machine — PLAN

A single-person, low-cost implementation of the institutional multifactor trading
architecture described in ["What Nobody Tells You About Being a Quant"](https://youtu.be/tzTftCzmr7k)
(The Quant Insider).

Crypto first, asset-agnostic by design, equities later.

## 1. The idea in one paragraph

The system is a **breadth machine**. It makes many small, independent,
slightly-better-than-even bets across a universe of assets, every day.
Its report card is the information ratio (active return / active risk), and the
fundamental law says IR ≈ skill × √breadth. An individual cannot out-skill a
quant firm, but the architecture — signals → alphas → risk model → optimizer →
execution → attribution, with disciplined point-in-time data underneath — is
fully buildable at hobby scale. Every institutional component has a cheap
substitute.

## 2. Institutional stack → poor man's stack

| Video component | Firm version | Our version |
| --- | --- | --- |
| Compute | AWS + Databricks + Spark cluster | One PC. Polars (lazy, parallel, out-of-core) covers crypto-scale data easily |
| Storage / warehouse | Delta Lake on S3 | Local Parquet files partitioned by date, append-only |
| Time travel / point-in-time | Delta transaction log | Append-only convention: never overwrite history; store `knowledge_date` alongside `event_date` |
| Live time-series DB | KDB+/Q + HTCondor grid | DuckDB + Polars in-process; APScheduler/cron for jobs |
| Alternative data vendors | Purchased TB-scale datasets | Free exchange APIs: OHLCV, funding rates, open interest, liquidations, on-chain stats |
| Security matching team | CUSIP/SEDOL/Bloomberg mapping | Asset master table mapping exchange symbols → canonical internal ID (still essential — BTC is `XBT` on some venues, tickers get delisted/renamed) |
| Research pods (R → prod) | Researcher + dev + tester teams | Research notebooks/scripts → productionized module, both written with Claude Code; the methodology doc is the spec Claude Code works from |
| Execution desk | Implementation team | Paper trading on Deribit testnet / exchange testnets first; tiny live size later |

## 3. Architecture (the whiteboard)

```flow
                    ┌─────────────────────────────────────────────┐
                    │                DATA LAYER                   │
                    │  loaders (per venue) → parquet store        │
                    │  asset master │ point-in-time │ auditing    │
                    └──────┬──────────────────────────────────────┘
                           │
        ┌──────────────────┼──────────────────┐
        ▼                  ▼                  ▼
  ┌───────────┐     ┌───────────┐      ┌───────────┐
  │ UNIVERSE  │     │  SIGNALS  │      │ RISK MODEL│
  │ liquidity │     │ momentum, │      │ factor    │
  │ filters   │     │ carry,    │      │ regression│
  └─────┬─────┘     │ value...  │      │ covariance│
        │           └─────┬─────┘      └─────┬─────┘
        │                 ▼                  │
        │           ┌───────────┐            │
        │           │  ALPHAS   │            │
        │           │ z-score × │            │
        │           │ IC × vol  │            │
        │           └─────┬─────┘            │
        └────────────┬────┴────┬─────────────┘
                     ▼         ▼
              ┌─────────────────────┐
              │ PORTFOLIO CONSTRUCT │  max α − λ·risk − costs
              │ (optimizer)         │  s.t. constraints
              └─────────┬───────────┘
                        ▼
              ┌─────────────────────┐
              │ EXECUTION           │  paper → tiny live
              │ implementation      │  shortfall tracking
              └─────────┬───────────┘
                        ▼
              ┌─────────────────────┐
              │ PERFORMANCE /       │──── feeds back to
              │ ATTRIBUTION         │     signal research
              └─────────────────────┘

  Cross-cutting: BACKTESTER (walk-forward, point-in-time), CONFIG, ALERTS
```

## 4. Modules

Each module is a Python package with a narrow public interface, its own tests,
and scratch scripts. Nothing imports "sideways" — everything communicates
through the parquet store and typed dataclasses. That is what makes the build
order below possible.

### M1 `datastore` — parquet store + asset master

- Append-only Parquet, partitioned `dataset/date=YYYY-MM-DD/*.parquet`.
- Read API returns Polars frames; write API enforces schema + no-overwrite.
- **Asset master**: canonical `asset_id`, per-venue symbol maps with validity
  date ranges (the crypto version of security matching / point-in-time IDs).
- Every dataset row carries `event_ts` and `ingested_ts` (knowledge date) so
  backtests can ask "what did I know then?" — the look-ahead-bias defence.

### M2 `loaders` — one loader per venue/dataset

- v1 targets (all free): daily+hourly OHLCV for top ~100 perps (ccxt),
  funding rates, open interest; Deribit options summary (reuse calendar-bot
  knowledge later).
- Each loader: fetch → validate → transform → append to datastore. Idempotent,
  resumable, unique per vendor cadence — exactly the video's "data loader".

### M3 `audit` — data auditing

- Per-dataset statistical checks on every refresh: row counts, coverage
  (% of universe present), null rates, price jump outliers vs. a second source.
- Threshold breaches raise alerts (Telegram, reusing the calendar-bot pattern)
  and can halt downstream factors. Cheap to build, disproportionate value.

### M4 `universe`

- Rules → daily universe membership list, stored like any other dataset.
- v1: top N by rolling median dollar volume, minimum listing age, exclude
  stablecoins and wrapped duplicates. Universe membership is point-in-time too.

### M5 `backtest` — walk-forward research engine

- Vectorized Polars event loop: for each rebalance date, expose only data with
  `ingested_ts ≤ date`, produce target weights, apply next-period returns minus
  cost model.
- Outputs: returns, turnover, IC time series, drawdowns. This is the module
  every later module is tested against, so it comes early.

### M6 `signals` → `alphas`

- A signal is a function `(datastore, date, universe) → score per asset`.
- v1 signal set (documented, no data mining yet): cross-sectional momentum,
  time-series momentum, carry (funding rate), short-term reversal,
  low-volatility. One file per signal + a required METHODOLOGY.md per signal
  (the video's "word document" — it becomes the spec Claude Code codes from).
- Alpha refinement per Grinold–Kahn: alpha = volatility × IC × z-score.
  IC estimated from the backtester, shrunk hard toward zero (be humble).

### M7 `risk` — factor risk model

- Cross-sectional regression of daily returns on: market beta, size (log mcap
  or volume proxy), momentum, volatility, and sector buckets (L1/L2 ecosystem
  tags: majors, DeFi, L2s, memes...).
- Factor covariance (EWMA + Ledoit–Wolf shrinkage) + specific variances.
  ~10 factors × ~100 assets: the "million pairwise covariances → factor
  covariances" collapse from the video, at a scale a laptop laughs at.

### M8 `portfolio` — construction/optimizer

- v1: simple and robust — rank-based long/short buckets with vol targeting and
  position caps. v2: mean-variance via cvxpy: max α − λ·σ² − costs, subject to
  market-neutrality, max weight, turnover cap, gross leverage cap.
- Output: target weights per rebalance, written to the store.

### M9 `execution`

- Paper broker first: fills at next bar open ± spread assumption; tracks
  positions, PnL. Then exchange testnet, then (much later, tiny) live.
- **Implementation shortfall**: run the zero-cost paper portfolio in parallel
  with the real/testnet one; the gap is your cost of trading. Alert on drift.

### M10 `attribution` — performance analysis

- Decompose realized PnL into factor bets vs. specific vs. costs.
- Track per-signal IC decay over time (factors decay; this tells you when).
- Daily report via Telegram/HTML: IR, exposures, drawdown, shortfall.

### M11 `pipeline` — the daily production run

- Scheduler chaining: loaders → audit → universe → alphas → risk → optimizer →
  execution → attribution, with per-stage failure handling (audit failure halts
  trading stages, not reporting stages). Crypto is 24/7 so the "trading window"
  pressure from the video is relaxed — pick one daily rebalance time (e.g.
  00:10 UTC) and keep the whole run under minutes, not half-days.

### Later: `equities` extension

- The interfaces above are asset-agnostic on purpose (asset master, loaders,
  signals take `(datastore, date, universe)`). Equities means: new loaders
  (e.g. EOD data), a real security master (tickers change, mergers — true
  security matching), borrow costs in the cost model. No core module changes.

## 5. Technology choices

- **Python 3.12+, Polars, DuckDB, PyArrow/Parquet** — the whole data stack.
- **cvxpy** for the optimizer (v2), **scikit-learn** only for regressions/shrinkage.
- **ccxt** for exchange data/execution abstraction; Deribit API where richer.
- **pytest** everywhere; **APScheduler** (or cron) for the pipeline.
- No cloud, no Spark, no KDB, no Docker until something actually hurts.
  Upgrade triggers are listed in TODO.md Phase 9.

## 6. Principles (from the video, adapted)

1. **Point-in-time or it didn't happen.** Every dataset records when you knew
   it. The backtester only reads through that lens.
2. **The risk model is not optional.** Newcomers underrate it; half the value
   of the machine is knowing what a position costs in volatility.
3. **Breadth over hero bets.** More independent bets beat a bigger single bet.
   Check correlation between signals; five correlated signals are one bet.
4. **Subtract as little value as possible.** Costs are death by a thousand
   cuts; model them pessimistically from day one, measure shortfall always.
5. **Documentation is the interface between research and production.** Each
   signal's METHODOLOGY.md is the contract — and it's also the prompt context
   that makes Claude Code productive on that module.
6. **Audit upstream, catch problems early.** Wrong data means trading on
   fiction; the audit module can halt the pipeline.
7. **Factors decay.** Attribution feeds research; expect to retire and
   refurbish signals.

## 7. What we deliberately skip

- Intraday/HFT anything (individual cost structure makes it a donation).
- Neural nets (the video itself notes most production research is regression
  and gradient-boosted trees; we start with regression only).
- Multi-region equities, terabyte alt-data, cluster compute.
- Live money until the paper machine has run unattended for weeks and the
  attribution says the edge survives costs.

---

## Phase 2 Implementation Notes (Loaders & Audit)

### Architectural Decision: Schemas in Module Namespaces

Dataset schemas (OHLCV, funding rate, open interest) are defined in
`loaders/schemas.py` and `audit/schemas.py` rather than centralized in
`config.py`. This follows the "narrow public interfaces" principle: each
module owns and exports its schema definitions via `__init__.py`.

**Why:** Keeps concerns separate (configuration vs. type definitions),
makes module dependencies explicit, and allows future modules (e.g., new
loaders for equities) to define their own schemas without polluting a
central config file.

### BaseLoader Pattern

All three loaders (OHLCV, funding rate, open interest) inherit from
`BaseLoader` (in `loaders/base.py`), which provides:

- **Idempotency checks:** Skip data already ingested for a date
- **Symbol resolution:** Bulk resolve venue symbols → canonical asset_ids via AssetMaster
- **Timestamp management:** Ensure `event_ts` and `ingested_ts` are present
- **Append wrapper:** Validate and append to datastore with error logging

This eliminates code duplication and enforces the point-in-time pattern
consistently across all three loaders.

### Backfill Runner & Checkpoint Tracking

The `BackfillRunner` orchestrates all three loaders in sequence (OHLCV →
funding → OI) and persists checkpoint state (last_ingested_date per dataset)
to JSON files. On retry, each loader resumes from its checkpoint rather than
re-fetching from scratch. Checkpoints are venue-specific:
`data/checkpoints/binance_backfill.json`, etc.

**Why:** Multi-year historical pulls can take hours or days. Checkpoints
enable resumable, incremental backfills without duplicating data or
wasting API quota.

### Audit Module: Five Checks

The `DataAudit` class runs five checks on each dataset:

1. **data_presence:** Fails if no data exists anywhere in the lookback window
   (`AUDIT_CONFIG.audit_lookback_days`, default 7 days back from the audit date)
2. **coverage:** % of assets present (threshold: 80% of universe)
3. **null_rate_*:** Per-column null % (threshold: 1%)
4. **freshness:** Data age check against a per-dataset threshold
   (`AUDIT_CONFIG.freshness_threshold_hours_by_dataset`, falling back to
   `freshness_threshold_hours` for any unlisted dataset)
5. **price_outliers:** Detects suspicious price jumps (threshold: 10%)

`audit_dataset()` reads a bounded lookback window (not just the exact audit
day) so data ingested late is correctly reported as **stale** (via the
freshness check, with a precise age) rather than being indistinguishable from
data that was **never ingested at all** (data_presence). A dataset directory
that doesn't exist yet (first-ever run) is also treated as "no data" rather
than surfacing as a raw execution error.

Each check returns an `AuditResult` with severity ("info", "warning", "error").
Critical errors set `should_halt_trading() = True`, halting the pipeline.
Telegram alerts are sent for any failed checks (configurable; requires
`TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID` env vars).

### Nightly Pipeline: Load → Audit → Report

The `NightlyPipeline` class orchestrates the full end-to-end flow:

- **Load stage:** Runs `BackfillRunner` (fetch last N days)
- **Audit stage:** Runs `DataAudit` on all four datasets; halts if critical failures
- **Report stage:** Prints dataset summaries

Supports **dry-run mode** (`--dry-run` flag) for testing without writing.
Failures in the load stage are non-fatal (continue to audit); failures in
audit can halt trading. The report stage always runs.

**CLI entry point:**
```bash
python -m pipeline.nightly --venue binance --days 1 --dry-run
```

### Datasets Produced (Phase 2)

1. **ohlcv_daily:** Daily candles (asset_id, venue, event_ts, ingested_ts, close, volume, ...)
2. **ohlcv_hourly:** Hourly candles (same schema as daily)
3. **funding_rate:** Funding rates (asset_id, venue, event_ts, ingested_ts, funding_rate, mark_price, index_price)
4. **open_interest:** OI snapshots (asset_id, venue, event_ts, ingested_ts, open_interest, open_interest_usd)

All maintain strict point-in-time discipline: `event_ts` = when the data
happened; `ingested_ts` = when we fetched it. Append-only to datastore;
never overwrite history.

### Testing Coverage (Phase 2)

- `tests/test_ohlcv.py`: 8 tests (fetch, append, symbol resolution, golden fixture)
- `tests/test_funding_rate.py`: 5 tests
- `tests/test_open_interest.py`: 5 tests
- `tests/test_backfill.py`: 6 tests (checkpoint save/load, resumability)
- `tests/test_audit.py`: 9 tests (all five audit checks, halt logic)
- `tests/test_nightly.py`: 8 tests (orchestration, dry-run, trading halt)

All tests use mocked ccxt exchanges and temporary datastores; no real API calls.
Golden tests verify hand-computed fixtures match exactly.

### Scratch Demos (Phase 2)

Each module has a demo script (`scratch/scratch_*.py`) that:
- Runs only in `PAPER=true` mode (safety guard)
- Uses temporary directories (doesn't pollute production data)
- Shows typical usage patterns

Example:
```bash
PAPER=true python scratch/scratch_ohlcv.py
```

### Future: Phase 3 (Universe)

Phase 2 produces raw OHLCV, funding, and OI data. Phase 3 will:
- Define daily universe membership rules (liquidity, listing age, exclusions)
- Filter datasets to universe members only
- Build universe filters for downstream signal/risk/portfolio modules

No breaking changes to Phase 2 loaders/audit.

---

## Phase 4 Implementation Notes (Backtester)

### The engine's contract

`backtest.Backtester` is the module every later module is tested against, so
its semantics are fixed and documented in `backtest/engine.py`:

- **Point-in-time exposure.** At rebalance date `t` the strategy receives a
  `RebalanceContext` whose reads go through `store.read(..., asof=t)` and are
  further filtered to `event_ts <= t`. The engine's price panel — used for
  fills and return accounting — is never handed to a strategy, so there is no
  code path by which a strategy can see a bar it could not have seen live.
- **Next-bar execution.** The close of the decision bar is the last thing the
  strategy saw, so weights fill at the *open of the following bar*. A rebalance
  date with no following bar cannot be executed and is dropped.
- **Holding periods tile the sample.** Weights set at `t_k` are held from
  `f(t_k)` open to `f(t_k+1)` open; asset returns are open-to-open, with no
  overlap and no gaps. The final rebalance is recorded but earns no return.
- **Drift-aware turnover.** Positions drift with returns between rebalances
  (`w * (1+r) / (1+r_p)`), and turnover is measured against the *drifted* book.
  Without this a buy-and-hold strategy would appear to pay costs every period.
- **Costs from day one.** Half the quoted spread on every traded dollar plus a
  linear impact stub (bps per $M traded), charged on traded notional at the
  start of each period. `ZERO_COST` exists for validation and the Phase 8
  zero-cost shadow portfolio.

### Two point-in-time modes (`pit_mode`) — a deliberate, flagged relaxation

Strict point-in-time (`ingested_ts <= asof`) is the correct defence, and stays
the default. But a **backfill stamps every row it pulls with the same
`ingested_ts`** (the moment the backfill ran), so under strict mode a backtest
over backfilled history sees literally nothing and every book comes back empty.
That is not a bug in the store — the store is being honest about when we
learned things — it is an inherent property of researching history you did not
collect as it happened.

So the engine offers `pit_mode`:

- `"ingestion"` (default, strict): `ingested_ts <= asof`. The number to trust;
  correct whenever the nightly pipeline collected the data day by day.
- `"event"` (opt-in, weaker): `event_ts <= asof` only. Still prevents a strategy
  from seeing a bar dated after the rebalance, but concedes protection against
  vendor revisions and genuinely-late data. Research indications, not
  live-fidelity results.

`DatastoreUniverse` takes the same flag. A Phase 3 universe snapshot dated `T`
was itself built only from data with `ingested_ts <= T`, so reusing it in event
mode introduces no look-ahead in the *rules* — only the possibility that the
snapshot was built from later-revised inputs.

This is the one place in the codebase where look-ahead protection can be
loosened, it can only be loosened explicitly, and both modes are covered by
tests.

### Annualization follows the holding period, not the bar

`BacktestConfig.periods_per_year` counts *bars* per year (365 for daily crypto
bars). A weekly rebalance holds each book for ~7 bars, so the engine divides by
the mean holding period before annualizing. Without this, weekly backtests
reported annualized returns roughly an order of magnitude too high.

### Module layout

- `backtest/engine.py` — `Backtester`, `RebalanceContext`, `PricePanel`,
  `BacktestResult`, `DatastoreUniverse`
- `backtest/costs.py` — `CostModel` (half-spread + impact stub), `ZERO_COST`
- `backtest/calendar.py` — daily/weekly/monthly rebalance calendars
- `backtest/metrics.py` — pure functions (total/annualized return, vol, IR,
  drawdown, hit rate, Spearman IC) plus `BacktestMetrics`
- `backtest/strategies.py` — reference books (buy-and-hold, equal weight,
  rank-based long/short) used for validation and as baselines to beat

Metrics are plain Python rather than Polars/NumPy expressions specifically so
that every number in the golden tests can be checked by hand.

### Testing Coverage (Phase 4)

- `tests/test_backtest.py`: 41 tests — the golden 3-asset fixture (every
  gross/net return, turnover, cost, equity value and metric derived by hand),
  buy-and-hold validation against the raw open-to-open series, point-in-time
  and `pit_mode` behaviour, universe/tradability handling, calendars, IC series
- `tests/test_backtest_metrics.py`: 36 tests — metrics, cost model, calendar

All use temporary datastores and synthetic bars; each runs in well under 100ms.

### Future: Phase 5 (Signals → alphas)

The engine already accepts named signal functions and produces a per-signal
rank IC series, which is exactly the input Phase 5 needs for IC estimation and
shrinkage (`alpha = vol × IC × z`). Phase 5 adds the signal interface/registry
and the first five signals; no engine changes are expected.
