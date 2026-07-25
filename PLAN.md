# Poor Man's Trading Machine — PLAN

A single-person, low-cost implementation of the institutional multifactor trading
architecture described in "What Nobody Tells You About Being a Quant"
(The Quant Insider, https://youtu.be/tzTftCzmr7k).

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
|---|---|---|
| Compute | AWS + Databricks + Spark cluster | One PC. Polars (lazy, parallel, out-of-core) covers crypto-scale data easily |
| Storage / warehouse | Delta Lake on S3 | Local Parquet files partitioned by date, append-only |
| Time travel / point-in-time | Delta transaction log | Append-only convention: never overwrite history; store `knowledge_date` alongside `event_date` |
| Live time-series DB | KDB+/Q + HTCondor grid | DuckDB + Polars in-process; APScheduler/cron for jobs |
| Alternative data vendors | Purchased TB-scale datasets | Free exchange APIs: OHLCV, funding rates, open interest, liquidations, on-chain stats |
| Security matching team | CUSIP/SEDOL/Bloomberg mapping | Asset master table mapping exchange symbols → canonical internal ID (still essential — BTC is `XBT` on some venues, tickers get delisted/renamed) |
| Research pods (R → prod) | Researcher + dev + tester teams | Research notebooks/scripts → productionized module, both written with Claude Code; the methodology doc is the spec Claude Code works from |
| Execution desk | Implementation team | Paper trading on Deribit testnet / exchange testnets first; tiny live size later |

## 3. Architecture (the whiteboard)

```
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
