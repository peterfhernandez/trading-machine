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
| Observability | Splunk / Datadog / ELK | Python `logging` → per-component rotating JSON files in `logs/` (10 MB rotation, 12-month retention, decoupled mechanisms); Telegram remains the alert channel — see `LOGGING.md` |
| CI / release | Jenkins or Buildkite, a staging environment, a release train | GitHub Actions: `ci.yml` gates the merge (ruff + 578 hermetic tests on 3.11 and 3.12), `deploy.yml` gates the deployment — it runs the suite against the incoming commit in a throwaway worktree *on the trading machine* before `git reset --hard` moves the live checkout |

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

  Cross-cutting: BACKTESTER (walk-forward, point-in-time), CONFIG, ALERTS, LOGGING
```

## 4. Modules

Each module is a Python package with a narrow public interface, its own tests,
and scratch scripts. Nothing imports "sideways" — everything communicates
through the parquet store and typed dataclasses. That is what makes the build
order below possible.

Two pieces of infrastructure are cross-cutting rather than sequential:
`config` (already central to every module) and `logging_config` (every module
below calls `get_logger(__name__)` to get a per-component rotating logger; the
full design, rotation/retention mechanism, and per-module retrofit map are in
[`LOGGING.md`](LOGGING.md)).

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
- **Python stdlib `logging`**, rotating per-component JSON files under `logs/`,
  for observability — see `LOGGING.md`.
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
8. **Observability by default.** Every module logs to its own rotating file
   under `logs/`; alerts (Telegram) are for a human's attention right now,
   logs are the durable record that lets an unattended run be reconstructed
   afterward. See `LOGGING.md`.

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

> **Phase 5.5 note:** "error logging" above was, until Phase 5.5, an
> unspecified phrase — nothing wrote to `LOGS_PATH`. See the Phase 5.5
> Implementation Notes below and `LOGGING.md` for what the append wrapper now
> actually logs.

### Market Types & Symbol Budget

Two venue-facing details the loaders got wrong at first, both of which surfaced
as audit coverage failures rather than as errors:

- **Market type.** ccxt defaults to spot markets, which have no funding rate and
  no open interest — those loaders raised per symbol and wrote nothing. Both perp
  loaders now open the venue with
  `defaultType=LOADER_CONFIG.perp_market_type` and select perpetual symbols
  (`BTC/USDT:USDT`; dated futures excluded). OHLCV stays on spot.
  Because the perp and spot namespaces use different symbol strings for the same
  asset, the nightly pipeline registers **both** in the asset master; canonical
  `asset_id` is the base asset either way, so datasets still join.
- **Filter before capping.** A venue's symbol list interleaves every quote
  currency alphabetically, so `exchange.symbols[:N]` followed by a USDT filter
  keeps only the USDT pairs that happen to sort early. `loaders.base.
  select_usdt_symbols` filters first, then caps at
  `LOADER_CONFIG.max_symbols_per_run` (200 — deliberately above
  `UNIVERSE_CONFIG.target_size` so the universe builder has more candidates than
  it keeps).

### Duplicates Are a Read-Side Concern

Append-only storage and de-duplication are the same design decision seen from
two ends. The writer must never drop a row it was handed: a bar re-fetched with
a different value is a *revision*, and the old value is exactly what a
point-in-time backtest needs to see. So the store keeps both, and every reader
that aggregates rows collapses each `(asset_id, event_ts)` to its latest
ingestion — `datastore.latest_per_bar`, used by the backtester's price panel,
the signal panels, the universe builder and the audit.

The ordering constraint is the whole subtlety: **collapse after the point-in-time
filter, never before**. Collapsing first lets a bar's later revision win, and the
`ingested_ts <= asof` filter then drops it, silently hiding a bar that *was*
knowable at `asof`. Readers that filter in memory per `asof` (the cached signal
panel) therefore collapse per `asof` rather than calling the shared helper.

Consequences worth stating: a rolling median over un-collapsed rows weights the
duplicated stretch of history (always the most recent days, since that is what
overlapping windows re-fetch) more heavily than the rest, which is how a
duplicate silently changes universe membership. And a "price jump" between two
ingestions of one bar is a revision, not a price move.

### Backfill Runner & Checkpoint Tracking

The `BackfillRunner` orchestrates all three loaders in sequence (OHLCV →
funding → OI) and persists checkpoint state (last_ingested_date per dataset)
to JSON files. Checkpoints are venue-specific:
`data/checkpoints/binance_backfill.json`, etc.

**Why:** Multi-year historical pulls can take hours or days. Checkpoints are
meant to enable resumable, incremental backfills without re-fetching history
already in the store.

**The checkpoint is now read back.** Each dataset records the event-time
*interval* it has covered (`covered_start`/`covered_end`); a run asks for a
window, `loaders.window.resume_window` trims it to what is missing, and the
loader is handed that window rather than a day count. Four decisions are worth
recording:

- **Coverage is an interval, not a high-water mark.** A single "last date"
  cannot answer "do we already have March 2024?", so a request reaching back
  before the covered start is fetched in full rather than skipped. Merging a
  disjoint window would claim the gap between them, so it does not: the later
  interval wins and the fragmentation is logged.
- **The overlap is re-fetched on purpose.** Resuming starts
  `refetch_overlap_days` before the covered end. The trailing bar of a run is
  usually incomplete (a daily candle fetched at 00:10 UTC covers ten minutes of
  the current day) and venues revise recent bars, so the last covered day has to
  be pulled again. Append-only storage plus `latest_per_bar` makes that cost API
  calls and disk, not correctness — which is why the two changes belong
  together.
- **Windows are paged.** A venue caps one response at ~1000 rows, so a window
  longer than a page truncated silently: a five-year daily request returned the
  first 1000 days and nothing said the rest was missing.
  `loaders.base.paginate_time_series` walks the window forward, stopping on an
  empty page, on a page that adds nothing new (a venue replaying the same page
  would otherwise loop forever), or at a page budget.
- **Perp datasets moved to the history endpoints.**
  `fetch_funding_rate`/`fetch_open_interest` return only the current value, so
  they could never honour a window; the loaders now prefer
  `fetch_funding_rate_history`/`fetch_open_interest_history` where the venue
  advertises them, and fall back to the snapshot otherwise. Under the fallback a
  historical window returns *nothing* rather than stamping today's value with a
  past timestamp. Binance's funding history carries the rate alone, so
  `mark_price`/`index_price` are null on historical rows — recorded in
  `AUDIT_CONFIG.nullable_columns_by_dataset` so the null-rate check warns
  instead of halting, while staying strict on `funding_rate` itself.

The runner also does one fetch per dataset per run (it used to do two:
`fetch()` to test for emptiness, then `run()`, which fetched again — the row
count now comes back from the run).

### Audit Module: Six Checks

The `DataAudit` class runs six checks on each dataset:

1. **data_presence:** Fails if no data exists anywhere in the lookback window
   (`AUDIT_CONFIG.audit_lookback_days`, default 7 days back from the audit date)
2. **coverage:** assets present vs. the point-in-time universe
   (`AUDIT_CONFIG.coverage_threshold_pct`, default 90%). The denominator is the
   latest `universe` snapshot with `event_ts <= asof`, read through
   `ingested_ts <= asof`; with no snapshot the check reports itself *not
   evaluated* rather than judging against a fabricated size
3. **null_rate_*:** Per-column null % (threshold: 1%)
4. **freshness:** Data age check against a per-dataset threshold
   (`AUDIT_CONFIG.freshness_threshold_hours_by_dataset`, falling back to
   `freshness_threshold_hours` for any unlisted dataset)
5. **price_outliers:** Detects suspicious price jumps (threshold: 10%)
6. **duplicate_bars:** How many rows in the window repeat a bar already stored.
   Never an error — re-ingestion is expected under append-only storage — but a
   rate that climbs run over run means the loaders are re-fetching history the
   store already has

`duplicate_bars` is measured on the raw read; every other check then runs
against `latest_per_bar(df)`, so a bar stored twice is not counted twice in a
null rate, and a revision is not read as a price jump between two bars.

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

> **Phase 5.5 note:** as of Phase 5.5, a halt (`should_halt_trading() ==
> True`) is also always logged at CRITICAL in `logs/audit.log`, written before
> the Telegram send is attempted — so the halt has a durable record even if
> the alert doesn't go out. `AuditResult` severity stays exactly as
> documented here; it is a data-quality taxonomy, not an application log
> level. See `LOGGING.md` §2 for the distinction.

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

- `tests/test_ohlcv.py`: 11 tests (fetch, append, symbol resolution and budget,
  rows-appended return, windowed fetch, golden fixture)
- `tests/test_funding_rate.py`: 15 tests (incl. perp market type, history
  endpoint vs. snapshot fallback, null mark/index on historical rows)
- `tests/test_open_interest.py`: 15 tests (incl. perp market type, history
  endpoint and its ccxt field names, window bounds)
- `tests/test_backfill.py`: 16 tests (checkpoint round-trip, one fetch per
  dataset, resume/extend/older-history/ignore-checkpoint windows)
- `tests/test_fetch_window.py`: 24 tests (window bounds, coverage merge rules,
  resume arithmetic incl. the legacy checkpoint format)
- `tests/test_pagination.py`: 12 tests (multi-page walks, the truncation the
  single call would have produced, loop guards, capability detection)
- `tests/test_audit.py`: 33 tests (all six checks, coverage denominator
  resolution, duplicate handling, expected-null columns, halt logic)
- `tests/test_nightly.py`: 18 tests (orchestration, dry-run, trading halt,
  asset master population across market types, --start/--end parsing)
- `tests/test_symbol_selection.py`: 10 tests (filter-before-cap, perp preference)
- `tests/test_dedupe.py`: 16 tests (latest-per-bar collapse, duplicate counting,
  universe metrics read through the collapse)

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

---

## Phase 5 Implementation Notes (Signals → alphas)

### The signal contract

A signal is `RebalanceContext -> {asset_id: score | None}`. Higher = more
attractive long, and `None` means **no view**, never `0.0` — a score of zero
asserts the asset is exactly average, which is a real position, so an asset the
signal cannot evaluate has to be absent from the cross-section instead. The
engine and `long_short_from_scores` already skip `None`, so this costs nothing
downstream and prevents a whole class of quiet bias.

No engine changes were needed, as Phase 4 predicted.

### Registration enforces the methodology doc

`signals.register` raises if `signals/methodology/<signal_id>.md` does not exist.
The project rule was already "the doc is the spec"; this makes it mechanical, so a
signal cannot be wired into a backtest or the pipeline before its spec exists. The
doc's `Deviations from this doc` and `Review log` sections are where design
changes are recorded, and they are expected to be filled in — a doc that never
changes while the code does is a stale doc.

### Signals do not import the backtester

Signals are consumed *by* the backtester, so nothing in `signals/` imports
`backtest/`. `score_universe` duck-types its `ctx`, and `signals/panel.py`
restates the two `pit_mode` strings rather than importing them, with a test
asserting they stay identical to the engine's. That keeps the dependency arrow
pointing one way for Phase 9, when the pipeline imports both.

### Shared cross-sectional transforms

`signals/transforms.py` owns winsorize + z-score for every signal, in that fixed
order: clipping before computing the mean and standard deviation stops one
blown-up score from dragging the location and scale of the whole cross-section.
`None` is preserved through both and excluded from the statistics.

### Two read paths, one of them cached

Production reads closes through `RebalanceContext`, which reopens every date
partition of `ohlcv_daily` per rebalance. A parameter grid walks the same window
hundreds of times, and that read is essentially all of its run time, so
`signals/panel.py` provides `CachedClosePanel`: one store read, then the same
per-`asof` filtering (`event_ts <= asof`, `ingested_ts < asof_date + 1 day` under
strict mode, latest ingestion per bar, gap-free tail) applied in memory.

The filtering is per-`asof`, so the cache cannot leak a later bar into an earlier
decision — but the guarantee rests on a test, not on the argument: the suite
asserts the cached path and the context path produce identical scores in both pit
modes, including with late-ingested bars. That test is what makes the cache an
optimization rather than a second implementation. `with_params` rebinds
parameters over the same loaded bars so a sweep reads the store once in total.

### markov_mean_reversion: three decisions worth recording

The signal was drafted before the real context API existed, and three things
changed on contact with it. All three are in the methodology doc; the summary is:

1. **`return_horizon_days` split from `state_window_days`.** One parameter for
   both meant standardizing a 20-day return against 20 observations of itself,
   consecutive ones sharing 19 of 20 days — not enough independent information to
   estimate a mean, a variance and `k-1` quantile cutoffs. A signal built on that
   would be a strong candidate for working at exactly one setting.
2. **State midpoints moved onto the transition matrix's window.** They were
   estimated over the full trailing history while transitions were counted over
   the lookback, so the probabilities and the values they weighted described two
   different periods.
3. **The estimation window is fixed** at `min_history_bars` (244 bars at the
   defaults). A score is therefore independent of how long an asset has been
   listed, and every asset's estimate spans an identically sized window.

Guards the draft did not have: a gap in the recent bars scores `None` (state
classification silently assumes evenly spaced bars, so a hole turns a 5-day return
into something else), and duplicate ingestions of the same bar are collapsed to
the latest, matching what the engine's price panel does.

`np.nanquantile` was replaced with an equivalent sort-based rolling quantile
(`_row_quantiles`); it is ~17x faster on the whole score and dominated sweep run
time, and a test asserts it matches numpy over random cases — the golden cutoffs
depend on that interpolation being identical.

### Walk-forward parameter grid

`scratch/scratch_markov_param_grid.py` is the tool that fills in Sections 4-5 of a
methodology doc. It selects parameters on prior folds only, evaluates on the next
fold, stitches the fold results into one out-of-sample series, and reports the gap
against the best full-sample cell — the overfitting tax — plus each parameter's
marginal effect and performance at 2x costs. It prints what it is *not*: full
sample numbers are labelled as not-evidence, and synthetic-mode results carry a
reminder that reversion was baked in by construction.

### The Phase 5 signal set

Five signals joined `markov_mean_reversion`, each with its methodology doc
written first. What is worth recording is not the formulas — those are in the
docs — but the three structural decisions they forced:

**One shared point-in-time reader, not five.** `signals/bars.py` owns
read-through-context → latest-ingestion-per-`(asset, bar)` → gap-free tail,
plus the numeric primitives (`trailing_return`, `log_returns`,
`realized_vol`). Five copies of that pipeline would be five chances to get the
point-in-time boundary or the duplicate collapse subtly wrong, in code where
being wrong looks like a slightly better backtest rather than like an error.
`signals/panel.py`'s `CachedClosePanel` stays separate: it is a sweep-time
optimization over one signal's parameters, not the production read path.

**`time_series_momentum` standardizes without demeaning.** Cross-sectional
z-scoring subtracts the universe mean, which for a time-series signal is
precisely the market-wide trend it exists to express: if every asset has
trended up, a demeaned score says "no view". `signals/transforms.py` therefore
grew `cross_sectional_scale` (winsorize, then divide by the cross-sectional
standard deviation, keeping the mean). Two consequences are accepted
deliberately: a book weighted by these score *levels* is not dollar-neutral,
and the engine's rank IC is invariant to the whole choice — ranking discards
both location and scale — so the IC series cannot detect getting this wrong.
Only a test can, and one does.

**One volatility estimator for the whole project.** `low_volatility` exports
`annualized_vol_universe`, and `signals/alpha.py` consumes exactly it rather
than defining its own. A second, subtly different volatility estimate
elsewhere in the codebase is how two parts of a system come to disagree about
risk without anyone noticing.

`carry` is the only signal that does not read `ohlcv_daily`, which is both its
main contribution to breadth (its errors have a different source) and its main
limitation: funding exists on perpetuals only, so a universe member without a
perp listing scores `None` at every rebalance rather than occasionally. Its
sign is pinned by an explicit golden test, because a carry signal negated twice
looks like a working momentum signal for a while.

### Alpha refinement: where the humility goes

`signals/alpha.py` implements Grinold–Kahn's `alpha = volatility × IC × z`. The
interesting part is the IC, not the product. A raw in-sample mean IC is
optimistic — measured on the data the parameters were chosen against, and
carrying an estimation error that only shrinks as `1/√n` — so `estimate_ic`
reports the raw mean *and* a shrunk value, and every consumer uses the shrunk
one.

Shrinkage is normal-normal: with a prior `IC ~ N(0, τ²)` and an observed mean
whose standard error is `se = ic_std/√n`,

    shrunk = mean_ic × τ² / (τ² + se²)

τ defaults to 0.02, which encodes "a genuine crypto signal is small" (§6
principle: shrink hard; the methodology template puts a real signal at
0.02–0.05) rather than an expectation of skill. The result is clipped at
`max_abs_ic = 0.10` — a guard against a short lucky sample, not part of the
Bayesian story. An estimate resting on fewer than two observations is shrunk to
exactly zero: one period cannot separate skill from luck, and its standard
error is undefined.

`combine_alphas` sums per-signal alphas, and a signal with no view contributes
nothing rather than pulling the sum toward zero — which is the entire reason
`None` exists instead of `0.0`. The consequence is that an asset carried by one
signal keeps that signal's full alpha, so the per-asset **view count** is
returned alongside: a portfolio that wants corroboration before sizing has the
information to require it.

### Breadth: two correlations, not one

`signals/breadth.py` answers §6 principle 3 ("five correlated signals are one
bet") with two different numbers, because they answer different questions:

- **Score correlation** — per rebalance, the rank correlation between two
  signals' cross-sections, averaged over rebalances. Do they *pick the same
  assets*? This is what decides whether a new signal is a rotation of an
  existing one. Computed per date and then averaged, not pooled: pooling lets
  the level differences between dates dominate a number that is supposed to be
  about relative ranking.
- **IC correlation** — the correlation of two signals' IC *time series*. Do
  they *work at the same times*? Two signals can pick entirely different assets
  and still be one bet if they both stop working in the same regime, and a
  portfolio built on score correlation alone would not see that coming.

`effective_breadth` converts the mean **absolute** pairwise correlation into an
independent-bet count, `n / (1 + (n-1)ρ̄)` — absolute because two signals that
reliably disagree are as redundant as two that agree, just sign-flipped.
`ScoreRecorder` wraps the signal functions the engine already calls once per
rebalance for its IC series, so collecting the scores costs one pass rather
than two.

`signals/` still imports nothing from `backtest/`, so the rank correlation here
is a small local implementation rather than a reuse of
`backtest.metrics.spearman_ic`; a test asserts the two agree over random cases.

### What Phase 5 deliberately did *not* produce

Section 5 (backtest evidence) and Section 6 (measured breadth) of all six
methodology docs are empty, and every signal's status is `draft`. There is no
multi-year backfill in the repository, and a backtest on synthetic data
measures the generator rather than the signal. The scratch demo makes this
concrete: on synthetic bars `cross_sectional_momentum` and
`time_series_momentum` score-correlate at 0.86, which is a plausible finding
about two signals that share a formation window — and still not evidence,
because the generator produced both series. Filling those sections needs
`python -m pipeline.nightly --start <5y ago>` against a real venue, and until
then the honest description of Phase 5 is "implemented and tested", not
"working".

### Testing Coverage (Phase 5)

- `tests/test_signals_phase5.py`: 124 tests — a hand-computed golden fixture
  per signal (formation return, realized volatility, funding average, and the
  final score, all derived in the test class's docstring), `carry`'s sign
  asserted explicitly, the property that `cross_sectional_scale` preserves a
  market-wide tilt where a z-score destroys it, point-in-time through a real
  store in both pit modes, every reject path returning `None`, the shared
  series reader's gap and duplicate handling, IC shrinkage behaviour
  (noisier and shorter series shrink harder; one period shrinks to zero), and
  the breadth machinery including that its Spearman matches the engine's
- `tests/test_signal_markov_mean_reversion.py`: 68 tests — a golden 10-bar
  fixture with every return, z-score, quantile cutoff, state, transition row,
  midpoint and the final score derived by hand in the test docstring;
  point-in-time tests against a real store in both pit modes (including that a
  later bar cannot change an earlier state, matrix, or score); every reject path
  returning `None` rather than `0.0`; the transforms; the registry's doc rule;
  cached-vs-context equivalence; and end-to-end runs through the engine

Most tests run in well under 100ms. A handful of store-backed ones take 100-200ms
because a signal needs at least ten bars of history and each date partition is a
separate parquet file; the one that runs two full backtests to compare read paths
is marked `integration`.

---

## Phase 5.5 Implementation Notes (Logging & Observability Retrofit)

### Why this phase exists

Phases 1-5 were built and tested without a logging story: `LOGS_PATH` existed
in `config.py` but nothing wrote to it, and §3's cross-cutting list named
CONFIG and ALERTS but not logging. Telegram alerts cover audit threshold
breaches, execution drift, and the kill switch — nothing else. A failure
inside, say, the backtester or a signal during an unattended nightly run had
no durable record. Phase 5.5 closes that gap before Phase 6 adds more
unattended-running modules (risk, portfolio, execution) on top of it. Full
design in [`LOGGING.md`](LOGGING.md); this section is a summary plus the
per-module retrofit map.

### Design summary

- One rotating JSON log file per component under `logs/` (`pipeline.log`,
  `datastore.log`, `loaders.log`, `audit.log`, `universe.log`, `backtest.log`,
  `signals.log`, plus `risk.log` / `portfolio.log` / `execution.log` /
  `attribution.log` reserved for Phases 6-9), each capped at 10 MB via
  `RotatingFileHandler`.
- Retention (12 months) is handled separately from rotation: rotated backups
  are named with a UTC timestamp at rotation time, not a bare numeric suffix,
  and `prune_old_logs()` deletes any backup older than 365 days. Size-triggered
  rotation and calendar-based retention are different mechanisms and don't
  compose cleanly through `backupCount` alone.
- Every log record carries a `run_id`, set once at the top of a pipeline
  invocation, so one nightly run's activity can be traced across all eleven
  log files.
- Logs, Telegram alerts, attribution reports, and `AuditResult` severities are
  four distinct concerns that were previously conflated: logs are the
  always-written technical record; alerts are real-time human notification
  (audit halts, drift, kill switch); reports are the daily business summary;
  `AuditResult` severity is a data-quality taxonomy internal to the audit
  module. A halt or alert is now always backed by a CRITICAL log line, so the
  log doesn't depend on the Telegram send succeeding.
- Reference implementation: `logging_config.py` (`get_logger`,
  `configure_logging`, `set_run_id`/`get_run_id`, `prune_old_logs`).

### Retrofit map — Phases 1-4 and current Phase 5 work

- **Datastore (M1):** `ParquetStore.append`/`.read` log row counts, dataset,
  and partition at INFO/DEBUG; the no-overwrite guard logs at WARNING before
  raising. `AssetMaster` symbol resolution logs unresolved symbols at WARNING
  rather than only raising.
- **Loaders/audit (M2/M3):** `BaseLoader`'s append wrapper — currently
  described only as "error logging" — becomes a proper INFO (stage timings,
  record counts) / ERROR (raised exceptions, still non-fatal per the nightly
  pipeline's stage handling) pair. `BackfillRunner` logs checkpoint save/load
  at DEBUG and resumption points at INFO. `DataAudit` logs each of the five
  checks' verdicts (INFO for pass, WARNING/ERROR per severity) and logs
  CRITICAL immediately before `should_halt_trading()` trips — independent of
  whether the Telegram send succeeds. `NightlyPipeline` logs stage entry/exit
  and duration for load/audit/report, and calls `prune_old_logs()` as its
  final step.
- **Universe (M4):** `UniverseBuilder.build_and_store` logs snapshot size,
  exclusion breakdown, and turnover at INFO.
- **Backtester (M5):** `Backtester.run` logs a per-run summary (date range,
  universe size, final metrics) at INFO; per-rebalance detail is available at
  DEBUG for research use, off by default so backtests over long histories
  don't flood the log.
- **Signals (M6, in progress):** `signals.register` logs registration (and the
  methodology-doc check) at INFO. `markov_mean_reversion`'s reject paths
  (`None` returns) log at DEBUG per asset — high-volume, so kept below INFO.

### Two things the design did not anticipate

Both surfaced on contact with the real source and are recorded in `LOGGING.md`
§6 as deviations rather than quietly implemented differently:

- **There is no "no-overwrite guard" to log before.** The retrofit map said the
  guard "logs at WARNING before it raises", but `ParquetStore.append` has no
  such check — it never overwrites, it writes `data_0001.parquet` beside
  `data_0000.parquet`. That path is not an error and logs DEBUG (worth seeing:
  a partition that keeps growing means re-fetched history). The *schema* check,
  which does raise, logs WARNING first.
- **`AssetMaster.resolve_symbol` needed an opt-out.** Logging every unresolved
  symbol at WARNING is right for the loaders — an unresolved symbol silently
  drops that asset's rows, and otherwise surfaces downstream as an
  unexplained audit coverage miss. But `NightlyPipeline._populate_asset_master`
  calls the same method to ask "is this symbol already mapped?", and on a first
  run every symbol on the venue would log a warning. Hence
  `warn_if_unresolved: bool = True` — additive, and the one place absence is
  the expected answer passes `False`.

### Testing

`caplog`-based tests assert a CRITICAL record is emitted on every halt path and
on any per-stage unhandled exception, and a dedicated test for
`prune_old_logs()` (fabricated file ages under `tmp_path`) confirms it deletes
only backups older than the retention window and never touches the live file.
Log message text itself isn't asserted beyond that — brittle and not the
point.

One test earns its place beyond the spec's list: the rotation namer's output is
fed to the pruner's glob and asserted to match. Rotation and retention are
deliberately decoupled mechanisms (§3.2), which means nothing else forces them
to agree on the backup filename format — and if they ever disagree, retention
silently never runs, with no symptom until the disk fills.

### Status

Applied. `datastore/`, `loaders/`, `audit/`, `universe/`, `backtest/`,
`signals/` and `pipeline/` all log through `logging_config.get_logger`;
`prune_old_logs()` runs as the nightly pipeline's final step (in a `finally`,
so a failed run still prunes) and standalone via
`python -m pipeline.prune_logs`. `risk/`, `portfolio/`, `execution/` and
`attribution/` have their log files reserved and build logging in as they are
written (Phases 6-9).

---

## Phase 5.6 Implementation Notes (CI, test isolation, observability fixes)

### Why this phase exists

Phase 5.5 answered "is logging wired in?" — every module takes
`get_logger(__name__)`, the halt path logs CRITICAL, retention runs nightly.
This phase asked the different question, "does it work?", by running the things
rather than reading them. Four of the answers were no. None of them were
visible to the test suite, and one of them is the reason a test suite that
never runs the CLI cannot tell you your CLI is silent.

### The `__main__` logger, and why no test could see it

`pipeline/nightly.py` does `logger = get_logger(__name__)` at module scope.
Imported, `__name__` is `"pipeline.nightly"` and the logger is
`tm.pipeline.nightly`, which inherits `tm.pipeline`'s rotating file handler.
Run as `python -m pipeline.nightly` — the invocation in the README, and the one
a scheduled job uses — `__name__` is `"__main__"`, the logger is `tm.__main__`,
which belongs to no component, has no file handler, and emits at INFO, below
the console threshold. The entire `logs/pipeline.log` story evaporated on the
only code path a human actually types. The same dry run, both ways: 18 records
imported, 0 records under `-m`.

Three things worth taking from it beyond the fix:

- **The fix belongs in `get_logger`, not in the two CLIs.** `resolve_logger_name`
  maps `"__main__"` back through `sys.modules["__main__"].__spec__.name`, so
  Phase 8's execution CLI and Phase 9's report CLI inherit the fix instead of
  re-discovering the bug.
- **The test has to be a subprocess.** Every import-based assertion passed
  throughout — importing the module is precisely what hides the defect. The
  regression test runs the CLI with `TM_LOG_DIR` pointed at a `tmp_path` and
  reads the file back, which meant `LogConfig.dir` needed an env override it
  did not have.
- **"Wired in" and "works" are different claims,** and the retrofit's own tests
  could only speak to the first. That generalizes to Phases 6-9: each new
  module's logging needs at least one test that exercises it the way it will be
  run.

### Rotation was destroying the thing it exists to preserve

The namer stamps a rotated backup with the UTC second. `RotatingFileHandler.
doRollover` renames onto `namer(<file>.1)` after removing whatever sits there,
so two rotations inside one second collapsed to one file and 10 MB of history
disappeared. Separately, `backupCount` was set to 100,000 — reasoning recorded
at the time as "large enough that rotation never drops a backup before the
pruner judges it by age", which misread `backupCount` as a passive limit. It is
a loop bound: every rotation walked it, one namer call and one `stat` per step,
~320 ms measured against 1.4 ms at `backupCount=5`.

`TimestampedRotatingFileHandler` rolls over directly to a unique name and never
consults `backupCount`. That is not a new decision, it is the one §3.2 of
`LOGGING.md` already claimed — rotation is size, retention is calendar, and
`backupCount` belongs to neither.

### Test isolation: the failure that only reproduces on the real machine

The reported regression was `{'BTC/USDT': 'BTC'} != {'BTC/USDT': None}` in a
logging test. Not a code defect: the test constructed a loader with an isolated
`store=` but no `asset_master=`, and `BaseLoader.__init__` fills that default
from the production `DATASTORE_PATH`. On a clean checkout that file does not
exist and nothing resolves; on a machine that has run the pipeline it holds
13,000 mappings and `BTC/USDT` resolves to `BTC`.

The shape of it is worth recording, because it is the worst-behaved kind of
test defect: **green everywhere except on the machine that runs the system for
real**, and failing there in a way that looks like broken code. Seven modules
carry a production-path default (`loaders.base`, `loaders.backfill`,
`audit.auditor`, `universe.builder`, `backtest.engine`, `pipeline.nightly`,
plus `config` itself). They bind `DATASTORE_PATH` by value at import, so
patching `config` alone does not reach them; the autouse fixture replaces the
name in each module's own namespace, and a test fails if an eighth module
appears without joining the list.

### The doc as a checked spec, not a required file

`register` enforced that `signals/methodology/<id>.md` exists. Existence turned
out to be nearly the weakest possible contract: it cannot notice that a doc
describes a different signal, and it cannot notice a parameter the code runs
and the doc never mentions. The second had happened three times out of six —
`max_gap_days` and `history_buffer_days` were live parameters, absent from §4,
in `short_term_reversal`, `time_series_momentum` and (one of them)
`low_volatility`. Half the specs described a different signal from the one
being backtested, and the "the doc is the spec" rule was doing no work.

Registration now parses the header table and the §4 parameter table and refuses
a mismatch. Two deliberate asymmetries:

- **The parameter check runs one way.** Fields must be documented; documented
  rows need not be fields. `markov_mean_reversion` documents `pooling` as
  considered and not implemented, which is exactly the kind of thing a
  methodology doc should be able to say.
- **Prose is never parsed.** Hypothesis, failure modes and the review log are
  for humans; a schema over them would produce writing aimed at the schema.

`Status` became a parsed enum (`draft` / `backtested` / `live-paper` /
`retired`) with the free text moved to a `Status note` row, so Phase 5's honest
summary is a query — `signal_statuses()` — rather than a paragraph someone has
to remember to update. `test_all_six_are_still_draft` is *designed* to fail on
the day a real backfill fills in a §5.

### CI: gate before the merge, verify before the pull

The repository had one workflow, firing on `pull_request: [closed]` and
resetting the laptop to `origin/main`. It ran no tests at any point, and it
carried three latent faults: a failed `git fetch` followed by a successful
`git reset` exited 0 and deployed the previous commit (PowerShell does not fail
a step on a native command's exit code, and a custom `shell:` string gets no
`$LASTEXITCODE` epilogue); it reset to `origin/main` whatever branch the PR had
merged into; and two merges in quick succession raced on one directory.

Split in two, because they answer different questions:

- **`ci.yml`** gates the merge — pull requests and pushes to `main`, on
  GitHub-hosted runners, Python 3.11 and 3.12. The suite is hermetic (every
  ccxt call mocked, every store a `tmp_path`), so 578 tests run in ~35 s with
  no network. Required-check territory.
- **`deploy.yml`** gates the *deployment*, which is a stronger claim than a
  status check: `git fetch` is non-destructive, so the incoming commit is
  checked out into a throwaway worktree and the suite is run **on the trading
  machine itself** before `git reset --hard` touches the live tree. A failure
  leaves the box on the last known-good commit.

Two constraints that shaped the deploy job. It must not `pip install -e .` in
the worktree — an editable install from a temporary directory would repoint the
machine's site-packages at a path the job then deletes — so a merge that adds a
dependency fails at import, which is the correct answer. And it must not
`git clean`: `data/` and `logs/` are git-ignored, and they are the datastore and
the durable run record.

### Testing Coverage (Phase 5.6)

- `tests/test_isolation.py`: 7 tests — every production-path default lands in
  the sandbox, the exact shape of the reported failure, and a completeness
  check that fails when a new module references `DATASTORE_PATH` without
  joining the fixture
- `tests/test_methodology_docs.py`: 27 tests — header and §4 parsing including
  the annotated-name form, the status enum rejecting free text, the one-way
  parameter check, registration refusing every kind of mismatch, and the real
  six-signal set against its real six docs
- `tests/test_logging.py`: +13 — the CLI-as-subprocess regression, `__main__`
  resolution, the level override reaching handlers as well as loggers,
  same-second rotation losing no record, and a canary asserting `caplog` can
  still see the non-propagating `tm` tree

578 passing overall.
