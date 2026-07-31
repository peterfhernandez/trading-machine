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

### Testing Coverage (Phase 5)

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
