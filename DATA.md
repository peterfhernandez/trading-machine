# DATA.md — getting the multi-year backfill that unblocks Phase 5

Phase 5's last item is blocked on data, not on code. Section 5 of all six
methodology docs is empty and all six signals are `draft` because there is no
multi-year history in this repository to backtest against.

This document is the action plan for producing that history. It is written to be
executed in order; every step has a command and an acceptance check.

**Decision, up front:** pull the history from Binance's **public data archive**
(`data.binance.vision`) with a new bulk loader, not from the ccxt API. The
archive is reachable from a US egress IP (verified 2026-08-01, the same address
where `api.binance.com` answers HTTP 451), needs no API key, has no rate limit,
publishes checksums, and covers 2020-01 → present. The existing ccxt loaders stay
exactly as they are and remain the nightly incremental path.

---

## 1. What the block actually requires

Less than "a full backfill of all four datasets". Only two datasets are read by
anything on the Phase 5 critical path:

| Dataset | Read by | Needed to unblock Phase 5? |
| --- | --- | --- |
| `ohlcv_daily` | `universe/builder.py`, `signals/bars.py`, `signals/panel.py`, the engine's price panel | **Yes — everything** |
| `funding_rate` | `signals/carry.py` | **Yes — `carry` only** |
| `ohlcv_hourly` | nothing | No |
| `open_interest` | nothing | No |

Verified by grep: no signal, no universe rule, and no backtest path touches
`ohlcv_hourly` or `open_interest`. They are Phase 6+ inputs (risk model, size
proxy).

**So the minimum viable pull is `ohlcv_daily` + `funding_rate`, ~200 symbols,
5 years.** That is roughly **37 MB compressed / ~24,000 small files**, and at
8-way concurrency downloads in **under 20 minutes**. Everything else is optional
and is scoped as such in §6.

---

## 2. Routes, and why the archive wins

Measured from this environment on 2026-08-01 (`curl`, no proxy tricks):

| Endpoint | Status | Use |
| --- | --- | --- |
| `api.binance.com`, `fapi.binance.com` | **451** restricted location | unusable here |
| `data.binance.vision` (bulk archive) | **200**, zips + `.CHECKSUM` | **Route A — recommended** |
| `data-api.binance.vision` (spot REST) | 200 | spot klines only; no funding |
| `www.okx.com` `/api/v5/...` candles + funding-rate-history | **200**, real data | Route B fallback |
| `api.bybit.com` | CloudFront country block | unusable here |
| `www.deribit.com` | 200 | options later, not this |

Archive coverage confirmed by S3 listing:
`futures/um/monthly/klines/BTCUSDT/1d/` holds **78 monthly files from 2020-01**,
and the bucket carries **938 USDT-M futures symbols**.

### Route A — Binance public archive (recommended)

- **Pros:** deepest history (2020-01), no key, no rate limit, checksummed,
  identical venue to the one the nightly pipeline will use in production, so
  archive history and future nightly rows land in the same `venue="binance"`
  namespace and join cleanly.
- **Cons:** needs a new loader (~200 lines) because the format is zipped CSV, not
  a ccxt response. It is the only new code this plan requires.

### Route B — ccxt against OKX

- **Pros:** zero new loader code — `OHLCVLoader("okx")` and
  `FundingRateLoader("okx")` should work today.
- **Cons:** a different venue from the production one, so the store carries two
  `venue` values and the universe/audit coverage checks need to agree on which;
  OKX funding settles 8-hourly like Binance but its history endpoint is
  paginated backwards with a ~3-month reach per instrument, so 5 years is many
  more calls than the archive's 60 files; and swap history starts later than
  Binance's for most alts.
- **Use it as:** the fallback if the archive format turns out to be a bigger job
  than estimated, or as a **second source for the audit's price-outlier check**
  (which currently has no second venue).

### Route C — run the existing nightly on the trading machine

`python -m pipeline.nightly --start 2021-08-01 --end 2026-08-01` on the
un-geo-blocked box. This is what `README.md` currently recommends and it still
works — but it is strictly slower (paged ccxt calls, `max_pages_per_symbol = 50`,
rate-limited to 2 req/s) and it can only be run from that one machine, which
means the research loop cannot iterate anywhere else. **Use Route A; keep Route C
as the thing that keeps the store current afterwards.**

---

## 3. Implementation — build the archive loader

Work in phase order and to the project's conventions: doc/spec first, then code,
then tests, then a scratch demo.

### Step 1 — `loaders/archive.py` (new module)

Add `BinanceVisionLoader`, subclassing `BaseLoader` so it inherits symbol
resolution, `event_ts`/`ingested_ts` stamping, and the logged append wrapper.
It must **not** import ccxt.

Public surface, mirroring the existing loaders:

```python
loader = BinanceVisionLoader(market="um", symbols=[...])   # "um" = USDT-M futures
loader.run_daily(window=FetchWindow(start, end))   -> rows appended to ohlcv_daily
loader.run_funding(window=...)                     -> rows appended to funding_rate
```

URL layout (all verified live):

```text
https://data.binance.vision/data/futures/um/monthly/klines/<SYM>/1d/<SYM>-1d-<YYYY-MM>.zip
https://data.binance.vision/data/futures/um/monthly/fundingRate/<SYM>/<SYM>-fundingRate-<YYYY-MM>.zip
https://data.binance.vision/data/spot/monthly/klines/<SYM>/1d/<SYM>-1d-<YYYY-MM>.zip
<any of the above>.CHECKSUM        # "<sha256>  <filename>"
```

Symbol and month enumeration — **list, do not probe.** 404-guessing months
wastes half the requests and cannot tell "not listed yet" from "gap":

```text
https://s3-ap-northeast-1.amazonaws.com/data.binance.vision?delimiter=/&prefix=data/futures/um/monthly/klines/
https://s3-ap-northeast-1.amazonaws.com/data.binance.vision?delimiter=/&prefix=data/futures/um/monthly/klines/BTCUSDT/1d/
```

The listing is XML, paginates at `MaxKeys=1000` via `marker=`, and gives each
symbol's first available month for free — which is also the **listing date** the
universe builder's `min_listing_age_days` rule needs.

**Four format details that will bite otherwise** (all confirmed by download):

1. **Futures klines have a CSV header row; spot klines do not.** Sniff the first
   line for `open_time` rather than assuming either.
2. Kline columns are
   `open_time,open,high,low,close,volume,close_time,quote_volume,count,taker_buy_volume,taker_buy_quote_volume,ignore`.
   `volume` (col 5) is **base** volume — which is what `OHLCV_SCHEMA` wants,
   because `universe/builder.py:92` computes dollar volume as `close * volume`.
   Do not substitute `quote_volume`.
3. `open_time` is epoch **milliseconds**; some 2025+ files switched to
   microseconds. Detect by magnitude (`> 1e14` → µs) rather than trusting the
   month.
4. Funding files are `calc_time,funding_interval_hours,last_funding_rate`. There
   is **no mark or index price** — leave both null. `AUDIT_CONFIG.
   nullable_columns_by_dataset["funding_rate"]` already permits exactly that, so
   the audit warns instead of halting. No config change needed.

Concurrency and politeness: a `ThreadPoolExecutor` at 8 workers, one retry with
backoff on non-200, and verify the `.CHECKSUM` for every file (it is one extra
tiny GET and it is the whole reason to prefer the archive over scraping).

Measured baseline: **~0.55 s per file sequentially**, so ~24,000 files is ~3.7 h
serial and ~20 min at 8 workers.

### Step 2 — asset master, without ccxt

`NightlyPipeline._populate_asset_master` builds the master from
`exchange.load_markets()`, which is unreachable here. The archive loader needs its
own registration path: for each archive symbol, register the **exact** archive
string (`BTCUSDT`, no slash) under `venue="binance"` with the first month seen as
the validity start.

Two decisions to make explicitly and write into the code comment:

- **Multiplier contracts.** ~~`1000BONKUSDT`, `1000SHIBUSDT`, `1MBABYDOGEUSDT`
  are the same underlying at a scaled contract size. Returns are invariant to a
  constant multiplier, so strip the leading `1000`/`1M` and map to the underlying
  base.~~ **This was wrong, and it is the defect §9 documents.** Keep the
  venue's own base: `1000SHIBUSDT → 1000SHIB`, `1MBABYDOGEUSDT → 1MBABYDOGE`.
  The stripping rule assumes a multiplier prefix always denotes the *same*
  asset at a different contract size. It does not: Binance also uses the prefix
  to disambiguate **two different tokens that share a ticker**, and it lists
  both at once — `1000CATUSDT` (2024-10..2026-07) beside `CATUSDT` (2026-07),
  `1000000BOBUSDT` (2025-06..2026-07) beside `BOBUSDT` (2025-11..2026-07).
  Stripping merges two unrelated price series into one `asset_id`, and
  `latest_per_bar` then picks between them by ingestion time — arbitrarily.
  Not stripping cannot merge anything, because the venue never gives two live
  contracts one name.
- **Quote filter.** Keep `*USDT` only. `BTCUSDC`, `BTCBUSD` and dated futures
  (`BTCUSDT_240329`) are separate listings of the same asset and would duplicate
  rows against one `asset_id`.
- **Collision guard.** Whatever the identity rule, assert it: two venue symbols
  resolving to one `asset_id` must refuse to ingest and name both symbols. That
  guard is also the detector for the two cases that *do* need price adjustment
  (a genuine redenomination, a reused equity ticker) — see §10.

### Step 3 — tests (`tests/test_archive_loader.py`)

Per `CLAUDE.md`: no network, every test under 100 ms. Fixture-driven —

- a golden zipped-CSV fixture built in `tmp_path`, one futures (header) and one
  spot (headerless), asserting the parsed frame matches a hand-written expected
  frame exactly;
- ms-vs-µs timestamp detection;
- funding rows land with null `mark_price`/`index_price` and pass
  `FUNDING_RATE_SCHEMA`;
- checksum mismatch raises rather than silently ingesting;
- the S3 XML listing parser against a captured response fixture, including the
  `IsTruncated`/`marker` continuation;
- `1000BONKUSDT → BONK` and `BTCUSDT_240329` excluded;
- the new module joins `tests/conftest.py::isolate_production_datastore`
  (`tests/test_isolation.py` will fail if it does not — that check exists
  precisely for this case).

### Step 4 — scratch demo (`scratch/scratch_archive_backfill.py`)

Real network, `PAPER` guard, temp datastore: pull **one symbol, three months**,
print the frame and the log tail via `start_demo_run("loaders")`. This is the
smoke test that the URL layout has not moved before committing to a 20-minute
run.

### Step 5 — run the backfill

```bash
# 1. one symbol, small window — proves the path end to end
PAPER=true python scratch/scratch_archive_backfill.py

# 2. the real pull, into the real store
python -m loaders.archive --market um --start 2021-08-01 --end 2026-08-01 \
    --datasets ohlcv_daily,funding_rate --max-symbols 200 --log-level INFO

# 3. build the point-in-time universe over that history
python -m universe.builder --venue binance --pit-mode event \
    --start 2021-09-01 --end 2026-08-01 --freq weekly
```

**Run one invocation at a time, and let it finish.** The first real run did not
(§9): two `loaders.archive` processes overlapped, which duplicated ~95,000 bars
and left the second pass truncated part-way through the alphabet. Nothing
prevents this today — `ParquetStore.append` numbers its output file from the
count already in the partition, so two *processes* race for one filename the
same way two threads would. Until the guard in §9 lands, the protection is
operational: start it, watch `logs/loaders.log`, do not start another.

Step 3 matters and is easy to forget: **the universe dataset is an input, not an
output.** `DatastoreUniverse` reads `universe` snapshots, and the audit's
coverage denominator is the latest snapshot — with no snapshots, every backtest
runs on an empty universe and the audit reports coverage as *not evaluated*.
Build snapshots at the rebalance frequency you intend to research at (weekly is
enough for a weekly rebalance and is ~260 snapshots instead of ~1800).

**`--pit-mode event` is not optional here, and leaving it off is the single
easiest way to lose an hour.** The universe builder reads bars point-in-time
like everything else, and `ParquetStore.read(asof=...)` filters `ingested_ts` —
which step 2 has just stamped with *today* on every row, whatever the bar's own
date. So a strict build of a snapshot dated 2021-09-01 asks what was known in
2021 and correctly answers "nothing": every date logs `No OHLCV data available`,
nothing is written, and the command exits 0. This is the same relaxation §4 step
1 describes for the backtester, and it has to be made in both places — snapshots
built in event mode feed backtests run in event mode. The command refuses the
strict run up front rather than discovering it 1,796 times:

```text
preflight failed: nothing in ohlcv_daily was ingested on or before 2026-08-01
(the earliest ingested_ts is 2026-08-02), so under pit_mode='ingestion' every
date in this range sees an empty store ... Re-run with --pit-mode event.
```

The command lives on `universe.builder`, not the `universe.build` this document
originally guessed at — one module named a letter away from an existing one is
the `data-api.binance.vision` trap in package form.

### Step 6 — acceptance checks before any research runs

```bash
python -m audit.acceptance --venue binance          # 0 accepted, 1 blocked, 2 could not start
python -m audit.acceptance --json                   # same, machine-readable
pytest && ruff check .
```

`audit/acceptance.py` is this checklist made executable, one check per bullet.
It earns being a command rather than a paragraph because of the property every
bullet shares: **each names an outcome the tool that produces it exits 0 on.**
`loaders.archive` reports success having skipped a corrupt month;
`universe.builder` reports success having written one snapshot; the store
reports success having stored the same bar twice, because storing it twice is
what append-only means. None of them raise, and each surfaces downstream as a
*quiet* result — a shorter signal history, a thinner cross-section, a book that
never changes — rather than as an error.

The bar to clear (thresholds are the defaults; `--min-years`, `--min-assets`,
`--min-funding-assets` and `--max-gap-days` move them for a deliberately
smaller pull):

- [ ] `ohlcv_daily` spans ≥ 4 years and holds ≥ 150 distinct `asset_id`s
- [ ] `funding_rate` spans the same window for ≥ 100 of them (fewer is expected
      and fine — not every spot listing has a perp, which is exactly `carry`'s
      documented breadth limitation)
- [ ] `count_duplicate_bars(df)` is 0 on a first archive run (the archive has no
      overlap; a non-zero count means the month loop double-counted a boundary).
      Measured on the **raw** frame, before the `latest_per_bar` collapse every
      other check runs behind — after it, this can only ever report zero.
      A raw count alone cannot say *which* cause it is, so the check classifies
      by ingestion cluster and by whether the copies agree on value: copies
      spanning runs that agree are a re-run (a warning — append-only storage
      working as designed), copies inside one run or copies that **disagree**
      are defects and block
- [ ] no two venue symbols resolve to one `asset_id`, and no `(asset_id,
      event_ts)` carries two materially different prices. A disagreement larger
      than a vendor revision means either a ticker collision (§9) or two
      instruments — spot and perp — written into one series
- [ ] every `ohlcv_daily` row for a venue comes from **one market type**. The
      archive pulls `futures/um` while the ccxt `OHLCVLoader` historically read
      spot, and the two land under the same `venue` and `asset_id` with no
      column recording which is which (§9)
- [ ] no asset has a gap > 3 days inside its own listed range —
      `signals/bars.py` trims to the gap-free tail, so an unnoticed hole
      silently shortens every signal's history. Nothing else in the project
      performs this check, and a signal that then rejects the asset for
      insufficient history looks exactly like a signal working as designed.
      "Inside its own listed range" is what makes it answerable: an asset listed
      in 2023 is not missing 2021. The threshold is *missing days*, so three
      consecutive absent bars passes and four fails
- [ ] `python -m pipeline.nightly --days 1` on the trading machine still resumes
      cleanly on top of the archive rows. **This document expected a checkpoint
      "written with the archive's covered interval" and there is none** —
      checkpoints belong to `BackfillRunner`, which `BinanceVisionLoader` does
      not use. The consequence is narrower than it sounds, which is why the
      check *warns* rather than blocks: with no coverage recorded
      `resume_window` returns the request unchanged, so `--days 1` fetches its
      day and appends beside the archive's rows under the same `venue` and
      `asset_id`. What it costs is a wide `--start` re-run, which would re-fetch
      years the store already holds — API budget and duplicate bars, not
      correctness
- [ ] the `universe` dataset holds one snapshot per rebalance date over the whole
      window, with a plausible member count — an empty or one-row `universe` is
      the step-3 failure above, and it surfaces downstream as an empty backtest
      rather than as an error. The cadence is *inferred* from the modal spacing
      of the snapshots present, so nothing has to remember which `--freq` the
      rebuild used, and a missing date shows up as a spacing wider than the mode
- [ ] `pytest` green, `ruff check .` clean

```bash
PAPER=true python scratch/scratch_acceptance.py   # four stores, each broken one way
```

---

## 4. Then the research — filling §5 and §6

Only after §3's checks pass. Nothing here needs new infrastructure; the tools
already exist.

1. **Use `--pit-mode event`, and label the results.** A bulk backfill stamps
   every row with one `ingested_ts` (the moment it ran), so strict
   `pit_mode="ingestion"` sees nothing before that date and every book comes back
   empty. This is already documented as a deliberate, explicit relaxation; the
   methodology docs require the numbers to be labelled *research indications, not
   live-fidelity results*. Live-fidelity numbers only start accruing from the
   day the nightly pipeline begins collecting day by day.

   The mode has to be the **same one §3 step 3 built the universe snapshots
   under**. A backtest in event mode over snapshots built in ingestion mode
   reads an empty universe (there are none), and one in ingestion mode over
   event-mode snapshots cannot see them either — their `ingested_ts` is the day
   the rebuild ran. Neither combination errors; both produce empty books.

2. **Walk-forward parameter grid, per signal.** Copy the pattern in
   `scratch/scratch_markov_param_grid.py` — it already selects on prior folds
   only, reports the overfitting tax against the best full-sample cell, the
   marginal effect of each parameter, and performance at 2× costs.

   ```bash
   PAPER=true python scratch/scratch_markov_param_grid.py \
       --grid full --folds 5 --pit-mode event \
       --out scratch/output/markov_grid.csv
   ```

   Generalise it to take a `--signal` argument rather than copying it six times.

3. **Fill §5 of each methodology doc** with the out-of-sample numbers: mean rank
   IC, IC IR, net-of-cost IR, drawdown, turnover, and the overfitting tax. Then
   move `Status` off `draft`. `tests/test_methodology_docs.py::
   test_all_six_are_still_draft` is *designed* to fail on that day — updating it
   is part of the change, not a breakage.

4. **Fill §6 (breadth) with measured correlations.**

   ```bash
   PAPER=true python scratch/scratch_signal_breadth.py --pit-mode event
   ```

   The synthetic 0.86 score correlation between `cross_sectional_momentum` and
   `time_series_momentum` is a fact about the generator. Replace it with the
   real number and record the effective independent-bet count.

5. **Keep the store current.** Once the archive history is in, hand the tail back
   to the nightly job on the trading machine (Route C). Archive months publish on
   a lag of a day or so, so the ccxt loaders own the recent edge and the archive
   owns history — which is also what makes the `ingested_ts` story truthful going
   forward.

---

## 5. Gotchas found in the code while writing this

Worth knowing before the run, not after.

- **The store partitions by `ingested_ts`, not `event_ts`**
  (`datastore/store.py:80`, and no loader overrides `partition_key`). A single
  bulk backfill therefore lands *entirely in one partition*,
  `data/parquet/ohlcv_daily/date=<the-day-you-ran-it>/`. Consequences: `read()`'s
  `date_range=` argument prunes on ingestion date and so cannot narrow a
  historical read; and that one directory holds a few hundred MB in a couple of
  files. Neither is wrong — it is the honest record of when the data was learned
  — but it is worth deciding deliberately whether the archive loader should pass
  `partition_key="event_ts"`. **Recommendation: leave the default.** Overriding
  it would make the backfill's partitions mean something different from every
  other write in the store, and the read path already filters in memory.
- **Chunk the archive appends.** `ParquetStore.append` builds the whole frame in
  memory and writes one file per partition. Append per (symbol, month) rather
  than accumulating 5 years × 200 symbols first.
- **`carry` will be `None` for a lot of the universe.** Spot-listed assets with
  no perp score no view at every rebalance. That is documented behaviour, and it
  means `carry`'s effective breadth is genuinely smaller — don't read it as a
  data bug.
- **Nothing needs `LOADER_CONFIG` changes.** `page_limit` and
  `max_pages_per_symbol` are ccxt concerns; the archive returns whole months.
  `max_symbols_per_run = 200` is still the right budget (above
  `UNIVERSE_CONFIG.target_size = 150`, so the builder has more candidates than it
  keeps).
- **The audit's `price_outliers` check still has no second venue.** If Route B is
  stood up as a second source, that check finally does what its docstring says.

---

## 6. Optional extras, in priority order

Explicitly out of scope for unblocking Phase 5; do them when the phase that needs
them arrives.

| Want | Source | Cost | When |
| --- | --- | --- | --- |
| `open_interest` | `futures/um/daily/metrics/<SYM>/<SYM>-metrics-<YYYY-MM-DD>.zip` — verified back to 2021-01, 5-minute granularity | **daily files only**: ~1,825 files × 200 symbols ≈ 365k requests, ~4 GB. Downsample to a daily close-of-day snapshot on ingest, and start with the top ~30 symbols | Phase 6 (size proxy) |
| `ohlcv_hourly` | same monthly klines path with `1h` | ~550 MB compressed, ~12k files | when a signal needs it — none does |
| Second venue for outlier checks | OKX via existing ccxt loaders | free, existing code | anytime |
| Options surface | Deribit (reachable) | — | much later |

---

## 7. Time and size budget

| Item | Estimate |
| --- | --- |
| Build + test `loaders/archive.py` | half a day |
| `ohlcv_daily`, 200 symbols × 5 y | ~12,000 files, ~26 MB zipped, ~15 min at 8 workers |
| `funding_rate`, 200 symbols × 5 y | ~12,000 files, ~11 MB zipped, ~15 min |
| Universe snapshots (weekly, 5 y) | ~260 builds, minutes |
| Walk-forward grid, six signals | hours of compute, unattended |
| **Total to a filled §5** | **~2 days of elapsed work** |

---

## 8. Open decisions for the operator

1. **Spot or futures klines for `ohlcv_daily`?** **Decided 2026-08-09: futures
   (`futures/um`), everywhere.** It matches the venue the funding rate comes
   from and is what a perp strategy would actually trade. The part this
   originally left implicit is what caused half of §9: the decision binds the
   **ccxt `OHLCVLoader` too**, which had been reading the venue's default spot
   markets since Phase 2. Two market types under one `venue` and one `asset_id`
   is not a preference, it is two different instruments in one price series.
   `OHLCVLoader` moves onto `LOADER_CONFIG.perp_market_type`, joining the
   funding-rate and open-interest loaders, and the choice is recorded in the
   methodology docs' §2 data-inputs section because it changes what the backtest
   is a backtest *of*.
2. **Backfill start date.** 2021-08-01 gives 5 years and avoids the thin,
   unrepresentative 2020 perp listings. Going back to 2020-01 adds a regime
   (the COVID crash, the 2020 bull run) at the cost of a much smaller universe.
3. **Where the store lives.** The archive route means the backfill can run
   anywhere — but the nightly job and the deploy gate live on the trading
   machine, and `data/` is git-ignored. Either run the backfill *there*, or plan
   how the parquet store gets copied across.

---

## 9. What the first real backfill found (2026-08-03 → 08-09)

The pull ran, the universe was rebuilt, and `python -m audit.acceptance` blocked
at **3 of 7**. `scratch/scratch_backfill_forensics.py` and a listing of the
bucket settled every one of the four failures. Three distinct defects were
underneath them, and the counts the gate printed do not separate them — which is
the point of writing this down rather than only fixing it.

### 9.1 Two canonicalisations of `asset_id`, in one codebase

`loaders/archive.py` stripped multiplier prefixes (`1000SHIBUSDT → SHIB`);
`pipeline/nightly.py::_populate_asset_master` used ccxt's `market["base"]`,
which keeps them (`1000SHIB/USDT:USDT → 1000SHIB`). `register_symbols` has a
guard against re-registering, but it matches the **literal symbol string**, and
`1000SHIBUSDT` is not `1000SHIB/USDT:USDT` — so it never fired. The store ended
up holding `1000000BOB`, `1000CAT`, `1000SHIB` *and* `BOB`, `CAT`, `SHIB` as
separate `asset_id`s.

The stripping rule rested on a premise that is false: that a multiplier prefix
always denotes the same asset at a different contract size. Measured against the
bucket on 2026-08-09 — 986 published `futures/um` kline symbols, 832 supported
after the USDT/dated filter, 830 distinct `asset_id`s under the strip rule, and
**exactly two collisions**:

| `asset_id` | Symbols merged into it | Published months | Overlap |
| --- | --- | --- | --- |
| `BOB` | `1000000BOBUSDT` / `BOBUSDT` | 2025-06..2026-07 / 2025-11..2026-07 | **9 months** |
| `CAT` | `1000CATUSDT` / `CATUSDT` | 2024-10..2026-07 / 2026-07 | **1 month** |

Both pairs traded **simultaneously**, which is what settles it. A redenomination
*replaces* one contract with another; it does not run both side by side for nine
months. These are two different tokens sharing a ticker, disambiguated by
Binance with a contract-size prefix, and the strip destroys the distinction.

Three independent confirmations, worth recording because each rules out a
different innocent explanation:

- **The ratios are not powers of ten.** `CAT` shows `0.001336` against `950.37`
  (7.1e5×) and `BOB` shows 2.86×. A scale difference would be exactly 1,000× or
  1,000,000×.
- **Funding disagrees, and funding is dimensionless.** The same underlying at two
  contract sizes has *identical* funding. `BOB`'s two series differ by 42–56×,
  one pinned at Binance's 5e-05 base rate and the other floating. `funding_rate`
  has no spot equivalent and no vendor revisions, so a disagreement there can
  only be two contracts.
- **The counts match the overlaps.** `BOB`'s 9-month kline overlap is ~273 days;
  the forensics reported **253** disagreeing `BOB` bars. Every `CAT` sample falls
  in 2026-07, its only overlapping month.

**Fix:** one shared canonicalisation, no stripping, used by every loader; plus a
collision guard that refuses to ingest two symbols under one `asset_id`. Note
what this costs and does not cost — under the new rule `1000SHIB` is simply the
name of the thing Binance lists, and the "two perfectly correlated asset_ids"
worry the strip was written to prevent requires both contracts to be listed at
once, which for a genuine multiplier pair does not happen.

### 9.2 Spot and perpetual prices spliced into one series

~112 assets showed ~61 disagreeing `ohlcv_daily` bars each and **zero**
disagreeing funding bars. 61 days is exactly 2026-06-01..2026-07-31, the window
where the ccxt runs overlap the archive run. The `OHLCVLoader` read the venue's
default **spot** markets; the archive was pulled with `--market um`
(**futures**). Same `venue`, same `asset_id`, two instruments, nothing recording
which — and `latest_per_bar` resolves it by ingestion time, so the series
silently switches instrument at the join.

This hides where §9.1 does not: spot and perp closes agree to a fraction of a
percent, so the ratio is ~1.00× and only the *count* of disagreements gives it
away. See §8 decision 1, now decided.

It also explains the `bar_gaps` failure. `AUDIO`'s 733-day hole
(2024-05-28..2026-06-01) begins after `AUDIOUSDT`'s last published archive month
(**2024-05**) — the perp was delisted, and the 2026 bars exist only because
ccxt's spot `AUDIO/USDT` supplied them. `BNX` is the genuinely different case:
its hole (2023-01-31..2023-02-22) sits *inside* published months 2022-04..2026-07,
so that one is a file the loader skipped and a re-pull recovers it.

### 9.3 Two loader invocations running at once

`funding_rate`'s largest ingestion cluster is stamped starting 13:49, *inside*
`ohlcv_daily`'s 13:39 + 19.7 min cluster: at least two `loaders.archive`
processes were live together. That accounts for the bulk of the duplication —
~95,000 bars, including assets with their entire 1,826-day history stored twice.

The forensics classified these as "one run emitted the bar twice", which is a
limitation of its own heuristic rather than a finding: `RUN_GAP_MINUTES = 30`
cannot separate invocations that *overlap*. Two things say otherwise. The
duplicated assets are alphabetically early (`BAND`, `BAT`, `BCH`, `BEL`) while
the 60 assets with no duplicate at all sort after "BL" (`BLUR`, `BMNR`, `BNB`,
`BNX`, `BOME`, `BRETT`) — a run that double-emits has no reason to stop at BL, an
interrupted second pass does. And the bucket listing shows `SHIB`, `BEL`,
`BAND`, `BAT` and `BCH` have **no** colliding symbol, so §9.1 cannot explain
them.

**Fix:** a collision-proof output filename (or a partition claim) in
`ParquetStore.append`, so two processes cannot race for `data_0007.parquet`;
`--skip-loaded` on the archive loader so a resumed run is cheap; and a wider,
better-founded clustering signal in the forensics.

### 9.4 The universe warm-up, which was never a defect

Six leading snapshots with no members, all carrying `listing_age` on the entire
cross-section, first populated snapshot 2021-09-01. The history was built from
2021-08-01 — the same date as the first bar — so for 30 days every asset fails
`UNIVERSE_CONFIG.min_listing_age_days`. **Zero** empty snapshots after that date.
This is why §3 step 5 says `--start 2021-09-01`, and the gate should treat
empties *before* the first populated snapshot as warm-up rather than as the
step-3 failure it is looking for.

Separately: member counts came back min/median/max **0/64/139** against
`target_size = 150`, and nothing flagged it because the gate's floor is a median
of 20. The cause is upstream — the archive selects symbols **alphabetically**
when no `--symbols` list is given, so the 200 pulled are not the 200 most liquid.
A liquidity-ranked selection belongs in the re-pull.

### 9.5 The re-pull

Given three overlapping defects in one dataset, repairing in place would mean
reasoning about which defect produced each row, with no column recording the
answer. The decision is a clean re-pull once the fixes land:

```bash
# 1. keep the old store until the new one is accepted -- the factors,
#    symbol sets and ingestion clusters can only be read from it
mv data/parquet data/parquet.pre-5.9

# 2. re-pull, perps only, symbols chosen by liquidity rather than alphabet
python -m loaders.archive --market um --start 2021-08-01 --end <today> \
    --datasets ohlcv_daily,funding_rate --symbols <liquidity-ranked list> \
    --log-level INFO

# 3. universe from past the listing-age warm-up
python -m universe.builder --venue binance --pit-mode event \
    --start 2021-09-01 --end <today> --freq weekly

# 4. only then
python -m audit.acceptance --venue binance
```

Do not delete `data/parquet.pre-5.9` until step 4 is green.

---

## 10. Price adjustments — deliberately not built yet

An earlier draft of the §9 remediation was going to treat `BOB` and `CAT` as
redenominations and build split-style adjustment factors to splice the two price
scales together. The listing evidence killed that: they are not redenominations,
no factor exists for them, and across all 832 supported symbols there is **not
one** genuine redenomination to exercise such a mechanism on.

It is still coming, because equities guarantee it — hundreds of splits a year,
plus dividends for total return, plus tickers that get reused by different
companies (the §9.1 problem at scale). `PLAN.md`'s Phase 10 already commits to
"a real security master (tickers change, mergers — true security matching)".

**It is scheduled as Phase 10a with an explicit trigger rather than built
speculatively**, for two reasons. There is nothing in the data for it to do
today, so it would ship untested against any real case. And its shape depends on
a decision not yet made: equity vendors differ in whether they ship
already-adjusted prices (convenient, but the series rewrites itself on every
split, which breaks append-only and makes yesterday's backtest unreproducible)
or raw prices plus a factor table (point-in-time honest, more work). Choosing
between those is not possible before the vendor is chosen.

**The trigger is the §9.1 collision guard**, which is the same query in all three
cases — two symbols resolving to one `asset_id` — distinguished only by whether
the validity windows overlap or abut:

| Guard fires on | Windows | Meaning | Action |
| --- | --- | --- | --- |
| `1000CATUSDT` / `CATUSDT` | overlap | two tokens, one ticker | keep separate — the Phase 5.9 rule |
| `1000XUSDT` / `1000000XUSDT` | abut | genuine redenomination | **build Phase 10a** |
| `GM` pre/post-2009 | abut | reused equity ticker | **build Phase 10a** |

So the guard built for crypto in Phase 5.9 is the thing that tells you when the
adjustment engine is needed, and equities inherits it rather than rediscovering
it.
