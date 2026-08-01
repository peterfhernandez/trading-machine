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

- **Multiplier contracts.** `1000BONKUSDT`, `1000SHIBUSDT`, `1MBABYDOGEUSDT` are
  the same underlying at a scaled contract size. Returns are invariant to a
  constant multiplier, so **strip the leading `1000`/`1M` and map to the
  underlying base** (`BONK`, `SHIB`) — otherwise `carry` and the price signals
  score what looks like two different assets that are perfectly correlated,
  which quietly halves the breadth report's honesty.
- **Quote filter.** Keep `*USDT` only. `BTCUSDC`, `BTCBUSD` and dated futures
  (`BTCUSDT_240329`) are separate listings of the same asset and would duplicate
  rows against one `asset_id`.

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
python -m universe.build --venue binance --start 2021-09-01 --end 2026-08-01
```

Step 3 matters and is easy to forget: **the universe dataset is an input, not an
output.** `DatastoreUniverse` reads `universe` snapshots, and the audit's
coverage denominator is the latest snapshot — with no snapshots, every backtest
runs on an empty universe and the audit reports coverage as *not evaluated*.
Build snapshots at the rebalance frequency you intend to research at (weekly is
enough for a weekly rebalance and is ~260 snapshots instead of ~1800).

If `universe.build` has no CLI yet, add one — it is a thin wrapper over
`UniverseBuilder.build_and_store` in a date loop.

### Step 6 — acceptance checks before any research runs

```python
from datastore import ParquetStore, latest_per_bar, count_duplicate_bars
from config import DATASTORE_PATH
store = ParquetStore(DATASTORE_PATH)

store.dataset_info("ohlcv_daily")     # expect ~200 assets x ~1800 bars
store.dataset_info("funding_rate")    # expect ~3 settlements/day/asset
```

The bar to clear:

- [ ] `ohlcv_daily` spans ≥ 4 years and holds ≥ 150 distinct `asset_id`s
- [ ] `funding_rate` spans the same window for ≥ 100 of them (fewer is expected
      and fine — not every spot listing has a perp, which is exactly `carry`'s
      documented breadth limitation)
- [ ] `count_duplicate_bars(df)` is 0 on a first archive run (the archive has no
      overlap; a non-zero count means the month loop double-counted a boundary)
- [ ] no asset has a gap > 3 days inside its own listed range —
      `signals/bars.py` trims to the gap-free tail, so an unnoticed hole
      silently shortens every signal's history
- [ ] `python -m pipeline.nightly --days 1` on the trading machine still resumes
      cleanly on top of the archive rows (checkpoint written with the archive's
      covered interval)
- [ ] `pytest` green, `ruff check .` clean

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

1. **Spot or futures klines for `ohlcv_daily`?** Futures (`futures/um`) matches
   the venue the funding rate comes from and is what a perp strategy would
   actually trade; spot has longer history for older assets. **Recommendation:
   futures/um**, and record the choice in the methodology docs' §2 data-inputs
   section, because it changes what the backtest is a backtest *of*.
2. **Backfill start date.** 2021-08-01 gives 5 years and avoids the thin,
   unrepresentative 2020 perp listings. Going back to 2020-01 adds a regime
   (the COVID crash, the 2020 bull run) at the cost of a much smaller universe.
3. **Where the store lives.** The archive route means the backfill can run
   anywhere — but the nightly job and the deploy gate live on the trading
   machine, and `data/` is git-ignored. Either run the backfill *there*, or plan
   how the parquet store gets copied across.
