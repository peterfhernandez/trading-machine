# Poor Man's Trading Machine — TODO

Phased build order. Each phase produces something runnable and tested before
the next begins. Work each item as a small Claude Code task: implement +
pytest tests + `scratch/scratch_*.py` demo.

Legend: [ ] todo · [~] in progress · [x] done

## Phase 0 — Scaffold

- [x] Repo skeleton: packages `datastore/ loaders/ audit/ universe/ backtest/
      signals/ risk/ portfolio/ execution/ attribution/ pipeline/`, plus
      `tests/ scratch/ config.py`
- [x] `config.py`: paths, venue keys (env vars), universe params, PAPER flag
- [x] pytest + ruff configured; CI optional
- [x] Copy CLAUDE.md into repo root

## Phase 1 — Datastore & asset master (M1)

- [x] Parquet store: append-only writer with schema enforcement, partition by
      dataset/date
- [x] Reader API (Polars lazy scans; date-range + column pruning)
- [x] `event_ts` / `ingested_ts` convention enforced by writer
- [x] Asset master: canonical asset_id, venue symbol maps with validity ranges
- [x] Tests: no-overwrite guarantee, point-in-time reads, symbol resolution
      across a rename/delisting fixture

## Phase 2 — Loaders & audit (M2, M3)

- [x] OHLCV loader (daily + hourly) for top ~150 USDT/USD perps via ccxt
- [x] Funding-rate loader; open-interest loader
- [x] Backfill runner (resumable, rate-limit aware) — pull 3–5 years history
      (windowed `[start, end]` fetching, checkpoint-driven resumption, paged
      venue responses)
- [x] Audit module: coverage %, null rates, outlier price jumps vs. second
      venue, freshness checks; Telegram alert on threshold breach
- [x] Nightly loader+audit job runnable end-to-end from one command

## Phase 3 — Universe (M4)

- [x] Liquidity metrics (rolling median dollar volume), listing-age filter,
      stablecoin/wrapped exclusion list
- [x] Daily universe membership written point-in-time to the store
- [x] Sanity scratch: plot universe size and turnover over history

## Phase 4 — Backtester (M5)

- [x] Walk-forward engine: rebalance calendar, point-in-time data exposure,
      next-bar execution, cost model (spread + impact stub)
- [x] Metrics: returns, vol, IR, drawdown, turnover, per-signal IC series
- [x] Golden tests: a hand-computed 3-asset fixture the engine must reproduce
      exactly
- [x] Validate: buy-and-hold BTC through the engine matches raw data

## Phase 5 — Signals → alphas (M6)

- [x] Signal interface + registry; METHODOLOGY.md template
      (registry refuses to register a signal whose methodology doc is missing)
- [x] Markov mean reversion (reversal family) — implemented + tested; backtest
      evidence still to be collected via the walk-forward parameter grid
- [x] Cross-sectional momentum (with skip-window)
- [x] Time-series momentum
- [x] Carry (funding rate)
- [x] Short-term reversal
- [x] Low-volatility
- [x] Shared point-in-time series reader + per-asset primitives
      (`signals/bars.py`) so five signals do not re-implement the
      latest-ingestion collapse and the gap-free trim five times
- [x] Alpha refinement: z-scoring, winsorizing, IC estimation from backtests,
      shrinkage; alpha = vol × IC × z (`signals/alpha.py`; normal-normal
      shrinkage with `prior_ic_std = 0.02`, capped at 0.10)
- [x] Signal correlation matrix report (breadth check: are these independent?)
      — `signals/breadth.py`: score correlation (do they pick the same
      assets), IC correlation (do they work at the same times), and the
      effective independent-bet count
- [ ] **Blocked on data**: run the walk-forward parameter grid against a real
      backfill and fill in Sections 4-6 of every methodology doc with the OOS
      numbers. This needs a multi-year `ohlcv_daily` + `funding_rate` backfill
      that no environment has produced yet, so every doc's §5 says "not yet
      collected" rather than carrying a synthetic number that would only
      measure the generator. Until this is done, all six signals stay
      `draft` — they are implemented and tested, not evidenced.

## Phase 5.5 — Logging & Observability Retrofit (cross-cutting)

Closes the observability gap flagged during a PLAN/README/TODO review:
`LOGS_PATH` existed but nothing wrote to it, logging wasn't listed as a
cross-cutting concern, and an unattended-run failure outside the audit module
had no durable record. Full design in `LOGGING.md`; reference implementation
in `logging_config.py`. This phase retrofits logging into the already-built
Phases 1-4 and current Phase 5 work; Phases 6-9 build logging in from the
start instead (see their checklists below).

- [x] Design the architecture: rotation (10 MB, `RotatingFileHandler`) and
      retention (12 months, decoupled time-based pruning) — `LOGGING.md`
- [x] Reference implementation: `logging_config.py` (`get_logger`,
      `configure_logging`, `set_run_id`/`get_run_id`, `prune_old_logs`) —
      smoke-tested (JSON output, run_id correlation, exception capture,
      retention pruning all verified working)
- [x] Confirmed `config.py` already has a `LogConfig`/`LOG_CONFIG` stub
      (`level` + `file`) from the Phase 0 scaffold, unused anywhere. Extend it
      in place — don't add a second config object — with `console_level`,
      `dir` (replaces the single `file` path), `max_bytes`, `retention_days`,
      `components`; see `LOGGING.md` §4 for the exact replacement. This keeps
      it consistent with `LOADER_CONFIG` / `AUDIT_CONFIG` / `UNIVERSE_CONFIG`,
      which already live in `config.py` the same way.
- [x] Apply that `LogConfig` change to the real `config.py`, and confirm
      nothing already reads the old `LOG_CONFIG.file` field before removing it
      (the change was already applied; a grep confirmed no reader, and
      `tests/test_logging.py` now asserts the field is gone)
- [x] Retrofit Phase 1: `ParquetStore.append`/`.read`, the no-overwrite guard,
      `AssetMaster` symbol resolution
      (the "no-overwrite guard" turned out not to exist as a raising check —
      `append` never overwrites, it writes a new numbered file beside what is
      there. That path logs DEBUG; the *schema* guard, which does raise, logs
      WARNING first. Recorded in `LOGGING.md` §6.)
- [x] Retrofit Phase 2: `BaseLoader`'s append wrapper (replaces the
      unspecified "error logging"), `BackfillRunner` checkpoints,
      `DataAudit`'s checks + the CRITICAL-before-halt log line,
      `NightlyPipeline` stage entry/exit/duration
- [x] Retrofit Phase 3: `UniverseBuilder.build_and_store` (snapshot size,
      exclusion breakdown, turnover against the previous snapshot)
- [x] Retrofit Phase 4: `Backtester.run` (INFO summary; DEBUG per-rebalance,
      opt-in)
- [x] Retrofit Phase 5: `signals.register`, every signal's reject paths at
      DEBUG (`markov_mean_reversion` plus the five added this phase)
- [x] Wire `prune_old_logs()` into `NightlyPipeline` as its final step; expose
      `python -m pipeline.prune_logs` standalone (with `--dry-run`)
- [x] Tests: `caplog` assertions that a CRITICAL record is emitted on every
      halt path and on unhandled per-stage exceptions; a `prune_old_logs()`
      unit test with fabricated file ages under `tmp_path`
      (`tests/test_logging.py`, 54 tests)
- [x] Update each existing scratch demo to show a sample log line
      (`scratch/log_demo.py`: one `start_demo_run("<component>")` call sets a
      `run_id` and prints that component's log tail at exit)
- [x] Add `logs/` to `.gitignore` (already present from the Phase 0 scaffold)

## Phase 6 — Risk model (M7)

- [ ] Wire logging per `LOGGING.md` as the module is built (`get_logger`,
      stage timings, error paths at each check/regression) — built in from
      the start, no retrofit needed
- [ ] Sector/ecosystem tagging in asset master
- [ ] Daily cross-sectional factor regressions (beta, size proxy, momentum,
      vol, sectors)
- [ ] EWMA factor covariance + Ledoit–Wolf shrinkage; specific variances
- [ ] Validation: predicted vs. realized portfolio vol on random portfolios
- [ ] Backtester upgraded to report ex-ante risk and risk-adjusted metrics

## Phase 7 — Portfolio construction (M8)

- [ ] Wire logging per `LOGGING.md` as the module is built — built in from
      the start, no retrofit needed
- [ ] v1 rank-based long/short with vol targeting, position caps
- [ ] v2 cvxpy optimizer: max α − λσ² − costs; market-neutral, max-weight,
      turnover, gross-leverage constraints
- [ ] Backtest full pipeline (signals → alphas → risk → optimizer) vs. v1;
      keep whichever wins net of costs

## Phase 8 — Execution & shortfall (M9)

- [ ] Wire logging per `LOGGING.md` as the module is built — built in from
      the start, no retrofit needed; the kill switch follows the same
      CRITICAL-log-before-alert rule as the audit halt
- [ ] Paper broker: positions, fills at next bar ± spread, PnL ledger
- [ ] Exchange testnet adapter (start with one venue)
- [ ] Zero-cost shadow portfolio + implementation-shortfall report
- [ ] Kill switch + max-daily-loss halt in config

## Phase 9 — Pipeline, attribution, ops (M10, M11)

- [ ] Wire logging per `LOGGING.md` as each module is built — built in from
      the start, no retrofit needed
- [ ] Daily scheduled run: load → audit → universe → alpha → risk → optimize →
      execute (paper) → report; audit failure halts trading stages, not
      reporting stages; `prune_old_logs()` runs as the final step
- [ ] Attribution: PnL split into factor / specific / costs; per-signal IC
      decay tracking
- [ ] Daily Telegram/HTML report
- [ ] Run unattended ≥ 4 weeks on paper; review attribution weekly — reviewing
      `logs/pipeline.log` (by `run_id`) is now part of that review, not just
      the attribution report
- [ ] Cloud/upgrade triggers documented (only if: backfills > hours, data >
      disk, or 24/7 uptime needed → a $5 VPS before anything fancier)

## Phase 10 — Decision gate, then equities

- [ ] Gate: does paper IR net of pessimistic costs justify tiny live capital?
      If no — iterate signals, that is normal; the machine is still the asset
- [ ] Equities extension: EOD equity loader + real security master (point-in-
      time tickers), borrow costs in cost model, re-run same pipeline

## Progress log

- 2026-07-24: Plan created from video analysis; phases defined.
- 2026-07-25: Phase 0 scaffold complete — directories, config.py, pytest+ruff configured, README.md created.
- 2026-07-26: Phase 1 complete — ParquetStore (append-only, point-in-time), AssetMaster (symbol resolution), 15 tests passing, scratch demo working.
- 2026-07-26: Phase 2 complete — OHLCV/funding/OI loaders via ccxt, backfill runner with checkpoint tracking, data audit (5 checks + Telegram alerts), nightly pipeline orchestration. 31 tests passing, 5 scratch demos working. Ready for Phase 3 (universe).
- 2026-07-27: Phase 3 complete — UniverseBuilder computes rolling median dollar
  volume, listing age, and stablecoin/wrapped exclusions from `ohlcv_daily`,
  ranks eligible assets by liquidity, caps at `target_size`, and writes
  point-in-time membership to the `universe` dataset (one row per asset
  considered, with `exclusion_reason` for anything left out). `compute_turnover`
  added for size/turnover monitoring. 15 tests passing, scratch demo shows
  weekly snapshots and a turnover event. Ready for Phase 4 (backtester).
- 2026-07-27: Phase 4 complete — walk-forward `Backtester` with next-bar
  execution, drift-aware turnover, spread+impact `CostModel`, rebalance
  calendar (daily/weekly/monthly), per-signal rank IC series, and a metrics
  module (returns, vol, IR, drawdown, hit rate, turnover, costs). Two
  point-in-time modes: strict `ingested_ts <= asof` by default, opt-in
  `pit_mode="event"` so backfilled history — where every row shares one
  ingestion timestamp — is researchable without silently pretending it is
  live-fidelity. Annualization follows the holding period, not the bar, so
  weekly rebalances no longer annualize as if daily. 77 tests passing (golden
  3-asset fixture reproduced exactly; buy-and-hold BTC matches the raw
  open-to-open series to 2e-16), scratch demo runs three strategies end to
  end. Ready for Phase 5 (signals → alphas).
- 2026-07-30: Phase 5 started — signal interface + registry (registration
  requires a methodology doc), shared cross-sectional transforms (winsorize then
  z-score, `None` preserved as "no view"), and the first signal:
  `markov_mean_reversion`. Built against the real `RebalanceContext` (one
  universe-wide read per rebalance, not one per asset). Three methodology
  decisions recorded in the doc: `return_horizon_days` split out of
  `state_window_days` (one parameter for both left 20 heavily-overlapping
  observations to estimate a mean, a variance and k-1 cutoffs), state midpoints
  moved onto the transition matrix's window so probabilities and the values they
  weight describe one period, and the estimation window fixed at
  `min_history_bars` (244 at defaults) so a score is independent of how much
  history an asset has. Guards added for gaps in the bar series and for
  duplicate ingestions of the same bar. 68 tests (golden 10-bar fixture with
  every z-score, cutoff, state, transition row, midpoint and the final score
  derived by hand; point-in-time tests through a real store in both pit modes;
  every reject path returns `None`, never `0.0`). Walk-forward parameter grid
  script written (`scratch/scratch_markov_param_grid.py`) — selects parameters on
  prior folds only and reports the overfitting tax, parameter sensitivity and
  cost sensitivity. Backtest evidence not yet collected: the grid has been run
  only on synthetic data as a smoke test, and Section 5 of the methodology doc
  is deliberately still empty.
- 2026-07-30: Fixed three nightly-pipeline faults that surfaced as
  "TRADING HALTED: critical audit failure" on `funding_rate`/`open_interest`
  with only 15 assets covered. (1) The audit's coverage check divided by a
  hardcoded 150 regardless of dataset or actual universe; the denominator is now
  the latest point-in-time `universe` snapshot (`event_ts <= asof`,
  `ingested_ts <= asof`), with the check reporting itself *not evaluated* (a
  warning, not a halt) when no snapshot exists, and `universe_size=` available
  as an explicit override. (2) The funding-rate and open-interest loaders sliced
  `exchange.symbols[:50]` *before* filtering to USDT pairs, so an alphabetically
  interleaved symbol list yielded ~15 of a 50-symbol budget; symbol selection
  moved to `loaders.base.select_usdt_symbols` (filter, then cap at
  `LOADER_CONFIG.max_symbols_per_run = 200`), shared by all three loaders.
  (3) Those two loaders ran against ccxt's default spot markets, which have
  neither funding nor open interest, and swallowed the resulting per-symbol
  errors; they now open the venue with
  `defaultType=LOADER_CONFIG.perp_market_type` and prefer perpetual symbols.
  The nightly pipeline registers both spot and perp symbol namespaces in the
  asset master (skipping already-mapped symbols instead of appending duplicates
  every run) and logs warning-severity checks so an unevaluated check is never
  silent. 263 tests passing; `scratch/scratch_perp_symbols.py` demos symbol
  selection and market types against a live venue.
- 2026-07-31: Stopped the nightly run re-fetching and double-counting data.
  (1) `BackfillRunner` called `loader.fetch()` to test for emptiness and then
  `loader.run()`, which fetched again — every dataset was pulled from the venue
  twice per run. The `run*()` methods now return the number of rows appended, so
  the runner fetches once and checkpoints on the returned count. (2) The store is
  append-only, so overlapping fetch windows, retried runs and vendor revisions
  all store the same bar more than once; the backtester's price panel and the
  signal panels already collapsed those to the latest ingestion, but the universe
  builder and the audit did not. `datastore.latest_per_bar` now owns that
  collapse (one implementation, previously duplicated in `backtest/engine.py` and
  `signals/markov_mean_reversion.py`) and the universe builder applies it before
  computing rolling median dollar volume — un-collapsed rows weighted the
  duplicated stretch of history, always the most recent days, more heavily than
  the rest, which silently moved universe membership. The audit gained a sixth
  check, `duplicate_bars` (warning, never a halt), and runs every other check
  against one row per bar, so a re-ingested bar no longer dilutes a null rate and
  a revision is no longer read as a price jump. Ordering is the subtlety and is
  documented: collapse *after* the point-in-time filter, never before. 292 tests
  passing; `scratch/scratch_duplicate_bars.py` demos an overlapping window
  end to end. Not addressed: the checkpoint file is still written but never read,
  so the fetch window is still `days_back` from now on every run — real windowed
  fetching needs `start`/`end` threaded through the loaders and, for funding rate
  and open interest, ccxt's `*_history` calls.
- 2026-07-31: Windowed, resumable fetching — the loaders take a `[start, end]`
  interval instead of "days back from now", and the checkpoint is finally read
  back. `loaders/window.py` adds `FetchWindow`, `Coverage` and `resume_window`;
  each dataset records the interval it has covered, and a re-run fetches only
  what is missing plus `LOADER_CONFIG.refetch_overlap_days` (1) of deliberate
  overlap — the trailing bar of a run is usually incomplete and venues revise
  recent bars, and the duplicates that creates are collapsed on read. Coverage is
  an interval rather than a high-water mark, so asking for history older than the
  covered start fetches it in full instead of skipping it, and a disjoint window
  never claims the gap between the two. `paginate_time_series` walks windows
  longer than one venue page (1000 rows): a five-year daily request previously
  came back as the first 1000 days with nothing to say the rest was missing.
  Funding rate and open interest moved onto ccxt's `*_history` endpoints where
  the venue advertises them (`fetch_funding_rate`/`fetch_open_interest` return
  only the current value and could never honour a window); under the snapshot
  fallback a historical window now returns nothing rather than stamping today's
  value with a past timestamp. Binance's funding history carries the rate alone,
  so `mark_price`/`index_price` are null on historical rows —
  `AUDIT_CONFIG.nullable_columns_by_dataset` records that so the null-rate check
  warns instead of halting, while staying strict on `funding_rate` itself.
  `python -m pipeline.nightly` gained `--start`, `--end` and
  `--ignore-checkpoint`. 353 tests passing;
  `scratch/scratch_windowed_fetch.py` demos pagination, resumption and the
  checkpoint end to end.
- 2026-07-31: Logging gap identified during a PLAN/README/TODO review:
  `LOGS_PATH` was declared in `config.py` but nothing wrote to it, the
  architecture's cross-cutting list named CONFIG and ALERTS but not logging,
  and an unattended-run failure outside the audit module (e.g. inside the
  backtester or a signal) had no durable record — only audit threshold
  breaches, execution drift, and the kill switch reach Telegram. Designed the
  fix end to end: one rotating JSON log file per pipeline component under
  `logs/` (10 MB rotation via `RotatingFileHandler`), 12-month retention
  handled as a separate, time-based pruning step (`prune_old_logs`, timestamped
  rotated filenames) rather than through `backupCount`, since size-triggered
  rotation and calendar-based retention don't compose through one knob;
  `run_id` correlation across every component's log file for a given pipeline
  run; and an explicit boundary between logs (durable technical record, always
  written), Telegram alerts (human notification, audit/drift/kill-switch only),
  attribution reports (daily business summary), and `AuditResult` severities
  (data-quality taxonomy) — a halt now always has a backing CRITICAL log line
  before the alert is attempted. Full design in `LOGGING.md`; reference
  implementation in `logging_config.py`, smoke-tested standalone (JSON output,
  run_id tagging, exception capture, and retention pruning all confirmed
  working). Added Phase 5.5 to retrofit this into the completed Phases 1-4 and
  current Phase 5 source; Phases 6-9 updated to wire logging in from the start
  instead of retrofitting later. Retrofit into the actual module source is
  still pending — this session had the three planning docs but not the
  repository itself.
- 2026-07-31: Phase 5 signal set complete — `cross_sectional_momentum` (90-day
  formation, 7-day skip), `time_series_momentum` (same window, divided by the
  asset's own volatility), `carry` (negated annualized funding),
  `short_term_reversal` (negated vol-scaled 5-day move) and `low_volatility`
  (negated annualized realized vol), each with a methodology doc written before
  the code. Three decisions worth recording. (1) A shared point-in-time series
  reader, `signals/bars.py`: five signals needed the same
  read-through-context → latest-ingestion-per-bar → gap-free-tail pipeline, and
  five copies of that is five chances to get the point-in-time boundary subtly
  wrong. (2) `time_series_momentum` standardizes with a new
  `cross_sectional_scale` (winsorize, then divide by the cross-sectional
  standard deviation) rather than a z-score, because demeaning deletes exactly
  the market-wide tilt a time-series signal exists to express — and the rank IC
  the engine reports is invariant to that choice, so only a test can catch
  getting it wrong. (3) `low_volatility` exports `annualized_vol_universe`, and
  `signals/alpha.py` consumes it, so the project has one volatility estimator
  rather than two that quietly disagree. Alpha refinement added
  (`signals/alpha.py`): IC estimation from the engine's IC series with
  normal-normal shrinkage (`shrunk = mean_ic × τ²/(τ² + se²)`, τ = 0.02, capped
  at 0.10, and an estimate resting on one period shrunk to exactly zero), then
  `alpha = volatility × IC × z`, and a combiner where a `None` view contributes
  nothing rather than dragging the sum toward zero. Breadth report added
  (`signals/breadth.py`): score correlation per rebalance (do two signals pick
  the same assets), IC correlation over time (do they work at the same times —
  two signals can pick different assets and still be one bet if they fail
  together), and `n / (1 + (n-1)ρ̄)` effective independent bets. 124 new tests
  (hand-computed golden fixture per signal including an explicit sign assertion
  for `carry`, point-in-time through a real store, every reject path returning
  `None`); 477 passing overall. Two scratch demos:
  `scratch/scratch_signals_phase5.py` and `scratch/scratch_signal_breadth.py`.
  **Not done, and deliberately so:** Sections 5-6 of all six methodology docs
  are still empty. No environment here has a multi-year backfill, and a
  synthetic backtest measures the generator rather than the signal — the demo
  already shows `cross_sectional_momentum` and `time_series_momentum`
  correlating at 0.86 on synthetic data, which is a plausible finding and still
  not evidence. All six signals stay `draft`.
- 2026-07-31: Phase 5.5 applied — logging retrofitted into Phases 1-5 source.
  Every module now takes its logger from `logging_config.get_logger(__name__)`
  (`tm.<component>.<module>`), so each writes to its own rotating JSON file
  under `logs/` and every record carries the `run_id`
  `NightlyPipeline.run()` sets. The halt path is the point of the phase:
  `DataAudit.should_halt_trading()` logs CRITICAL itself — inside the method,
  not at the call sites, so *every* route to a halt is recorded — and it does
  so before `send_alerts()` is attempted, which is what makes the durable
  record independent of Telegram being up. `NightlyPipeline._stage` logs entry,
  exit and duration per stage and CRITICAL with the stage name on an unhandled
  exception; `prune_old_logs()` runs in a `finally`, so retention is enforced
  even on a failed run (the pipeline that fails nightly is the one whose logs
  grow fastest) and a pruning failure never fails the run.
  `python -m pipeline.prune_logs [--dry-run]` exposes the same for a research
  box that never runs the nightly job. Two things the spec did not anticipate:
  the "no-overwrite guard" it described does not exist (`ParquetStore.append`
  never overwrites — it writes a new file beside the existing partition), so
  that path logs DEBUG while the schema check, which does raise, logs WARNING
  first; and `AssetMaster.resolve_symbol` gained
  `warn_if_unresolved=False` because the nightly pipeline calls it to ask "is
  this symbol already mapped?" and on a first run every symbol on the venue
  would otherwise log a warning. Both recorded in `LOGGING.md` §6. 54 new tests
  (`tests/test_logging.py`): CRITICAL on every halt path and on unhandled stage
  exceptions, ERROR when a loader's append raises, and `prune_old_logs()`
  against fabricated file ages — including that the namer's output matches the
  pruner's glob, without which retention would silently never run. All 14
  existing scratch demos call `start_demo_run("<component>")` and print that
  component's log tail on exit. 531 tests passing.
