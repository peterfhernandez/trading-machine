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
- [ ] Cross-sectional momentum (with skip-window)
- [ ] Time-series momentum
- [ ] Carry (funding rate)
- [ ] Short-term reversal
- [ ] Low-volatility
- [ ] Alpha refinement: z-scoring, winsorizing, IC estimation from backtests,
      shrinkage; alpha = vol × IC × z
      (cross-sectional winsorize + z-score done in `signals/transforms.py`;
      IC estimation and shrinkage still to do)
- [ ] Signal correlation matrix report (breadth check: are these independent?)
- [ ] Run `scratch/scratch_markov_param_grid.py` against the real backfill and
      fill in Sections 4-5 of the markov methodology doc with the OOS numbers

## Phase 6 — Risk model (M7)

- [ ] Sector/ecosystem tagging in asset master
- [ ] Daily cross-sectional factor regressions (beta, size proxy, momentum,
      vol, sectors)
- [ ] EWMA factor covariance + Ledoit–Wolf shrinkage; specific variances
- [ ] Validation: predicted vs. realized portfolio vol on random portfolios
- [ ] Backtester upgraded to report ex-ante risk and risk-adjusted metrics

## Phase 7 — Portfolio construction (M8)

- [ ] v1 rank-based long/short with vol targeting, position caps
- [ ] v2 cvxpy optimizer: max α − λσ² − costs; market-neutral, max-weight,
      turnover, gross-leverage constraints
- [ ] Backtest full pipeline (signals → alphas → risk → optimizer) vs. v1;
      keep whichever wins net of costs

## Phase 8 — Execution & shortfall (M9)

- [ ] Paper broker: positions, fills at next bar ± spread, PnL ledger
- [ ] Exchange testnet adapter (start with one venue)
- [ ] Zero-cost shadow portfolio + implementation-shortfall report
- [ ] Kill switch + max-daily-loss halt in config

## Phase 9 — Pipeline, attribution, ops (M10, M11)

- [ ] Daily scheduled run: load → audit → universe → alpha → risk → optimize →
      execute (paper) → report; audit failure halts trading stages
- [ ] Attribution: PnL split into factor / specific / costs; per-signal IC
      decay tracking
- [ ] Daily Telegram/HTML report
- [ ] Run unattended ≥ 4 weeks on paper; review attribution weekly
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
