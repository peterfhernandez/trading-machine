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

- [ ] Walk-forward engine: rebalance calendar, point-in-time data exposure,
      next-bar execution, cost model (spread + impact stub)
- [ ] Metrics: returns, vol, IR, drawdown, turnover, per-signal IC series
- [ ] Golden tests: a hand-computed 3-asset fixture the engine must reproduce
      exactly
- [ ] Validate: buy-and-hold BTC through the engine matches raw data

## Phase 5 — Signals → alphas (M6)

- [ ] Signal interface + registry; METHODOLOGY.md template
- [ ] Cross-sectional momentum (with skip-window)
- [ ] Time-series momentum
- [ ] Carry (funding rate)
- [ ] Short-term reversal
- [ ] Low-volatility
- [ ] Alpha refinement: z-scoring, winsorizing, IC estimation from backtests,
      shrinkage; alpha = vol × IC × z
- [ ] Signal correlation matrix report (breadth check: are these independent?)

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
