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
      **How to get the data: `DATA.md`.** Binance's public archive
      (`data.binance.vision`) serves 2020-01 → present as checksummed monthly
      zips and answers 200 from the same address where the API answers 451, so
      the backfill no longer has to run on the trading machine. Route, format
      gotchas, acceptance checks and the research steps that follow are all
      there.

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

## Phase 5.6 — CI, test isolation, and the observability fixes

Phase 5.5 asked "is logging wired in?" and the answer was yes. This phase asked
"does it work?", which turned out to be a different question with four
different answers. Plus the CI that would have caught some of it.

- [x] **The pipeline's own log file was empty on the documented code path.**
      `get_logger(__name__)` under `python -m pipeline.nightly` resolves
      `__name__` to `"__main__"`, giving `tm.__main__` — no component, no file
      handler, and INFO is below the console threshold. Measured on one dry
      run: 18 records when imported, 0 records via `-m`. Fixed in
      `logging_config.resolve_logger_name` (so every future `python -m` entry
      point is covered, not just these two) and tested by *running the CLI in a
      subprocess* — every import-based test passed throughout
- [x] **DEBUG was unreachable.** `LogConfig.level` was the literal `"INFO"`,
      so the per-asset signal reject reasons and the backtester's
      per-rebalance detail could only be seen by editing `config.py`.
      `TM_LOG_LEVEL`, `TM_CONSOLE_LOG_LEVEL`, `--log-level`,
      `--console-log-level`, and `logging_config.set_level()`
- [x] **Rotation could destroy a backup.** The namer stamps to the second, and
      `RotatingFileHandler.doRollover` `os.remove`s a colliding name before
      renaming onto it — two rotations inside one second lost 10 MB of
      history. Also `backupCount = 100_000` made every rotation walk 100k
      `stat` calls: ~320 ms measured, against 1.4 ms at `backupCount=5`.
      Both fixed by `TimestampedRotatingFileHandler`, which rolls over
      directly to a unique name and never consults `backupCount` — retention
      is `retention_days`' job, which is what `LOGGING.md` §3.2 always claimed
- [x] **Log files existed for phases that do not.** `risk.log`,
      `portfolio.log`, `execution.log`, `attribution.log` sat at 0 bytes since
      the retrofit. Handlers are created on first use
- [x] `resolve_symbol` warned once per duplicate mapping per run — hundreds of
      WARNINGs a night for the expected state of an append-only asset master.
      DEBUG when the duplicates agree; WARNING kept for the case it was
      presumably meant for, two `asset_id`s behind one venue symbol
- [x] `logging.basicConfig()` in the nightly `__main__` had never affected a
      single record (`tm` does not propagate); removed
- [x] `scratch_windowed_fetch.py` silenced `logging.getLogger("loaders")`,
      which is not a logger this project uses; `tm.loaders`
- [x] **Test isolation.** `BaseLoader` (and six other modules) default to the
      production `DATASTORE_PATH`, so a test that passed `store=` but not
      `asset_master=` read the developer's real asset master — green on a
      clean checkout, failing on a machine that has run the pipeline, and the
      failure looks like a code defect. Autouse fixture in `tests/conftest.py`
      redirects all seven; `tests/test_isolation.py` fails if a new module
      joins the list without joining the fixture
- [x] **The methodology doc is checked against the code.** Registration parses
      the header table and the §4 parameter table and refuses a doc that names
      a different signal or family, or that omits a parameter the code runs.
      Three of six omitted one or more (`max_gap_days`,
      `history_buffer_days`); those docs now document them. `Status` is a
      parsed enum, so `signal_statuses()` answers "which signals have
      evidence?" instead of a paragraph in the README
- [x] **Pre-merge CI** (`.github/workflows/ci.yml`): pull requests and pushes
      to `main`, Python 3.11, 3.12 and 3.14, ruff + full suite + advisory
      mypy, and a check that commit messages carry no tool attribution. Make it
      a required check on `main`
- [x] **Deploy tests before it pulls** (`.github/workflows/deploy.yml`,
      replacing `pr-merge.yml`): fetch (non-destructive) → check the incoming
      commit out into a throwaway worktree → run the suite there → only then
      `git reset --hard`. Also fixes: a failed `git fetch` followed by a
      successful `reset` used to exit 0 and deploy the *previous* commit
      (PowerShell does not fail a step on a native command's exit code); the
      job reset to `origin/main` whatever branch the PR merged into; and two
      merges in quick succession ran two resets against one directory
- [x] **pytest ≥ 8.4 pinned.** `caplog` only attaches to non-propagating
      loggers from 8.4, and `tm` does not propagate — on an older pytest the
      "a halt leaves a CRITICAL record" assertions fail, and the negative ones
      pass having captured nothing. Canary test added
- [x] One source of truth for pytest config (`pytest.ini`; the duplicate block
      in `pyproject.toml` was dead and warned on every run), and ruff/mypy
      target the `requires-python` floor rather than a version CI does not pin
- [x] Tests: 47 new (`tests/test_isolation.py`, `tests/test_methodology_docs.py`,
      and the CLI-subprocess, rotation, level-override and caplog-canary cases
      in `tests/test_logging.py`); 578 passing
- [x] `scratch/scratch_observability.py` demonstrates all of it end to end
- [x] **Cleared the ruff backlog and made lint a hard gate.** 168 findings →
      zero: `ruff check --fix` handled 163 (`Optional[X]` → `X | None`, unused
      imports, import ordering, `timezone.utc` → `datetime.UTC`), and five
      needed a human (two `== True` filters, a `zip(strict=True)`, a
      `raise ... from e`, and a `noqa` for `doRollover`, whose casing belongs
      to the stdlib method it overrides). No behaviour change; the suite passed
      unchanged. `continue-on-error` is off, so a finding now fails CI
- [x] **The formatter is deliberately not a gate.** `ruff format --check`
      would have reformatted 62 of 87 files — a large diff for no correctness
      gain — so `ci.yml` runs `ruff check` only. The consequence is that line
      length is unenforced: `E501` stays in the ignore list, where it used to
      claim the formatter handled it. Re-enabling it means 50 findings ruff
      cannot fix (it will not rewrap code), so that is a separate decision
- [x] **A green suite exited 1 on Windows.** `python -m pytest tests/` printed
      every test as PASSED and then died with `PermissionError: [WinError 5]`
      in pytest's own `cleanup_dead_symlinks`: the stale `pytest-current` link
      in `%TEMP%\pytest-of-<user>\` cannot be removed with `os.unlink` there
      (a directory link needs `RemoveDirectoryW`), pytest's removal is
      unguarded, and session-finish hooks run in a `finally` that catches only
      `exit.Exception` — so it escapes and the process exits 1. `deploy.yml`
      keys `git reset --hard` off that exit code, so the trading machine was
      refusing commits whose tests had all passed. Shimmed in
      `tests/conftest.py` (rmdir fallback, cannot raise, installed at both call
      sites since `_pytest.tmpdir` binds the name at import);
      `tests/test_tmpdir_cleanup.py` runs pytest in a subprocess over a seeded
      stale link and asserts the exit code, with a negative control that fails
      if the harness ever stops reproducing the crash
- [x] **`python -m scratch.<demo>` died on its first import.** Phase 5.5 gave
      every demo `from log_demo import start_demo_run`, which resolves only
      when the demo's own directory leads `sys.path` — i.e. when it is run as a
      script. Under `-m` the repo root leads and `scratch/` is nowhere on the
      path, so all fifteen demos raised `ModuleNotFoundError: No module named
      'log_demo'` before printing a line. `scratch/__init__.py` puts both
      directories on the path and is imported before the module runs, so future
      demos inherit the fix; `tests/test_scratch_demos.py` runs both
      invocations in subprocesses, because the test process has `scratch/`
      reachable through neither entry and cannot see the defect in-process
- [x] Make `ci.yml` a **required status check** on `main` in the repository's
      branch protection settings — the workflow gates nothing until it is
      (this is a GitHub setting, not something a file in the repo can do)
- [x] **CI runs the suite on Windows**, on `windows-latest` at 3.14 — the
      platform and version the trading machine runs. Three defects had by then
      reached `main` with every CI job green and failed on the box afterwards
      (pytest's temp-dir cleanup exiting 1, cp1252 stdout, an open log handle
      blocking a temp-dir removal); all three are POSIX/Windows differences
      that a Python-version matrix cannot reach. A separate job, not an `os`
      dimension: a dimension renames the three existing checks, and those names
      are what branch protection requires on `main`
- [ ] Add **`tests (windows, python 3.14)`** to the required status checks on
      `main`. Same category as the item above it — a GitHub setting, not
      something a file in the repo can do — and until it is taken the Windows
      job reports without blocking a merge
- [x] **`python -m scratch.scratch_observability` died on Windows** with a
      `FileNotFoundError` on the log file it had just asked a child pipeline to
      write. The child had exited 1 without writing anything: the demo built
      its environment from a four-key dict, and `pipeline.nightly` imports
      `ccxt` — so `ssl` and `socket`, whose extension modules do not load on
      Windows without the environment the interpreter was started with. The
      demo captured the child's stderr and never printed it, so the one place
      the reason was written down was thrown away. Environment inherited then
      overridden (the pattern `tests/test_logging.py` already used); the child's
      stderr is printed when the log file is missing;
      `tests/test_scratch_demos.py::_run` had the same literal-dict fragility
      (a POSIX `PATH`, a guessed `SYSTEMROOT`) and was fixed with it
- [x] **A redirected `python -m pipeline.nightly` failed its own report.**
      `sys.stdout` takes the locale encoding when it is not a terminal —
      cp1252 on a default Windows install — and `_report_stage` prints `✓` and
      `🛑`. The `print` raised `UnicodeEncodeError`, the stage failed, the run
      was logged CRITICAL, and a live run exited 1: piping the output changed
      the outcome of the pipeline. The CLI now reconfigures both streams with
      `errors="replace"`, leaving the encoding itself alone

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
- 2026-08-01: Phase 5.6 — audited the Phase 5.5 retrofit rather than extending
  it, which found that the mechanism itself was broken in four places. The one
  that matters: **`python -m pipeline.nightly` wrote nothing to
  `logs/pipeline.log`**. Run as a module, `__name__` is `"__main__"`, so
  `get_logger(__name__)` returned `tm.__main__` — a logger under no component,
  with no file handler, and INFO sits below the console threshold — so the
  `run_id` banner, every stage entry/exit/duration and the CRITICAL on an
  unhandled stage failure all went nowhere on the exact invocation the README
  documents and the scheduled job uses. The same dry run: 18 records via
  `NightlyPipeline().run()`, 0 via `-m`. Every test passed throughout, because
  importing the module gives it the right name; the fix is in
  `resolve_logger_name` (covering every future `python -m` entry point) and the
  test now runs the CLI in a subprocess. Also: DEBUG was unreachable without
  editing `config.py`, so the per-asset reject reasons the retrofit added could
  not be produced (`TM_LOG_LEVEL`, `--log-level`, `set_level()`); a
  same-second rotation destroyed a backup, because the namer stamps to the
  second and `doRollover` removes a colliding name before renaming onto it, and
  `backupCount=100_000` cost ~320 ms of `stat` calls per rotation against 1.4 ms
  at 5 (both fixed by rolling over directly to a unique name and not consulting
  `backupCount` at all, which is what §3.2 always claimed); and file handlers
  were created eagerly for `risk`/`portfolio`/`execution`/`attribution`, four
  0-byte files claiming phases that do not exist. `resolve_symbol`'s duplicate
  warning was downgraded to DEBUG when the duplicates agree — an append-only
  asset master accumulates identical mappings by design, and warning per symbol
  per run buried the case worth seeing, two `asset_id`s behind one venue symbol.
  **Test isolation:** the reported failure (`{'BTC/USDT': 'BTC'} !=
  {'BTC/USDT': None}`) was not a code defect — the test passed an isolated
  `store=` but no `asset_master=`, and `BaseLoader` filled that in from the
  production `DATASTORE_PATH`, so on a machine with a real asset master the
  symbols resolved. Seven modules carry that default; an autouse fixture
  redirects all of them, and a test fails if an eighth appears without joining
  the list. **The doc is now checked against the code:** registration parses the
  methodology header and §4 parameter table and refuses a doc naming a different
  signal or family, or omitting a parameter the code runs — three of six
  omitted `max_gap_days` and/or `history_buffer_days`, so half the "specs"
  described a different signal from the one being backtested. `Status` became a
  parsed enum, so `signal_statuses()` replaces a paragraph. **CI:** `ci.yml`
  runs ruff and the full suite on 3.11, 3.12 and 3.14 for every PR (3.14 added
  later, when the exit-code defect showed it was the gap that mattered; make it
  required on `main`); `deploy.yml` replaces `pr-merge.yml` and tests before it
  pulls —
  fetch, check the incoming commit out into a throwaway worktree, run the suite
  there, and only then `git reset --hard`, so a failure leaves the trading box
  on the last known-good commit. Three latent faults in the old workflow fixed
  on the way: a failed `git fetch` followed by a successful `reset` exited 0 and
  deployed the previous commit, the job reset to `origin/main` regardless of
  which branch the PR merged into, and concurrent merges raced on one directory.
  pytest pinned to ≥ 8.4 (below it `caplog` cannot see the non-propagating `tm`
  tree and the halt-logging assertions go vacuous), duplicate pytest config
  removed, ruff/mypy pointed at the `requires-python` floor. 578 tests passing;
  `scratch/scratch_observability.py` demonstrates the lot.
- 2026-08-01: Venue access, recorded because it gates the Phase 5 backfill:
  Binance answers **HTTP 451 "restricted location"** to every request from a US
  egress IP — spot and `fapi`, public endpoints included — and ccxt reports it
  as a bare `NetworkError` with the status and reason stripped, so the loaders
  see an unreachable venue, write nothing, and the run ends as an audit
  `data_presence` halt. Confirmed reachable from a blocked address:
  `data-api.binance.vision` (public spot only — no funding rate, no open
  interest, so no `carry`), `api.binance.us` (separate entity, no perps),
  Deribit, Kraken, Coinbase, Gate, KuCoin, Bitget, MEXC. The backfill should run
  on the machine that runs the pipeline. Worth adding when that happens: a venue
  preflight that logs the HTTP status and response body, so "we are blocked"
  stops looking identical to "the venue is down".
- 2026-08-01: **A passing suite exited 1 on Windows**, which meant the deploy
  gate was rejecting good commits. `python -m pytest tests/` printed every test
  as PASSED and then raised `PermissionError: [WinError 5] Access is denied:
  '...\Temp\pytest-of-Peter\pytest-current'` out of pytest's own temp-directory
  housekeeping. Not this project's code, but this project's exit code. The
  chain, all inside pytest: `make_numbered_dir` keeps a `pytest-current` link
  beside its numbered temp dirs and re-points it through `_force_symlink`,
  which starts by unlinking the old one — on Windows a link to a *directory* is
  removed with `RemoveDirectoryW`, not the `DeleteFileW` behind `os.unlink`, so
  that fails, and `_force_symlink` swallows every error by design. The link
  therefore keeps pointing at an older run; that run's directory is eventually
  cleaned up (`keep=3`); and `cleanup_dead_symlinks` then calls the same failing
  `unlink()` on the now-dangling link — the one removal in `_pytest/pathlib.py`
  that is not inside a `try` (the other six are). Session-finish hooks are
  invoked from `wrap_session`'s `finally`, which catches only `exit.Exception`,
  so it is not even downgraded to an INTERNAL_ERROR status: it propagates
  through `_console_main` and the interpreter exits 1. The `1 passed` summary
  never prints either, because the crash happens below the terminal reporter's
  own hook — which is why the output jumps straight from the last PASSED line to
  a traceback. Fixed with a shim in `tests/conftest.py` that keeps pytest's
  semantics (remove links whose target is gone), falls back to `rmdir`, and
  cannot raise; installed into `_pytest.pathlib` *and* `_pytest.tmpdir`, since
  the latter binds the name at import time and is the call site in the
  traceback, and installed unconditionally rather than under `os.name == "nt"`
  so CI exercises it. It self-heals: the stale link is removed at the end of the
  first run after the fix, and the next run creates a live one. 15 new tests
  (`tests/test_tmpdir_cleanup.py`) — the unit ones simulate Windows by making
  `unlink` refuse, and two integration ones run pytest in a subprocess over a
  seeded stale link and assert the **exit code**, because nothing in-process can
  observe the thing that was broken. The negative control paid for itself
  immediately: the first harness seeded the link under a `pytest-of-<user>`
  directory pytest never looked at, so the positive test was green for no
  reason. 594 tests passing.
- 2026-08-01: **Python 3.14 added to the CI matrix** (`3.11`, `3.12`, `3.14`),
  which the exit-code defect above surfaced: it was reported from a local run
  on 3.14, and CI tested 3.11 and 3.12 only. The gap matters more than a
  version-support question, because `deploy.yml` installs nothing — it runs the
  suite in the throwaway worktree with *the trading machine's own* interpreter.
  So the version with the final say over whether a commit is adopted was the
  one version nothing verified. Confirmed before adding it rather than after:
  the full suite installs and passes on 3.14 (594 passed, ~34 s; polars 1.43,
  pyarrow 25, pandas 3.0, numpy 2.5, ccxt 4.5 all have 3.14 wheels), though the
  only 3.14 build available in the environment that checked was `3.14.0rc2`, so
  CI on GitHub's 3.14 release is the first run against a final build. 3.13 was
  deliberately left out: nothing runs on it, and `requires-python = ">=3.11"`
  is a floor rather than a promise about every version in between — a matrix
  entry should name a version somebody actually uses. `ruff` stays on
  `target-version = "py311"` and mypy on `python_version = "3.11"`: both are
  about the *floor* of supported syntax, which 3.14 does not move.
- 2026-08-01: **The scratch demos could not be run as modules.**
  `python -m scratch.scratch_audit` raised `ModuleNotFoundError: No module
  named 'log_demo'` on line one. Phase 5.5 opened every demo with
  `from log_demo import start_demo_run`, which resolves only under
  `python scratch/scratch_audit.py` — there `sys.path[0]` is the script's own
  directory. Under `-m` `sys.path[0]` is the repo root and `scratch/` is not on
  the path at all, so the import failed for all fifteen demos before any of
  them printed a line. Fixed with `scratch/__init__.py`, which inserts both the
  scratch directory (for `log_demo`) and the repo root (for `config`,
  `logging_config` and the project packages): `python -m` imports the package
  before it runs the module, so the inserts happen first and a demo added later
  inherits the fix instead of rediscovering the bug. The direct-script form is
  untouched — `log_demo` still inserts the repo root itself, so a demo run as a
  script does not depend on the package having been imported. 9 new tests
  (`tests/test_scratch_demos.py`): every `scratch_*` module imported as a
  package submodule, plus both invocations of `scratch_audit` run end to end.
  They are subprocesses for the same reason the `python -m pipeline.nightly`
  regression test is: pytest runs from the repo root with `scratch/` reachable
  through neither entry, so nothing in-process reproduces the failing path.
  Removing `scratch/__init__.py` fails 5 of the 9, which is the negative
  control. 603 tests passing.
- 2026-08-01: **The observability demo could not run its own subprocess.**
  `python -m scratch.scratch_observability` printed `exit code: 1` and then
  died with `FileNotFoundError` on `<tmp>\module-run\pipeline.log` — the file
  its child `python -m pipeline.nightly` was supposed to have written. Two
  separate defects, neither in the logging mechanism the demo exists to show.
  (1) **The child was handed a four-key environment** — `PATH`, `PAPER`,
  `TM_LOG_DIR`, `PYTHONPATH` — and `pipeline.nightly` imports `ccxt` at module
  scope, so `ssl` and `socket` with it; on Windows those extension modules do
  not load without the environment the interpreter was started with
  (`SYSTEMROOT` chief among them). The child therefore failed at import, before
  any logging was configured, which is why there was no `pipeline.log` to read.
  The environment is now inherited and then overridden — the pattern
  `tests/test_logging.py`'s own CLI regression test already used, and which the
  demo had diverged from. `tests/test_scratch_demos.py::_run` carried the same
  literal dict, with a POSIX `PATH` hardcoded and `SYSTEMROOT` guessed as
  `C:\Windows`, and was fixed alongside it. (2) **The demo threw away the one
  thing that explained the failure:** it captured the child's stderr and never
  printed it, then indexed straight into a file that did not exist, so a
  four-line traceback in the parent stood in for a diagnosis in the child. A
  missing log file is now reported as the finding it is, with the child's own
  stderr attached, and `main()` returns non-zero. Found on the way, and worse:
  **a redirected `python -m pipeline.nightly` failed its own report stage.**
  `sys.stdout` takes the locale encoding when it is not a terminal — cp1252 on
  a default Windows install — and `_report_stage` prints `✓` and `🛑`, which
  cp1252 cannot encode. The `print` raised `UnicodeEncodeError`, `_stage`
  logged the stage CRITICAL, `run()` logged the run as failed, and a live
  (non-dry) run exited 1. Piping the output changed the outcome of the
  pipeline, and `deploy.yml`-style automation keying off an exit code would
  have believed it. The CLI now reconfigures stdout and stderr with
  `errors="replace"`, leaving the encoding alone — a glyph the console cannot
  show degrades to `?` instead of ending the night's run. 10 new tests: the
  demo's child environment inherits the parent's and still overrides the three
  values it controls; the demo runs end to end as a module; and the CLI under
  `PYTHONIOENCODING=cp1252` exits 0, prints its report, logs no CRITICAL, and
  still emits the real glyphs on a UTF-8 console. The cp1252 pair fails without
  the fix, which is the negative control. 613 tests passing.
- 2026-08-01: **The deploy gate caught what three green CI jobs could not.**
  The previous entry's fixes let `scratch_observability` reach the end of
  `main()` for the first time on Windows, where it died in
  `TemporaryDirectory.__exit__` with `PermissionError: [WinError 32] The
  process cannot access the file because it is being used by another process`
  — *after* all five sections had printed, so it read as an interpreter fault
  rather than a demo fault. Sections 2 and 3 point log files at a temporary
  directory, a `RotatingFileHandler` holds its file open, and Windows will not
  delete an open file; POSIX unlinks one without complaint, which is why the
  local run and all three GitHub-hosted CI jobs (3.11, 3.12, 3.14) were green
  on the same commit. The leak was not confined to the handlers the demo
  creates deliberately: section 2 calls `configure_logging(cfg)`, which
  replaces the module-level *active* config, so every later `get_logger` opens
  its component file under the temp directory too — the handle that actually
  blocked the cleanup belonged to `datastore.log`, opened by nothing more than
  `import signals` in section 5. `release_temp_log_handlers()` closes every
  `tm` file handler and restores the real `LOG_CONFIG`, called in a `finally`
  so an exception mid-demo still releases them, and section 5 moved outside the
  temp directory entirely — its records belong in the real component files the
  way every other demo's do. 4 new tests, and the interesting part is their own
  isolation: `_ensure_component_handler` returns early when a component logger
  already has a file handler, so the first version of these tests left handlers
  attached and the next test opened no file at all — its "nothing is held open
  under tmp_path" assertion passed for the wrong reason, and passed against a
  deliberately broken release function. The autouse fixture detaches handlers
  with its own code rather than the code under test, which is what makes the
  negative control (2 of 4 failing) mean anything. 617 tests passing. Worth
  recording about the gate itself: `deploy.yml` did exactly what it was built
  for — the suite failed in the throwaway worktree, `git reset --hard` never
  ran, and the trading machine stayed on the last known-good commit while
  `main` carried a commit that fails there.
- 2026-08-01: **Windows joined `ci.yml`.** The gap the entry above exposed is
  structural rather than particular to that bug: `ci.yml` ran three Linux jobs,
  `deploy.yml` was the only thing that ever ran the suite on Windows, and it
  runs *after* the merge. Three defects have now taken that route — pytest's
  temp-directory cleanup exiting 1, a cp1252 stdout killing the nightly report,
  and a log handler left open on a temp directory Windows then refused to
  delete — every one a filesystem or console difference no Python-version
  matrix can reach. A `tests (windows, python 3.14)` job now runs the suite on
  `windows-latest` before the merge. Two limits, both deliberate: 3.14 only,
  because the platform is the question the job exists to answer and Windows
  minutes bill at 2x; and a separate job rather than an `os` dimension on the
  existing matrix, because a dimension renames `tests (python 3.11)` to
  `tests (ubuntu-latest, python 3.11)` and those three names are what branch
  protection has marked required on `main` — a rename leaves the required
  checks waiting forever on names nothing reports. No lint or mypy step on the
  Windows job either: neither is platform-dependent, and running them twice
  buys an identical answer for double the wall clock. Adding the new check to
  the required list is a repository setting and is on the checklist above,
  unticked; until it is taken the job reports without blocking.
- 2026-08-01: **`.env` is now loaded by the code that needs it.** Running the
  test suite locally sent a real Telegram message to the trading machine's bot;
  running the same commit in CI did not. Neither was a test doing anything
  unusual: `tests/test_logging.py` exercises the audit halt path against a
  fabricated `coverage: only 3 of 150` failure and calls the real
  `DataAudit.send_alerts()`, whose only guard is `ALERT_CONFIG.enabled` —
  `bool(token and chat_id)`, read from `os.getenv` at import. Nothing in the
  repository had ever read a `.env` file (no `python-dotenv` dependency, no
  `load_dotenv`, nothing in `pytest.ini` or `conftest.py`); the credentials
  reached the process because VS Code's `python.envFile` defaults to
  `${workspaceFolder}/.env` and populates the environment it launches the
  interpreter in. So the same command behaved differently depending on how it
  was started — the `python -m pipeline.nightly` logger and the
  `python -m scratch.<demo>` import a third time, and the reason a secret
  living only in `.env` still has to be treated as loaded everywhere.
  `config._load_dotenv` fixes it in the one place it can work: at the top of
  `config.py`, above the first `getenv`. Three decisions. It is **anchored to
  `PROJECT_ROOT`**, because a CWD-relative load is correct exactly when you
  test it and silently loads nothing from a scheduled job's working directory.
  The **real environment wins** (`key not in os.environ`, not assignment) —
  `deploy.yml` sets `PAPER` and `TM_LOG_DIR` for its verification run on the
  machine whose repo root holds the real `.env`, and a file that overrode them
  would point the verification run at the live log directory. And it is
  **stdlib rather than `python-dotenv`**, because `deploy.yml` deliberately
  runs the suite without a `pip install` step, so a new dependency fails there
  at import until someone installs it by hand. 19 tests
  (`tests/test_dotenv.py`), of which the two that matter are subprocesses: the
  test process has already imported `config` from the repo root, so nothing
  in-process can see either the working-directory anchoring or the fact that
  the load must precede the dataclass defaults. They build a fake project root
  — a copy of `config.py` beside a `.env` — rather than writing into the real
  one, which a test must never do. Both negative controls reproduce: making the
  path CWD-relative fails 2, and moving the load below `AlertConfig` fails the
  `ALERT_CONFIG.enabled` test while every unit test still passes, which is
  exactly the failure mode the ordering test exists for. 636 tests passing;
  `scratch/scratch_dotenv.py` reports what was loaded, by name and masked,
  never a value. **Known consequence, not fixed here:** the local test-run
  alert is now deterministic rather than editor-dependent, and it will also
  fire from `deploy.yml`'s verification run. The alert has no test seam — it
  goes straight to `requests.post` — so giving it one is its own change.
