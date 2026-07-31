# Poor Man's Trading Machine — Logging & Observability Architecture

This is the spec for application logging, in the same sense that a signal's
`METHODOLOGY.md` is the spec for that signal: code is written and reviewed
against it, and it is updated (with a note in its own review log) when reality
disagrees with it. `PLAN.md` §4 and §6 and `TODO.md` Phase 5.5 both point here
rather than duplicating the design.

## 1. The gap this closes

Before this document, logging was undesigned. `LOGS_PATH` existed as a config
path and nothing wrote to it. `PLAN.md`'s architecture diagram named
`CONFIG` and `ALERTS` as cross-cutting concerns but not logging. The only
concrete reference to logging anywhere was `BaseLoader`'s "append wrapper...
with error logging," unspecified beyond that phrase. Telegram alerts cover
audit threshold breaches, execution drift, and the kill switch — nothing else.
Since the nightly pipeline is designed to run unattended (Phase 9: "run
unattended ≥ 4 weeks on paper"), an exception in the backtester, a signal, the
optimizer, or execution had no durable record: not an alert (alerts are
audit/drift/kill-switch only), not a report (reports are daily business
summaries), and nothing was writing to `LOGS_PATH`.

The goal: every stage of the pipeline writes a durable, structured,
per-component log, rotated at 10 MB, retained 12 months, correlated by a
`run_id`, so a failed unattended run can be reconstructed after the fact
without depending on an alert having fired.

## 2. Logs vs. alerts vs. reports vs. audit severities

These were being conflated. They are four different concerns and all four
stay:

| Concern | Purpose | Audience | Trigger | Where |
| --- | --- | --- | --- | --- |
| **Logs** | durable technical record of what happened | engineer, after the fact | always, every run, every module | `logs/<component>.log` |
| **Alerts** (Telegram) | "look now" | human, real-time | audit threshold breach, execution drift, kill switch | Telegram |
| **Reports** (Telegram/HTML) | business summary | human, daily cadence | scheduled, end of pipeline | Telegram/HTML |
| `AuditResult` severity | data-quality taxonomy | the audit module, callers | per check | in-process object (also logged) |

A halt or an alert must always have a matching **CRITICAL** log line written
*before* the Telegram send is attempted, so the durable record does not depend
on the alert succeeding (Telegram being down is exactly the kind of thing that
should still show up in the log).

## 3. Design decisions

### 3.1 One rotating file per component, plus one orchestration file

```
logs/
  pipeline.log       — orchestration: run_id, stage entry/exit, durations, halts
  datastore.log       — ParquetStore, AssetMaster
  loaders.log         — OHLCV/funding/OI loaders, BackfillRunner
  audit.log           — DataAudit, the five checks, halt decisions
  universe.log        — UniverseBuilder
  backtest.log        — Backtester
  signals.log         — registry + individual signals
  risk.log            — reserved, Phase 6
  portfolio.log       — reserved, Phase 7
  execution.log       — reserved, Phase 8
  attribution.log     — reserved, Phase 9
```

One file per component (rather than one giant file, or one file per run) so a
specific stage can be inspected in isolation, while `pipeline.log` gives the
orchestration-level view and, via `run_id`, the thread to pull on across every
other file for a given run.

### 3.2 Rotation and retention are two different mechanisms

`RotatingFileHandler(maxBytes=10_000_000, ...)` rotates **on size**. A
12-month **retention** window is a calendar concept, and the two don't compose
through `backupCount` alone — you cannot know in advance how many 10 MB
rotations equal a year, because that depends on log volume, which varies by
component and by how much is happening (a quiet `risk.log` rotates far less
often than `loaders.log` during a backfill).

So the two are decoupled deliberately:

- **Rotation** is size-triggered, at 10 MB, via `RotatingFileHandler`. A
  custom `namer` stamps each rotated backup with the UTC time of rotation
  (`loaders.log.20260731T041502Z.log`) instead of a bare numeric suffix, so a
  backup's age is legible from its filename rather than inferred from file
  mtimes across renames.
- **Retention** is time-triggered: `prune_old_logs()` deletes any *rotated
  backup* (never the live `*.log` file currently being written) whose
  timestamp is older than 365 days. It runs as the last step of the nightly
  pipeline, and is also exposed as `python -m pipeline.prune_logs` for anyone
  running components outside the nightly job.

### 3.3 Structured (JSON) file logs, human-readable console

File logs are one JSON object per line — `ts`, `level`, `logger`, `run_id`,
`message`, `module`, `line`, and `exception` (full traceback) when present.
This is deliberate given the "reconstruct an unattended run after the fact"
goal: a scripted post-mortem (or even `duckdb read_json_auto('logs/*.log')`)
can query across every component's log by `run_id` without a text-log parser.
The console handler (used for `scratch/` demos and interactive runs) stays
human-readable — nobody wants to eyeball JSON at a terminal.

### 3.4 `run_id` correlation

A `run_id` (UTC timestamp string, e.g. `20260731T041502Z`) is set once, at the
top of `NightlyPipeline.run()` or any ad hoc script, via a `contextvars`-based
context and attached to every log record through a `logging.Filter`. Grepping
`run_id=20260731T041502Z` across every file in `logs/` reconstructs one run's
activity across every stage it touched. Scratch scripts that don't call
`set_run_id()` get `run_id=-`, which is fine — they're not the unattended-run
case this exists for.

### 3.5 Log levels

| Level | Used for |
| --- | --- |
| DEBUG | per-asset/per-bar detail (signal reject reasons, per-rebalance backtest detail) — off by default, opt in for research |
| INFO | stage start/end, record counts, durations, key decisions (symbols loaded, universe size, audit check verdicts that pass) |
| WARNING | non-fatal issues — matches `AuditResult` severity "warning"; unresolved symbols; a loader falling back |
| ERROR | a caught exception that doesn't halt the run (e.g. one loader failing; the nightly pipeline's load stage is documented as non-fatal) |
| CRITICAL | anything that halts trading or an alert firing — audit `should_halt_trading()`, the kill switch, an unhandled exception in a pipeline stage |

This is an application-logging level scheme; it sits alongside, not on top of,
`AuditResult`'s info/warning/error severity taxonomy, which is a data-quality
concept internal to the audit module and stays exactly as documented in
`PLAN.md`.

### 3.6 Logger naming

Loggers are named `tm.<component>[.<submodule>]` — e.g. `tm.loaders.ohlcv`,
`tm.audit`, `tm.signals.markov_mean_reversion`. Every module gets exactly one
new public call: `get_logger(__name__)`, mirroring the "narrow public
interface per module" principle already in `PLAN.md` §4 — logging doesn't
introduce a second way to depend on another module.

## 4. Configuration

`config.py` already had a `LogConfig`/`LOG_CONFIG` stub from the Phase 0
scaffold (`level: str = "INFO"`, `file: Path = LOGS_PATH /
"trading_machine.log"`) — declared but never consumed anywhere, which is
exactly the gap this document exists to close. Rather than add a second,
competing config object, `LogConfig` is extended in place, and its single
`file` field (one path) is replaced by `dir` (a directory holding one file
per component — see §3.1):

```python
@dataclass
class LogConfig:
    """Logging configuration."""

    level: str = "INFO"
    console_level: str = "WARNING"       # new
    dir: Path = LOGS_PATH                # replaces the old single `file` field
    max_bytes: int = 10 * 1024 * 1024    # new — 10 MB, per spec
    retention_days: int = 365            # new — 12 months, per spec
    components: tuple[str, ...] = (      # new
        "pipeline", "datastore", "loaders", "audit", "universe",
        "backtest", "signals", "risk", "portfolio", "execution",
        "attribution",
    )


LOG_CONFIG = LogConfig()
```

This lives in `config.py`, alongside `LOADER_CONFIG` / `AUDIT_CONFIG` /
`UNIVERSE_CONFIG` — logging settings are values, not schemas, so they follow
the same "all configuration lives in `config.py`" rule those three already
follow. Rotation size and retention window are fixed constants here, not
env-tunable — they were specified directly, not derived from a measured log
volume (see §9, open items). `level`/`console_level` can be made
env-overridable the same way other settings in `config.py` already are, if
that's the existing convention there.

## 5. Reference implementation

`logging_config.py` (delivered alongside this document) imports `LOG_CONFIG`
from `config.py` and owns only the logging *mechanism*:

- `get_logger(name)` — the one function every module imports.
- `configure_logging()` — wires the per-component `RotatingFileHandler`s plus
  a shared console handler; idempotent, called automatically by `get_logger`.
- `set_run_id()` / `get_run_id()` / `new_run_id()` — the correlation context.
- `prune_old_logs()` — retention pruning; returns the list of deleted paths so
  it can log its own action and so tests can assert on it.

`expired_log_backups()` was added alongside `prune_old_logs()` so
`python -m pipeline.prune_logs --dry-run` can list what would be deleted
without deleting it, and so the two can never disagree about which files are
expired.

**Status: wired in** (Phase 5.5, 2026-07-31). `datastore/`, `loaders/`,
`audit/`, `universe/`, `backtest/`, `signals/` and `pipeline/` all take their
loggers from `get_logger(__name__)`. `risk/`, `portfolio/`, `execution/` and
`attribution/` have files reserved and build logging in as they are written.

## 6. Retrofit map — Phases 1-4 and current Phase 5

Additive only: existing public interfaces, tested behaviour, and return values
are unchanged. Every item below is a new `log.*()` call inside an existing
function, not a signature change.

- **Datastore (M1).** `ParquetStore.append` logs dataset, partition and row
  count at INFO; `.read` logs at DEBUG, because the backtester reads inside a
  per-rebalance loop and INFO there would bury every other component in the
  shared log. `AssetMaster` symbol resolution logs an unresolved symbol at
  WARNING instead of only returning `None`.

  > **Deviation (applied).** This section said "the no-overwrite guard logs at
  > WARNING before it raises". There is no such guard: `append` never
  > overwrites — it writes `data_0001.parquet` beside `data_0000.parquet` — so
  > there is nothing to raise. That path logs **DEBUG** instead (a partition
  > that keeps accumulating files means re-fetched history, which is worth
  > seeing but is not a fault). The *schema* validation, which does raise, logs
  > WARNING first, which is what this line was reaching for.

  > **Deviation (applied).** `AssetMaster.resolve_symbol` gained
  > `warn_if_unresolved: bool = True`. WARNING is right for the loaders, where
  > an unresolved symbol silently drops an asset's rows and shows up downstream
  > only as an unexplained audit coverage miss. But
  > `NightlyPipeline._populate_asset_master` calls the same method to ask "is
  > this already mapped?" before adding it, and on a first run every symbol on
  > the venue would log a warning. Additive keyword; the one caller for which
  > absence is the expected answer passes `False`.
- **Loaders/audit (M2/M3).** `BaseLoader`'s append wrapper — previously just
  "error logging," unspecified — becomes INFO (stage timings, records
  fetched/validated/appended) plus ERROR (the exception, still non-fatal per
  the nightly pipeline's documented stage handling) at the point it's caught.
  `BackfillRunner` logs checkpoint save/load at DEBUG and each resumption
  point at INFO. `DataAudit` logs every one of the five checks' verdicts (INFO
  pass, WARNING/ERROR per severity) and logs CRITICAL immediately before
  `should_halt_trading()` trips — independent of whether the Telegram send
  succeeds. `NightlyPipeline` logs stage entry/exit and duration for
  load/audit/report, and calls `prune_old_logs()` as its final step.
- **Universe (M4).** `UniverseBuilder.build_and_store` logs snapshot size,
  the exclusion-reason breakdown, and turnover at INFO.
- **Backtester (M5).** `Backtester.run` logs a per-run summary (date range,
  universe size, final metrics) at INFO; per-rebalance detail is DEBUG-only so
  a multi-year backtest doesn't flood the log by default.
- **Signals (M6).** `signals.register` logs registration at INFO and logs a
  WARNING before refusing a signal whose methodology doc is missing. Every
  signal's reject paths (`None` returns) log at DEBUG per asset — six signals
  x a 150-asset universe x every rebalance is high-volume by nature, so it is
  kept below INFO deliberately. `signals.breadth` logs the effective
  independent-bet count and redundant-pair count at INFO.

## 7. Built in from the start — Phases 6-9

Risk, portfolio, execution, and attribution don't exist yet, so they don't
need retrofitting — logging is now a listed line item in each phase's
checklist in `TODO.md`, alongside the tests and scratch demo every phase
already requires. `execution`'s kill switch and max-daily-loss halt (Phase 8)
follow the same CRITICAL-log-before-alert rule as the audit halt.

## 8. Testing guidance

Don't assert exact log message text broadly — brittle, and not the point. Do
assert, via `pytest`'s `caplog`:

- A CRITICAL record is emitted on every halt path (`should_halt_trading()`
  becoming true, the Phase 8 kill switch) and on any unhandled exception in a
  pipeline stage.
- An ERROR record is emitted when a loader raises inside the append wrapper.
- `prune_old_logs()`, given a `tmp_path` with fabricated file ages, deletes
  only backups older than `retention_days`, leaves newer ones and the live
  `*.log` file untouched, and returns exactly the deleted paths.
- The rotation namer's output matches the pruner's glob. Rotation and
  retention are deliberately decoupled (§3.2), so nothing else forces them to
  agree on the backup filename format — and if they ever disagree, retention
  silently never runs, with no symptom until the disk fills.

## 9. Open items

- **10 MB / 12 months were specified directly**, not derived from a measured
  log volume — there's no real backfill running against this yet. Worth
  revisiting once Phase 2's loaders are logging for real and actual rotation
  frequency per component is observed; a component that rotates every few
  minutes under real load says something different than one that rotates
  monthly.
- **Kill-switch delivery beyond log + Telegram** (e.g. a channel that doesn't
  depend on the bot token being valid) is out of scope here; flagged for
  Phase 8 if it turns out to matter.

## 10. Review log

| Date | Reviewer | Change / decision |
| --- | --- | --- |
| 2026-07-31 | peter | Created (design only; `logging_config.py` written and smoke-tested, nothing wired in). |
| 2026-07-31 | peter | Phase 5.5 applied across Phases 1-5 source. Two deviations recorded in §6: the "no-overwrite guard" does not exist as a raising check, and `resolve_symbol` needed a `warn_if_unresolved` opt-out for the pipeline's membership probe. Added `expired_log_backups()` for `--dry-run`, and a test asserting the namer and the pruner agree on the backup filename format. §9's open item — 10 MB / 12 months were specified, not measured — still stands: no real backfill has run, so no component's real rotation frequency is known yet. |
