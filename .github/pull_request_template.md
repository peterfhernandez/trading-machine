## Summary

<!-- What changed and why, in a sentence or two. -->

## Phase / module

<!-- Which TODO.md phase and module(s) this touches, e.g. "Phase 4 — backtest (M5)". -->

## Point-in-time discipline

- [ ] No look-ahead bias introduced (backtest/research code only reads `ingested_ts <= asof`)
- [ ] Datastore writes are append-only; no history overwritten
- [ ] N/A — no datastore reads/writes in this change

## Test plan

- [ ] New/updated tests added under `tests/`; each runs in <100ms (network/exchange/filesystem mocked)
- [ ] `pytest` passes locally
- [ ] Scratch demo added/updated under `scratch/` where applicable

## Docs

- [ ] `METHODOLOGY.md` added/updated for any new or changed signal
- [ ] `README.md` / `TODO.md` progress log updated if this completes or advances a phase item
