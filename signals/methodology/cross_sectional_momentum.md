# Methodology: cross_sectional_momentum

| Field | Value |
| --- | --- |
| Signal ID | `cross_sectional_momentum` |
| Family | momentum |
| Author | peter |
| Created | 2026-07-31 |
| Last reviewed | 2026-07-31 |
| Status | draft |
| Status note | implemented + tested; backtest evidence pending a real backfill |

## 1. Hypothesis

**Mechanism:** capital moves toward assets that have already been moving, and it
moves slowly. An asset that has outperformed over the last few months attracts
attention, listings, liquidity, and narrative; the flows that follow arrive over
weeks rather than instantly, so the price keeps drifting in the direction it was
already going. Underreaction is the flip side of the same coin: information about
a protocol's adoption or a token's supply schedule diffuses through a retail-heavy
holder base gradually rather than being repriced at once.

**Why it should persist (who is on the other side, and why they keep trading):**
the sellers into a rally are disposition-effect holders realizing gains and
early-round allocations unlocking on a fixed schedule — both selling for reasons
unrelated to the asset's forward return. Neither group is going to stop: the
disposition effect is a stable behavioural bias, and unlock calendars are written
into token contracts years ahead.

**What would make the mechanism stop working:** momentum dies when flows stop
being slow. A universe dominated by systematic cross-sectional traders arbitrages
the drift away; so does a market where every asset moves together (in a
correlation-1 crash the cross-section carries no information, only the market
does). Both are observable — the first as a steady IC decay, the second as
cross-sectional dispersion collapsing.

**Prior expectation:** rank IC of roughly 0.02–0.04. Cross-sectional momentum is
the most-documented anomaly in the literature and the most-traded, so the honest
prior is "real but small and crowded".

## 2. Data inputs

| Input | Dataset | Field(s) | Revised after publication? |
| --- | --- | --- | --- |
| Daily closes | `ohlcv_daily` | `close` | yes — venues revise recent bars, which is why the loaders re-fetch an overlap and readers collapse to the latest ingestion |

**Point-in-time contract:** the signal reads only through `RebalanceContext`
(`signals.bars.close_series` → `ctx.ohlcv(...)`), so `event_ts <= asof` always
holds and `ingested_ts <= asof` holds under the default `pit_mode="ingestion"`.

- [x] All inputs come from the context; the signal opens no files, makes no
      network calls, and reads no future-dated fields.

**Minimum history:** `lookback_days + skip_days + 1` = **98 bars** at the
defaults. Assets with less are scored `None` (excluded from the cross-section),
never `0.0`.

Bars are additionally trimmed to the most recent gap-free stretch
(`max_gap_days = 1`): a hole in the series would make the "90-day return" span
more than 90 days.

## 3. Construction

1. Read the trailing `lookback_days + skip_days + 1 + history_buffer_days`
   calendar days of closes per universe asset; collapse each `(asset, bar)` to
   its latest ingestion; trim to the most recent gap-free stretch, capped at
   `min_history_bars` trailing bars.
2. Formation return: `close[-1 - skip_days] / close[-1 - skip_days - lookback_days] - 1`.
   Simple (not log) return: the signal is a ranking, and simple returns are what
   a reader checking the arithmetic against a price chart would compute.
3. Assets whose base price is non-positive or non-finite score `None`.
4. Winsorize the cross-section at `winsorize_pct` from each tail.
5. Cross-sectionally z-score within the universe (mean 0, sd 1).

Steps 4 and 5 are `signals.transforms.cross_sectional_zscore`, in that fixed
order — clipping before computing the mean and sd stops one blown-up score from
setting the scale for everyone.

**Sign convention:** higher score = more attractive = larger long weight. A
higher formation return means a higher score; the signal is *not* negated.

**Skip window:** 7 bars. The last week of a price series is dominated by
short-horizon reversal (which `short_term_reversal` trades deliberately, with the
opposite sign). Leaving it in the formation window mixes two opposing signals into
one number and cancels part of both.

**Cross-sectional or time-series:** cross-sectional. The z-score subtracts the
universe mean, so a market-wide rally produces no net view — that is the
intended difference from `time_series_momentum`, which keeps it.

## 4. Parameters

| Parameter | Value | Range tested | Why this value |
| --- | --- | --- | --- |
| `lookback_days` | 90 | 30–180 (grid pending) | ~3 months is the classic formation window and long enough that a single week cannot dominate it |
| `skip_days` | 7 | 0–14 (grid pending) | one week of short-term reversal excluded; see the skip-window note above |
| `max_gap_days` | 1 | — | daily bars; anything wider changes what the return measures |
| `winsorize_pct` | 2.5 | 0–5 | shared default across signals so cross-signal correlations are not an artifact of different clipping |
| `history_buffer_days` | 30 | — | ordinary missing bars should not push an eligible asset below the minimum |

**Parameter sensitivity:** not yet established. Both `lookback_days` and
`skip_days` need the walk-forward grid treatment
(`scratch/scratch_markov_param_grid.py` is the pattern) against a real backfill
before this signal is trusted at any particular setting. **Until that is done, a
number from this signal is a construction, not evidence.**

## 5. Backtest evidence

**Not yet collected.** The repository has no multi-year backfill in it, so there
is no honest number to put here — and a synthetic-data result would only measure
the generator. Filling this section requires:

1. `python -m pipeline.nightly --start <5y ago> --end <today>` (hours, resumable).
2. A walk-forward parameter grid over `lookback_days` × `skip_days`, selecting on
   prior folds only.
3. `pit_mode="event"` on backfilled history (every row shares one `ingested_ts`),
   labelled as research indications, not live-fidelity results.

**Run config:** _pending_

| Metric | Full sample | First half | Second half |
| --- | --- | --- | --- |
| Mean rank IC | | | |
| IC volatility | | | |
| IC IR | | | |
| Ann. return (net) | | | |
| Ann. vol | | | |
| Information ratio | | | |
| Max drawdown | | | |
| Avg turnover | | | |
| Total costs | | | |

**Cost sensitivity:** pending. Momentum's turnover is moderate (positions persist
for weeks), so it should survive 2x costs more comfortably than the reversal
signals — that is a prediction to check, not a result.

**Decay:** pending.

## 6. Breadth check (correlation with existing signals)

Run `scratch/scratch_signal_breadth.py` (or `signals.breadth.breadth_report`)
against a real backfill to fill this in.

| Against | Score correlation | Return correlation |
| --- | --- | --- |
| `time_series_momentum` | expected high (same formation window) | |
| `short_term_reversal` | expected negative | |
| `markov_mean_reversion` | expected negative | |
| `carry` | expected near zero | |
| `low_volatility` | expected mildly negative | |

**Verdict:** pending measurement. The pair to watch is
`time_series_momentum`: it shares the formation window, and the only structural
difference is that it divides by the asset's own volatility and skips the
demeaning. If the measured score correlation exceeds ~0.7 — which is plausible —
the two are one bet expressed twice, and the doc must either justify both or
retire one. Sharing a formation window is a reason to expect correlation, not a
justification for keeping both.

## 7. Alpha refinement

Per Grinold–Kahn, `alpha = volatility × IC × z` (`signals.alpha`):

- **IC estimate used:** `IcEstimate.shrunk_ic` from the backtest IC series —
  pending, per Section 5.
- **Shrinkage applied:** normal-normal with `prior_ic_std = 0.02`, capped at
  `max_abs_ic = 0.10`.
- **Volatility estimate:** `low_volatility.annualized_vol_universe` (60-day
  realized, annualized) — deliberately the same estimator the volatility signal
  uses, so risk is not defined twice in the codebase.
- **Resulting alpha scale:** pending.

## 8. Known failure modes

| Failure mode | Symptom you would see | Mitigation (or "accepted") |
| --- | --- | --- |
| Momentum crash after a market-wide liquidation | the losers rebound hardest; the long/short book takes its worst drawdown in the *recovery*, not the crash | accepted in v1; Phase 7's vol targeting and Phase 6's risk model are where this is sized rather than avoided |
| Formation window still contains a crash | the signal keeps shorting names that already bottomed | partly mitigated by the 7-day skip; fully addressed only by a regime filter, which is out of scope |
| A newly-listed asset's first 98 bars | scored `None` rather than as a huge momentum name | mitigated: `min_history_bars` + `min_listing_age_days` in the universe |
| Cross-sectional dispersion collapses (everything moves together) | z-scores stay in range but ranks become noise; IC → 0 with no other symptom | not mitigated; visible only in the attribution's IC decay tracking (Phase 9) |
| A stale price series | a flat tail makes the formation return look small and stable | partly mitigated by the gap-free trim; a venue that keeps printing an unchanged close is caught by the audit's null/freshness checks, not here |

**Regimes where this is expected to lose money:** sharp trend reversals
(especially the first weeks of a recovery from a liquidation), and range-bound
markets where the formation window's ranking is noise.

**Data quality dependencies:** the `freshness` and `duplicate_bars` audit checks.
A stale `ohlcv_daily` silently freezes the formation return; a climbing duplicate
rate means overlapping re-fetches, which the reader collapses but which also
signals that the recent window is the most-rewritten part of the history.

## 9. Implementation notes

- **Module:** `signals/cross_sectional_momentum.py`
- **Registry key:** `cross_sectional_momentum`
- **Tests:** `tests/test_signals_phase5.py` — golden hand-computed formation
  return, point-in-time test through a real store, `None` on insufficient
  history and on a gap.
- **Scratch demo:** `scratch/scratch_signals_phase5.py`

**Deviations from this doc:** none.

## 10. Review log

| Date | Reviewer | Change / decision |
| --- | --- | --- |
| 2026-07-31 | peter | Created; implemented and unit-tested. Backtest evidence (§5) and breadth measurement (§6) deliberately left empty — no real backfill available. Status stays `draft` until they are filled. |
