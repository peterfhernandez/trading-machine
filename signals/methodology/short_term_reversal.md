# Methodology: short_term_reversal

| Field | Value |
| --- | --- |
| Signal ID | `short_term_reversal` |
| Family | reversal |
| Author | peter |
| Created | 2026-07-31 |
| Last reviewed | 2026-07-31 |
| Status | draft (implemented + tested; backtest evidence pending a real backfill) |

## 1. Hypothesis

**Mechanism:** a large move over a few days is usually part liquidity provision,
not information. Someone needed to get out of a position quickly, or a
liquidation cascade forced size through a thin book; the price overshoots, and it
comes back when the pressure stops. Buying the assets that just fell hardest and
selling the ones that just ran hardest is being paid to provide the liquidity
that was missing.

**Why it should persist (who is on the other side, and why they keep trading):**
forced sellers — liquidated leveraged positions, and funds meeting redemptions —
who are trading on a deadline rather than on a view. Crypto's perpetual market
structure makes this unusually mechanical: liquidation engines sell into the
book at whatever price clears, and they do it more the further the price has
already moved.

**What would make the mechanism stop working:** the premium is compensation for
being the buyer when nobody else is, so it disappears when the move *is*
information — a hack, a delisting, a depeg. It also disappears when market making
capital is abundant, since the overshoot is arbitraged before a daily-rebalanced
signal can see it.

**Prior expectation:** rank IC of roughly 0.02–0.05 gross — higher than the
momentum signals — but with turnover high enough that costs are the whole
question. This is the signal most likely to look excellent gross and unprofitable
net.

## 2. Data inputs

| Input | Dataset | Field(s) | Revised after publication? |
| --- | --- | --- | --- |
| Daily closes | `ohlcv_daily` | `close` | yes — venues revise recent bars, and this signal reads *only* the most recent bars, so it is the signal most exposed to revisions |

**Point-in-time contract:** reads only through `RebalanceContext`
(`signals.bars.close_series`).

- [x] All inputs come from the context; the signal opens no files, makes no
      network calls, and reads no future-dated fields.

**Minimum history:** `max(lookback_days, vol_window_days) + 1` = **31 bars** at
the defaults (the volatility window dominates). Assets with less score `None`.

**Revision exposure is worth stating plainly:** the whole signal lives in the
last five bars, which are exactly the bars a venue is most likely to revise and
the loaders most likely to re-fetch. Under `pit_mode="ingestion"` the backtest
sees the version of the bar that was knowable at the rebalance, which is correct;
under `pit_mode="event"` it sees the final revised value, which flatters this
signal more than any other in the set. An event-mode number for
`short_term_reversal` should be read with that specifically in mind.

## 3. Construction

1. Read and trim as the other price signals do (latest ingestion per bar,
   gap-free tail, capped at `min_history_bars`).
2. Recent return: `close[-1] / close[-1 - lookback_days] - 1`.
3. Realized volatility: standard deviation (ddof=1) of the trailing
   `vol_window_days` daily log returns; requires `min_vol_observations` finite
   returns.
4. Horizon volatility: `bar_vol × sqrt(lookback_days)`.
5. Raw score: `-(recent_return / horizon_vol)` when `vol_scale` (the default),
   else `-recent_return`.
6. Winsorize the cross-section at `winsorize_pct`, then cross-sectionally
   z-score.

**Volatility scaling is on by default and does more work here than in any other
signal.** Without it the extremes of the cross-section are selected by volatility
rather than by overshoot: the biggest raw five-day moves belong to the most
volatile names regardless of direction, so an unscaled signal is mechanically a
short-high-vol / long-low-vol bet wearing a reversal costume — and one that would
correlate heavily with `low_volatility` for reasons that have nothing to do with
reversal. `vol_scale=False` is exposed so the grid can measure that claim.

**Sign convention:** higher score = more attractive = larger long weight. The
negation in step 5 is the signal: a large recent *gain* is an unattractive long.

**Skip window:** none, deliberately — the recent window is the signal, not
contamination of it. This is the exact complement of `cross_sectional_momentum`'s
7-day skip, and the two are expected to be negatively correlated because of it.

**Cross-sectional or time-series:** cross-sectional.

## 4. Parameters

| Parameter | Value | Range tested | Why this value |
| --- | --- | --- | --- |
| `lookback_days` | 5 | 1–21 (grid pending) | a week of overshoot; one day is mostly bid-ask noise at a daily bar, a month is momentum territory |
| `vol_scale` | True | {True, False} (grid pending) | see the note above |
| `vol_window_days` | 30 | 20–90 (grid pending) | short enough to reflect the asset's current regime, since the overshoot is judged against *current* noise |
| `min_vol_observations` | 20 | — | two-thirds of the vol window |
| `winsorize_pct` | 2.5 | 0–5 | shared default across signals |

**Parameter sensitivity:** not yet established, and this is the signal where the
answer matters most: a reversal signal that works at exactly one lookback is
almost certainly fitting the sample's particular liquidation events. Needs the
walk-forward grid against a real backfill.

## 5. Backtest evidence

**Not yet collected** — same prerequisites as `cross_sectional_momentum.md` §5,
plus the two specific to this signal:

- Run it in **both** pit modes and compare. A large gap between event mode and
  ingestion mode is the revision exposure described in §2 showing up as fake
  alpha.
- Report net-of-cost numbers at 1x and 2x costs *first*, before the gross IC.
  Gross performance for a 5-day reversal signal is not evidence of anything.

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

**Cost sensitivity:** pending, and decisive. With a 5-day lookback and a daily or
weekly rebalance, turnover will be the highest of any signal in the set. If the
IR at 2x costs is not clearly positive, this signal does not ship regardless of
its gross IC.

**Decay:** pending.

## 6. Breadth check (correlation with existing signals)

| Against | Score correlation | Return correlation |
| --- | --- | --- |
| `markov_mean_reversion` | expected high — same family, same idea | |
| `cross_sectional_momentum` | expected negative | |
| `time_series_momentum` | expected negative | |
| `carry` | expected weak positive | |
| `low_volatility` | expected weak; would be strong if `vol_scale` were off | |

**Verdict:** pending measurement. `markov_mean_reversion` is the pair that
matters: both are reversal-family and both trade overshoot, and if their score
correlation exceeds ~0.7 then the Markov signal's extra machinery — state
discretization, transition matrix, midpoints — is buying nothing over a negated
five-day return, which would be a strong argument for retiring the complicated
one. That comparison is the main reason this signal exists in its deliberately
simple form.

## 7. Alpha refinement

- **IC estimate used:** `IcEstimate.shrunk_ic` — pending.
- **Shrinkage applied:** normal-normal, `prior_ic_std = 0.02`, cap 0.10.
- **Volatility estimate:** `low_volatility.annualized_vol_universe`. As with
  `time_series_momentum`, the signal already divides by a volatility estimate, so
  the alpha step's multiplication partly undoes it — noted, applied uniformly for
  now, to be resolved when Phase 6 supplies a real forecast.
- **Resulting alpha scale:** pending.

## 8. Known failure modes

| Failure mode | Symptom you would see | Mitigation (or "accepted") |
| --- | --- | --- |
| The move was information (hack, depeg, delisting announcement) | the signal buys the asset that just fell 40% and it keeps falling | not mitigated in v1; a news/event filter is out of scope, and the universe's liquidity rules only catch it after the fact |
| Costs eat the edge | positive gross IC, negative net return, turnover near 100% per rebalance | measured, not mitigated: the 2x-cost line in §5 is the gate |
| Bar revisions | strong event-mode results that do not reproduce in ingestion mode | measured by running both modes (§5) |
| Volatility estimate stale after a regime break | during a vol spike the 30-day window understates current noise, so every score looks extreme | partly mitigated by winsorization; a shorter vol window trades this against noise, which the grid should test |
| Liquidation cascade still running at the rebalance | maximum long exactly into the second leg down | accepted; this is the risk being paid for |

**Regimes where this is expected to lose money:** sustained one-directional
trends (the reversal never comes), and multi-day liquidation cascades where each
day's overshoot is followed by a larger one.

**Data quality dependencies:** `price_outliers` above all — a single bad print in
the last five bars *is* the signal for that asset, so a false outlier produces a
maximum-size position on fictional data. `duplicate_bars` matters more here too:
the recent window is exactly the stretch that overlapping re-fetches rewrite.

## 9. Implementation notes

- **Module:** `signals/short_term_reversal.py`
- **Registry key:** `short_term_reversal`
- **Tests:** `tests/test_signals_phase5.py` — golden hand-computed
  return/vol/score, a sign test (a riser scores below a faller), the
  `vol_scale=False` path, point-in-time, and every reject path.
- **Scratch demo:** `scratch/scratch_signals_phase5.py`

**Deviations from this doc:** none.

## 10. Review log

| Date | Reviewer | Change / decision |
| --- | --- | --- |
| 2026-07-31 | peter | Created. Volatility scaling made the default after noting an unscaled version is mechanically a low-volatility bet. Built deliberately simple so it can serve as the control for `markov_mean_reversion` in the breadth check. §5/§6 pending a real backfill. |
