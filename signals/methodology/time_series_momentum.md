# Methodology: time_series_momentum

| Field | Value |
| --- | --- |
| Signal ID | `time_series_momentum` |
| Family | momentum |
| Author | peter |
| Created | 2026-07-31 |
| Last reviewed | 2026-07-31 |
| Status | draft (implemented + tested; backtest evidence pending a real backfill) |

## 1. Hypothesis

**Mechanism:** an asset that has risen over the past quarter tends to keep
rising, *on its own terms* — not relative to its peers. The mechanism is the same
slow-flow underreaction that drives cross-sectional momentum, but the bet is
directional: when the whole asset class is trending, being long the asset class
is the trade, and a cross-sectionally demeaned signal cannot express that.

**Why it should persist (who is on the other side, and why they keep trading):**
leveraged holders who are liquidated on drawdowns and re-enter on strength, and
discretionary traders who add on confirmation. Both trade *because* of the trend,
which mechanically extends it. In crypto specifically, perpetual funding and
liquidation cascades make position sizing procyclical in a way that has no
equivalent in equities.

**What would make the mechanism stop working:** the trend premium is a
compensation for taking the crash risk of being on the wrong side of a reversal.
It stops paying when reversals become frequent enough to eat the drift — a
choppy, range-bound market is exactly the regime where a time-series trend
follower bleeds.

**Prior expectation:** rank IC of roughly 0.02–0.03 measured cross-sectionally,
but note that rank IC systematically *understates* a time-series signal: the
engine's IC is computed on the cross-section, and this signal's directional
content lives in the cross-sectional mean, which ranking discards. The IC number
is a lower bound on this signal's value and should be read alongside the realized
return of a book built from it.

## 2. Data inputs

| Input | Dataset | Field(s) | Revised after publication? |
| --- | --- | --- | --- |
| Daily closes | `ohlcv_daily` | `close` | yes — venues revise recent bars |

**Point-in-time contract:** reads only through `RebalanceContext`
(`signals.bars.close_series`).

- [x] All inputs come from the context; the signal opens no files, makes no
      network calls, and reads no future-dated fields.

**Minimum history:** `skip_days + max(lookback_days, vol_window_days) + 1` =
**91 bars** at the defaults. Assets with less score `None`.

## 3. Construction

1. Read and trim as in `cross_sectional_momentum` (latest ingestion per bar,
   gap-free tail, capped at `min_history_bars`).
2. Formation return: `close[-1 - skip_days] / close[-1 - skip_days - lookback_days] - 1`.
3. Realized volatility: standard deviation (ddof=1) of the trailing
   `vol_window_days` daily **log** returns, ending at the same bar the formation
   window ends on. Requires `min_vol_observations` finite returns.
4. Horizon volatility: `bar_vol × sqrt(lookback_days)`.
5. Raw score: `formation_return / horizon_vol` — the trend measured in units of
   the asset's own noise over the same horizon.
6. Winsorize the cross-section at `winsorize_pct` from each tail.
7. **Scale to unit dispersion without demeaning**
   (`transforms.cross_sectional_scale`).

**Step 7 is the load-bearing decision in this doc.** Z-scoring subtracts the
cross-sectional mean, which is exactly the market-wide trend this signal exists
to express: if every asset has trended up, a demeaned score reports "no view"
while a time-series signal should report "long". Scaling alone makes the scores
comparable in magnitude while preserving the tilt. Two consequences follow and
are accepted deliberately:

- A book built from these scores is **not** dollar-neutral. `long_short_from_scores`
  will still produce a neutral book because it ranks; a weighting scheme that
  uses the score levels will not, and that is the point.
- The rank IC the engine reports for this signal is invariant to step 7
  altogether (ranking discards both location and scale), so the IC series cannot
  detect a mistake here. That is what the golden test in
  `tests/test_signals_phase5.py` is for.

**Sign convention:** higher score = more attractive = larger long weight.

**Skip window:** 0 by default. Unlike the cross-sectional version, the
short-horizon reversal that motivates a skip window is a *relative* effect; the
time-series signal is dominated by the drift over the whole window, so the
default keeps the most recent bar. `skip_days` is exposed so the grid can test
that claim rather than assume it.

**Cross-sectional or time-series:** time-series (per-asset standardization
against its own volatility; no cross-sectional demeaning).

## 4. Parameters

| Parameter | Value | Range tested | Why this value |
| --- | --- | --- | --- |
| `lookback_days` | 90 | 30–180 (grid pending) | same window as `cross_sectional_momentum`, so the two differ only in the standardization — which is what the breadth check is meant to isolate |
| `skip_days` | 0 | 0–7 (grid pending) | see the skip-window note |
| `vol_window_days` | 60 | 30–120 (grid pending) | long enough to be a stable scale estimate, short enough to track a regime change |
| `min_vol_observations` | 40 | — | two-thirds of the vol window; a volatility from a handful of points is a number, not an estimate |
| `winsorize_pct` | 2.5 | 0–5 | shared default across signals |

**Parameter sensitivity:** not yet established; needs the walk-forward grid
against a real backfill. Until then, a number from this signal is a
construction, not evidence.

## 5. Backtest evidence

**Not yet collected** — same reason and same prerequisites as
`cross_sectional_momentum.md` §5.

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

**Cost sensitivity:** pending.

**Decay:** pending. Worth measuring separately in trending and range-bound
sub-samples; a pooled IC hides the regime dependence that defines this signal.

## 6. Breadth check (correlation with existing signals)

| Against | Score correlation | Return correlation |
| --- | --- | --- |
| `cross_sectional_momentum` | expected high — same formation window | |
| `short_term_reversal` | expected negative | |
| `markov_mean_reversion` | expected negative | |
| `carry` | expected near zero | |
| `low_volatility` | expected negative (the vol divisor puts high-vol assets nearer zero) | |

**Verdict:** pending measurement, and the honest expectation is that this and
`cross_sectional_momentum` are highly correlated in *ranks*. If they are, the case
for keeping both rests entirely on the un-demeaned level — i.e. on the directional
view — which means the justification has to be a realized-return comparison of the
two books, not a correlation number. If that comparison does not favour keeping
both, this doc records the retirement of one of them.

## 7. Alpha refinement

- **IC estimate used:** `IcEstimate.shrunk_ic` — pending. Note the §1 caveat that
  cross-sectional rank IC understates this signal.
- **Shrinkage applied:** normal-normal, `prior_ic_std = 0.02`, cap 0.10.
- **Volatility estimate:** `low_volatility.annualized_vol_universe`. The signal
  already divides by its own volatility estimate, so multiplying by volatility
  again in the alpha step partly undoes that — this is a known tension to
  resolve in Phase 6 when the risk model provides a proper forecast; for now the
  Grinold–Kahn form is applied uniformly across signals rather than special-cased.
- **Resulting alpha scale:** pending.

## 8. Known failure modes

| Failure mode | Symptom you would see | Mitigation (or "accepted") |
| --- | --- | --- |
| Whipsaw in a range-bound market | steady bleed, high turnover, IC near zero with no drawdown event to point at | accepted; this is the premium being paid back |
| Trend reversal at the top | the largest positions are in the assets about to fall hardest | accepted in v1; sized by Phase 6/7 |
| Volatility divisor collapses (a nearly-flat asset) | a tiny drift divided by a tiny volatility produces an enormous score | mitigated: `realized_vol` returns NaN for degenerate dispersion, and winsorization clips what survives |
| Every asset trends together | the un-demeaned scores all have the same sign; a ranked book has no view while a level-weighted book is fully directional | intended behaviour, but it means the two book constructions diverge exactly when it matters — flagged for Phase 7 |

**Regimes where this is expected to lose money:** choppy sideways markets, and
the first weeks after a trend reverses.

**Data quality dependencies:** `freshness` (a stale series flattens both the
formation return and the volatility, and the ratio can stay plausible while both
inputs are wrong) and `price_outliers` (one bad print inflates the volatility and
shrinks the score toward zero for weeks).

## 9. Implementation notes

- **Module:** `signals/time_series_momentum.py`
- **Registry key:** `time_series_momentum`
- **Tests:** `tests/test_signals_phase5.py` — golden hand-computed return/vol/score,
  a test that the standardization preserves the cross-sectional mean's sign
  (the property step 7 exists for), point-in-time, and every reject path.
- **Scratch demo:** `scratch/scratch_signals_phase5.py`

**Deviations from this doc:** none.

## 10. Review log

| Date | Reviewer | Change / decision |
| --- | --- | --- |
| 2026-07-31 | peter | Created. Chose scale-without-demeaning over z-scoring (§3 step 7) so the time-series view survives standardization; recorded the two consequences rather than leaving them implicit. §5/§6 left empty pending a real backfill. |
