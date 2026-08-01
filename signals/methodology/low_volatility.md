# Methodology: low_volatility

| Field | Value |
| --- | --- |
| Signal ID | `low_volatility` |
| Family | volatility |
| Author | peter |
| Created | 2026-07-31 |
| Last reviewed | 2026-07-31 |
| Status | draft |
| Status note | implemented + tested; backtest evidence pending a real backfill |

## 1. Hypothesis

**Mechanism:** the low-volatility anomaly is the observation that high-volatility
assets deliver *worse* risk-adjusted returns than low-volatility ones — the
opposite of what a risk-return tradeoff predicts. The standard explanation is
leverage aversion combined with a preference for lottery-like payoffs: investors
who want more return but cannot or will not borrow buy high-volatility assets
instead of levering low-volatility ones, bidding the volatile names up and their
forward returns down. In crypto the leverage constraint is weaker (perps are
freely available) but the lottery preference is far stronger — a meaningful part
of the market is explicitly shopping for 100x outcomes, and pays for the chance.

**Why it should persist (who is on the other side, and why they keep trading):**
buyers of lottery tickets, who are not going to stop, because the appeal is the
skew rather than the expected value. The arbitrage that would flatten it —
levering up a basket of boring assets and shorting the volatile ones — requires
sustained leverage through drawdowns, which is exactly what a leverage-averse or
capital-constrained participant cannot do.

**What would make the mechanism stop working:** in crypto the whole universe is
high-volatility by any other asset class's standard, so the cross-sectional
spread being exploited is between "very volatile" and "extremely volatile", and
it can compress. It also inverts in a strong bull market, where the highest-beta
names simply return the most and a low-vol book underperforms for quarters at a
time.

**Prior expectation:** rank IC of roughly 0.01–0.03 — the weakest prior of the
five. This is also the signal most likely to be *subsumed by the risk model* in
Phase 6: a volatility ranking is close to a beta ranking, and a factor model that
already prices beta may leave nothing here.

## 2. Data inputs

| Input | Dataset | Field(s) | Revised after publication? |
| --- | --- | --- | --- |
| Daily closes | `ohlcv_daily` | `close` | yes — venues revise recent bars |

**Point-in-time contract:** reads only through `RebalanceContext`
(`signals.bars.close_series`).

- [x] All inputs come from the context; the signal opens no files, makes no
      network calls, and reads no future-dated fields.

**Minimum history:** `min_observations + 1` = **41 bars** at the defaults; the
estimate itself uses up to `vol_window_days` = 60 trailing returns. Assets with
less score `None`.

## 3. Construction

1. Read and trim as the other price signals do (latest ingestion per bar,
   gap-free tail, capped at `vol_window_days + 1` bars).
2. Daily **log** returns over the trimmed series.
3. Realized volatility: standard deviation (ddof=1) of the trailing
   `vol_window_days` finite log returns; requires `min_observations` of them.
   A degenerate (zero-dispersion) series scores `None` — a constant price is not
   a zero-risk asset, it is an unpriced one.
4. Annualize: `bar_vol × sqrt(periods_per_year)` with `periods_per_year = 365`
   (crypto trades 24/7, matching `BACKTEST_CONFIG.periods_per_year`).
   Annualization is monotone and cannot change ranks or the standardized scores;
   it exists so the raw diagnostics read as a percentage a human recognizes.
5. Raw score: `-annualized_vol`.
6. Winsorize the cross-section at `winsorize_pct`, then cross-sectionally
   z-score.

**Sign convention:** higher score = more attractive = larger long weight. Low
volatility is the attractive side, hence the negation in step 5 — done *in the
signal*, never by reading the ranking backwards downstream, because the IC
series, `long_short_from_scores` and the alpha step all assume the convention
holds.

**Note on the winsorization.** Realized volatility has a long right tail (one
liquidation cascade dominates a 60-day window), so the clipping is asymmetric in
*effect* even though the parameter is symmetric: the upper clip binds far more
often than the lower one. That is the intended behaviour — it stops a single
blown-up asset from setting the scale for the whole cross-section — but it means
the parameter is doing more here than in the other signals.

**Skip window:** none. Volatility is a level, not a formation-window return;
there is no short-horizon reversal to skip past.

**Cross-sectional or time-series:** cross-sectional.

## 4. Parameters

| Parameter | Value | Range tested | Why this value |
| --- | --- | --- | --- |
| `vol_window_days` | 60 | 20–120 (grid pending) | two months: long enough that the estimate is stable (volatility ranks are persistent, so turnover stays low), short enough to track a regime change |
| `min_observations` | 40 | — | two-thirds of the window; a volatility from a handful of points is a number, not an estimate |
| `periods_per_year` | 365 | — | crypto trades 24/7; matches the backtester's annualization |
| `max_gap_days` | 1 | — | daily bars |
| `winsorize_pct` | 2.5 | 0–5 | shared default; see the note above on why it does more work here |
| `history_buffer_days` | 30 | not tuned | extra calendar days requested from the store so ordinary missing bars do not push an eligible asset under the minimum |

**Parameter sensitivity:** not yet established. Expected to be the *least*
sensitive signal in the set — volatility rankings are highly persistent, so
neighbouring window lengths should produce nearly the same cross-section. If the
grid shows this signal is sharply sensitive to `vol_window_days`, that is
evidence something is wrong with the estimate rather than evidence of a tuning
opportunity.

## 5. Backtest evidence

**Not yet collected** — same prerequisites as `cross_sectional_momentum.md` §5.

One extra thing to record when it is: the realized volatility of a book built
from this signal, not just its return. A low-volatility signal that produces a
high-volatility portfolio has a construction bug (almost certainly in the
weighting, not the score), and the return series alone will not reveal it.

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

**Cost sensitivity:** pending. Turnover should be the lowest of the five
(volatility ranks change slowly), so this signal should survive 2x costs most
comfortably — a prediction to check.

**Decay:** pending. Worth splitting by market direction: the anomaly is expected
to invert in strong bull markets, so a pooled IC that averages a bull and a bear
regime could report approximately zero while the signal is working reliably in
both directions.

## 6. Breadth check (correlation with existing signals)

| Against | Score correlation | Return correlation |
| --- | --- | --- |
| `cross_sectional_momentum` | expected weak | |
| `time_series_momentum` | expected negative (its vol divisor pulls high-vol assets toward zero) | |
| `short_term_reversal` | expected weak *because* that signal is vol-scaled; would be strong if it were not | |
| `markov_mean_reversion` | expected weak | |
| `carry` | expected weak | |

**Verdict:** pending measurement. The expectation is that this signal adds real
breadth, since it ranks on a completely different quantity (a second moment
rather than a first). The risk is not redundancy against another *signal* but
redundancy against the Phase 6 **risk model** — if the factor model prices
volatility/beta directly, this signal's alpha may be entirely absorbed as a
factor exposure. That is a Phase 6 question and is flagged here so it is asked
rather than discovered.

## 7. Alpha refinement

- **IC estimate used:** `IcEstimate.shrunk_ic` — pending.
- **Shrinkage applied:** normal-normal, `prior_ic_std = 0.02`, cap 0.10.
- **Volatility estimate:** `annualized_vol_universe` in this same module — which
  makes this the one signal where `alpha = volatility × IC × z` multiplies a
  quantity by (a standardized transform of) itself. The result is a deliberate
  softening: a very low-volatility asset gets a high `z` but a small volatility
  multiplier, so its alpha is smaller than the score alone suggests. That is the
  correct Grinold–Kahn behaviour (alpha is an expected *return*, and a
  low-volatility asset's returns are smaller in magnitude), but it is unintuitive
  enough to be worth stating before someone "fixes" it.
- **Resulting alpha scale:** pending.

**This module is the project's single volatility estimator.**
`annualized_vol_universe` is exported specifically so `signals.alpha` and any
later consumer use the same numbers the signal ranks on. A second, subtly
different volatility estimator elsewhere in the codebase is how two parts of a
system quietly disagree about risk.

## 8. Known failure modes

| Failure mode | Symptom you would see | Mitigation (or "accepted") |
| --- | --- | --- |
| Strong bull market | the highest-vol names outperform for quarters; the book underperforms steadily with no single bad day | accepted; this is the anomaly's known regime dependence |
| A stale or flat price series | zero measured volatility makes an unpriced asset look maximally attractive | mitigated: degenerate dispersion scores `None`, not a maximal score |
| One bad print | a false 50% bar inflates 60 days of volatility, so the asset is shorted for two months on one bad tick | partly mitigated by winsorization; depends on the `price_outliers` audit check catching it upstream |
| A newly-listed asset's early quiet period | thin trading looks like low volatility | mitigated by `min_observations` and the universe's listing-age filter |
| The risk model prices it away (Phase 6) | the signal's alpha becomes a factor exposure with no residual | not a failure of this signal so much as its expected fate; measured in Phase 6 |

**Regimes where this is expected to lose money:** sustained bull markets, and
recoveries from a crash where the highest-beta names rebound hardest.

**Data quality dependencies:** `price_outliers` (one bad bar contaminates the
whole window) and `freshness` (a stale series produces an artificially *low*
volatility, i.e. an artificially *attractive* score — the failure direction that
puts capital into the broken asset rather than out of it).

## 9. Implementation notes

- **Module:** `signals/low_volatility.py`
- **Registry key:** `low_volatility`
- **Tests:** `tests/test_signals_phase5.py` — golden hand-computed volatility and
  score, the sign convention (a quiet asset outranks a violent one), the
  degenerate-series reject path, point-in-time, and that
  `annualized_vol_universe` agrees with `diagnose_series`.
- **Scratch demo:** `scratch/scratch_signals_phase5.py`

**Deviations from this doc:** none.

## 10. Review log

| Date | Reviewer | Change / decision |
| --- | --- | --- |
| 2026-07-31 | peter | Created. Exported `annualized_vol_universe` so the alpha step and the signal share one volatility estimator rather than defining risk twice. Recorded the expectation that Phase 6's risk model may absorb this signal entirely. §5/§6 pending a real backfill. |
