# Methodology: markov_mean_reversion

| Field | Value |
| --- | --- |
| Signal ID | `markov_mean_reversion` |
| Family | reversal |
| Author | |
| Created | 2026-07-28 |
| Last reviewed | 2026-07-30 |
| Status | draft |
| Status note | implemented; backtest evidence not yet collected |

## 1. Hypothesis

**Mechanism:** Crypto perpetual futures are dominated by leveraged, flow-driven
participants. Funding-rate squeezes, cascading liquidations, and momentum-chasing
retail flow regularly push price further from fair value than fundamentals
justify. This creates short-horizon overshoots that tend to revert once the
forcing flow (liquidations, margin calls) exhausts itself. A discrete-state
Markov chain is used instead of a simple z-score reversal because the *strength
and shape* of reversion is state-dependent and asymmetric: a mild pullback in a
neutral regime behaves differently from a liquidation-driven crash, and extreme
up-moves may not revert the same way extreme down-moves do. The Markov
framework lets the data express that asymmetry directly, via the estimated
transition matrix, rather than assuming a single linear reversion coefficient.

**Why it should persist (who is on the other side, and why they keep trading):**
The counterparties absorbing this flow are leveraged momentum traders and
forced sellers/buyers (liquidations, margin calls) — participants trading on
urgency or forced deleveraging rather than information about fair value. As
long as crypto perps remain a venue with high retail leverage and automatic
liquidation engines, this flow should keep recurring. The edge should decay if
open interest / leverage in the market structurally falls, or if enough
capital specifically arbitrages this pattern away.

**Prior expectation:** rank IC of roughly 0.02–0.05. Given this is a
reversal signal on a noisy, already-crowded style (mean reversion is one of
the more commonly harvested crypto factors), the humble end of that range is
the reasonable prior going in.

## 2. Data inputs

| Input | Dataset | Field(s) | Revised after publication? |
| --- | --- | --- | --- |
| Perp close price | `ohlcv_daily` | `close` | no |

The realized-volatility input named in the original draft is not a separate
input: the standardization in construction step 2 is computed from the same
`close` series, so there is nothing else to read. Funding rate and open interest
remain *candidate conditioning variables* for a later version (see Section 8,
trend/regime filter) and are deliberately not used yet — a v1 that already
conditions on three datasets cannot be attributed when it fails.

**Point-in-time contract:**

- [x] All inputs come from the context; the signal opens no files, makes no
      network calls, and reads no future-dated fields.

The signal reads `ctx.ohlcv(lookback_days=..., columns=[...])`, which returns one
long-format frame covering the whole universe, filtered to `event_ts <= asof`
(and `ingested_ts <= asof` under the default `pit_mode="ingestion"`). It reads
**once per rebalance**, not once per asset.

State cutoffs and transition matrices are estimated using only data available as
of `ctx.asof`: the matrix is a rolling, point-in-time estimate, never one matrix
fit on the whole history. Within the estimation window every derived series is
causal — the return, the z-score, the quantile cutoffs, and the state at bar `i`
depend only on bars `<= i` — so a bar that arrives later cannot change a state
already classified. This is the most likely place for accidental look-ahead in
this signal and has explicit test coverage (Section 9).

**Duplicate bars:** the store is append-only, so a backfill overlapping an
earlier run leaves two copies of the same bar. The signal keeps the latest
ingestion of each `(asset, bar)`, matching what the backtester's price panel
does.

**Minimum history:** `min_history_bars(params)` bars —
`first_state_index + matrix_lookback_days + 1`, which is **244 bars** at the
default parameters. That is the number of bars at which every bar inside the
transition lookback is itself classifiable. Assets with fewer score `None`.

**Fixed estimation window:** exactly the last `min_history_bars` bars are used
and any earlier history is discarded, so a score does not depend on how long an
asset has been listed, and every asset's score is estimated over an identically
sized window.

**Gap handling:** state classification assumes evenly spaced bars — a hole would
silently turn a 5-day return into one spanning more than five days. The signal
uses the gap-free tail of the window: gaps wider than `max_gap_days` older than
the window cost nothing, a gap inside it shortens the usable history and
therefore scores the asset `None`.

## 3. Construction

Per asset, on the fixed estimation window:

1. **Rolling return.** `r[i] = close[i] / close[i - return_horizon_days] - 1`.
2. **Standardize.** `z[i] = (r[i] - mean) / sd`, where mean and sd (sample,
   ddof=1) are taken over the trailing `state_window_days` window of returns
   **ending at i**. Requires at least `min_zscore_obs` non-null returns in that
   window and a non-degenerate sd; otherwise `z[i]` is undefined.
3. **Discretize.** Cutoffs are the `n_states - 1` interior quantiles
   (`1/k, …, (k-1)/k`, linearly interpolated) of the z-scores over the trailing
   `state_window_days` window ending at `i`. State index 0 is the most negative
   (strong-down) through `n_states - 1` (strong-up). A z-score exactly on a
   cutoff falls in the **higher** state. Requires at least `min_edge_obs`
   non-null z-scores in the window; otherwise the bar is unclassified and takes
   part in no transition.
4. **Transition matrix.** Count one-step transitions among the states in the
   trailing `matrix_lookback_days + 1` bars and normalize each row to
   probabilities. Transitions touching an unclassified bar are skipped.
5. **Expected next state.** `expected_next_z = Σ_j P(current → j) · midpoint[j]`,
   where `midpoint[j]` is the mean z observed in state `j` **over the same
   trailing window the transitions were counted on** (see "One shared window"
   below). Probabilities are renormalized over states that have a midpoint; by
   construction every reachable state has one, so this is defensive only.
6. **Raw score, then sign flip.** `raw = current_z - expected_next_z`, and
   `score = -raw`. Raw is most positive when the model expects the asset to fall,
   whereas the engine's convention is higher = more attractive long.
7. **Cross-section.** Winsorize the scores across the universe at
   `winsorize_pct` from each tail, **then** z-score them (in that order —
   clipping before computing the mean and sd keeps one blown-up score from
   dragging the location and scale of the whole cross-section).

**One shared window (design decision, 2026-07-30).** The transition matrix and
the state midpoints are both estimated over `matrix_lookback_days`. The original
draft's implementation estimated midpoints over the full trailing history while
counting transitions over the lookback, so the probabilities and the values they
weighted described two different periods. They now describe one. Combined with
the fixed estimation window this also removes the dependence on how much history
was passed in.

**Sign convention:** higher score = more attractive = larger long weight.

**Skip window:** none. Unlike momentum, a reversion signal wants the most recent
bar included — that bar is exactly what is reverting. Revisit if microstructure
noise on the most recent bar proves to be a problem.

**Cross-sectional or time-series:** primarily time-series (the state and the
transition matrix are per-asset), with a cross-sectional winsorize + z-score at
the final step for portfolio construction. Open question below: whether
transition matrices should be pooled across assets for stability.

**Units, and what the score is not.** The score is a difference in z-score space
of overlapping `return_horizon_days` returns. It is not a forecast of the next
bar's return in return units, and it should not be read as one; it is an ordering
of assets by expected reversion, which is what the rank IC measures and what
rank-based portfolio construction consumes.

## 4. Parameters

| Parameter | Value | Range to test | Why this value |
| --- | --- | --- | --- |
| `n_states` (k) | 5 (draft) | 3, 5, 7 | 5 balances nonlinearity against per-state sample size |
| `return_horizon_days` (h) | 5 (draft) | 3, 5, 10 | short-horizon overshoot is the thing being measured |
| `state_window_days` | 60 (draft) | 40, 60, 90 | enough observations to standardize and to place `k - 1` cutoffs |
| `matrix_lookback_days` | 180 (draft) | 90, 180, 250 | long enough for stable per-state transition counts |
| `min_obs_per_state` | 10 (draft) | 5, 10, 20 | minimum transitions out of the *current* state before a score is emitted; the guard for the rare-state failure mode in Section 8 |
| `max_gap_days` | 1 | not tuned | daily bars must be consecutive; a hole in the window invalidates the state series rather than merely thinning it |
| `winsorize_pct` | 2.5% (draft) | not yet tuned | standard starting point |
| `history_buffer_days` | 30 | not tuned | extra calendar days requested from the store so ordinary missing bars do not push an eligible asset under the minimum |
| `pooling` | per-asset (draft) | per-asset vs. pooled | open question — pooled is likely more stable but less asset-specific. Not implemented; per-asset only for now |

**`return_horizon_days` split from `state_window_days` (deviation from the
original draft, 2026-07-30).** The draft used one parameter for both the return
horizon and the standardization window, at 20 days. That leaves 20 observations
of a 20-day overlapping return — consecutive observations share 19 of 20 days —
to estimate a mean, a variance, and `k - 1` quantile cutoffs. There is not enough
independent information there for those estimates to mean much, and a signal
built on them would be a strong candidate for working at exactly one parameter
setting and nowhere else. They are now separate parameters, defaulting to a
5-day horizon standardized over 60 days, and the sweep below varies them
independently. The overlap issue does not disappear (a 5-day return sampled daily
still overlaps 4 days in 5) but 60 observations of it is a defensible sample.

**Parameter sensitivity:** not yet evaluated. `scratch/scratch_markov_param_grid.py`
is the tool that will populate this: it walks the grid above forward through the
sample and reports both the in-sample-selected and out-of-sample performance. If
the signal only works at one `n_states` / `matrix_lookback_days` combination,
that is evidence of overfitting, not of a working signal, and must be stated
plainly here once known.

## 5. Backtest evidence

**Not yet run.** Placeholder — do not fill with assumed or illustrative numbers.
Populate from `scratch/scratch_markov_param_grid.py` against a real backfill, and
record the run configuration below alongside the results.

**Run config:** rebalance `daily|weekly|monthly` · universe `<provider>` ·
costs `spread_bps=__, impact_bps_per_million=__` · window `YYYY-MM-DD → YYYY-MM-DD` ·
`pit_mode="ingestion"|"event"`

> A backfilled history stamps every row with one `ingested_ts`, so the sweep will
> almost certainly run in `pit_mode="event"`. Those numbers are research
> indications, not live-fidelity results, and must be labelled as such here.

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

**Out-of-sample vs. in-sample selection:** record the walk-forward OOS IR next to
the best full-sample IR. The gap between them is the overfitting tax and is the
single most informative number the sweep produces.

**Cost sensitivity:** IR at 2x assumed costs = ___. A signal that only survives
at optimistic costs does not survive.

**Decay:** does IC weaken monotonically across the sample?

## 6. Breadth check (correlation with existing signals)

**Not yet run.** No other signal is in the registry yet, so there is nothing to
correlate against; this section becomes live with the first of the Phase 5
signals. In particular, when a plain short-horizon z-score reversal signal lands,
this signal must be checked against it: a Markov-state formulation is only worth
shipping if it differs meaningfully in scores and/or returns from the simplest
version of the same idea, rather than being a more complicated way of computing
the same bet.

| Against | Score correlation | Return correlation |
| --- | --- | --- |
| | | |

## 7. Alpha refinement

Not yet computed — depends on the Section 5 IC estimate.

## 8. Known failure modes

| Failure mode | Symptom you would see | Mitigation (or "accepted") |
| --- | --- | --- |
| Trend regimes (sustained bull/bear runs) | "Extreme" states keep extending instead of reverting; signal repeatedly fades a real trend and loses | Not mitigated in v1. Candidate: condition the transition matrix on a trend/volatility regime, or gate the signal on a market-wide trend filter |
| Transition matrix estimated on too few observations for rare states (e.g. strong-down) | Noisy, overfit transition probabilities for exactly the extreme states the signal leans on hardest | Implemented: `min_obs_per_state` transitions out of the current state are required, else the score is `None` (`reject_reason="sparse_transition_row"`) |
| Regime shift in market structure (leverage caps, exchange changes) | Historical matrix no longer reflects current dynamics; live IC drifts down without warning | Rolling re-estimation (by design) + Phase 9 IC-decay monitoring |
| Liquidation cascades that keep cascading (exchange or protocol failure) | Signal buys an oversold state and the crash continues on genuinely new information, not just forced flow | Explicitly accepted. Candidate later: market-wide circuit breaker / vol filter |
| Overlapping-return autocorrelation | Quantile cutoffs and z-scores look better estimated than they are, because consecutive observations share most of their days | Accepted and stated: the horizon and the standardization window are separate parameters and both are swept, so the sensitivity is measured rather than assumed |
| Stale or wrongly-adjusted prices | State classification silently corrupted; the signal trades a data artefact | Upstream `ohlcv_daily` audit (coverage, null rate, price-jump outliers, freshness) plus the `max_gap_days` guard in this signal |

**Regimes where this is expected to lose money:** strong sustained trends
(especially post-halving bull runs or prolonged bear markets) where extremes
persist rather than mean-revert, and true-information-driven crashes rather
than leverage-driven ones.

**Data quality dependencies:** the `ohlcv_daily` freshness and price-outlier
audit checks. A stale series makes the most recent state wrong, which is the one
the signal acts on; an unadjusted price jump manufactures a fake extreme state.

## 9. Implementation notes

- **Module:** `signals/markov_mean_reversion.py`
- **Registry key:** `markov_mean_reversion`
- **Shared helpers:** `signals/transforms.py` (winsorize, cross-sectional
  z-score), `signals/panel.py` (`CachedClosePanel`, a point-in-time close panel
  that reads the store once for parameter sweeps)
- **Tests:** `tests/test_signal_markov_mean_reversion.py` — golden fixture with
  hand-computed z-scores, states, transition matrix, midpoints and score; a
  point-in-time test confirming a future bar cannot change a past state
  classification, a transition matrix, or a score; and insufficient-history /
  gap / sparse-state tests confirming `None` rather than `0.0`
- **Scratch demo:** `scratch/scratch_signal_markov_mean_reversion.py`
- **Parameter sweep:** `scratch/scratch_markov_param_grid.py`

**Public interface:**

```python
from signals import markov_mean_reversion as mmr

mmr.score_series(closes)                       # one asset, pure numpy
mmr.diagnose_series(closes)                    # + states, matrix, midpoints, reject_reason
mmr.score_universe(ctx, standardize=True)      # {asset_id: score | None}
mmr.make_signal(params)                        # ctx -> scores, for Backtester(signals=...)
```

**Deviations from this doc as originally drafted:** two, both recorded above with
rationale — `return_horizon_days` split out of `state_window_days` (Section 4),
and midpoints moved onto the transition matrix's window (Section 3). Three
parameters absent from the original table are documented in it now:
`min_obs_per_state`, `max_gap_days`, `history_buffer_days`. The `ctx.ohlcv`
signature in the draft implementation (`ctx.ohlcv(asset, field=, lookback=)`) does
not exist; the real context returns one universe-wide frame per read, and the
module is built around that.

## 10. Review log

| Date | Reviewer | Change / decision |
| --- | --- | --- |
| 2026-07-28 | | Created — hypothesis, construction, and draft parameters written; backtest evidence pending |
| 2026-07-30 | | Implemented against the real `RebalanceContext`. Split `return_horizon_days` from `state_window_days`; moved state midpoints onto the transition-matrix window; fixed the estimation window at `min_history_bars`; documented `min_obs_per_state`, `max_gap_days`, `history_buffer_days`; added the gap guard and the duplicate-bar rule. Backtest evidence still pending — sweep script written, not yet run against a real backfill |

> Factors decay. Re-review on a fixed cadence (Phase 9 attribution tracks
> per-signal IC decay) and record retirement here rather than deleting the file.
