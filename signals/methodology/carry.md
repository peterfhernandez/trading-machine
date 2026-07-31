# Methodology: carry

| Field | Value |
| --- | --- |
| Signal ID | `carry` |
| Family | carry |
| Author | peter |
| Created | 2026-07-31 |
| Last reviewed | 2026-07-31 |
| Status | draft (implemented + tested; backtest evidence pending a real backfill) |

## 1. Hypothesis

**Mechanism:** a perpetual swap has no expiry, so the funding rate is the
mechanism that tethers it to spot: when the perp trades above spot, longs pay
shorts, and vice versa. Persistently positive funding is therefore a direct
measurement of crowded leveraged long demand — and it is a *price* paid by that
demand, not a forecast. Being long an asset whose funding is negative means being
paid to hold it; being long one whose funding is deeply positive means paying a
carry cost every eight hours before any price move. The hypothesis is that this
cost is not fully compensated by higher subsequent returns, so ranking the
universe by the negative of funding earns the spread.

**Why it should persist (who is on the other side, and why they keep trading):**
retail leveraged longs, who are price-insensitive to funding because it is small
per settlement (a few basis points) and invisible next to the leverage they are
using. They keep paying because the fee is structurally disclosed but
behaviourally ignored — the same reason people carry credit-card balances. On the
other side, the arbitrage that would flatten it (short perp / long spot) requires
spot inventory and exchange credit that a leveraged retail trader does not have.

**What would make the mechanism stop working:** cheaper basis-trade capital.
Every dollar of institutional cash-and-carry compresses the funding spread; a
mature market with deep basis desks has thin, mean-reverting funding and no
premium left to harvest. That shows up as the cross-sectional dispersion of
funding falling over time, which is directly observable.

**Prior expectation:** rank IC of roughly 0.02–0.04, and — unusually for this
project — a component of the return that is close to mechanical rather than
predictive: the funding actually gets paid. That makes it the most defensible
signal in the set and the one most likely to shrink as the market matures.

## 2. Data inputs

| Input | Dataset | Field(s) | Revised after publication? |
| --- | --- | --- | --- |
| Perpetual funding rate | `funding_rate` | `funding_rate` | no — a settled funding rate is a realized cash flow, not an estimate |

`mark_price` and `index_price` are *not* used: Binance's funding **history**
endpoint carries the rate alone (both are null on historical rows, recorded in
`AUDIT_CONFIG.nullable_columns_by_dataset`), so a signal that used them would
work on snapshot data and silently score nothing on backfilled history.

**Point-in-time contract:** reads only through `RebalanceContext`
(`signals.bars.dataset_series` → `ctx.read("funding_rate", ...)`).

- [x] All inputs come from the context; the signal opens no files, makes no
      network calls, and reads no future-dated fields.

**Minimum history:** `min_observations` = **3 funding prints** inside the
trailing `lookback_days` = 7 days, with the most recent no more than
`max_staleness_days` = 2 days old. Assets with less score `None`.

**Note on coverage:** funding exists only on perpetuals. A universe member with
no perp listing scores `None` at every rebalance, not occasionally — so this
signal's effective breadth is smaller than the universe size, and both the
breadth report and any book built on it should be read with that in mind.

## 3. Construction

1. Read the trailing `lookback_days` of `funding_rate` rows for universe assets,
   point-in-time; collapse each `(asset, event_ts)` to its latest ingestion.
   **No gap trimming** — funding settles on the venue's schedule, so a sparse
   series is sparse, not corrupted; the guards in step 2 are what protect
   against holes.
2. Keep rows with `asof - lookback_days < event_ts <= asof`. Require at least
   `min_observations`, and require `asof - max(event_ts) <= max_staleness_days`.
   A perp that stopped printing funding is not a perp with low funding, and
   without the staleness guard those two are the same number.
3. Mean funding rate per settlement over the window.
4. Raw score: `-mean_funding × fundings_per_year`, i.e. the annualized carry
   accruing to the **long** side. Annualization is monotone and cannot change
   ranks; it exists so the raw score reads as a yield.
5. Winsorize the cross-section at `winsorize_pct`, then cross-sectionally
   z-score.

**Sign convention:** higher score = more attractive = larger long weight.
Positive funding means longs pay, so positive funding must produce a *negative*
score — hence the negation in step 4. A carry signal negated twice looks like a
working momentum signal for a while, which is why the sign is pinned by an
explicit golden test rather than left to review.

**Skip window:** none. Funding is a realized cash flow at a point in time, not a
formation-window statistic, so there is no short-horizon reversal to skip past.

**Cross-sectional or time-series:** cross-sectional.

## 4. Parameters

| Parameter | Value | Range tested | Why this value |
| --- | --- | --- | --- |
| `lookback_days` | 7 | 1–30 (grid pending) | a week smooths the 8-hourly noise without averaging away a regime change; funding is persistent over days, not months |
| `min_observations` | 3 | — | one day of settlements at the standard 8-hour cadence |
| `max_staleness_days` | 2 | — | tolerates one missed venue publication; anything older is a data problem, not a low rate |
| `fundings_per_year` | 1095 | — | 3 settlements/day × 365; venue-specific and would need changing for a venue on a different cadence |
| `winsorize_pct` | 2.5 | 0–5 | shared default; matters more here than elsewhere because funding spikes to 10x its usual level during squeezes |

**Parameter sensitivity:** not yet established. `lookback_days` is the one that
matters — too short and the signal chases settlement noise, too long and it
averages across the regime change it should be reacting to. Needs the
walk-forward grid against a real backfill.

## 5. Backtest evidence

**Not yet collected.** In addition to the OHLCV backfill the other signals need,
this one needs `funding_rate` history from the venue's
`fetchFundingRateHistory` endpoint (the loaders already prefer it; the snapshot
fallback returns nothing for a historical window rather than stamping today's
value with a past timestamp, so a backfill run against a venue without the
history endpoint produces no evidence at all here).

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

**Cost sensitivity:** pending. Funding is persistent, so turnover should be low
and the signal should survive 2x costs better than the reversal signals — a
prediction to check.

**Decay:** pending, and this is the signal where decay is most expected on
theory: see §1.

## 6. Breadth check (correlation with existing signals)

| Against | Score correlation | Return correlation |
| --- | --- | --- |
| `cross_sectional_momentum` | expected mildly negative (winners attract leveraged longs, so funding rises) | |
| `time_series_momentum` | expected mildly negative | |
| `short_term_reversal` | expected positive but weak | |
| `markov_mean_reversion` | expected weak | |
| `low_volatility` | expected weak | |

**Verdict:** pending measurement. This is the signal most likely to add genuine
breadth — it is the only one in the set that does not read a price series at all,
so its errors have a different source. That is an argument for expecting low
correlation, not a substitute for measuring it.

## 7. Alpha refinement

- **IC estimate used:** `IcEstimate.shrunk_ic` — pending.
- **Shrinkage applied:** normal-normal, `prior_ic_std = 0.02`, cap 0.10.
- **Volatility estimate:** `low_volatility.annualized_vol_universe`.
- **Resulting alpha scale:** pending. Worth noting that the raw score is already
  in return units (an annualized yield), so this is the one signal where the raw
  number can be sanity-checked directly against the alpha it produces.

## 8. Known failure modes

| Failure mode | Symptom you would see | Mitigation (or "accepted") |
| --- | --- | --- |
| Funding spike during a squeeze | the signal goes maximally short the asset that is squeezing, right before the liquidation cascade continues | partly mitigated by winsorization and the 7-day average; fundamentally accepted — this is the risk the carry pays for |
| Venue stops publishing funding for an asset | a stale rate would look like a stable low rate | mitigated: `max_staleness_days` scores it `None` |
| Delisting in progress | funding goes wild in the final days of a perp's life | not mitigated here; the universe's liquidity filter is the first line of defence |
| Universe member with no perp | permanently `None`, so breadth is quietly smaller than the universe size | accepted and documented (§2); the breadth report shows the scored count |
| Venue on a non-8-hour funding cadence | `fundings_per_year` is wrong, so the annualization is wrong | ranks are unaffected (monotone rescaling), alpha levels are not — must be fixed before a second venue is added |

**Regimes where this is expected to lose money:** violent directional squeezes,
where the crowded side keeps winning for long enough to overwhelm the carry; and
a maturing market where basis desks compress the spread to nothing.

**Data quality dependencies:** the `coverage` and `freshness` checks on
`funding_rate` — coverage because this signal is the only consumer of that
dataset and a silent drop in perp coverage shrinks its breadth invisibly, and
freshness because the staleness guard turns stale data into `None` rather than
into a wrong score, which is the right behaviour but shows up as breadth
disappearing rather than as an error.

## 9. Implementation notes

- **Module:** `signals/carry.py`
- **Registry key:** `carry`
- **Tests:** `tests/test_signals_phase5.py` — golden hand-computed annualized
  carry **including an explicit sign assertion**, the staleness and
  observation-count reject paths, point-in-time through a real store.
- **Scratch demo:** `scratch/scratch_signals_phase5.py`

**Deviations from this doc:** none.

## 10. Review log

| Date | Reviewer | Change / decision |
| --- | --- | --- |
| 2026-07-31 | peter | Created. Decided against using `mark_price`/`index_price` (null on Binance funding history — would have worked on snapshots and silently failed on backfills). Added the staleness guard after noting that a stale rate and a low rate are indistinguishable without it. §5/§6 pending a real backfill. |
