# Methodology: <signal_name>

> Copy this file to `signals/methodology/<signal_name>.md` and fill it in
> **before** writing `signals/<signal_name>.py`. The doc is the spec; the code
> follows the doc. If the code ends up disagreeing with the doc, the doc is
> wrong until it is updated — fix it in the same change.
>
> Delete these blockquote instructions as you fill each section in.

| Field | Value |
| --- | --- |
| Signal ID | `<signal_name>` (must match the registry key and module name) |
| Family | momentum / carry / reversal / volatility / value / other |
| Author | |
| Created | YYYY-MM-DD |
| Last reviewed | YYYY-MM-DD |
| Status | draft / backtested / live-paper / retired |

## 1. Hypothesis

> One paragraph, in plain English, stating *why* this should predict returns —
> the economic or behavioural mechanism, not the arithmetic. "Prices trend
> because flows are slow" is a hypothesis; "the 30-day return predicts the next
> 30-day return" is a restatement of the formula.
>
> Then state what would make the mechanism stop working. A signal whose failure
> conditions you cannot name is a signal you cannot size.

**Mechanism:**

**Why it should persist (who is on the other side, and why they keep trading):**

**Prior expectation:** rank IC of roughly ___ (be humble; 0.02–0.05 is a real
crypto signal, 0.20 is a bug or look-ahead).

## 2. Data inputs

> Every input, with the dataset it comes from and how stale it can be at
> decision time. If a field is revised by the venue after publication, say so —
> that is the difference between a strict-mode and event-mode backtest.

| Input | Dataset | Field(s) | Revised after publication? |
| --- | --- | --- | --- |
| | `ohlcv_daily` | `close` | no |

**Point-in-time contract:** the signal reads only through `RebalanceContext`
(`ctx.ohlcv(...)`, `ctx.read(...)`), so `event_ts <= asof` always holds and
`ingested_ts <= asof` holds under the default `pit_mode="ingestion"`. Confirm
here that the signal needs no other data source:

- [ ] All inputs come from the context; the signal opens no files, makes no
      network calls, and reads no future-dated fields.

**Minimum history:** ___ bars. Assets with less are scored `None` (excluded from
the cross-section), never `0.0` — a missing score is not a neutral view.

## 3. Construction

> The exact recipe, in enough detail that someone could reimplement it from this
> section alone and get identical numbers. State the order of operations —
> winsorize-then-z-score and z-score-then-winsorize are different signals.

1. Compute ...
2. Winsorize at ...
3. Cross-sectionally z-score within the universe ...

**Sign convention:** higher score = more attractive = larger long weight.
(The engine's IC series assumes this; a signal that predicts with the wrong sign
should be negated here, not "read backwards" downstream.)

**Skip window:** ___ (e.g. momentum usually skips the most recent bar to avoid
contaminating the signal with short-term reversal).

**Cross-sectional or time-series:** ___

## 4. Parameters

| Parameter | Value | Range tested | Why this value |
| --- | --- | --- | --- |
| `lookback_days` | | | |
| `skip_days` | | | |
| `winsorize_pct` | | | |

**Parameter sensitivity:** state whether performance survives across the tested
range. A signal that only works at one lookback is an overfit, not a signal —
say so plainly here rather than shipping it quietly.

## 5. Backtest evidence

> Fill from a `Backtester` run. Record the run configuration, not just the
> outcome, so the numbers can be reproduced.

**Run config:** rebalance `daily|weekly|monthly` · universe `<provider>` ·
costs `spread_bps=__, impact_bps_per_million=__` · window `YYYY-MM-DD → YYYY-MM-DD` ·
`pit_mode="ingestion"|"event"`

> If `pit_mode="event"`, say why (almost always: the history is backfilled, so
> every row shares one `ingested_ts`). Event-mode numbers are research
> indications, not live-fidelity results — label them as such here.

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

**Cost sensitivity:** IR at 2x assumed costs = ___. A signal that only survives
at optimistic costs does not survive.

**Decay:** does IC weaken monotonically across the sample? Note the trend and
the horizon at which the signal stops paying.

## 6. Breadth check (correlation with existing signals)

> The fundamental law rewards *independent* bets. Five correlated signals are
> one bet with extra steps. Report the rank correlation of this signal's scores
> against every signal already in the registry.

| Against | Score correlation | Return correlation |
| --- | --- | --- |
| | | |

**Verdict:** does this add breadth, or is it a rotation of something we already
have? If correlation with an existing signal exceeds ~0.7, justify keeping both
or retire one.

## 7. Alpha refinement

Per Grinold–Kahn, `alpha = volatility × IC × z`:

- **IC estimate used:** ___ (from the backtest IC series)
- **Shrinkage applied:** ___ (shrink hard toward zero; in-sample IC is optimistic)
- **Volatility estimate:** ___
- **Resulting alpha scale:** ___

## 8. Known failure modes

> Be specific and concrete. "Might not work in all regimes" is not a failure
> mode. "Whipsaws in the first week after a market-wide liquidation, because the
> lookback still contains the crash" is.

| Failure mode | Symptom you would see | Mitigation (or "accepted") |
| --- | --- | --- |
| | | |

**Regimes where this is expected to lose money:**

**Data quality dependencies:** which audit check failing would silently corrupt
this signal?

## 9. Implementation notes

- **Module:** `signals/<signal_name>.py`
- **Registry key:** `<signal_name>`
- **Tests:** `tests/test_signal_<signal_name>.py` — must include a golden fixture
  with hand-computed scores, a point-in-time test (no future bar changes a past
  score), and a test that assets with insufficient history score `None`.
- **Scratch demo:** `scratch/scratch_signal_<signal_name>.py`

**Deviations from this doc:** none / list them and why.

## 10. Review log

| Date | Reviewer | Change / decision |
| --- | --- | --- |
| YYYY-MM-DD | | Created |

> Factors decay. Re-review on a fixed cadence (Phase 9 attribution tracks
> per-signal IC decay) and record retirement here rather than deleting the file
> — a retired signal's doc is the record of what stopped working and when.
