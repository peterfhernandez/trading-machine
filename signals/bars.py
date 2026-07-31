"""Shared point-in-time series reading and per-asset primitives for signals.

Every signal after `markov_mean_reversion` needs the same two things: a
point-in-time series per asset (read through `RebalanceContext`, latest
ingestion per bar, gap-free tail) and a handful of numeric primitives over that
series (a trailing return, a realized volatility). Repeating that per signal
would mean five chances to get the point-in-time boundary subtly wrong, so it
lives here once.

**What this module guarantees.** Reads go through `ctx.read`/`ctx.ohlcv`, so
`event_ts <= asof` always holds and `ingested_ts <= asof` holds under the
default `pit_mode="ingestion"`. Rows are then collapsed to the latest ingestion
per `(asset_id, event_ts)` — append-only storage means a re-fetched bar is
stored twice — and trimmed to the most recent gap-free stretch, because every
primitive below assumes evenly spaced bars. Collapse happens **after** the
point-in-time filter (the context has already applied it), never before; see
`datastore.dedupe` for why the order matters.

**What it does not do.** No cross-sectional work (that is
`signals.transforms`), and no caching (that is `signals.panel`). Nothing here
opens a file or makes a network call.
"""

from collections.abc import Sequence
from dataclasses import dataclass

import numpy as np
import polars as pl

from datastore import latest_per_bar

OHLCV_DATASET = "ohlcv_daily"

_EPS = 1e-12


@dataclass(frozen=True)
class Series:
    """One asset's point-in-time series: ascending bars, one array per column.

    Attributes:
        event_ts: Bar timestamps, ascending, gap-free within `max_gap_days`.
        columns: `{column name: values aligned to event_ts}`.
    """

    event_ts: np.ndarray
    columns: dict[str, np.ndarray]

    def __len__(self) -> int:
        return int(self.event_ts.size)

    def column(self, name: str) -> np.ndarray:
        """One column's values, or an empty array if the column is absent."""
        return self.columns.get(name, np.empty(0))


def gap_free_start(
    event_ts: np.ndarray, max_gap_days: int, max_bars: int | None = None
) -> int:
    """Index where the most recent gap-free stretch of `event_ts` begins.

    Every primitive in this module treats consecutive array entries as
    consecutive bars, so a hole in the history silently turns a "5-day return"
    into something longer. Trimming to `max_bars` first means an *old* gap costs
    an asset nothing while a recent one drops it.

    Args:
        event_ts: Ascending bar timestamps.
        max_gap_days: Largest permitted spacing between consecutive bars.
        max_bars: Keep at most this many trailing bars before looking for gaps.

    Returns:
        Start index into `event_ts` (0 when the whole series is usable).
    """
    if event_ts.size == 0:
        return 0

    start = 0 if max_bars is None else max(0, event_ts.size - max_bars)
    stamps = np.asarray(event_ts[start:], dtype="datetime64[s]")
    limit = np.timedelta64(int(max_gap_days) * 86_400, "s")
    gaps = np.flatnonzero(np.diff(stamps) > limit)
    if gaps.size:
        start += int(gaps[-1]) + 1
    return start


def dataset_series(
    ctx,
    dataset: str,
    columns: Sequence[str],
    lookback_days: int,
    max_gap_days: int | None = None,
    max_bars: int | None = None,
) -> dict[str, Series]:
    """Point-in-time series per universe asset, read through `ctx`.

    Args:
        ctx: Rebalance context (supplies `asof`, `universe`, and the reads).
        dataset: Dataset name in the store (e.g. "funding_rate").
        columns: Value columns to return; rows with a null or non-finite value
            in **any** of them are dropped, so the returned arrays are aligned
            and finite.
        lookback_days: Calendar days of history to request.
        max_gap_days: Trim to the most recent stretch with no larger gap; None
            keeps the series as read (correct for irregularly-timed datasets
            such as funding, where "gaps" are the venue's schedule, not a hole).
        max_bars: Keep at most this many trailing bars.

    Returns:
        `{asset_id: Series}`, only for assets with at least one usable row.
    """
    read_columns = ["asset_id", "event_ts", "ingested_ts", *columns]
    frame = ctx.read(dataset, columns=read_columns, lookback_days=lookback_days)
    if len(frame) == 0:
        return {}

    universe = set(ctx.universe)
    frame = frame.filter(pl.col("asset_id").is_in(list(universe)))
    for column in columns:
        frame = frame.filter(pl.col(column).is_not_null() & pl.col(column).is_finite())
    if len(frame) == 0:
        return {}

    # Append-only storage keeps every ingestion of a bar; aggregate the latest.
    frame = latest_per_bar(frame, sort_by=["asset_id", "event_ts"])
    return _partition(frame, columns, max_gap_days, max_bars)


def close_series(
    ctx,
    lookback_days: int,
    columns: Sequence[str] = ("close",),
    max_gap_days: int = 1,
    max_bars: int | None = None,
) -> dict[str, Series]:
    """Point-in-time daily bars per universe asset (`ohlcv_daily`, via `ctx.ohlcv`).

    `ctx.ohlcv` already restricts to the context's venue, timeframe and
    universe; this adds the finite-value filter, the latest-ingestion collapse
    and the gap-free trim that every price-based signal needs.
    """
    read_columns = ["asset_id", "venue", "timeframe", "event_ts", "ingested_ts", *columns]
    frame = ctx.ohlcv(lookback_days=lookback_days, columns=read_columns)
    if len(frame) == 0:
        return {}

    for column in columns:
        frame = frame.filter(pl.col(column).is_not_null() & pl.col(column).is_finite())
    if len(frame) == 0:
        return {}

    frame = latest_per_bar(frame, sort_by=["asset_id", "event_ts"])
    return _partition(frame, columns, max_gap_days, max_bars)


def _partition(
    frame: pl.DataFrame,
    columns: Sequence[str],
    max_gap_days: int | None,
    max_bars: int | None,
) -> dict[str, Series]:
    """Split a collapsed frame into one `Series` per asset, trimmed to the tail."""
    frame = frame.sort(["asset_id", "event_ts"])
    out: dict[str, Series] = {}
    for part in frame.partition_by("asset_id", maintain_order=True):
        event_ts = part["event_ts"].to_numpy()
        if max_gap_days is None:
            start = 0 if max_bars is None else max(0, event_ts.size - max_bars)
        else:
            start = gap_free_start(event_ts, max_gap_days, max_bars)
        out[part["asset_id"][0]] = Series(
            event_ts=event_ts[start:],
            columns={c: part[c].to_numpy().astype(float)[start:] for c in columns},
        )
    return out


# ---------------------------------------------------------------------------
# Per-asset primitives (pure numpy; no I/O, no context)
# ---------------------------------------------------------------------------


def trailing_return(closes: np.ndarray, lookback: int, skip: int = 0) -> float:
    """Simple return over `lookback` bars ending `skip` bars before the last one.

    With `skip=0` this is `closes[-1] / closes[-1 - lookback] - 1`. `skip`
    excludes the most recent bars from the formation window — the momentum
    convention, which keeps the short-horizon reversal that lives in the last
    few days out of a signal that is trying to measure a trend.

    Returns NaN if the window does not fit or the base price is not positive.
    """
    end = closes.size - 1 - skip
    start = end - lookback
    if start < 0 or end < 0 or end >= closes.size:
        return float("nan")
    base = closes[start]
    if not np.isfinite(base) or base <= 0.0:
        return float("nan")
    value = closes[end] / base - 1.0
    return float(value) if np.isfinite(value) else float("nan")


def log_returns(closes: np.ndarray) -> np.ndarray:
    """Bar-over-bar log returns; non-positive prices produce NaN.

    Log returns are used for every volatility estimate in `signals/` (simple
    returns are used for the signals' own return measures). Volatility is a
    scale parameter and log returns are the additive quantity whose standard
    deviation scales with `sqrt(horizon)`, which is exactly what the
    `sqrt(horizon)` factors elsewhere in this module assume.
    """
    if closes.size < 2:
        return np.empty(0)
    prior, current = closes[:-1], closes[1:]
    with np.errstate(divide="ignore", invalid="ignore"):
        ratio = np.where((prior > 0.0) & (current > 0.0), current / prior, np.nan)
        return np.where(np.isfinite(ratio) & (ratio > 0.0), np.log(ratio), np.nan)


def realized_vol(
    closes: np.ndarray, window: int, min_observations: int | None = None
) -> float:
    """Per-bar standard deviation (ddof=1) of the trailing `window` log returns.

    Args:
        closes: Ascending close series.
        window: Number of trailing log returns to use.
        min_observations: Finite returns required; defaults to `window`.

    Returns:
        Per-bar volatility, or NaN when there are too few observations or the
        dispersion is degenerate (a constant price series is not a zero-risk
        asset, it is an unpriced one).
    """
    needed = window if min_observations is None else min_observations
    returns = log_returns(closes)
    if returns.size == 0:
        return float("nan")

    tail = returns[-window:] if returns.size > window else returns
    finite = tail[np.isfinite(tail)]
    if finite.size < max(2, needed):
        return float("nan")

    vol = float(finite.std(ddof=1))
    return vol if np.isfinite(vol) and vol > _EPS else float("nan")
