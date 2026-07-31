"""Collapsing repeated ingestions of the same bar, at the read boundary.

The store is append-only: a bar the loaders fetch twice is stored twice, under
two `ingested_ts` values, and neither copy is ever deleted. That is the point —
it is what lets a backtest ask what a bar looked like *before* a vendor revised
it, and it is why the duplicate can never be fixed by writing more carefully.
Overlapping windows, a crashed run retried, and genuine vendor revisions all
produce the same shape on disk.

So duplicates are a *read-side* concern: every consumer that aggregates rows
(a median, a null rate, a count) has to decide which copy of a bar it means.
For all current consumers the answer is the same — the latest thing we knew
about that bar as of the read — which is what `latest_per_bar` returns.
"""

from collections.abc import Sequence

import polars as pl

DEFAULT_BAR_KEYS: tuple[str, ...] = ("asset_id", "event_ts")


def latest_per_bar(
    df: pl.DataFrame,
    keys: Sequence[str] = DEFAULT_BAR_KEYS,
    ingested_col: str = "ingested_ts",
    sort_by: Sequence[str] | None = None,
) -> pl.DataFrame:
    """Keep only the most recently ingested row for each bar.

    A "bar" is identified by `keys` — `(asset_id, event_ts)` by default, which
    is the grain of every dataset the loaders write (each timeframe is its own
    dataset, so the timeframe is not part of the key).

    Point-in-time note: this must run *after* any `ingested_ts <= asof` filter,
    never before. Collapsing first would let a bar's later revision decide which
    row survives, and then the asof filter would drop it — silently hiding a bar
    that was knowable at `asof`. Callers that filter in memory (the cached
    signal panel) therefore do their own per-`asof` collapse instead of calling
    this.

    Args:
        df: Frame straight from the store (or already filtered by venue/asof)
        keys: Columns identifying one bar
        ingested_col: Knowledge-date column; the maximum wins
        sort_by: Ordering for the result (default: `keys`). Polars' group_by does
            not guarantee output order, so this is what makes the result stable.

    Returns:
        A frame with one row per bar. Returns `df` unchanged when it is empty or
        does not carry the columns to deduplicate on.
    """
    if len(df) == 0:
        return df

    keys = list(keys)
    if ingested_col not in df.columns or any(k not in df.columns for k in keys):
        return df

    ordering = list(sort_by) if sort_by is not None else keys
    ordering = [c for c in ordering if c in df.columns]

    collapsed = df.sort(ingested_col).group_by(keys).last()
    return collapsed.sort(ordering) if ordering else collapsed


def count_duplicate_bars(
    df: pl.DataFrame,
    keys: Sequence[str] = DEFAULT_BAR_KEYS,
) -> int:
    """How many rows `latest_per_bar` would drop — 0 when every bar is unique."""
    if len(df) == 0:
        return 0

    keys = list(keys)
    if any(k not in df.columns for k in keys):
        return 0

    return len(df) - df.select(keys).unique().height
