"""Cached point-in-time close panel for parameter sweeps.

Reading `ohlcv_daily` through `RebalanceContext` at every rebalance is correct,
and it is what production does. It also re-scans parquet once per rebalance date,
which for a parameter grid — the same window walked many times over — is where
nearly all the wall-clock goes.

`CachedClosePanel` reads the store **once**, keeps the per-asset bars in memory,
and applies the same filters per `asof` that the context applies:

- `event_ts <= asof`
- `ingested_ts < asof_date + 1 day` under `pit_mode="ingestion"` (the same
  inclusive-of-the-asof-day rule `ParquetStore.read(asof=...)` uses), no
  ingestion filter under `pit_mode="event"`
- the latest surviving ingestion of each `(asset, bar)`
- the gap-free tail, per `MarkovMeanReversionParams.max_gap_days`

The filtering is per-`asof`, not done once up front, so the cache cannot leak a
later bar into an earlier decision. `tests/test_signal_markov_mean_reversion.py`
asserts the cached path and the context path return identical scores; that test
is what keeps this an optimization rather than a second implementation.
"""

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta

import numpy as np
import polars as pl

from datastore import ParquetStore

from .markov_mean_reversion import (
    DEFAULT_PARAMS,
    MarkovMeanReversionParams,
    contiguous_tail,
    min_history_bars,
)

logger = logging.getLogger(__name__)

OHLCV_DATASET = "ohlcv_daily"

# Restated rather than imported from `backtest.engine`: signals are consumed by
# the backtester, so importing it here would point the dependency backwards. The
# test suite asserts these stay identical to the engine's definitions.
PIT_INGESTION = "ingestion"
PIT_EVENT = "event"
PIT_MODES = (PIT_INGESTION, PIT_EVENT)


@dataclass(frozen=True)
class AssetBars:
    """One asset's bars, sorted by `(event_ts, ingested_ts)` ascending."""

    event_ts: np.ndarray
    ingested_ts: np.ndarray
    close: np.ndarray


class CachedClosePanel:
    """`ctx -> {asset_id: closes}` panel source backed by one store read.

    Shaped to be passed straight to `score_universe(..., panel_source=...)` or
    `make_signal(..., panel_source=...)`.
    """

    def __init__(
        self,
        store: ParquetStore,
        params: MarkovMeanReversionParams = DEFAULT_PARAMS,
        venue: str = "binance",
        timeframe: str = "1d",
        pit_mode: str = PIT_INGESTION,
        dataset: str = OHLCV_DATASET,
    ):
        """
        Args:
            store: Datastore to read once.
            params: Signal parameters (sets how much history each score needs).
            venue: Venue to keep.
            timeframe: Bar timeframe to keep.
            pit_mode: "ingestion" (strict) or "event" — must match the mode the
                backtester runs in, or the cache and the engine disagree about
                what was knowable.
            dataset: OHLCV dataset name.

        Raises:
            ValueError: If `pit_mode` is not a known mode.
        """
        if pit_mode not in PIT_MODES:
            raise ValueError(f"Unknown pit_mode {pit_mode!r}; expected one of {PIT_MODES}")

        self.params = params
        self.venue = venue
        self.timeframe = timeframe
        self.pit_mode = pit_mode
        self.bars = self._load(store, dataset)

    def _load(self, store: ParquetStore, dataset: str) -> dict[str, AssetBars]:
        """Read the whole dataset once, unfiltered by `asof`, and index it by asset."""
        try:
            frame = store.read(
                dataset,
                columns=["asset_id", "venue", "timeframe", "event_ts", "ingested_ts", "close"],
            )
        except FileNotFoundError:
            logger.warning("No %s in the store; cached panel is empty", dataset)
            return {}

        if len(frame) == 0:
            return {}

        if "venue" in frame.columns:
            frame = frame.filter(pl.col("venue") == self.venue)
        if "timeframe" in frame.columns:
            frame = frame.filter(pl.col("timeframe") == self.timeframe)
        frame = frame.filter(pl.col("close").is_not_null() & pl.col("close").is_finite())
        if len(frame) == 0:
            return {}

        frame = frame.sort(["asset_id", "event_ts", "ingested_ts"])
        bars: dict[str, AssetBars] = {}
        for part in frame.partition_by("asset_id", maintain_order=True):
            bars[part["asset_id"][0]] = AssetBars(
                event_ts=part["event_ts"].to_numpy(),
                ingested_ts=part["ingested_ts"].to_numpy(),
                close=part["close"].to_numpy().astype(float),
            )
        return bars

    def with_params(self, params: MarkovMeanReversionParams) -> "CachedClosePanel":
        """A panel over the same loaded bars, sliced for different parameters.

        A parameter sweep changes how much history each score needs but not the
        bars themselves, so the store is read once and every grid cell reuses it.
        """
        clone = object.__new__(CachedClosePanel)
        clone.params = params
        clone.venue = self.venue
        clone.timeframe = self.timeframe
        clone.pit_mode = self.pit_mode
        clone.bars = self.bars  # shared, read-only
        return clone

    def closes_asof(self, asset_id: str, asof: datetime) -> np.ndarray:
        """The gap-free tail of `asset_id`'s closes knowable at `asof`."""
        series = self.bars.get(asset_id)
        if series is None:
            return np.empty(0)

        # Same bounded window `context_close_panel` asks the context for, so an
        # asset whose bars stop well before `asof` is unscorable on both paths
        # rather than only on the slow one.
        lookback = min_history_bars(self.params) + self.params.history_buffer_days
        visible = (series.event_ts <= np.datetime64(asof, "us")) & (
            series.event_ts >= np.datetime64(asof - timedelta(days=lookback), "us")
        )
        if self.pit_mode == PIT_INGESTION:
            # ParquetStore.read(asof=...) keeps anything ingested on the asof
            # calendar day, so match that boundary exactly.
            cutoff = np.datetime64(
                datetime(asof.year, asof.month, asof.day) + timedelta(days=1), "us"
            )
            visible &= series.ingested_ts < cutoff
        if not visible.any():
            return np.empty(0)

        event_ts = series.event_ts[visible]
        close = series.close[visible]

        # Sorted by (event_ts, ingested_ts), so the last row of each event_ts run
        # is the latest ingestion that survived the filter.
        if event_ts.size > 1:
            latest = np.append(event_ts[1:] != event_ts[:-1], True)
            event_ts, close = event_ts[latest], close[latest]

        return contiguous_tail(event_ts, close, self.params)

    def __call__(self, ctx) -> dict[str, np.ndarray]:
        """Closes for every asset in `ctx.universe`, as knowable at `ctx.asof`."""
        return {asset_id: self.closes_asof(asset_id, ctx.asof) for asset_id in ctx.universe}
