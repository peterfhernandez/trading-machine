"""Dataset schemas for loaders (OHLCV, funding rates, open interest)."""

import polars as pl
from datastore import DatasetSchema


OHLCV_SCHEMA = DatasetSchema(
    name="ohlcv",
    fields={
        "asset_id": pl.Utf8,
        "venue": pl.Utf8,
        "timeframe": pl.Utf8,
        "event_ts": pl.Datetime("us"),
        "ingested_ts": pl.Datetime("us"),
        "open": pl.Float64,
        "high": pl.Float64,
        "low": pl.Float64,
        "close": pl.Float64,
        "volume": pl.Float64,
    },
)

FUNDING_RATE_SCHEMA = DatasetSchema(
    name="funding_rate",
    fields={
        "asset_id": pl.Utf8,
        "venue": pl.Utf8,
        "event_ts": pl.Datetime("us"),
        "ingested_ts": pl.Datetime("us"),
        "funding_rate": pl.Float64,
        "mark_price": pl.Float64,
        "index_price": pl.Float64,
    },
)

OPEN_INTEREST_SCHEMA = DatasetSchema(
    name="open_interest",
    fields={
        "asset_id": pl.Utf8,
        "venue": pl.Utf8,
        "event_ts": pl.Datetime("us"),
        "ingested_ts": pl.Datetime("us"),
        "open_interest": pl.Float64,
        "open_interest_usd": pl.Float64,
    },
)
