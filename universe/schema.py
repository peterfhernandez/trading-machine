"""Dataset schema for universe membership."""

import polars as pl

from datastore import DatasetSchema


UNIVERSE_SCHEMA = DatasetSchema(
    name="universe",
    fields={
        "asset_id": pl.Utf8,
        "venue": pl.Utf8,
        "event_ts": pl.Datetime("us"),
        "ingested_ts": pl.Datetime("us"),
        "in_universe": pl.Boolean,
        "dollar_volume_median": pl.Float64,
        "listing_age_days": pl.Int64,
        "rank": pl.Int64,
        "exclusion_reason": pl.Utf8,
    },
)
