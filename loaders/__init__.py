"""Loaders package: fetch data from venues and ingest into datastore."""

from loaders.base import BaseLoader
from loaders.schemas import OHLCV_SCHEMA, FUNDING_RATE_SCHEMA, OPEN_INTEREST_SCHEMA
from loaders.ohlcv import OHLCVLoader, load_ohlcv

__all__ = [
    "BaseLoader",
    "OHLCVLoader",
    "load_ohlcv",
    "OHLCV_SCHEMA",
    "FUNDING_RATE_SCHEMA",
    "OPEN_INTEREST_SCHEMA",
]
