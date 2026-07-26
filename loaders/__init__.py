"""Loaders package: fetch data from venues and ingest into datastore."""

from loaders.base import BaseLoader
from loaders.schemas import OHLCV_SCHEMA, FUNDING_RATE_SCHEMA, OPEN_INTEREST_SCHEMA
from loaders.ohlcv import OHLCVLoader, load_ohlcv
from loaders.funding_rate import FundingRateLoader, load_funding_rates
from loaders.open_interest import OpenInterestLoader, load_open_interest

__all__ = [
    "BaseLoader",
    "OHLCVLoader",
    "load_ohlcv",
    "FundingRateLoader",
    "load_funding_rates",
    "OpenInterestLoader",
    "load_open_interest",
    "OHLCV_SCHEMA",
    "FUNDING_RATE_SCHEMA",
    "OPEN_INTEREST_SCHEMA",
]
