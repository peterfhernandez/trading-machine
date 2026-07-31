"""Backfill runner: orchestrate loaders with resumable checkpoint tracking."""

import json
import logging
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

from config import DATASTORE_PATH, LOADER_CONFIG
from datastore import ParquetStore, AssetMaster
from loaders.ohlcv import OHLCVLoader
from loaders.funding_rate import FundingRateLoader
from loaders.open_interest import OpenInterestLoader


logger = logging.getLogger(__name__)


class BackfillRunner:
    """Orchestrate backfill across all loaders with checkpoint persistence."""

    def __init__(
        self,
        venue: str = "binance",
        checkpoint_dir: Optional[Path] = None,
        store: Optional[ParquetStore] = None,
        asset_master: Optional[AssetMaster] = None,
    ):
        self.venue = venue
        checkpoint_path = checkpoint_dir or (DATASTORE_PATH.parent / "checkpoints")
        self.checkpoint_dir = Path(checkpoint_path)
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
        self.store = store or ParquetStore(DATASTORE_PATH)
        self.asset_master = asset_master or AssetMaster(DATASTORE_PATH / "asset_master.parquet")

    def _checkpoint_file(self) -> Path:
        """Get checkpoint file path for this venue."""
        return self.checkpoint_dir / f"{self.venue}_backfill.json"

    def load_checkpoint(self) -> dict:
        """Load checkpoint state. Returns dict with last_ingested_date per loader."""
        checkpoint_file = self._checkpoint_file()
        if checkpoint_file.exists():
            try:
                with open(checkpoint_file, "r") as f:
                    data = json.load(f)
                logger.info(f"Loaded checkpoint from {checkpoint_file}")
                return data
            except Exception as e:
                logger.warning(f"Failed to load checkpoint: {e}; starting fresh")
        return {}

    def save_checkpoint(self, checkpoint: dict) -> None:
        """Save checkpoint state."""
        checkpoint_file = self._checkpoint_file()
        try:
            with open(checkpoint_file, "w") as f:
                json.dump(checkpoint, f, indent=2)
            logger.info(f"Saved checkpoint to {checkpoint_file}")
        except Exception as e:
            logger.error(f"Failed to save checkpoint: {e}")

    def run(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        days_back: int = 1825,
    ) -> None:
        """Run backfill for all loaders.

        Args:
            start_date: Start date for backfill (default: days_back ago)
            end_date: End date for backfill (default: today)
            days_back: How many days of history to pull (default: 5 years)
        """
        if end_date is None:
            end_date = datetime.now(timezone.utc).replace(tzinfo=None)
        if start_date is None:
            start_date = end_date - timedelta(days=days_back)

        logger.info(f"Starting backfill for {self.venue}")
        logger.info(f"Date range: {start_date.date()} to {end_date.date()}")

        checkpoint = self.load_checkpoint()

        try:
            self._backfill_ohlcv(start_date, end_date, checkpoint)
            self._backfill_funding_rates(start_date, end_date, checkpoint)
            self._backfill_open_interest(start_date, end_date, checkpoint)
            self.save_checkpoint(checkpoint)
            logger.info("Backfill completed successfully")
        except Exception as e:
            logger.error(f"Backfill failed: {e}", exc_info=True)
            self.save_checkpoint(checkpoint)
            raise

    def _backfill_ohlcv(self, start_date: datetime, end_date: datetime, checkpoint: dict) -> None:
        """Backfill OHLCV data."""
        logger.info("=" * 60)
        logger.info("Starting OHLCV backfill...")

        lookback_days = (end_date - start_date).days
        loader = OHLCVLoader(
            self.venue,
            lookback_days=lookback_days,
            store=self.store,
            asset_master=self.asset_master,
        )

        try:
            # run_daily() fetches and appends in one pass. Calling fetch() first
            # to test for emptiness would pull every symbol from the venue twice
            # per run, for nothing -- the row count comes back from the run.
            logger.info(f"Fetching daily OHLCV for {self.venue}...")
            rows = loader.run_daily()
            if rows > 0:
                checkpoint["ohlcv_daily"] = end_date.isoformat()
                logger.info(f"OHLCV daily backfill completed; {rows} rows")
            else:
                logger.warning("No OHLCV daily data fetched")

            time.sleep(1)

            logger.info(f"Fetching hourly OHLCV for {self.venue}...")
            rows_hourly = loader.run_hourly()
            if rows_hourly > 0:
                checkpoint["ohlcv_hourly"] = end_date.isoformat()
                logger.info(f"OHLCV hourly backfill completed; {rows_hourly} rows")
            else:
                logger.warning("No OHLCV hourly data fetched")
        except Exception as e:
            logger.error(f"OHLCV backfill failed: {e}")

    def _backfill_funding_rates(self, start_date: datetime, end_date: datetime, checkpoint: dict) -> None:
        """Backfill funding rates."""
        logger.info("=" * 60)
        logger.info("Starting Funding Rate backfill...")

        lookback_days = (end_date - start_date).days
        loader = FundingRateLoader(
            self.venue,
            lookback_days=lookback_days,
            store=self.store,
            asset_master=self.asset_master,
        )

        try:
            logger.info(f"Fetching funding rates for {self.venue}...")
            rows = loader.run()
            if rows > 0:
                checkpoint["funding_rate"] = end_date.isoformat()
                logger.info(f"Funding rate backfill completed; {rows} rows")
            else:
                logger.warning("No funding rate data fetched")
        except Exception as e:
            logger.error(f"Funding rate backfill failed: {e}")

    def _backfill_open_interest(self, start_date: datetime, end_date: datetime, checkpoint: dict) -> None:
        """Backfill open interest."""
        logger.info("=" * 60)
        logger.info("Starting Open Interest backfill...")

        lookback_days = (end_date - start_date).days
        loader = OpenInterestLoader(
            self.venue,
            lookback_days=lookback_days,
            store=self.store,
            asset_master=self.asset_master,
        )

        try:
            logger.info(f"Fetching open interest for {self.venue}...")
            rows = loader.run()
            if rows > 0:
                checkpoint["open_interest"] = end_date.isoformat()
                logger.info(f"Open interest backfill completed; {rows} rows")
            else:
                logger.warning("No open interest data fetched")
        except Exception as e:
            logger.error(f"Open interest backfill failed: {e}")


def run_backfill(
    venue: str = "binance",
    days: int = 30,
    checkpoint_dir: Optional[Path] = None,
) -> None:
    """Run backfill end-to-end.

    Args:
        venue: Exchange name (default: "binance")
        days: How many days of history to pull (default: 30)
        checkpoint_dir: Directory for checkpoint files (default: data/checkpoints)
    """
    end_date = datetime.now(timezone.utc).replace(tzinfo=None)
    start_date = end_date - timedelta(days=days)

    runner = BackfillRunner(venue, checkpoint_dir)
    runner.run(start_date, end_date, days_back=days)
