"""Backfill runner: orchestrate loaders over explicit, resumable windows."""

import json
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

from config import DATASTORE_PATH, LOADER_CONFIG
from datastore import ParquetStore, AssetMaster
from loaders.ohlcv import OHLCVLoader
from loaders.funding_rate import FundingRateLoader
from loaders.open_interest import OpenInterestLoader
from loaders.window import Coverage, FetchWindow, resume_window, utc_now
from logging_config import get_logger


logger = get_logger(__name__)

DATASETS = ("ohlcv_daily", "ohlcv_hourly", "funding_rate", "open_interest")


class BackfillRunner:
    """Orchestrate backfill across all loaders with resumable checkpoints.

    Each dataset records the event-time interval it has covered. A run asks for
    a window, the checkpoint trims it to what is actually missing (keeping a
    small re-fetch overlap for the incomplete trailing bar and vendor
    revisions), and the loader is handed that window rather than a day count.
    """

    def __init__(
        self,
        venue: str = "binance",
        checkpoint_dir: Optional[Path] = None,
        store: Optional[ParquetStore] = None,
        asset_master: Optional[AssetMaster] = None,
        ignore_checkpoint: bool = False,
    ):
        self.venue = venue
        checkpoint_path = checkpoint_dir or (DATASTORE_PATH.parent / "checkpoints")
        self.checkpoint_dir = Path(checkpoint_path)
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
        self.store = store or ParquetStore(DATASTORE_PATH)
        self.asset_master = asset_master or AssetMaster(DATASTORE_PATH / "asset_master.parquet")
        self.ignore_checkpoint = ignore_checkpoint

    def _checkpoint_file(self) -> Path:
        """Get checkpoint file path for this venue."""
        return self.checkpoint_dir / f"{self.venue}_backfill.json"

    def load_checkpoint(self) -> dict:
        """Load checkpoint state. Returns dict with coverage per dataset."""
        checkpoint_file = self._checkpoint_file()
        if checkpoint_file.exists():
            try:
                with open(checkpoint_file, "r") as f:
                    data = json.load(f)
                logger.debug(f"Loaded checkpoint from {checkpoint_file}: {sorted(data)}")
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
            logger.debug(f"Saved checkpoint to {checkpoint_file}: {sorted(checkpoint)}")
        except Exception as e:
            logger.error(f"Failed to save checkpoint: {e}")

    def coverage(self, checkpoint: dict, dataset: str) -> Optional[Coverage]:
        """What `dataset` has already covered, or None to fetch everything."""
        if self.ignore_checkpoint:
            return None
        return Coverage.from_json(checkpoint.get(dataset))

    def plan(self, checkpoint: dict, dataset: str, requested: FetchWindow) -> Optional[FetchWindow]:
        """The part of `requested` still missing for `dataset` (None if nothing)."""
        window = resume_window(
            requested,
            self.coverage(checkpoint, dataset),
            overlap_days=LOADER_CONFIG.refetch_overlap_days,
        )
        if window is None:
            logger.info(f"{dataset}: already covers {requested}; nothing to fetch")
        elif window != requested:
            logger.info(f"{dataset}: resuming, {requested} narrowed to {window}")
        return window

    def _record(self, checkpoint: dict, dataset: str, window: FetchWindow) -> None:
        """Extend a dataset's recorded coverage with a window just fetched."""
        covered = self.coverage(checkpoint, dataset)
        merged = covered.merge(window) if covered else Coverage(window.start, window.end)
        checkpoint[dataset] = merged.to_json()

    def run(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        days_back: int = 1825,
    ) -> None:
        """Run backfill for all loaders.

        Args:
            start_date: Start of the window (default: `days_back` before the end)
            end_date: End of the window (default: now)
            days_back: Window length when `start_date` is not given
        """
        if end_date is None:
            end_date = utc_now()
        if start_date is None:
            start_date = end_date - timedelta(days=days_back)

        requested = FetchWindow(start=start_date, end=end_date)

        logger.info(f"Starting backfill for {self.venue}")
        logger.info(f"Requested window: {requested}")
        if self.ignore_checkpoint:
            logger.info("Ignoring checkpoint; the full window will be re-fetched")

        checkpoint = self.load_checkpoint()

        try:
            self._backfill_ohlcv(requested, checkpoint)
            self._backfill_funding_rates(requested, checkpoint)
            self._backfill_open_interest(requested, checkpoint)
            self.save_checkpoint(checkpoint)
            logger.info("Backfill completed successfully")
        except Exception as e:
            logger.error(f"Backfill failed: {e}", exc_info=True)
            self.save_checkpoint(checkpoint)
            raise

    def _backfill_ohlcv(self, requested: FetchWindow, checkpoint: dict) -> None:
        """Backfill OHLCV data (daily and hourly)."""
        logger.info("=" * 60)
        logger.info("Starting OHLCV backfill...")

        daily_window = self.plan(checkpoint, "ohlcv_daily", requested)
        hourly_window = self.plan(checkpoint, "ohlcv_hourly", requested)
        if daily_window is None and hourly_window is None:
            return

        loader = OHLCVLoader(
            self.venue,
            lookback_days=requested.days,
            store=self.store,
            asset_master=self.asset_master,
        )

        try:
            if daily_window is not None:
                # run_daily() fetches and appends in one pass. Calling fetch()
                # first to test for emptiness would pull every symbol from the
                # venue twice per run, for nothing.
                logger.info(f"Fetching daily OHLCV for {self.venue} over {daily_window}...")
                rows = loader.run_daily(window=daily_window)
                if rows > 0:
                    self._record(checkpoint, "ohlcv_daily", daily_window)
                    logger.info(f"OHLCV daily backfill completed; {rows} rows")
                else:
                    logger.warning("No OHLCV daily data fetched")

                time.sleep(1)

            if hourly_window is not None:
                logger.info(f"Fetching hourly OHLCV for {self.venue} over {hourly_window}...")
                rows_hourly = loader.run_hourly(window=hourly_window)
                if rows_hourly > 0:
                    self._record(checkpoint, "ohlcv_hourly", hourly_window)
                    logger.info(f"OHLCV hourly backfill completed; {rows_hourly} rows")
                else:
                    logger.warning("No OHLCV hourly data fetched")
        except Exception as e:
            logger.error(f"OHLCV backfill failed: {e}", exc_info=True)

    def _backfill_funding_rates(self, requested: FetchWindow, checkpoint: dict) -> None:
        """Backfill funding rates."""
        logger.info("=" * 60)
        logger.info("Starting Funding Rate backfill...")

        window = self.plan(checkpoint, "funding_rate", requested)
        if window is None:
            return

        loader = FundingRateLoader(
            self.venue,
            lookback_days=requested.days,
            store=self.store,
            asset_master=self.asset_master,
        )

        try:
            logger.info(f"Fetching funding rates for {self.venue} over {window}...")
            rows = loader.run(window=window)
            if rows > 0:
                self._record(checkpoint, "funding_rate", window)
                logger.info(f"Funding rate backfill completed; {rows} rows")
            else:
                logger.warning("No funding rate data fetched")
        except Exception as e:
            logger.error(f"Funding rate backfill failed: {e}", exc_info=True)

    def _backfill_open_interest(self, requested: FetchWindow, checkpoint: dict) -> None:
        """Backfill open interest."""
        logger.info("=" * 60)
        logger.info("Starting Open Interest backfill...")

        window = self.plan(checkpoint, "open_interest", requested)
        if window is None:
            return

        loader = OpenInterestLoader(
            self.venue,
            lookback_days=requested.days,
            store=self.store,
            asset_master=self.asset_master,
        )

        try:
            logger.info(f"Fetching open interest for {self.venue} over {window}...")
            rows = loader.run(window=window)
            if rows > 0:
                self._record(checkpoint, "open_interest", window)
                logger.info(f"Open interest backfill completed; {rows} rows")
            else:
                logger.warning("No open interest data fetched")
        except Exception as e:
            logger.error(f"Open interest backfill failed: {e}", exc_info=True)


def run_backfill(
    venue: str = "binance",
    days: int = 30,
    checkpoint_dir: Optional[Path] = None,
    ignore_checkpoint: bool = False,
) -> None:
    """Run backfill end-to-end.

    Args:
        venue: Exchange name (default: "binance")
        days: How many days of history to pull (default: 30)
        checkpoint_dir: Directory for checkpoint files (default: data/checkpoints)
        ignore_checkpoint: Re-fetch the whole window even if already covered
    """
    end_date = utc_now()
    start_date = end_date - timedelta(days=days)

    runner = BackfillRunner(venue, checkpoint_dir, ignore_checkpoint=ignore_checkpoint)
    runner.run(start_date, end_date, days_back=days)
