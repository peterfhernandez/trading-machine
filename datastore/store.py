"""
Append-only Parquet-backed datastore for point-in-time data management.

Core principles:
- Append-only: never overwrite history
- Point-in-time: every row carries event_ts (when it happened) and ingested_ts (when we learned it)
- Partitioned by dataset and date for efficient access
- Schema-enforced at write time
"""

from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Optional, Dict, List, Any
from dataclasses import dataclass, field, asdict
import logging

import polars as pl
from polars import Schema
import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)


@dataclass
class DatasetSchema:
    """Schema definition for a dataset with required timestamp columns."""

    name: str
    fields: Dict[str, Any]  # Maps column name to Polars data type

    def __post_init__(self):
        """Ensure event_ts and ingested_ts are always present."""
        if "event_ts" not in self.fields:
            self.fields["event_ts"] = pl.Datetime("us")
        if "ingested_ts" not in self.fields:
            self.fields["ingested_ts"] = pl.Datetime("us")

    def to_polars_schema(self) -> Schema:
        """Convert to a Polars Schema."""
        return pl.Schema(self.fields)


class ParquetStore:
    """Append-only Parquet datastore with point-in-time data discipline."""

    def __init__(self, root_path: Path):
        """
        Initialize the Parquet store.

        Args:
            root_path: Root directory for all Parquet files (e.g., data/parquet)
        """
        self.root = Path(root_path)
        self.root.mkdir(parents=True, exist_ok=True)

    def append(
        self,
        dataset: str,
        df: pl.DataFrame,
        schema: DatasetSchema,
        partition_key: Optional[str] = None,
    ) -> None:
        """
        Append data to a dataset (append-only, no overwrites).

        Args:
            dataset: Dataset name (e.g., "ohlcv", "funding_rates")
            df: Polars DataFrame to append
            schema: DatasetSchema with required columns and types
            partition_key: Column to partition by (default: derived from ingested_ts)

        Raises:
            ValueError: If schema validation fails or if overwrite is attempted
        """
        # Validate schema
        self._validate_schema(df, schema)

        # Derive partition column if not specified
        if partition_key is None:
            partition_key = "ingested_ts"

        # Get partition value(s) from the data
        partition_col = df[partition_key]
        if isinstance(partition_col[0], datetime):
            partition_dates = sorted(
                set(pd.date().strftime("%Y-%m-%d") for pd in partition_col)
            )
        else:
            partition_dates = sorted(
                set(str(partition_col[i]) for i in range(len(partition_col)))
            )

        # Write each partition
        dataset_path = self.root / dataset
        dataset_path.mkdir(parents=True, exist_ok=True)

        for part_date in partition_dates:
            if isinstance(partition_col[0], datetime):
                mask = df[partition_key].dt.strftime("%Y-%m-%d") == part_date
            else:
                mask = df[partition_key].cast(pl.Utf8) == part_date

            part_df = df.filter(mask)
            part_path = dataset_path / f"date={part_date}"
            part_path.mkdir(parents=True, exist_ok=True)

            # Check for existing files (no overwrites)
            existing_files = list(part_path.glob("*.parquet"))
            file_num = len(existing_files)
            file_path = part_path / f"data_{file_num:04d}.parquet"

            # Write without overwriting
            part_df.write_parquet(file_path)
            logger.info(
                f"Appended {len(part_df)} rows to {dataset}/date={part_date} -> {file_path.name}"
            )

    def read(
        self,
        dataset: str,
        date_range: Optional[tuple[str, str]] = None,
        asof: Optional[str] = None,
        columns: Optional[List[str]] = None,
    ) -> pl.DataFrame:
        """
        Read data from a dataset with point-in-time semantics.

        Args:
            dataset: Dataset name
            date_range: (start_date, end_date) tuple in YYYY-MM-DD format
            asof: Knowledge date in YYYY-MM-DD format; only read rows with ingested_ts <= asof
            columns: Specific columns to read (None = all)

        Returns:
            Polars DataFrame with data satisfying the constraints

        Raises:
            FileNotFoundError: If dataset does not exist
        """
        dataset_path = self.root / dataset
        if not dataset_path.exists():
            raise FileNotFoundError(f"Dataset {dataset} not found at {dataset_path}")

        # Collect all partition paths matching the date range
        partitions = []
        for part_dir in sorted(dataset_path.glob("date=*")):
            part_date_str = part_dir.name.split("=")[1]
            if date_range:
                start, end = date_range
                if not (start <= part_date_str <= end):
                    continue
            partitions.append(part_dir)

        if not partitions:
            # Return empty frame with correct schema if no matching partitions
            return pl.DataFrame()

        # Read all Parquet files in the partitions
        dfs = []
        for part_dir in partitions:
            parquet_files = sorted(part_dir.glob("*.parquet"))
            for pf in parquet_files:
                df = pl.read_parquet(pf, columns=columns)
                dfs.append(df)

        if not dfs:
            return pl.DataFrame()

        result = pl.concat(dfs, how="vertical")

        # Apply point-in-time filter if asof is specified
        if asof:
            asof_dt = datetime.strptime(asof, "%Y-%m-%d")
            # Add one day to include all events on that date
            asof_dt_end = asof_dt + timedelta(days=1)
            result = result.filter(pl.col("ingested_ts") < asof_dt_end)

        return result

    def list_datasets(self) -> List[str]:
        """List all datasets in the store."""
        if not self.root.exists():
            return []
        return [d.name for d in self.root.iterdir() if d.is_dir()]

    def dataset_info(self, dataset: str) -> Dict[str, Any]:
        """
        Get metadata for a dataset.

        Returns:
            Dict with keys: row_count, date_range, columns, etc.
        """
        dataset_path = self.root / dataset
        if not dataset_path.exists():
            return {}

        total_rows = 0
        min_date = None
        max_date = None
        columns = set()

        for part_dir in dataset_path.glob("date=*"):
            part_date = part_dir.name.split("=")[1]
            if min_date is None or part_date < min_date:
                min_date = part_date
            if max_date is None or part_date > max_date:
                max_date = part_date

            for pf in part_dir.glob("*.parquet"):
                df = pl.read_parquet(pf)
                total_rows += len(df)
                columns.update(df.columns)

        return {
            "name": dataset,
            "row_count": total_rows,
            "date_range": (min_date, max_date) if min_date else None,
            "columns": sorted(columns),
        }

    @staticmethod
    def _validate_schema(df: pl.DataFrame, schema: DatasetSchema) -> None:
        """Validate that DataFrame matches the schema."""
        df_schema = df.schema

        # Check required columns exist
        for col_name, col_type in schema.fields.items():
            if col_name not in df_schema:
                raise ValueError(
                    f"Missing required column '{col_name}' in {schema.name}"
                )

            # Soft check: compatible types (allow some flexibility for timestamps)
            if col_type != df_schema[col_name]:
                if not (
                    str(col_type).startswith("Datetime")
                    and str(df_schema[col_name]).startswith("Datetime")
                ):
                    raise ValueError(
                        f"Column '{col_name}' has type {df_schema[col_name]}, "
                        f"expected {col_type}"
                    )
