from datastore import ParquetStore

store = ParquetStore("data/parquet")

datasets = ["ohlcv_daily", "ohlcv_hourly", "funding_rate", "open_interest"]
for ds in datasets:
    info = store.dataset_info(ds)
    rows = info.get('row_count', 0)
    date_range = info.get('date_range', (None, None))
    print(f"{ds:20s} {rows:>10,} rows  {date_range[0]} → {date_range[1]}")