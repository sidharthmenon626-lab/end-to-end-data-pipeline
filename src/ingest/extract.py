"""
Source Extraction Module.
Reads delta batches from source Parquet landing zone (data/raw/)
filtering strictly by watermark boundary.
"""

import os
from datetime import datetime, timezone
from typing import Dict, List, Any, Iterator
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.compute as pc
import pandas as pd

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
RAW_BASE_DIR = os.path.join(PROJECT_ROOT, "data", "raw")


def get_landing_file(source_name: str) -> str:
    """
    Resolves the canonical parquet landing path for a given source.
    """
    return os.path.join(RAW_BASE_DIR, source_name, f"{source_name}.parquet")


def extract_batches(source_name: str, watermark: datetime, batch_size: int = 25000) -> Iterator[List[Dict[str, Any]]]:
    """
    Streams incremental delta record batches from parquet landing zone.
    Applies strict watermark threshold comparison: timestamp > watermark.
    """
    file_path = get_landing_file(source_name)
    if not os.path.exists(file_path):
        return

    ts_col = "event_timestamp" if source_name in ["subscriptions", "events"] else "source_updated_at"

    if watermark.tzinfo is not None:
        watermark_utc = watermark.astimezone(timezone.utc)
    else:
        watermark_utc = watermark.replace(tzinfo=timezone.utc)

    dataset = ds.dataset(file_path, format="parquet")
    pa_wm = pa.scalar(watermark_utc, type=pa.timestamp("ns", tz="UTC"))
    filter_expr = pc.field(ts_col) > pa_wm

    scanner = dataset.scanner(filter=filter_expr, batch_size=batch_size)
    for record_batch in scanner.to_batches():
        if record_batch.num_rows == 0:
            continue
        df = record_batch.to_pandas()
        # Convert NaN values to None for clean SQL NULL insertion
        df = df.where(pd.notnull(df), None)
        yield df.to_dict(orient="records")


def extract_customer_batch(watermark: datetime, batch_size: int = 25000) -> List[Dict[str, Any]]:
    """
    Extracts customer profile changes occurred after watermark.
    """
    for batch in extract_batches("customers", watermark, batch_size=batch_size):
        return batch
    return []


def extract_orders_batch(watermark: datetime, batch_size: int = 25000) -> List[Dict[str, Any]]:
    """
    Extracts orders placed or modified after watermark.
    """
    for batch in extract_batches("orders", watermark, batch_size=batch_size):
        return batch
    return []


def extract_subscriptions_batch(watermark: datetime, batch_size: int = 25000) -> List[Dict[str, Any]]:
    """
    Extracts SaaS subscription state transition events.
    """
    for batch in extract_batches("subscriptions", watermark, batch_size=batch_size):
        return batch
    return []


def extract_events_batch(watermark: datetime, batch_size: int = 25000) -> List[Dict[str, Any]]:
    """
    Extracts append-only clickstream telemetry events.
    """
    for batch in extract_batches("events", watermark, batch_size=batch_size):
        return batch
    return []
