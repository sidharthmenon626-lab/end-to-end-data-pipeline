"""
Source Extraction Module.
Reads delta batches from source Parquet landing zone (data/raw/)
filtering by watermark boundary with configurable late-data lookback windows.
Provides structured ExtractionBatch metadata separating extraction from loading.
"""

import logging
import os
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds

logger = logging.getLogger(__name__)

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
RAW_BASE_DIR = os.path.join(PROJECT_ROOT, "data", "raw")


@dataclass
class ExtractionBatch:
    """
    Metadata-rich extraction payload separating extraction concerns from warehouse loading.
    """

    source_name: str
    start_watermark: datetime
    end_watermark: datetime
    records: list[dict[str, Any]]
    row_count: int
    inserts_count: int
    updates_count: int
    deletes_count: int


def get_landing_file(source_name: str) -> str:
    """
    Resolves the canonical parquet landing path for a given source.
    """
    return os.path.join(RAW_BASE_DIR, source_name, f"{source_name}.parquet")


def extract_batches(
    source_name: str, watermark: datetime, batch_size: int = 25000, lookback_window: timedelta | None = None
) -> Iterator[ExtractionBatch]:
    """
    Streams incremental delta record batches from parquet landing zone.

    Args:
        source_name: Source stream identifier (customers, orders, subscriptions, events).
        watermark: High-water mark timestamp from previous committed run.
        batch_size: Number of records to read per chunk.
        lookback_window: Optional time window subtracted from watermark to catch late-arriving data.
    """
    file_path = get_landing_file(source_name)
    if not os.path.exists(file_path):
        logger.warning(f"Source landing file not found: {file_path}")
        return

    ts_col = "event_timestamp" if source_name in ["subscriptions", "events"] else "source_updated_at"

    # Normalize watermark to UTC
    watermark_utc = watermark.astimezone(UTC) if watermark.tzinfo is not None else watermark.replace(tzinfo=UTC)

    # Apply lookback window if configured and not at historical epoch
    epoch = datetime(1970, 1, 1, tzinfo=UTC)
    if lookback_window and watermark_utc > epoch:
        effective_watermark = max(epoch, watermark_utc - lookback_window)
        logger.info(
            f"[{source_name}] Applying lookback window of {lookback_window}: "
            f"Querying from {effective_watermark.isoformat()} (Committed watermark: {watermark_utc.isoformat()})"
        )
    else:
        effective_watermark = watermark_utc

    dataset = ds.dataset(file_path, format="parquet")
    pa_wm = pa.scalar(effective_watermark, type=pa.timestamp("ns", tz="UTC"))
    filter_expr = pc.field(ts_col) >= pa_wm if lookback_window else pc.field(ts_col) > pa_wm

    scanner = dataset.scanner(filter=filter_expr, batch_size=batch_size)
    for record_batch in scanner.to_batches():
        if record_batch.num_rows == 0:
            continue

        df = record_batch.to_pandas()
        df = df.where(pd.notnull(df), None)
        records = df.to_dict(orient="records")

        # Compute mutation breakdown
        ops = [r.get("_source_op", "I") for r in records]
        inserts = ops.count("I")
        updates = ops.count("U")
        deletes = ops.count("D")

        # Determine timestamp range within batch
        batch_timestamps = [r[ts_col] for r in records if r.get(ts_col) is not None]
        batch_max_ts = max(batch_timestamps) if batch_timestamps else effective_watermark
        if hasattr(batch_max_ts, "to_pydatetime"):
            batch_max_ts = batch_max_ts.to_pydatetime()
        if batch_max_ts.tzinfo is None:
            batch_max_ts = batch_max_ts.replace(tzinfo=UTC)

        yield ExtractionBatch(
            source_name=source_name,
            start_watermark=effective_watermark,
            end_watermark=batch_max_ts,
            records=records,
            row_count=len(records),
            inserts_count=inserts,
            updates_count=updates,
            deletes_count=deletes,
        )


def extract_full(source_name: str, batch_size: int = 25000) -> Iterator[ExtractionBatch]:
    """
    Executes a full-table extraction starting from the Unix epoch.
    """
    epoch = datetime(1970, 1, 1, tzinfo=UTC)
    return extract_batches(source_name, epoch, batch_size=batch_size, lookback_window=None)


def extract_incremental(
    source_name: str, watermark: datetime, batch_size: int = 25000, lookback_window: timedelta | None = None
) -> Iterator[ExtractionBatch]:
    """
    Executes incremental watermark-based extraction with optional lookback window.
    """
    return extract_batches(source_name, watermark, batch_size=batch_size, lookback_window=lookback_window)


# ------------------------------------------------------------------------------
# Backwards Compatibility Wrappers for Legacy Callers
# ------------------------------------------------------------------------------


def extract_customer_batch(watermark: datetime, batch_size: int = 25000) -> list[dict[str, Any]]:
    for batch in extract_batches("customers", watermark, batch_size=batch_size):
        return batch.records
    return []


def extract_orders_batch(watermark: datetime, batch_size: int = 25000) -> list[dict[str, Any]]:
    for batch in extract_batches("orders", watermark, batch_size=batch_size):
        return batch.records
    return []


def extract_subscriptions_batch(watermark: datetime, batch_size: int = 25000) -> list[dict[str, Any]]:
    for batch in extract_batches("subscriptions", watermark, batch_size=batch_size):
        return batch.records
    return []


def extract_events_batch(watermark: datetime, batch_size: int = 25000) -> list[dict[str, Any]]:
    for batch in extract_batches("events", watermark, batch_size=batch_size):
        return batch.records
    return []
