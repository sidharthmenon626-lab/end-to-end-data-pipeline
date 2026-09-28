"""
Source Extraction Module.
Reads delta batches either directly from live PostgreSQL source databases (ecom / saas on Neon)
or falls back to local Parquet landing zone (data/raw/).
Filters by watermark boundary with configurable late-data lookback windows.
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
from sqlalchemy import text

from src.utils.db import get_source_engine

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


SOURCE_QUERIES = {
    "customers": """
        SELECT 
            c.customer_id::text AS customer_id,
            c.first_name,
            c.last_name,
            c.primary_email AS email,
            COALESCE(c.country, 'India') AS country_code,
            'PRO' AS plan_tier,
            UPPER(COALESCE(c.lifecycle_stage, 'ACTIVE')) AS account_status,
            c.acquisition_channel,
            c.created_at AS source_updated_at,
            'I' AS _source_op,
            FALSE AS _is_deleted
        FROM ecom.customers c
        WHERE c.created_at > :wm
        ORDER BY c.created_at ASC
        LIMIT :limit OFFSET :offset;
    """,
    "orders": """
        SELECT 
            o.order_id::text AS order_id,
            o.customer_id::text AS customer_id,
            o.status AS order_status,
            o.created_at AS order_timestamp,
            COALESCE(o.total, 0.00) AS order_amount_usd,
            COALESCE(o.discount, 0.00) AS discount_usd,
            COALESCE(pm.method_name, 'card') AS payment_method,
            COALESCE(a.country, 'India') AS shipping_country,
            o.created_at AS source_updated_at,
            'I' AS _source_op
        FROM ecom.orders o
        LEFT JOIN ecom.payment_intents pi ON o.order_id = pi.order_id
        LEFT JOIN ecom.payment_methods pm ON pi.payment_method_id = pm.payment_method_id
        LEFT JOIN ecom.addresses a ON o.shipping_address_id = a.address_id
        WHERE o.created_at > :wm
        ORDER BY o.created_at ASC
        LIMIT :limit OFFSET :offset;
    """,
    "subscriptions": """
        SELECT 
            se.event_id::text AS subscription_event_id,
            se.subscription_id::text AS subscription_id,
            se.user_id::text AS customer_id,
            COALESCE(se.to_plan, s.plan, 'STARTER') AS plan_tier,
            COALESCE(se.mrr_delta, s.mrr, 0.00) AS monthly_recurring_revenue,
            UPPER(COALESCE(s.status, se.event_type, 'ACTIVE')) AS status,
            'monthly' AS billing_frequency,
            COALESCE(s.start_date, se.event_time) AS started_at,
            s.cancelled_at AS cancelled_at,
            se.event_time AS event_timestamp,
            'I' AS _source_op
        FROM saas.subscription_events se
        LEFT JOIN saas.subscriptions s ON se.subscription_id = s.subscription_id
        WHERE se.event_time > :wm
        ORDER BY se.event_time ASC
        LIMIT :limit OFFSET :offset;
    """,
    "events": """
        SELECT 
            e.event_id::text AS event_id,
            COALESCE(e.user_id::text, 'anonymous') AS customer_id,
            ('sess_' || COALESCE(e.user_id::text, 'anon') || '_' || e.event_id) AS session_id,
            e.event_type AS event_name,
            'desktop' AS device_category,
            'unknown' AS operating_system,
            ('/app/' || e.event_type) AS page_path,
            e.occurred_at AS event_timestamp
        FROM saas.events e
        WHERE e.occurred_at > :wm
        ORDER BY e.occurred_at ASC
        LIMIT :limit OFFSET :offset;
    """
}


def _extract_from_db(
    source_name: str, effective_watermark: datetime, batch_size: int
) -> Iterator[ExtractionBatch]:
    """
    Extracts batches directly from live PostgreSQL database sources.
    """
    db_key = "ecom" if source_name in ["customers", "orders"] else "saas"
    engine = get_source_engine(db_key)
    if not engine:
        raise ValueError(f"No source database engine configured for '{db_key}'")

    ts_col = "event_timestamp" if source_name in ["subscriptions", "events"] else "source_updated_at"
    query_str = SOURCE_QUERIES[source_name]
    offset = 0

    logger.info(f"[{source_name}] Extracting live from remote {db_key.upper()} PostgreSQL (watermark > {effective_watermark.isoformat()})...")

    with engine.connect() as conn:
        while True:
            result = conn.execute(text(query_str), {"wm": effective_watermark, "limit": batch_size, "offset": offset})
            rows = [dict(r._mapping) for r in result]
            if not rows:
                break

            ops = [r.get("_source_op", "I") for r in rows]
            inserts = ops.count("I")
            updates = ops.count("U")
            deletes = ops.count("D")

            batch_timestamps = [r[ts_col] for r in rows if r.get(ts_col) is not None]
            batch_max_ts = max(batch_timestamps) if batch_timestamps else effective_watermark
            if hasattr(batch_max_ts, "to_pydatetime"):
                batch_max_ts = batch_max_ts.to_pydatetime()
            if batch_max_ts.tzinfo is None:
                batch_max_ts = batch_max_ts.replace(tzinfo=UTC)

            yield ExtractionBatch(
                source_name=source_name,
                start_watermark=effective_watermark,
                end_watermark=batch_max_ts,
                records=rows,
                row_count=len(rows),
                inserts_count=inserts,
                updates_count=updates,
                deletes_count=deletes,
            )

            offset += len(rows)
            if len(rows) < batch_size:
                break


def _extract_from_parquet(
    source_name: str, effective_watermark: datetime, batch_size: int, lookback_window: timedelta | None
) -> Iterator[ExtractionBatch]:
    """
    Extracts batches from local Parquet landing zone (fallback).
    """
    file_path = get_landing_file(source_name)
    if not os.path.exists(file_path):
        logger.warning(f"Source landing file not found: {file_path}")
        return

    ts_col = "event_timestamp" if source_name in ["subscriptions", "events"] else "source_updated_at"
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

        ops = [r.get("_source_op", "I") for r in records]
        inserts = ops.count("I")
        updates = ops.count("U")
        deletes = ops.count("D")

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


def extract_batches(
    source_name: str, watermark: datetime, batch_size: int = 25000, lookback_window: timedelta | None = None
) -> Iterator[ExtractionBatch]:
    """
    Streams incremental delta record batches from either live Neon DB or parquet files.
    """
    watermark_utc = watermark.astimezone(UTC) if watermark.tzinfo is not None else watermark.replace(tzinfo=UTC)
    epoch = datetime(1970, 1, 1, tzinfo=UTC)
    if lookback_window and watermark_utc > epoch:
        effective_watermark = max(epoch, watermark_utc - lookback_window)
        logger.info(
            f"[{source_name}] Applying lookback window of {lookback_window}: "
            f"Querying from {effective_watermark.isoformat()} (Committed watermark: {watermark_utc.isoformat()})"
        )
    else:
        effective_watermark = watermark_utc

    db_key = "ecom" if source_name in ["customers", "orders"] else "saas"
    engine = get_source_engine(db_key)

    if engine and source_name in SOURCE_QUERIES:
        try:
            yield from _extract_from_db(source_name, effective_watermark, batch_size)
            return
        except Exception as e:
            logger.warning(f"[{source_name}] Remote DB extraction failed: {e}. Falling back to parquet landing.")

    yield from _extract_from_parquet(source_name, effective_watermark, batch_size, lookback_window)


def extract_full(source_name: str, batch_size: int = 25000) -> Iterator[ExtractionBatch]:
    epoch = datetime(1970, 1, 1, tzinfo=UTC)
    return extract_batches(source_name, epoch, batch_size=batch_size, lookback_window=None)


def extract_incremental(
    source_name: str, watermark: datetime, batch_size: int = 25000, lookback_window: timedelta | None = None
) -> Iterator[ExtractionBatch]:
    return extract_batches(source_name, watermark, batch_size=batch_size, lookback_window=lookback_window)


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
