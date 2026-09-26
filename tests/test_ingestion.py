"""
Integration Test Suite for Ingestion & CDC Pipeline.
Validates live PostgreSQL target, CDC mutations, sequence protection,
watermark tracking with audit metadata, idempotency, and late-arriving data lookback.
"""

from datetime import UTC, datetime, timedelta

from sqlalchemy import text

from src.ingest.cdc import CDCHandler
from src.ingest.pipeline import IngestionPipeline
from src.ingest.watermark import WatermarkStore
from src.utils.db import get_engine


def test_warehouse_connection():
    """Verify live connectivity and required pipeline schemas."""
    engine = get_engine()
    with engine.connect() as conn:
        version = conn.execute(text("SELECT version();")).scalar()
        assert "PostgreSQL" in version

        res = conn.execute(
            text(
                "SELECT schema_name FROM information_schema.schemata WHERE schema_name IN ('raw', 'staging', 'marts');"
            )
        ).fetchall()
        found_schemas = {r[0] for r in res}
        assert {"raw", "staging", "marts"}.issubset(found_schemas)


def test_cdc_mutations_and_sequence_guard():
    """Verify INSERT, UPDATE, DELETE tombstone mutations, and out-of-order protection."""
    engine = get_engine()
    test_id = "CUST-PYTEST-002"

    with engine.begin() as conn:
        conn.execute(text("DELETE FROM raw.raw_customers WHERE customer_id = :id"), {"id": test_id})

    t_base = datetime.now(UTC)

    # 1. Insert Baseline
    rec_insert = [
        {
            "customer_id": test_id,
            "first_name": "Test",
            "last_name": "User",
            "email": "test@test.com",
            "country_code": "US",
            "plan_tier": "FREE",
            "account_status": "ACTIVE",
            "acquisition_channel": "Direct",
            "source_updated_at": t_base,
            "_source_op": "I",
            "_is_deleted": False,
            "_batch_id": "TEST-01",
        }
    ]
    assert CDCHandler.upsert_customers(rec_insert) == 1

    with engine.connect() as conn:
        row = (
            conn.execute(
                text(
                    "SELECT plan_tier, account_status, _source_op, _is_deleted FROM raw.raw_customers WHERE customer_id = :id"
                ),
                {"id": test_id},
            )
            .mappings()
            .first()
        )
        assert row["plan_tier"] == "FREE"
        assert row["_is_deleted"] is False

    # 2. Update Mutation
    t_update = t_base + timedelta(hours=2)
    rec_update = [
        {
            "customer_id": test_id,
            "first_name": "Test",
            "last_name": "User",
            "email": "test@test.com",
            "country_code": "US",
            "plan_tier": "ENTERPRISE",
            "account_status": "ACTIVE",
            "acquisition_channel": "Direct",
            "source_updated_at": t_update,
            "_source_op": "U",
            "_is_deleted": False,
            "_batch_id": "TEST-02",
        }
    ]
    assert CDCHandler.upsert_customers(rec_update) == 1

    with engine.connect() as conn:
        row = (
            conn.execute(
                text("SELECT plan_tier, _source_op, _is_deleted FROM raw.raw_customers WHERE customer_id = :id"),
                {"id": test_id},
            )
            .mappings()
            .first()
        )
        assert row["plan_tier"] == "ENTERPRISE"
        assert row["_source_op"] == "U"
        assert row["_is_deleted"] is False

    # 3. Out-of-Order Stale Mutation Guard: Older timestamp should be rejected
    t_stale = t_base + timedelta(hours=1)
    rec_stale = [
        {
            "customer_id": test_id,
            "first_name": "Test",
            "last_name": "User",
            "email": "test@test.com",
            "country_code": "US",
            "plan_tier": "STALE_TIER",
            "account_status": "ACTIVE",
            "acquisition_channel": "Direct",
            "source_updated_at": t_stale,
            "_source_op": "U",
            "_is_deleted": False,
            "_batch_id": "TEST-STALE",
        }
    ]
    CDCHandler.upsert_customers(rec_stale)

    with engine.connect() as conn:
        row = (
            conn.execute(text("SELECT plan_tier FROM raw.raw_customers WHERE customer_id = :id"), {"id": test_id})
            .mappings()
            .first()
        )
        assert row["plan_tier"] == "ENTERPRISE", "Stale event overwritten current state!"

    # 4. Delete Tombstone
    t_delete = t_update + timedelta(hours=3)
    rec_delete = [
        {
            "customer_id": test_id,
            "first_name": "Test",
            "last_name": "User",
            "email": "test@test.com",
            "country_code": "US",
            "plan_tier": "ENTERPRISE",
            "account_status": "CHURNED",
            "acquisition_channel": "Direct",
            "source_updated_at": t_delete,
            "_source_op": "D",
            "_is_deleted": True,
            "_batch_id": "TEST-03",
        }
    ]
    assert CDCHandler.upsert_customers(rec_delete) == 1

    with engine.connect() as conn:
        row = (
            conn.execute(
                text("SELECT account_status, _source_op, _is_deleted FROM raw.raw_customers WHERE customer_id = :id"),
                {"id": test_id},
            )
            .mappings()
            .first()
        )
        assert row["account_status"] == "CHURNED"
        assert row["_source_op"] == "D"
        assert row["_is_deleted"] is True


def test_watermark_persistence():
    """Verify watermark store updates and retrieves timestamps, batch IDs, and status."""
    wm_store = WatermarkStore()
    test_source = "pytest_source"
    target_time = datetime(2026, 3, 25, 12, 0, 0, tzinfo=UTC)
    test_batch_id = "BATCH-PYTEST-01"

    wm_store.update_watermark(
        source_name=test_source, new_watermark=target_time, records_count=150, batch_id=test_batch_id, status="SUCCESS"
    )
    details = wm_store.get_watermark_details(test_source)
    assert details is not None
    assert details["last_batch_id"] == test_batch_id
    assert details["status"] == "SUCCESS"
    assert wm_store.get_watermark(test_source) == target_time


def test_pipeline_idempotency():
    """Verify running pipeline against fully ingested sources produces zero row delta."""
    engine = get_engine()
    tables = ["raw_customers", "raw_orders", "raw_subscriptions", "raw_events"]

    def get_counts():
        with engine.connect() as conn:
            return {t: conn.execute(text(f"SELECT COUNT(*) FROM raw.{t}")).scalar() for t in tables}

    counts_before = get_counts()
    pipeline = IngestionPipeline()
    pipeline.run_all()
    counts_after = get_counts()

    for t in tables:
        assert counts_after[t] == counts_before[t], f"Table raw.{t} row count changed on re-ingestion!"


def test_late_arriving_data_lookback():
    """Verify late-arriving records within lookback window are extracted and safely upserted."""
    engine = get_engine()
    test_order_id = "ORD-LATE-TEST-99"

    with engine.begin() as conn:
        conn.execute(text("DELETE FROM raw.raw_orders WHERE order_id = :id"), {"id": test_order_id})

    # Simulate: Watermark is at T_now
    t_now = datetime.now(UTC)
    t_delayed = t_now - timedelta(minutes=45)  # Arrived 45 min late

    # Ingest late-arriving record using lookback
    late_order = [
        {
            "order_id": test_order_id,
            "customer_id": "CUST-000001",
            "order_status": "COMPLETED",
            "order_timestamp": t_delayed,
            "order_amount_usd": 199.99,
            "discount_usd": 0.00,
            "payment_method": "STRIPE",
            "shipping_country": "US",
            "source_updated_at": t_delayed,
            "_source_op": "I",
            "_batch_id": "BATCH-LATE-01",
        }
    ]
    CDCHandler.upsert_orders(late_order)

    with engine.connect() as conn:
        res = (
            conn.execute(
                text("SELECT order_id, order_amount_usd, order_timestamp FROM raw.raw_orders WHERE order_id = :id"),
                {"id": test_order_id},
            )
            .mappings()
            .first()
        )
        assert res is not None
        assert res["order_id"] == test_order_id
        assert float(res["order_amount_usd"]) == 199.99
