"""
Integration Test Suite for Ingestion & CDC Pipeline.
Validates live PostgreSQL target, CDC mutations, watermark tracking, and pipeline idempotency.
"""

from datetime import datetime, timezone
import pytest
from sqlalchemy import text
from src.utils.db import get_engine
from src.ingest.cdc import CDCHandler
from src.ingest.watermark import WatermarkStore
from src.ingest.pipeline import IngestionPipeline


def test_warehouse_connection():
    """Verify live connectivity and required pipeline schemas."""
    engine = get_engine()
    with engine.connect() as conn:
        version = conn.execute(text("SELECT version();")).scalar()
        assert "PostgreSQL" in version

        res = conn.execute(
            text("SELECT schema_name FROM information_schema.schemata WHERE schema_name IN ('raw', 'staging', 'marts');")
        ).fetchall()
        found_schemas = {r[0] for r in res}
        assert {"raw", "staging", "marts"}.issubset(found_schemas)


def test_cdc_mutations():
    """Verify INSERT, UPDATE, and DELETE tombstone mutations."""
    engine = get_engine()
    test_id = "CUST-PYTEST-001"
    now = datetime.now(timezone.utc)

    # 1. Insert Baseline
    rec_insert = [{
        "customer_id": test_id,
        "first_name": "Test",
        "last_name": "User",
        "email": "test@test.com",
        "country_code": "US",
        "plan_tier": "FREE",
        "account_status": "ACTIVE",
        "acquisition_channel": "Direct",
        "source_updated_at": now,
        "_source_op": "I",
        "_is_deleted": False,
        "_batch_id": "TEST-01"
    }]
    assert CDCHandler.upsert_customers(rec_insert) == 1

    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT plan_tier, account_status, _source_op, _is_deleted FROM raw.raw_customers WHERE customer_id = :id"),
            {"id": test_id}
        ).mappings().first()
        assert row["plan_tier"] == "FREE"
        assert row["_is_deleted"] is False

    # 2. Update Mutation
    rec_update = [{
        "customer_id": test_id,
        "first_name": "Test",
        "last_name": "User",
        "email": "test@test.com",
        "country_code": "US",
        "plan_tier": "ENTERPRISE",
        "account_status": "ACTIVE",
        "acquisition_channel": "Direct",
        "source_updated_at": datetime.now(timezone.utc),
        "_source_op": "U",
        "_is_deleted": False,
        "_batch_id": "TEST-02"
    }]
    assert CDCHandler.upsert_customers(rec_update) == 1

    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT plan_tier, _source_op, _is_deleted FROM raw.raw_customers WHERE customer_id = :id"),
            {"id": test_id}
        ).mappings().first()
        assert row["plan_tier"] == "ENTERPRISE"
        assert row["_source_op"] == "U"
        assert row["_is_deleted"] is False

    # 3. Delete Tombstone
    rec_delete = [{
        "customer_id": test_id,
        "first_name": "Test",
        "last_name": "User",
        "email": "test@test.com",
        "country_code": "US",
        "plan_tier": "ENTERPRISE",
        "account_status": "CHURNED",
        "acquisition_channel": "Direct",
        "source_updated_at": datetime.now(timezone.utc),
        "_source_op": "D",
        "_is_deleted": True,
        "_batch_id": "TEST-03"
    }]
    assert CDCHandler.upsert_customers(rec_delete) == 1

    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT account_status, _source_op, _is_deleted FROM raw.raw_customers WHERE customer_id = :id"),
            {"id": test_id}
        ).mappings().first()
        assert row["account_status"] == "CHURNED"
        assert row["_source_op"] == "D"
        assert row["_is_deleted"] is True


def test_watermark_persistence():
    """Verify watermark store updates and retrieves timestamps correctly."""
    wm_store = WatermarkStore()
    test_source = "pytest_source"
    target_time = datetime(2026, 3, 25, 12, 0, 0, tzinfo=timezone.utc)
    
    wm_store.update_watermark(test_source, target_time, 150)
    retrieved_time = wm_store.get_watermark(test_source)
    assert retrieved_time == target_time


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
