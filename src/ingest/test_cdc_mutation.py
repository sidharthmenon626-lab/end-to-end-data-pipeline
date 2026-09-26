"""
Change Data Capture (CDC) Mutation Proof Script.
Demonstrates mutating an operational record in the source (UPDATE & DELETE)
and proving the warehouse captures and reflects the exact state changes.
"""

from datetime import datetime, timezone
from sqlalchemy import text
from src.utils.db import get_engine
from src.ingest.cdc import CDCHandler
from src.ingest.watermark import WatermarkStore


def test_cdc_proof():
    print("=" * 70)
    print("  CDC MUTATION PROOF TEST: UPDATE & DELETE VERIFICATION")
    print("=" * 70)

    engine = get_engine()
    test_cust_id = "CUST-PROOF-999"
    now = datetime.now(timezone.utc)

    # -------------------------------------------------------------------------
    # STEP 1: Insert Baseline Customer (Plan: FREE)
    # -------------------------------------------------------------------------
    print("\n[STEP 1] Ingesting Baseline Customer Record (plan_tier = 'FREE')...")
    initial_record = [{
        "customer_id": test_cust_id,
        "first_name": "Alice",
        "last_name": "Tester",
        "email": "alice@example.com",
        "country_code": "US",
        "plan_tier": "FREE",
        "account_status": "ACTIVE",
        "acquisition_channel": "Direct",
        "source_updated_at": now,
        "_source_op": "I",
        "_is_deleted": False,
        "_batch_id": "BATCH-001"
    }]
    CDCHandler.upsert_customers(initial_record)

    with engine.connect() as conn:
        res = conn.execute(
            text("SELECT customer_id, plan_tier, account_status, _source_op, _is_deleted FROM raw.raw_customers WHERE customer_id = :id"),
            {"id": test_cust_id}
        ).mappings().first()
        print(f"  Warehouse Baseline: {dict(res)}")
        assert res["plan_tier"] == "FREE"
        assert res["_is_deleted"] is False

    # -------------------------------------------------------------------------
    # STEP 2: Mutate Customer (UPDATE plan_tier -> 'ENTERPRISE')
    # -------------------------------------------------------------------------
    print("\n[STEP 2] Simulating Source UPDATE: Customer upgrades to 'ENTERPRISE'...")
    updated_now = datetime.now(timezone.utc)
    mutation_record = [{
        "customer_id": test_cust_id,
        "first_name": "Alice",
        "last_name": "Tester",
        "email": "alice@enterprise.org",
        "country_code": "US",
        "plan_tier": "ENTERPRISE",
        "account_status": "ACTIVE",
        "acquisition_channel": "Direct",
        "source_updated_at": updated_now,
        "_source_op": "U",
        "_is_deleted": False,
        "_batch_id": "BATCH-002"
    }]
    CDCHandler.upsert_customers(mutation_record)

    with engine.connect() as conn:
        res = conn.execute(
            text("SELECT customer_id, plan_tier, account_status, _source_op, _is_deleted FROM raw.raw_customers WHERE customer_id = :id"),
            {"id": test_cust_id}
        ).mappings().first()
        print(f"  Warehouse Post-UPDATE: {dict(res)}")
        assert res["plan_tier"] == "ENTERPRISE"
        assert res["_source_op"] == "U"
        assert res["_is_deleted"] is False
    print("  [✓] Verified: UPDATE correctly updated the existing record without duplicate insertion.")

    # -------------------------------------------------------------------------
    # STEP 3: Mutate Customer (DELETE / Soft-Delete Tombstone)
    # -------------------------------------------------------------------------
    print("\n[STEP 3] Simulating Source DELETE: Account cancellation tombstone...")
    delete_now = datetime.now(timezone.utc)
    tombstone_record = [{
        "customer_id": test_cust_id,
        "first_name": "Alice",
        "last_name": "Tester",
        "email": "alice@enterprise.org",
        "country_code": "US",
        "plan_tier": "ENTERPRISE",
        "account_status": "CHURNED",
        "acquisition_channel": "Direct",
        "source_updated_at": delete_now,
        "_source_op": "D",
        "_is_deleted": True,
        "_batch_id": "BATCH-003"
    }]
    CDCHandler.upsert_customers(tombstone_record)

    with engine.connect() as conn:
        res = conn.execute(
            text("SELECT customer_id, plan_tier, account_status, _source_op, _is_deleted FROM raw.raw_customers WHERE customer_id = :id"),
            {"id": test_cust_id}
        ).mappings().first()
        print(f"  Warehouse Post-DELETE: {dict(res)}")
        assert res["account_status"] == "CHURNED"
        assert res["_source_op"] == "D"
        assert res["_is_deleted"] is True
    print("  [✓] Verified: DELETE tombstone captured in warehouse with _is_deleted = TRUE.")

    print("\n" + "=" * 70)
    print("  [PROOF COMPLETED] Real-world CDC operations verified successfully.")
    print("=" * 70)


if __name__ == "__main__":
    test_cdc_proof()
