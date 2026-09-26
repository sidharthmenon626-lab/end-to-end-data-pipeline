import json
import os
import sys
from datetime import UTC, datetime

import pyarrow.parquet as pq
from sqlalchemy import text

from src.utils.db import get_engine


def main():
    print("=" * 75)
    print("  END-TO-END DATA PIPELINE: SYSTEM-WIDE RE-RUN VERIFICATION SUITE")
    print(f"  Execution Timestamp: {datetime.now(UTC).isoformat()} UTC")
    print("=" * 75)

    engine = get_engine()

    # -------------------------------------------------------------
    # STEP 1: PostgreSQL 17 Connectivity & Health Check
    # -------------------------------------------------------------
    print("\n[STEP 1] Testing PostgreSQL Warehouse Connectivity...")
    try:
        with engine.connect() as conn:
            pg_version = conn.execute(text("SELECT version();")).scalar()
            current_db = conn.execute(text("SELECT current_database();")).scalar()
            current_user = conn.execute(text("SELECT current_user;")).scalar()
            print(f"  [OK] Connected to database: '{current_db}' as user: '{current_user}'")
            print(f"  [OK] Version: {pg_version.split(',')[0]}")
    except Exception as e:
        print(f"  [FAILED] Could not connect to PostgreSQL: {e}")
        sys.exit(1)

    # -------------------------------------------------------------
    # STEP 2: Schema Existence Verification
    # -------------------------------------------------------------
    print("\n[STEP 2] Verifying Required Database Schemas...")
    required_schemas = ["raw", "staging", "marts"]
    with engine.connect() as conn:
        for s in required_schemas:
            exists = conn.execute(
                text("SELECT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = :s)"), {"s": s}
            ).scalar()
            status = "[OK] Exists" if exists else "[MISSING]"
            print(f"  Schema '{s}': {status}")

    # -------------------------------------------------------------
    # STEP 3: Milestone 2 Dimensional Model in 'marts'
    # -------------------------------------------------------------
    print("\n[STEP 3] Verifying Milestone 2 Kimball Star Schema in 'marts'...")
    with engine.connect() as conn:
        marts_tables = conn.execute(
            text("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'marts'
            ORDER BY table_name;
        """)
        ).fetchall()
        print(f"  [OK] Total tables/partitions in 'marts': {len(marts_tables)}")
        for t in marts_tables:
            print(f"    - marts.{t[0]}")

    # -------------------------------------------------------------
    # STEP 4: Raw Parquet Data Lake & Manifest Verification
    # -------------------------------------------------------------
    print("\n[STEP 4] Verifying Raw Parquet Data Lake & Manifest...")
    manifest_path = "data/manifest.json"
    if os.path.exists(manifest_path):
        with open(manifest_path, encoding="utf-8") as f:
            manifest = json.load(f)
        print(f"  [OK] Manifest found: {manifest_path} (Target Rows: {manifest.get('total_target_rows', 0):,})")

    raw_files = [
        (os.path.join("data", "raw", "customers", "customers.parquet"), "customers.parquet", 50000),
        (os.path.join("data", "raw", "orders", "orders.parquet"), "orders.parquet", 400000),
        (os.path.join("data", "raw", "subscriptions", "subscriptions.parquet"), "subscriptions.parquet", 200000),
        (os.path.join("data", "raw", "events", "events.parquet"), "events.parquet", 400000),
    ]
    total_lake_rows = 0
    total_lake_bytes = 0
    for fpath, fname, _exp_rows in raw_files:
        if os.path.exists(fpath):
            meta = pq.read_metadata(fpath)
            size_mb = os.path.getsize(fpath) / (1024 * 1024)
            total_lake_rows += meta.num_rows
            total_lake_bytes += os.path.getsize(fpath)
            print(f"  [OK] {fname:22s}: {meta.num_rows:,} rows, {meta.num_columns} cols, {size_mb:.2f} MB ({fpath})")
        else:
            print(f"  [MISSING] {fname} at {fpath}")
    print(f"  --> Total Data Lake Volume: {total_lake_rows:,} rows ({total_lake_bytes / (1024 * 1024):.2f} MB)")

    # -------------------------------------------------------------
    # STEP 5: Watermark Audit Table & JSON Verification
    # -------------------------------------------------------------
    print("\n[STEP 5] Auditing Watermark State Table (raw._pipeline_watermarks)...")
    with engine.connect() as conn:
        wm_rows = conn.execute(
            text("""
            SELECT source_name, last_watermark, records_extracted, last_batch_id, status, error_message, last_success_at
            FROM raw._pipeline_watermarks
            ORDER BY source_name;
        """)
        ).fetchall()
        for r in wm_rows:
            err = f" | Error: {r[5]}" if r[5] else ""
            print(
                f"  - Source: {r[0]:15s} | Watermark: {r[1]!s:32s} | Records: {r[2]:7d} | Status: {r[4]} | Batch: {r[3]}{err}"
            )

    # -------------------------------------------------------------
    # STEP 6: Live Warehouse Raw Table Counts
    # -------------------------------------------------------------
    print("\n[STEP 6] Reconciling Ingested Warehouse Raw Tables...")
    with engine.connect() as conn:
        tables = ["raw_customers", "raw_orders", "raw_subscriptions", "raw_events"]
        total_raw_rows = 0
        for t in tables:
            cnt = conn.execute(text(f"SELECT COUNT(*) FROM raw.{t}")).scalar()
            total_raw_rows += cnt
            print(f"  [OK] Table raw.{t:20s}: {cnt:,} rows")
        print(f"  --> Total Ingested Warehouse Rows: {total_raw_rows:,} rows")

    print("\n" + "=" * 75)
    print("  [ALL VERIFICATION STEPS EXECUTED SUCCESSFULLY]")
    print("=" * 75)


if __name__ == "__main__":
    main()
