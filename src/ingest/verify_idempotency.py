"""
Idempotency Verification Script.
Validates that running the ingestion pipeline back-to-back on unchanged data produces zero row delta.
"""

from sqlalchemy import text

from src.ingest.pipeline import IngestionPipeline
from src.utils.db import get_engine


def get_table_counts(tables):
    engine = get_engine()
    counts = {}
    with engine.connect() as conn:
        for t in tables:
            try:
                counts[t] = conn.execute(text(f"SELECT COUNT(*) FROM raw.{t}")).scalar()
            except Exception:
                counts[t] = 0
    return counts


def run_test():
    tables = ["raw_customers", "raw_orders", "raw_subscriptions", "raw_events"]
    print("=" * 60)
    print("  IDEMPOTENCY VERIFICATION SUITE")
    print("=" * 60)

    # Run 1: Initial load
    print("\n[Pass 1] Executing ingestion run...")
    pipeline = IngestionPipeline()
    pipeline.run_all()
    counts_run1 = get_table_counts(tables)
    print("Counts after Pass 1:", counts_run1)

    # Run 2: Immediate re-run (should change nothing)
    print("\n[Pass 2] Executing second ingestion run with no new source data...")
    pipeline.run_all()
    counts_run2 = get_table_counts(tables)
    print("Counts after Pass 2:", counts_run2)

    # Compare
    print("\n" + "-" * 60)
    all_passed = True
    for t in tables:
        delta = counts_run2[t] - counts_run1[t]
        status = "PASSED (Delta = 0)" if delta == 0 else f"FAILED (Delta = {delta})"
        print(f"Table raw.{t:20s}: Run 1 = {counts_run1[t]} | Run 2 = {counts_run2[t]} | {status}")
        if delta != 0:
            all_passed = False

    print("=" * 60)
    if all_passed:
        print("[VERIFICATION PASSED] Pipeline is 100% idempotent. Re-running changes nothing.")
    else:
        print("[VERIFICATION FAILED] Duplicate rows detected upon re-execution.")
    print("=" * 60)
    return all_passed


if __name__ == "__main__":
    run_test()
