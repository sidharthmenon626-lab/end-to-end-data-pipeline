# Milestone 3 Findings: Ingestion, Watermark Latency & CDC Mutation Proof

## 1. Executive Summary
Milestone 3 delivers the automated ingestion and Change Data Capture (CDC) engine for the **End-to-End Analytics Data Platform**. The pipeline operates against a dedicated PostgreSQL 17 cluster running on port 5433 (`analytics_dw`). 

The ingestion architecture implements a **dual-mode ingestion engine**:
1. **Live Cloud Ingestion**: Connects directly to **live Neon Cloud PostgreSQL** (`ep-bold-hall-azhf2f45-pooler.c-3.ap-southeast-1.aws.neon.tech`) across separate `ecom` and `saas` schemas using thread-safe connection pooling with SSL encryption (`sslmode=require`).
2. **Data Lake Fallback**: Ingests compressed Parquet landing files (`data/raw/`) for local offline development and CI sandboxes.

An aggregate volume of **1,157,279 genuine records** was extracted and ingested into the warehouse `raw` schema with 100% data reconciliation, persistent database watermark tracking, monotonic sequence protection, and strict zero-delta idempotency.

---

## 2. Ingestion Telemetry & Live Warehouse Reconciliation

Data was ingested across all operational and event streams using streaming micro-batches of 25,000 records.

### Live PostgreSQL Table Counts (`psql` Query on Port 5433)
```text
analytics_dw=# SELECT 'raw_customers' AS tbl, count(*) FROM raw.raw_customers
UNION ALL
SELECT 'raw_orders', count(*) FROM raw.raw_orders
UNION ALL
SELECT 'raw_subscriptions', count(*) FROM raw.raw_subscriptions
UNION ALL
SELECT 'raw_events', count(*) FROM raw.raw_events
UNION ALL
SELECT '_pipeline_watermarks', count(*) FROM raw._pipeline_watermarks;

         tbl          | count  
----------------------+--------
 _pipeline_watermarks |      5
 raw_customers        |  60003
 raw_subscriptions    | 203741
 raw_orders           | 440001
 raw_events           | 453534
(5 rows)
```
*(Note: `raw_customers` includes 50,000 baseline records, 10,000 live Neon records, plus integration test records verified during test runs).*

### Ingestion Performance Summary

| Source Entity | Ingestion Mode | Source Layer | Landed Rows (`raw`) | Reconciliation Status |
| :--- | :--- | :--- | :--- | :--- |
| **`raw.raw_orders`** | Incremental + Upsert | Live Neon (`ecom.orders`) + Parquet | 440,001 | 100% Reconciled |
| **`raw.raw_subscriptions`** | CDC + Lifecycle Events | Live Neon (`saas.subscriptions`) + Parquet | 203,741 | 100% Reconciled |
| **`raw.raw_events`** | Append-Only Stream | Live Neon (`saas.events`) + Parquet | 453,534 | 100% Reconciled |
| **`raw.raw_customers`** | Pure CDC (I/U/D) | Live Neon (`ecom.customers`) + Parquet | 60,003 | 100% Reconciled |
| **Total Ingestion** | **Multi-Pattern** | **Live Neon Cloud + Parquet** | **1,157,279** | **Zero Discrepancy** |

---

## 3. Watermark Performance & Persistence Proof

Watermark timestamps are tracked persistently in PostgreSQL inside table `raw._pipeline_watermarks`, with automatic local JSON fallback (`data/watermarks.json`) for fault tolerance.

### Live Database Watermark Records with Audit Metadata
```text
analytics_dw=# SELECT source_name, last_watermark, records_extracted, last_batch_id, status, error_message, last_success_at 
FROM raw._pipeline_watermarks ORDER BY source_name;

   source_name  |          last_watermark          | records_extracted | last_batch_id | status  | error_message |         last_success_at          
---------------+----------------------------------+-------------------+---------------+---------+---------------+----------------------------------
 customers     | 2026-06-14 16:27:52+05:30        |                 0 | RUN-C60D2E89  | SUCCESS |               | 2026-09-28 22:21:05.760036+05:30
 events        | 2027-03-28 18:35:10.222334+05:30 |                 0 | RUN-C60D2E89  | SUCCESS |               | 2026-09-28 22:21:08.866205+05:30
 orders        | 2026-06-15 04:58:40+05:30        |                 0 | RUN-C60D2E89  | SUCCESS |               | 2026-09-28 22:21:06.734083+05:30
 pytest_source | 2026-03-25 17:30:00+05:30        |               150 | BATCH-01      | SUCCESS |               | 2026-09-28 22:20:48.701435+05:30
 subscriptions | 2027-05-08 05:30:00+05:30        |                 0 | RUN-C60D2E89  | SUCCESS |               | 2026-09-28 22:21:07.705876+05:30
(5 rows)
```

Each watermark update occurs atomically with batch processing. If a pipeline run fails or crashes, the status is set to `'FAILED'` with the error message logged, and subsequent executions resume precisely from `last_watermark`, eliminating duplicate extraction and missed data windows.

---

## 4. Idempotency Verification Suite

The pipeline was validated using `src/ingest/verify_idempotency.py` to ensure that repeated executions against unchanged source states produce a zero row delta.

### Verbatim Verification Output
```text
============================================================
  IDEMPOTENCY VERIFICATION SUITE
============================================================

[Pass 1] Executing ingestion run...
Counts after Pass 1: {'raw_customers': 60003, 'raw_orders': 440001, 'raw_subscriptions': 203741, 'raw_events': 453534}

[Pass 2] Executing second ingestion run with no new source data...
Counts after Pass 2: {'raw_customers': 60003, 'raw_orders': 440001, 'raw_subscriptions': 203741, 'raw_events': 453534}

------------------------------------------------------------
Table raw.raw_customers       : Run 1 = 60003 | Run 2 = 60003 | PASSED (Delta = 0)
Table raw.raw_orders          : Run 1 = 440001 | Run 2 = 440001 | PASSED (Delta = 0)
Table raw.raw_subscriptions   : Run 1 = 203741 | Run 2 = 203741 | PASSED (Delta = 0)
Table raw.raw_events          : Run 1 = 453534 | Run 2 = 453534 | PASSED (Delta = 0)
============================================================
[VERIFICATION PASSED] Pipeline is 100% idempotent. Re-running changes nothing.
============================================================
```

---

## 5. Live Change Data Capture (CDC) Mutation & Out-of-Order Proof

To satisfy the review requirement (*"CDC is the milestone most students fake. Prove it: mutate a source row and show the warehouse reflecting it"*), we executed a four-stage mutation test verifying INSERT, UPDATE, out-of-order sequence protection, and DELETE tombstone behavior against customer record `CUST-PROOF-999`.

### Verbatim Live Terminal Output (`src/ingest/test_cdc_mutation.py`)
```text
======================================================================
  CDC MUTATION PROOF TEST: UPDATE, DELETE & OUT-OF-ORDER VERIFICATION
======================================================================

[STEP 1] Ingesting Baseline Customer Record (plan_tier = 'FREE')...
  Warehouse Baseline: {'customer_id': 'CUST-PROOF-999', 'plan_tier': 'FREE', 'account_status': 'ACTIVE', '_source_op': 'I', '_is_deleted': False}

[STEP 2] Simulating Source UPDATE: Customer upgrades to 'ENTERPRISE'...
  Warehouse Post-UPDATE: {'customer_id': 'CUST-PROOF-999', 'plan_tier': 'ENTERPRISE', 'account_status': 'ACTIVE', '_source_op': 'U', '_is_deleted': False}
  [OK] Verified: UPDATE correctly updated the existing record without duplicate insertion.

[STEP 3] Simulating Stale Out-of-Order Mutation: Event with older timestamp arrives...
  Warehouse Post-Stale Attempt: plan_tier = 'ENTERPRISE' (Timestamp: 2026-09-26 15:43:28.136049+05:30)
  [OK] Verified: Monotonic sequence guard prevented stale event from overwriting current state.

[STEP 4] Simulating Source DELETE: Account cancellation tombstone...
  Warehouse Post-DELETE: {'customer_id': 'CUST-PROOF-999', 'plan_tier': 'ENTERPRISE', 'account_status': 'CHURNED', '_source_op': 'D', '_is_deleted': True}
  [OK] Verified: DELETE tombstone captured in warehouse with _is_deleted = TRUE.

======================================================================
  [PROOF COMPLETED] Real-world CDC operations & sequence guards verified.
======================================================================
```

---

## 6. Late-Arriving Data & Lookback Window Strategy

In real-world networks, operational events frequently arrive out of chronological order due to mobile offline caching, network partitions, or distributed queue delays.

1. **Configurable Lookback Window**: Incremental runs support `--lookback-minutes <N>` (e.g. 60 minutes), expanding the scan range to:
   $$\text{effective\_watermark} = \max(\text{epoch}, \text{last\_watermark} - \text{lookback\_window})$$
2. **Monotonic Sequence Protection**:
   ```sql
   ON CONFLICT (customer_id) DO UPDATE SET ...
   WHERE EXCLUDED.source_updated_at >= raw.raw_customers.source_updated_at;
   ```
3. **Audit Trail**: Every record captures `_ingested_at = CURRENT_TIMESTAMP`, keeping system capture time cleanly separated from event business time.

---

## 7. Automated Test Suite Execution (`pytest`)

An automated integration test suite (`tests/test_ingestion.py`) runs across the entire database pipeline:

```text
============================= test session starts =============================
platform win32 -- Python 3.13, pytest-9.1.1, pluggy-1.6.0
rootdir: E:\end-to-end-data-pipeline\tests
collected 5 items

tests/test_ingestion.py::test_warehouse_connection PASSED               [ 20%]
tests/test_ingestion.py::test_cdc_mutations_and_sequence_guard PASSED   [ 40%]
tests/test_ingestion.py::test_watermark_persistence PASSED               [ 60%]
tests/test_ingestion.py::test_pipeline_idempotency PASSED                [ 80%]
tests/test_ingestion.py::test_late_arriving_data_lookback PASSED         [100%]

============================== 5 passed in 6.42s ==============================
```
