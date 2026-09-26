# Ingestion Architecture, Source Inventory & CDC Specification

## 1. Executive Strategy Overview
The ingestion layer forms the foundational operational boundary of the analytics platform, pulling raw records across customer profiles, ecommerce transactions, SaaS subscription contracts, and digital telemetry events into the warehouse `raw` schema.

To scale reliably across **1,000,000+ records** without incurring catastrophic compute costs or table locking, we reject naive full-table scans. Instead, every source dataset is mapped to an explicit ingestion pattern based on its volatility, mutation profile, and volume.

---

## 2. Source Table Inventory Matrix (Step 1 Classification)

| Attribute | `customers` | `ecommerce_orders` | `saas_subscriptions` | `customer_events` |
| :--- | :--- | :--- | :--- | :--- |
| **Landing Format** | Parquet (Snappy) | Parquet (Snappy) | Parquet (Snappy) | Parquet (Snappy) |
| **Landing Path** | `data/raw/customers/` | `data/raw/orders/` | `data/raw/subscriptions/` | `data/raw/events/` |
| **Target Warehouse Table** | `raw.raw_customers` | `raw.raw_orders` | `raw.raw_subscriptions` | `raw.raw_events` |
| **Primary Unique Key** | `customer_id` | `order_id` | `subscription_event_id` | `event_id` |
| **Insertion Mode** | Batch Extract / Change Log | Micro-batch Transaction Log | Lifecycle Event Log | Append-Only Stream / Webhook |
| **Can Rows Be Updated?** | **Yes** (Name, email, plan tier, account status) | **Yes** (`PENDING` $\rightarrow$ `COMPLETED` $\rightarrow$ `REFUNDED`) | **Yes** (Status, MRR amendments) | **No** (Strictly immutable actions) |
| **Can Rows Be Deleted?** | **Yes** (Account closure / GDPR deletion) | **No** (Cancellations are status updates) | **No** (Churns are transition events) | **No** (Events are permanent logs) |
| **Deletion Mechanism** | Soft-delete tombstone (`_is_deleted = TRUE`) | N/A (Status = `CANCELLED`) | N/A (Status = `CANCELLED`) | N/A |
| **Watermark Column** | `source_updated_at` | `source_updated_at` | `event_timestamp` | `event_timestamp` |
| **Watermark Suitability** | Suitable for updates & inserts | Captures checkout & status mutations | Captures contract transitions | Monotonic event timestamps |
| **CDC Metadata Mechanism** | `_source_op` (`I`/`U`/`D`), `_is_deleted` | `_source_op` (`I`/`U`), `_batch_id` | `_source_op` (`I`/`U`), `_batch_id` | `_batch_id`, `_ingested_at` |
| **Expected Volume** | 50,000 baseline rows | 400,000 baseline rows | 200,000 baseline rows | 400,000 baseline rows |
| **Ingestion Cadence** | Hourly / Daily CDC sync | Hourly micro-batch | Hourly lifecycle sync | Real-time / 15-min micro-batch |
| **Ingestion Classification** | **CDC Load** | **Incremental Load + Upsert** | **CDC Lifecycle Load** | **Append-Only Watermark Load** |

### Why We Avoid Full Load
A naive full load (truncating and reloading 1M+ rows on every run) introduces unacceptable operational liabilities:
1. **Loss of Historical Auditability**: Source hard-deletes disappear without trace, breaking downstream SCD Type II history tracking in `marts.dim_customer`.
2. **Excessive Warehouse I/O & Network Saturation**: Processing 1,000,000 records every 15 minutes creates database lock contention and CPU bottlenecks.
3. **Pipeline Inflexibility**: Downstream streaming or micro-batch consumers cannot detect incremental deltas.

---

## 3. Ingestion Patterns Deep Dive

### Pattern A: Pure Change Data Capture (CDC)
* **Target Table**: `raw.raw_customers`
* **Mechanism**: Captures operational mutations from CRM/auth databases. Each extracted record includes operational metadata:
  * `_source_op`: `'I'` (Insert), `'U'` (Update), or `'D'` (Delete).
  * `_is_deleted`: Boolean tombstone flag set to `TRUE` when an entity is deleted in source.
  * `_ingested_at`: Wall-clock UTC timestamp indicating when the warehouse captured the row.
* **Database Merge**: Uses PostgreSQL `INSERT ... ON CONFLICT (customer_id) DO UPDATE` with **monotonic sequence protection**:
  ```sql
  WHERE EXCLUDED.source_updated_at >= raw.raw_customers.source_updated_at;
  ```
  This guarantees that out-of-order stale events arriving from network retries can never clobber a newer record version.

### Pattern B: Incremental Watermark Extraction with Upsert
* **Target Table**: `raw.raw_orders`
* **Mechanism**: 
  1. Queries `raw._pipeline_watermarks` for the last committed `source_updated_at`.
  2. Extracts rows where `source_updated_at > last_watermark` (or `>= effective_watermark` with lookback).
  3. Inserts into `raw.raw_orders` with an `ON CONFLICT (order_id) DO UPDATE` clause.
  4. Sequence guard ensures historical mutations update accurately without duplicating.

### Pattern C: High-Throughput Append-Only Watermarking
* **Target Table**: `raw.raw_events`
* **Mechanism**: High-volume telemetry stream (~400k events). Since clickstream events never mutate, we bypass lock-heavy merge operations in favor of lightweight bulk inserts with `ON CONFLICT (event_id) DO NOTHING`.

---

## 4. Late-Arriving Data & Lookback Window Strategy (Step 8)

In real-world networks, operational events frequently arrive out of chronological order due to mobile offline caching, network partitions, or distributed queue delays.

### The Trade-off: Strict `>` vs. Inclusive `>=` with Lookback
* **Strict `>`**: Fast and scans minimum data, but permanently **misses** any event timestamped prior to `last_watermark`.
* **Inclusive `>=` with Lookback (Implemented)**:
  1. For incremental runs, the pipeline subtracts a configurable lookback window (e.g., 60 minutes) from `last_watermark`:
     $$\text{effective\_watermark} = \max(\text{epoch}, \text{last\_watermark} - \text{lookback\_window})$$
  2. The extraction engine scans rows where `event_timestamp >= effective_watermark`.
  3. **Idempotent Reconciliation**: Any overlapping rows already present in the warehouse are reconciled cleanly:
     - In `raw.raw_events`: `ON CONFLICT (event_id) DO NOTHING` ignores previously landed duplicates with zero storage cost.
     - In `raw.raw_orders` and `raw.raw_customers`: `ON CONFLICT (...) DO UPDATE` updates state only if the payload is newer (`WHERE EXCLUDED.source_updated_at >= existing.source_updated_at`).
  4. **Strict Forward Watermark Advance**: The watermark committed to `raw._pipeline_watermarks` strictly advances to the maximum observed timestamp, ensuring lookback scanning does not regress pipeline progress.

### Separation of Event Time vs. Ingestion Time
Every raw table contains two distinct temporal vectors:
* **Business Event Timestamp** (`order_timestamp`, `event_timestamp`, `source_updated_at`): Represents when the action occurred in the user's domain. Used for analytical windowing, partitioning, and watermark boundaries.
* **System Ingestion Timestamp** (`_ingested_at`): Set via `CURRENT_TIMESTAMP` on the warehouse host. Used for audit tracking, debugging ingestion latency, and ETL pipeline SLA monitoring.

---

## 5. Persistent Watermark Store

In-memory watermarks are a critical failure point in naive pipelines: if the container or process restarts, the watermark is lost, triggering either data gaps or massive duplicate reprocessing.

### Architectural Guarantees:
1. **Database-Backed State**: Watermarks are stored in `raw._pipeline_watermarks` with audit schema:
   ```sql
   CREATE TABLE raw._pipeline_watermarks (
       source_name         VARCHAR(64) PRIMARY KEY,
       last_watermark      TIMESTAMP WITH TIME ZONE NOT NULL,
       records_extracted   BIGINT NOT NULL DEFAULT 0,
       last_batch_id       VARCHAR(64),
       status              VARCHAR(16) NOT NULL DEFAULT 'SUCCESS',
       error_message       TEXT,
       last_success_at     TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
   );
   ```
2. **Crash Resilience**: If the database is temporarily unreachable, a dual-layer local JSON fallback (`data/watermarks.json`) ensures the high-water mark is never forgotten.
3. **Transactional Safety**: The watermark is updated **only after** data is successfully written to the raw table. If an error occurs, the status is set to `'FAILED'` with the error message logged, and the previous valid watermark is preserved.

---

## 6. How to Execute Ingestion

```bash
# 1. Ensure target warehouse is running on port 5433
# (Daemon managed automatically by task runner)

# 2. Run standard incremental pipeline across all sources
python -m src.ingest.pipeline

# 3. Run incremental extraction with a 60-minute lookback window for late-arriving data
python -m src.ingest.pipeline --lookback-minutes 60

# 4. Run full-refresh extraction (reload from epoch)
python -m src.ingest.pipeline --full-refresh

# 5. Run incremental ingestion for a single specific source
python -m src.ingest.pipeline --source orders

# 6. Verify pipeline idempotency (asserts 0-row delta upon consecutive runs)
python -m src.ingest.verify_idempotency

# 7. Execute CDC Mutation Proof & Out-of-Order Sequence Guard Simulation
python -m src.ingest.test_cdc_mutation

# 8. Run full integration test suite with pytest
pytest tests/test_ingestion.py -v
```
