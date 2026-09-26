# Ingestion Architecture & Strategy Specification

## 1. Executive Strategy Overview
The ingestion layer forms the foundational boundary of the analytics platform, pulling raw records across ecommerce transactions, subscription contracts, and digital telemetry events into the warehouse `raw` schema. 

To achieve maximum throughput and cost efficiency over **1,000,000+ records**, we reject naive full-table scans. Instead, every source dataset is mapped to an explicit ingestion pattern based on its volatility, mutation profile, and volume:

| Source Dataset | Ingestion Pattern | Primary Tracking Key | Mutation Profile | Rationale |
| :--- | :--- | :--- | :--- | :--- |
| **`customers`** | **Change Data Capture (CDC)** | `customer_id` + `source_updated_at` | Highly mutable (`INSERT`, `UPDATE`, `DELETE`) | Customer profiles evolve (tier upgrades, contact changes, churn). Full refresh is too expensive and loses change history; strict CDC merges capture updates and soft-delete tombstones. |
| **`ecommerce_orders`** | **Incremental Watermark + Upsert** | `order_id` + `source_updated_at` | Append-mostly with status mutations | Orders are primarily append-only transactions, but orders transition states (`PENDING` $\rightarrow$ `COMPLETED` $\rightarrow$ `REFUNDED`). Watermark filters new changes; upsert prevents duplicates. |
| **`saas_subscriptions`** | **CDC + Lifecycle Events** | `subscription_event_id` + `event_timestamp` | State transitions + MRR deltas | Subscription transitions are discrete lifecycle events. Captured with watermarking to record upgrades/downgrades while updating contract state. |
| **`customer_events`** | **Append-Only Watermark Incremental** | `event_id` + `event_timestamp` | 100% Immutable Append-Only | High-volume client telemetry (~400,000 records). Events are immutable once generated. Strict watermark append (`ON CONFLICT DO NOTHING`) avoids costly upsert locks. |

---

## 2. Ingestion Patterns Deep Dive

### Pattern A: Change Data Capture (CDC)
* **Target Table**: `raw.raw_customers`
* **Mechanism**: Captures operational mutations from CRM/auth databases. Each extracted record includes operational metadata:
  * `_source_op`: `'I'` (Insert), `'U'` (Update), or `'D'` (Delete).
  * `_is_deleted`: Boolean tombstone flag set to `TRUE` when an entity is deleted in source.
  * `_ingested_at`: Wall-clock UTC timestamp indicating when the warehouse captured the row.
* **Database Merge**: Uses PostgreSQL `INSERT ... ON CONFLICT (customer_id) DO UPDATE` to maintain the single accurate current state in the `raw` layer while preserving audit metadata for downstream dbt SCD Type II snapshots.

### Pattern B: Incremental Watermark Extraction with Upsert
* **Target Table**: `raw.raw_orders`
* **Mechanism**: 
  1. Queries `raw._pipeline_watermarks` for the last committed `source_updated_at`.
  2. Extracts rows where `source_updated_at > last_watermark`.
  3. Inserts into `raw.raw_orders` with an `ON CONFLICT (order_id) DO UPDATE` clause.
  4. Commits the new watermark in the same transaction.

### Pattern C: High-Throughput Append-Only Watermarking
* **Target Table**: `raw.raw_events`
* **Mechanism**: High-volume telemetry stream (~400k events). Since clickstream events never mutate, we bypass lock-heavy merge operations in favor of lightweight bulk inserts with `ON CONFLICT (event_id) DO NOTHING`.

---

## 3. Persistent Watermark Management

In-memory watermarks are a critical failure point in naive pipelines: if the container or process restarts, the watermark is lost, triggering either data gaps or massive duplicate reprocessing.

### Architectural Guarantees:
1. **Database-Backed State**: Watermarks are stored in `raw._pipeline_watermarks` within the warehouse.
2. **Crash Resilience**: If the database is temporarily unreachable, a dual-layer local JSON fallback (`data/watermarks.json`) ensures the high-water mark is never forgotten.
3. **Transactional Safety**: In production runs, the watermark update statement executes within the same atomic transaction as the data upsert. If the batch fails at row 99,999, the watermark rolls back automatically.

---

## 4. How to Execute Ingestion

```bash
# 1. Ensure target warehouse is running
docker compose up -d

# 2. Run full pipeline across all sources
python -m src.ingest.pipeline

# 3. Run incremental extraction for a single source
python -m src.ingest.pipeline --source orders

# 4. Verify pipeline idempotency
python -m src.ingest.verify_idempotency

# 5. Execute CDC Mutation Proof Simulation
python -m src.ingest.test_cdc_mutation
```
