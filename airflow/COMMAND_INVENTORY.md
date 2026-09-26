# Pipeline Command Inventory

This inventory documents the operational characteristics of each proven command before orchestration in Apache Airflow.

---

## 1. Raw Ingestion Command

* **Command**: `python -m src.ingest.pipeline` (or `python src/ingest/pipeline.py`)
* **Working Directory**: `E:\end-to-end-data-pipeline`
* **Required Environment Variables**:
  - `DB_HOST`: `localhost` (default)
  - `DB_PORT`: `5433` (PostgreSQL 17 cluster)
  - `DB_USER`: `pipeline_user`
  - `DB_PASSWORD`: `pipeline_secure_pass`
  - `DB_NAME`: `analytics_dw`
  - `PYTHONPATH`: `E:\end-to-end-data-pipeline`
* **Expected Inputs**:
  - Parquet landing files in `data/raw/`:
    - `customers/customers.parquet` (50,000 baseline records)
    - `orders/orders.parquet` (400,000 baseline records)
    - `subscriptions/subscriptions.parquet` (200,000 baseline records)
    - `events/events.parquet` (400,000 baseline records)
  - Existing watermark state in table `raw._pipeline_watermarks` or fallback `data/watermarks.json`
* **Expected Outputs**:
  - Rows landed in `raw.raw_customers`, `raw.raw_orders`, `raw.raw_subscriptions`, `raw.raw_events`
  - Updated watermark timestamps, batch IDs, and `SUCCESS` status in `raw._pipeline_watermarks`
* **Typical Runtime**: 8 to 12 seconds for 1,050,000 records
* **Failure Modes**:
  - Database connectivity refusal (e.g. port 5433 unreachable)
  - Parquet schema corruption or missing source file
  - Nonzero exit code `1` with logged traceback
* **Idempotency Guarantee**: **100% Idempotent**. Uses native PostgreSQL `ON CONFLICT (...) DO UPDATE` with monotonic sequence protection (`WHERE EXCLUDED.source_updated_at >= target.source_updated_at`). Running back-to-back produces a zero row delta.

---

## 2. dbt Transformation & Test Build Command

* **Command**: `dbt build --profiles-dir .`
* **Working Directory**: `E:\end-to-end-data-pipeline\dbt`
* **Required Environment Variables**:
  - `DB_HOST`, `DB_PORT`, `DB_USER`, `DB_PASSWORD`, `DB_NAME`
* **Expected Inputs**:
  - Clean rows in `raw` schema
  - Models in `dbt/models/staging/` and `dbt/models/marts/`
* **Expected Outputs**:
  - Views created in `staging`: `stg_customers`, `stg_orders`, `stg_subscriptions`, `stg_events`
  - Tables materialized in `marts`: `dim_date`, `dim_customer`, `fact_orders`, `fact_subscription_events`
  - 103 generic and singular data quality tests executed
  - 4 enforced schema contracts validated
* **Typical Runtime**: 22 to 26 seconds
* **Failure Modes**:
  - Schema contract mismatch (e.g. column data type or unexpected nulls)
  - Data test assertion failure (nonzero rows returned by singular test)
  - SQL syntax or relation dependency error
  - Nonzero exit code `1` with dbt error summary
* **Idempotency Guarantee**: **100% Idempotent**. Views are replaced atomically (`CREATE OR REPLACE VIEW`), and mart tables are reconstructed cleanly via temporary swap tables (`CREATE TABLE ...__dbt_tmp AS ...`). Re-running updates warehouse state deterministically.

---

## 3. Data-Quality & Reconciliation Assertion Command

* **Command**: `python -m src.utils.verify_all_steps`
* **Working Directory**: `E:\end-to-end-data-pipeline`
* **Required Environment Variables**:
  - `DB_HOST`, `DB_PORT`, `DB_USER`, `DB_PASSWORD`, `DB_NAME`, `PYTHONPATH`
* **Expected Inputs**:
  - Live PostgreSQL database tables across `raw` and `marts`
  - Parquet metadata in `data/raw/`
* **Expected Outputs**:
  - Reconciled row counts matching landing volume
  - Zero nulls in key identifiers
  - Watermark table health check
  - Return code `0` on total verification pass
* **Typical Runtime**: 3 to 5 seconds
* **Failure Modes**:
  - Unreconciled row counts between lake and warehouse
  - Missing required schemas or marts tables
  - Nonzero exit code `1`
* **Idempotency Guarantee**: **100% Idempotent** (read-only verification query suite).

---

## 4. Documentation Generation Command

* **Command**: `dbt docs generate --profiles-dir .`
* **Working Directory**: `E:\end-to-end-data-pipeline\dbt`
* **Required Environment Variables**:
  - `DB_HOST`, `DB_PORT`, `DB_USER`, `DB_PASSWORD`, `DB_NAME`
* **Expected Inputs**:
  - Compiled dbt manifest in `dbt/target/manifest.json`
  - Warehouse catalog metadata
* **Expected Outputs**:
  - `dbt/target/catalog.json`
  - `dbt/target/manifest.json`
  - `dbt/target/index.html` (interactive data dictionary & lineage graph)
* **Typical Runtime**: 4 to 6 seconds
* **Failure Modes**:
  - Missing compiled models or database permission error
  - Nonzero exit code `1`
* **Idempotency Guarantee**: **100% Idempotent** (overwrites static documentation JSON artifacts).
