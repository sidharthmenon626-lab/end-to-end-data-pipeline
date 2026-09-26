# Capstone Defense & Technical Interview Playbook

This document provides a battle-tested **10-minute presentation guide** and an **engineering defense playbook** with anticipated technical grilling questions for defending the End-to-End Analytics Data Platform.

---

## Part 1: 10-Minute Presentation Script & Slide Outline

### Minute 0:00 – 3:00 | Business Problem & Architectural Flow
* **Hook & Core Problem**:
  > *"Modern digital commerce businesses struggle with fractured customer data. When checkout orders, SaaS recurring subscriptions, and telemetry event streams live in separate silos, calculating true Customer Lifetime Value (LTV), cohort retention, and churn becomes error-prone and unreliable. Our mission was to engineer a unified, production-grade analytics data platform handling over 1,000,000 records that delivers a single source of truth with sub-minute batch latency."*
* **Architecture Walkthrough**:
  > *"The platform ingests 1,050,000 Parquet records across 4 independent sources into PostgreSQL 17. Rather than doing brute-force full table overwrites, we implemented a persistent high-watermark state engine in `raw._pipeline_watermarks` with a 30-minute lookback window to capture late-arriving events. Change Data Capture (CDC) mutations are deterministically sequenced by record timestamp, supporting inserts, in-place updates, and soft deletes via tombstones."*
* **Proof of Scale**:
  > *"Across all 4 raw tables, we reconciled 1,050,004 records with verified 0-delta idempotency on repeated executions."*

### Minute 3:00 – 5:00 | Kimball Dimensional Modeling & Data Contracts
* **Serving Layer Design**:
  > *"In the `marts` layer, we implemented a classic Kimball star schema: `dim_date` providing calendar attribution, `dim_customer` tracking historical profile mutations as a Slowly Changing Dimension (SCD Type II), and two high-volume fact tables: `fact_orders` (400,001 rows) and `fact_subscription_events` (200,000 rows). To prevent query degradation, both fact tables are monthly range-partitioned."*
* **Data Contracts & Quality Testing**:
  > *"Every staging and mart model enforces strict dbt schema contracts (`contract: {enforced: true}`). If a column type mismatches or a mandatory field contains a null, the build fails immediately. Across our models, we run 103 automated tests—including uniqueness, non-null, referential foreign key integrity, and business invariants like `discount_usd <= order_amount_usd`—with a 100% pass rate."*

### Minute 5:00 – 7:00 | Airflow 3 Orchestration & SRE Failure Resilience
* **Workflow Architecture**:
  > *"The end-to-end flow is orchestrated by an Apache Airflow 3 DAG (`analytics_end_to_end_pipeline`) with strict linear task dependencies: Ingestion → Staging → Marts → Data Contracts → Observability Monitoring."*
* **Fault Tolerance & Retries**:
  > *"To safeguard against transient database lock contention or network timeouts, tasks implement exponential backoff retries (`delay = 30s * 2^attempt`, max 5 minutes). If a task permanently exhausts its retry budget, an automated `on_failure_callback` triggers `log_pipeline_incident`, writing structured JSON diagnostic telemetry to `airflow/logs/incidents.log` for immediate SRE triage."*
* **Deterministic Backfills**:
  > *"Because our tasks are parameterized by `logical_date`, any historical time window can be backfilled idempotently without corrupting current warehouse state."*

### Minute 7:00 – 9:00 | Continuous Observability, Monitoring & CI/CD
* **13-Point Health Checks**:
  > *"We built an automated observability module (`src/monitoring.py`) that monitors warehouse health across 4 core operational pillars: Data Freshness, Volume Drift, Schema Integrity, and Runtime SLAs. It outputs terminal dashboards and saves machine-readable JSON to `data/monitoring_summary.json`."*
* **Proving Anomaly Detection**:
  > *"To ensure our monitoring is not a cosmetic dashboard, we built an anomaly testing suite (`--simulate-failure`) that injects stale timestamps, artificial volume drops, and null primary keys. The monitor successfully identified all 3 anomalies with a FAIL status, confirmed by automated pytest assertions."*
* **CI/CD Automation**:
  > *"Every push and pull request triggers a multi-stage GitHub Actions workflow: running Ruff linting, DAG compilation verification, PostgreSQL 17 container initialization, pytest suites, dbt contract builds, and observability validation."*

### Minute 9:00 – 10:00 | Business ROI & Future Scaling
* **Business Takeaway**:
  > *"By replacing ad-hoc reporting with an automated, contract-enforced platform running in ~38 seconds, we eliminated silent data corruption, delivered unified customer LTV analytics, and established a scalable foundation ready for cloud data warehouse migration as business volumes expand."*

---

## Part 2: Technical Q&A Grilling Preparation

### Question 1: "Why PostgreSQL instead of a dedicated columnar store like Snowflake or ClickHouse?"
* **Answer**:
  > *"For a dataset of ~1.05 million records (28 MB Parquet), PostgreSQL 17 provides an optimal balance of full ACID compliance, native table range-partitioning, and lightweight containerization without incurring cloud warehouse compute costs or minimum cluster overhead. More importantly, the transformation layer is written in standard SQL with dbt. If data volume grows to 100M+ rows, we can repoint `profiles.yml` to Snowflake, BigQuery, or ClickHouse with zero SQL refactoring."*

### Question 2: "How do you guarantee true idempotency when re-running ingestion?"
* **Answer**:
  > *"Idempotency is guaranteed through a dual-mechanism:
  > 1. Persistent watermarks stored in `raw._pipeline_watermarks` record the highest extracted timestamp.
  > 2. For entity tables (`customers`, `orders`), we use SQL `ON CONFLICT (id) DO UPDATE` (upsert) logic rather than blind append. If an identical batch is re-run, existing rows are updated to the exact same values without incrementing row counts. This was proven empirically with our `verify_idempotency.py` script showing exactly 0 row delta across 1,050,004 records."*

### Question 3: "How does SCD Type II in `dim_customer` prevent revenue attribution bias?"
* **Answer**:
  > *"In an e-commerce + SaaS model, a user might place an order while on the 'Starter' plan, and three months later upgrade to 'Enterprise'. Under SCD Type I (in-place overwrite), all historical orders are retroactively attributed to 'Enterprise', distorting historical marketing ROI and cohort economics. By maintaining `valid_from`, `valid_to`, `is_current`, and a surrogate key (`customer_sk`), `fact_orders` joins to the exact customer dimension record active at the moment of checkout (`order_timestamp BETWEEN valid_from AND valid_to`)."*

### Question 4: "What happens if a dbt contract fails or a source schema shifts unexpectedly?"
* **Answer**:
  > *"Because dbt model contracts are enforced (`contract: {enforced: true}`), dbt validates column names and data types before executing DDL. If upstream source parquet files introduce an incompatible data type or missing column, dbt aborts the model build immediately with a clear error. In Airflow, this halts downstream mart transformations, activates the exponential retry policy, and if unrecovered, triggers the `on_failure_callback` to log an incident, preventing corrupted data from entering serving marts."*

### Question 5: "How does your monitoring module distinguish between normal fluctuations and genuine data drift?"
* **Answer**:
  > *"Our volume check uses a calibrated percentage variance threshold (`+-5.0%`) calculated against historical baseline counts in `data/manifest.json`. Minor day-to-day variances within 5% trigger a `PASS`. A shift between 5% and 20% triggers a `WARN` for investigation, and changes beyond 20% trigger a `FAIL`. Freshness evaluates the delta between the logical execution timestamp and the latest record timestamp against a 24-hour threshold."*

### Question 6: "How does Airflow 3 differ from Airflow 2 in this project?"
* **Answer**:
  > *"Airflow 3 introduces a modernized scheduler with task-level isolation, standard task runners, and strict POSIX/Windows execution boundaries. In our Windows development environment, we implemented runtime compatibility shims for non-POSIX socket inheritance and signal handlers, while maintaining full compatibility with Airflow 3's task SDK and incident callback APIs."*
