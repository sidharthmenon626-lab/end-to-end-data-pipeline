# End-to-End Analytics Data Platform

[![CI Pipeline](https://github.com/yourusername/end-to-end-data-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/yourusername/end-to-end-data-pipeline/actions)
[![dbt Test](https://img.shields.io/badge/dbt-passing-16a34a.svg)](https://docs.getdbt.com/)
[![Airflow DAG](https://img.shields.io/badge/Airflow-orchestrated-0284c7.svg)](https://airflow.apache.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

> A production-grade analytics platform unifying ecommerce transactions, SaaS subscription lifecycles, and client telemetry events into an analytics-ready dimensional warehouse. Engineered for ~1,000,000 records with incremental CDC loading, robust star schema modeling (SCD Type II), automated data quality enforcement, and Airflow orchestration.

---

## Architecture Overview

```
                                      ARCHITECTURE DIAGRAM
 ┌──────────────────────┐   ┌──────────────────────┐   ┌──────────────────────┐
 │  E-Commerce Orders   │   │  SaaS Subscriptions  │   │  Telemetry Events    │
 │     (~400k rows)     │   │     (~200k rows)     │   │     (~400k rows)     │
 └──────────┬───────────┘   └──────────┬───────────┘   └──────────┬───────────┘
            │                          │                          │
            └──────────────────────────┼──────────────────────────┘
                                       │
                                       ▼ (src/ingest: Watermark + CDC)
                            ┌─────────────────────┐
                            │      RAW LAYER      │
                            │   (PostgreSQL DW)   │
                            └──────────┬──────────┘
                                       │
                                       ▼ (dbt: Staging & Typecasting)
                            ┌─────────────────────┐
                            │    STAGING LAYER    │
                            │  (Clean Views/CTEs) │
                            └──────────┬──────────┘
                                       │
                                       ▼ (dbt: Marts, Contracts & SCD II)
                            ┌─────────────────────┐
                            │     MARTS LAYER     │
                            │ (Star Schema Model) │
                            └──────────┬──────────┘
                                       │
                  ┌────────────────────┴────────────────────┐
                  ▼                                         ▼
       ┌───────────────────────┐                 ┌───────────────────────┐
       │   Analytics & BI      │                 │ Data Quality & Alerts │
       │ (Revenue, LTV, Churn) │                 │  (Monitoring Checks)  │
       └───────────────────────┘                 └───────────────────────┘
```

---

## Project Highlights & Business Scope

* **Unified Customer View**: Joins high-velocity ecommerce order transactional flows with recurring SaaS subscription lifecycles.
* **Volume Handling**: Engineered for **1,000,000+ rows**, deliberately addressing data skew, boundary edge cases, and performance tuning that break toy sandboxes.
* **Change Data Capture (CDC)**: Incremental ingestion with watermark state persistence and mutation capture (`INSERT`/`UPDATE`/`DELETE`).
* **SCD Type II Modeling**: Maintains complete historical fidelity of changing customer profiles and tier migrations (`valid_from`, `valid_to`, `is_current`).
* **Contract-Enforced Transformations**: All marts governed by dbt schema contracts, unique/not-null tests, and custom singular assertion tests.
* **Production Orchestration**: End-to-end Airflow DAG with task retries, exponential backoffs, and idempotent backfill support.

---

## Repository Structure

```text
end-to-end-data-pipeline/
├── .github/workflows/      # Automated CI testing (linting, dbt test, DAG verification)
├── airflow/
│   ├── dags/               # Production pipeline DAGs
│   └── BACKFILL.md         # Documented historical backfill procedures
├── dbt/
│   ├── models/             # Staging views & Star Schema dimensional marts
│   └── tests/              # Singular tests verifying business invariants
├── data/
│   ├── raw/                # Landing zone for raw partitioned extracts
│   └── manifest.json       # Landing manifest & record reconciliation counts
├── docs/
│   └── modeling.md         # Star schema design, SCD II trade-offs, grain statements
├── findings/               # Executive memos and milestone findings
├── src/
│   ├── ingest/             # Extractors, watermark managers, and CDC loaders
│   ├── utils/              # Database connection pools and loggers
│   └── monitoring.py       # Data freshness and row-count drift monitoring
├── warehouse/
│   ├── ddl/                # Mart table schemas and indexing specifications
│   ├── init.sql            # Container database initialization script
│   └── connection.md       # Target warehouse credentials & role mapping
├── docker-compose.yml      # Local containerized PostgreSQL warehouse
├── requirements.txt        # Python dependency manifest
└── README.md               # Repository documentation
```

---

## Quickstart & Local Setup

### 1. Prerequisites
* Python 3.10+
* Docker & Docker Compose
* Git

### 2. Environment Configuration
Clone the repository and copy the environment template:
```bash
cp .env.example .env
```
*(Customize passwords or ports inside `.env` if desired; default local ports are configured for immediate startup).*

### 3. Spin Up Warehouse Container
Start the local PostgreSQL analytics warehouse:
```bash
docker compose up -d
```

### 4. Install Dependencies
```bash
python -m venv .venv
# Linux / macOS
source .venv/bin/activate
# Windows PowerShell
.venv\Scripts\Activate.ps1

pip install -r requirements.txt
```

### 5. Verify Target Connectivity
Run the pre-flight connection verification script:
```bash
python -m src.utils.test_connection
```

---

## Milestone Progress

- [x] **Milestone 1**: Repo setup + warehouse target + dataset landing manifest
- [ ] **Milestone 2**: Model the marts — star schema, SCD Type II, partitioning
- [ ] **Milestone 3**: Ingestion — incremental loads + CDC
- [ ] **Milestone 4**: Transform with dbt — models, tests, docs, contracts
- [ ] **Milestone 5**: Orchestrate with Airflow — DAG, retries, backfills
- [ ] **Milestone 6**: CI + observability + executive summary + live defense

---

## License
MIT License.
