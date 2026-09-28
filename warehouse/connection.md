# Warehouse Target & Connection Specification

This document details the configuration, schema architecture, access roles, and connectivity protocol for the Analytics Warehouse and remote source databases.

> [!IMPORTANT]
> **Zero Credentials in Git**: Never commit host passwords, API keys, or raw connection strings with secrets to this repository. All credentials must be injected dynamically via environment variables or a local `.env` file (which is strictly excluded via `.gitignore`).

---

## 1. Warehouse Architecture Overview

| Parameter | Specification | Notes |
| :--- | :--- | :--- |
| **Engine** | PostgreSQL 17 (or cloud equivalent: BigQuery / Snowflake) | Dedicated local cluster on port `5433` |
| **Default Port** | `5433` | Configurable via `WAREHOUSE_PORT` |
| **Target Database** | `analytics_dw` | Configurable via `WAREHOUSE_DB` |
| **Default User** | `pipeline_user` | Configurable via `WAREHOUSE_USER` |
| **Encoding** | `UTF8` | Standardized character set |
| **Timezone** | `UTC` | All timestamps normalized to UTC |

---

## 2. Schema Hierarchy & Boundaries

The warehouse adheres to a strict 3-tier medallion architecture:

```
[ Sources: Live Neon Cloud PostgreSQL & Parquet Landing ]
                           │
                           ▼
┌─────────────────────────────────────────────────────┐
│                     schema: raw                     │  <-- Raw ingestion tables, append-only staging, immutable
└──────────────────────────┬──────────────────────────┘
                           │
                           ▼ (dbt transformations)
┌─────────────────────────────────────────────────────┐
│                   schema: staging                   │  <-- Cleaned, cast, deduplicated views / ephemeral models
└──────────────────────────┬──────────────────────────┘
                           │
                           ▼ (dbt marts & SCD II)
┌─────────────────────────────────────────────────────┐
│                    schema: marts                    │  <-- Dimensional star schema (dim_customer, fact_orders, etc.)
└─────────────────────────────────────────────────────┘
```

1. **`raw`**:
   - **Purpose**: Direct landing destination for extracts (orders, subscriptions, user events).
   - **Guarantees**: Source fidelity preserved; columns mirror source formats; no business transformations applied.
2. **`staging`**:
   - **Purpose**: Intermediate dbt transformation models.
   - **Guarantees**: Standardized column naming (`snake_case`), explicit data casting, casing normalization, and deduplication.
3. **`marts`**:
   - **Purpose**: Business intelligence and analytics consumption layer.
   - **Guarantees**: Star schema design, SCD Type II history tracking, enforced grain, range-partitioned facts.

---

## 3. Remote Source Databases (Neon Cloud PostgreSQL)

The ingestion engine can connect directly to live cloud-hosted PostgreSQL instances:

| Source Domain | Database Engine | Host Provider | Schema | Auth Role |
| :--- | :--- | :--- | :--- | :--- |
| **E-Commerce** | PostgreSQL 16+ | Neon AWS (`ap-southeast-1`) | `ecom` | `ecom_ro_user` (Read-Only) |
| **SaaS Subscriptions** | PostgreSQL 16+ | Neon AWS (`ap-southeast-1`) | `saas` | `saas_ro_user` (Read-Only) |

### Source Connection Environment Variables
```bash
ECOM_SOURCE_URL=postgresql://ecom_ro_user:******@ep-bold-hall-azhf2f45-pooler.c-3.ap-southeast-1.aws.neon.tech:5432/neondb
SAAS_SOURCE_URL=postgresql://saas_ro_user:******@ep-bold-hall-azhf2f45-pooler.c-3.ap-southeast-1.aws.neon.tech:5432/neondb
```
*Note: Connections to Neon cloud require SSL encryption (`sslmode=require`). When remote URLs are not configured in `.env`, the ingestion pipeline falls back to compressed Parquet files in `data/raw/`.*

---

## 4. Role-Based Access Control (RBAC) Pattern

In a production environment, access is segregated into distinct roles:

| Role Name | Scope | Permissions |
| :--- | :--- | :--- |
| `loader_role` | Ingestion pipelines (`src/ingest`) | `USAGE`, `CREATE`, `INSERT` on `raw` schema |
| `transformer_role` | Transformation pipeline (`dbt`) | `SELECT` on `raw`; `ALL` on `staging` & `marts` |
| `analyst_role` | BI tools & downstream consumers | `USAGE`, `SELECT` strictly on `marts` schema |

*For local development and CI sandboxes, the single `pipeline_user` holds administration rights across all three schemas.*

---

## 5. Warehouse Connection Configuration

Warehouse connections are established using standard SQLAlchemy / JDBC connection URI formatting constructed at runtime from environment variables:

```text
postgresql://${WAREHOUSE_USER}:${WAREHOUSE_PASSWORD}@${WAREHOUSE_HOST}:${WAREHOUSE_PORT}/${WAREHOUSE_DB}
```

### Required Environment Variables

| Variable | Description | Example (Local Dev) |
| :--- | :--- | :--- |
| `WAREHOUSE_HOST` | Database server address | `localhost` |
| `WAREHOUSE_PORT` | Listening port | `5433` |
| `WAREHOUSE_DB` | Database catalog name | `analytics_dw` |
| `WAREHOUSE_USER` | Authorized pipeline username | `pipeline_user` |
| `WAREHOUSE_PASSWORD` | User secret / password | *(set in .env)* |

---

## 6. Verification Command

To verify active connection parameters against the live target:

```bash
python -m src.utils.test_connection
```
