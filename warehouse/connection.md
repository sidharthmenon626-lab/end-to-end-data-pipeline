# Warehouse Target & Connection Specification

This document details the configuration, schema architecture, access roles, and connectivity protocol for the Analytics Warehouse. 

> [!IMPORTANT]
> **Zero Credentials in Git**: Never commit host passwords, API keys, or raw connection strings with secrets to this repository. All credentials must be injected dynamically via environment variables or a local `.env` file (which is excluded via `.gitignore`).

---

## 1. Warehouse Architecture Overview

| Parameter | Specification | Notes |
| :--- | :--- | :--- |
| **Engine** | PostgreSQL 16 (or cloud equivalent: BigQuery / Snowflake) | Packaged locally via Docker container |
| **Default Port** | `5432` | Configurable via `WAREHOUSE_PORT` |
| **Target Database** | `analytics_dw` | Configurable via `WAREHOUSE_DB` |
| **Default User** | `pipeline_user` | Configurable via `WAREHOUSE_USER` |
| **Encoding** | `UTF8` | Standardized character set |
| **Timezone** | `UTC` | All timestamps stored in UTC |

---

## 2. Schema Hierarchy & Boundaries

The warehouse adheres to a strict 3-tier medallion architecture:

```
[ Data Landing: data/raw/ ]
            │
            ▼
┌─────────────────────────┐
│       schema: raw       │  <-- Raw ingestion tables, append-only staging, immutable
└───────────┬─────────────┘
            │
            ▼ (dbt transformations)
┌─────────────────────────┐
│     schema: staging     │  <-- Cleaned, cast, deduplicated views / ephemeral models
└───────────┬─────────────┘
            │
            ▼ (dbt marts & SCD II)
┌─────────────────────────┐
│      schema: marts      │  <-- Dimensional star schema (dim_customer, fact_orders, etc.)
└─────────────────────────┘
```

1. **`raw`**:
   - **Purpose**: Direct landing destination for extracts (orders, subscriptions, user events).
   - **Guarantees**: Source fidelity preserved; columns mirror source formats; no business transformations applied.
2. **`staging`**:
   - **Purpose**: intermediate dbt transformation models.
   - **Guarantees**: Standardized column naming (`snake_case`), explicit data casting, deduplication.
3. **`marts`**:
   - **Purpose**: Business intelligence and analytics consumption layer.
   - **Guarantees**: Star schema design, SCD Type II history tracking, enforced grain, partition/cluster keys.

---

## 3. Role-Based Access Control (RBAC) Pattern

In a production environment, access is segregated into distinct roles:

| Role Name | Scope | Permissions |
| :--- | :--- | :--- |
| `loader_role` | Ingestion pipelines (`src/ingest`) | `USAGE`, `CREATE`, `INSERT` on `raw` schema |
| `transformer_role` | Transformation pipeline (`dbt`) | `SELECT` on `raw`; `ALL` on `staging` & `marts` |
| `analyst_role` | BI tools & downstream consumers | `USAGE`, `SELECT` strictly on `marts` schema |

*For local development and CI sandboxes, the single `pipeline_user` holds administration rights across all three schemas.*

---

## 4. Connection String Configuration

Connections are established using standard SQLAlchemy / JDBC connection URI formatting constructed at runtime from environment variables:

```text
postgresql://${WAREHOUSE_USER}:${WAREHOUSE_PASSWORD}@${WAREHOUSE_HOST}:${WAREHOUSE_PORT}/${WAREHOUSE_DB}
```

### Required Environment Variables

| Variable | Description | Example (Local Dev) |
| :--- | :--- | :--- |
| `WAREHOUSE_HOST` | Database server address | `localhost` |
| `WAREHOUSE_PORT` | Listening port | `5432` |
| `WAREHOUSE_DB` | Database catalog name | `analytics_dw` |
| `WAREHOUSE_USER` | Authorized pipeline username | `pipeline_user` |
| `WAREHOUSE_PASSWORD` | User secret / password | *(set in .env)* |

---

## 5. Verification Command

To verify your active connection parameters against the live target:

```bash
python -m src.utils.test_connection
```
