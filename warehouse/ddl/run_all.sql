-- ==============================================================================
-- Master Marts DDL Execution Script
-- Executes all dimensional and fact table definitions in strict dependency order.
-- ==============================================================================

BEGIN;

-- 1. Ensure serving schema exists
CREATE SCHEMA IF NOT EXISTS marts;

-- 2. Execute Dimensions
\ir dim_date.sql
\ir dim_customer.sql

-- 3. Execute Range-Partitioned Facts
\ir fact_orders.sql
\ir fact_subscription_events.sql

COMMIT;
