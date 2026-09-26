-- ==============================================================================
-- Table: marts.fact_subscription_events
-- Description: Event fact table capturing SaaS subscription lifecycle transitions.
-- Grain: Exactly one row per discrete subscription lifecycle transition.
-- Partitioning: Declarative range partitioning by event_timestamp (monthly).
-- ==============================================================================

CREATE SCHEMA IF NOT EXISTS marts;

DROP TABLE IF EXISTS marts.fact_subscription_events CASCADE;

CREATE TABLE marts.fact_subscription_events (
    subscription_event_id   VARCHAR(64) NOT NULL,             -- Unique event identifier
    event_timestamp         TIMESTAMP WITH TIME ZONE NOT NULL,-- Exact UTC event timestamp (Partition Key)
    event_date_key          INTEGER NOT NULL,                 -- Foreign key to dim_date (YYYYMMDD)
    subscription_id         VARCHAR(64) NOT NULL,             -- Contract / subscription identifier
    customer_sk             BIGINT NOT NULL,                  -- Foreign key to dim_customer(customer_sk)
    customer_id             VARCHAR(64) NOT NULL,             -- Degenerate dimension key
    event_type              VARCHAR(32) NOT NULL,             -- SIGNUP, RENEWAL, UPGRADE, DOWNGRADE, CHURN, REACTIVATION
    previous_plan_tier      VARCHAR(32),                      -- Prior tier (NULL on new SIGNUP)
    new_plan_tier           VARCHAR(32) NOT NULL,             -- Current active tier after transition
    billing_frequency       VARCHAR(16) NOT NULL,             -- MONTHLY, ANNUAL
    previous_mrr_usd        NUMERIC(10, 2) NOT NULL DEFAULT 0.00, -- Monthly Recurring Revenue prior to event
    new_mrr_usd             NUMERIC(10, 2) NOT NULL,          -- Monthly Recurring Revenue after event
    mrr_delta_usd           NUMERIC(10, 2) NOT NULL,          -- new_mrr_usd - previous_mrr_usd
    days_active_prior       INTEGER DEFAULT 0,                -- Tenure in days prior to event
    created_at              TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,

    -- Composite primary key required by PostgreSQL partitioned table semantics
    CONSTRAINT pk_fact_subscription_events PRIMARY KEY (subscription_event_id, event_timestamp),
    CONSTRAINT chk_fact_sub_event_type CHECK (event_type IN ('SIGNUP', 'RENEWAL', 'UPGRADE', 'DOWNGRADE', 'CHURN', 'REACTIVATION')),
    CONSTRAINT chk_fact_sub_mrr_delta CHECK (mrr_delta_usd = new_mrr_usd - previous_mrr_usd)
) PARTITION BY RANGE (event_timestamp);

-- Comments on table and attributes
COMMENT ON TABLE marts.fact_subscription_events IS 'Range-partitioned event fact table recording subscription upgrades, churn, and MRR changes.';
COMMENT ON COLUMN marts.fact_subscription_events.event_type IS 'Lifecycle transition category (SIGNUP, UPGRADE, DOWNGRADE, CHURN).';
COMMENT ON COLUMN marts.fact_subscription_events.mrr_delta_usd IS 'Net change in Monthly Recurring Revenue produced by this transition.';

-- ------------------------------------------------------------------------------
-- Partitions: Default Catch-All + Monthly Ranges
-- ------------------------------------------------------------------------------

-- Default partition ensures safety against out-of-range dates during backfills
CREATE TABLE IF NOT EXISTS marts.fact_subscription_events_default 
PARTITION OF marts.fact_subscription_events DEFAULT;

-- Sample Monthly Partitions
CREATE TABLE IF NOT EXISTS marts.fact_subscription_events_2025_h2 
PARTITION OF marts.fact_subscription_events 
FOR VALUES FROM ('2025-07-01 00:00:00+00') TO ('2026-01-01 00:00:00+00');

CREATE TABLE IF NOT EXISTS marts.fact_subscription_events_2026_01 
PARTITION OF marts.fact_subscription_events 
FOR VALUES FROM ('2026-01-01 00:00:00+00') TO ('2026-02-01 00:00:00+00');

CREATE TABLE IF NOT EXISTS marts.fact_subscription_events_2026_02 
PARTITION OF marts.fact_subscription_events 
FOR VALUES FROM ('2026-02-01 00:00:00+00') TO ('2026-03-01 00:00:00+00');

CREATE TABLE IF NOT EXISTS marts.fact_subscription_events_2026_03 
PARTITION OF marts.fact_subscription_events 
FOR VALUES FROM ('2026-03-01 00:00:00+00') TO ('2026-04-01 00:00:00+00');

CREATE TABLE IF NOT EXISTS marts.fact_subscription_events_2026_04 
PARTITION OF marts.fact_subscription_events 
FOR VALUES FROM ('2026-04-01 00:00:00+00') TO ('2026-05-01 00:00:00+00');

CREATE TABLE IF NOT EXISTS marts.fact_subscription_events_2026_05 
PARTITION OF marts.fact_subscription_events 
FOR VALUES FROM ('2026-05-01 00:00:00+00') TO ('2026-06-01 00:00:00+00');

CREATE TABLE IF NOT EXISTS marts.fact_subscription_events_2026_06 
PARTITION OF marts.fact_subscription_events 
FOR VALUES FROM ('2026-06-01 00:00:00+00') TO ('2026-07-01 00:00:00+00');

-- ------------------------------------------------------------------------------
-- Indexes: Automatically propagated to all underlying partitions
-- ------------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_fact_sub_customer_sk ON marts.fact_subscription_events (customer_sk, event_timestamp);
CREATE INDEX IF NOT EXISTS idx_fact_sub_type ON marts.fact_subscription_events (event_type, event_timestamp);
CREATE INDEX IF NOT EXISTS idx_fact_sub_subscription_id ON marts.fact_subscription_events (subscription_id);
