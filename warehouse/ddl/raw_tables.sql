-- ==============================================================================
-- Raw Layer Schema & Ingestion Landing Tables
-- Target schema: raw
-- ==============================================================================

CREATE SCHEMA IF NOT EXISTS raw;

-- 1. Pipeline Persistent Watermark Store
CREATE TABLE IF NOT EXISTS raw._pipeline_watermarks (
    source_name             VARCHAR(64) PRIMARY KEY,
    last_watermark          TIMESTAMP WITH TIME ZONE NOT NULL,
    records_extracted       BIGINT NOT NULL DEFAULT 0,
    last_batch_id           VARCHAR(64),
    status                  VARCHAR(16) NOT NULL DEFAULT 'SUCCESS',
    error_message           TEXT,
    last_success_at         TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 2. Raw Customers Landing Table (CDC Target)
CREATE TABLE IF NOT EXISTS raw.raw_customers (
    customer_id             VARCHAR(64) PRIMARY KEY,
    first_name              VARCHAR(64),
    last_name               VARCHAR(64),
    email                   VARCHAR(255),
    country_code            VARCHAR(8),
    plan_tier               VARCHAR(32),
    account_status          VARCHAR(32),
    acquisition_channel     VARCHAR(64),
    source_updated_at       TIMESTAMP WITH TIME ZONE NOT NULL,
    _source_op              CHAR(1) NOT NULL DEFAULT 'I',     -- 'I' = Insert, 'U' = Update, 'D' = Delete
    _is_deleted             BOOLEAN NOT NULL DEFAULT FALSE,   -- Tombstone flag for soft-deleted records
    _batch_id               VARCHAR(64) NOT NULL,
    _ingested_at            TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 3. Raw Orders Landing Table (Watermark + Upsert Target)
CREATE TABLE IF NOT EXISTS raw.raw_orders (
    order_id                VARCHAR(64) PRIMARY KEY,
    customer_id             VARCHAR(64) NOT NULL,
    order_status            VARCHAR(32) NOT NULL,
    order_timestamp         TIMESTAMP WITH TIME ZONE NOT NULL,
    order_amount_usd        NUMERIC(12, 2) NOT NULL,
    discount_usd            NUMERIC(12, 2) DEFAULT 0.00,
    payment_method          VARCHAR(32),
    shipping_country        VARCHAR(8),
    source_updated_at       TIMESTAMP WITH TIME ZONE NOT NULL,
    _source_op              CHAR(1) NOT NULL DEFAULT 'I',
    _batch_id               VARCHAR(64) NOT NULL,
    _ingested_at            TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 4. Raw Subscriptions Landing Table (CDC & Lifecycle Target)
CREATE TABLE IF NOT EXISTS raw.raw_subscriptions (
    subscription_event_id   VARCHAR(64) PRIMARY KEY,
    subscription_id         VARCHAR(64) NOT NULL,
    customer_id             VARCHAR(64) NOT NULL,
    plan_tier               VARCHAR(32) NOT NULL,
    monthly_recurring_revenue NUMERIC(10, 2) NOT NULL,
    status                  VARCHAR(32) NOT NULL,
    billing_frequency       VARCHAR(16) NOT NULL,
    started_at              TIMESTAMP WITH TIME ZONE NOT NULL,
    cancelled_at            TIMESTAMP WITH TIME ZONE,
    event_timestamp         TIMESTAMP WITH TIME ZONE NOT NULL,
    _source_op              CHAR(1) NOT NULL DEFAULT 'I',
    _batch_id               VARCHAR(64) NOT NULL,
    _ingested_at            TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 5. Raw Telemetry Events Landing Table (Append-Only Target)
CREATE TABLE IF NOT EXISTS raw.raw_events (
    event_id                VARCHAR(64) PRIMARY KEY,
    customer_id             VARCHAR(64) NOT NULL,
    session_id              VARCHAR(64) NOT NULL,
    event_name              VARCHAR(64) NOT NULL,
    device_category         VARCHAR(32),
    operating_system        VARCHAR(32),
    page_path               VARCHAR(255),
    event_timestamp         TIMESTAMP WITH TIME ZONE NOT NULL,
    _batch_id               VARCHAR(64) NOT NULL,
    _ingested_at            TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_raw_orders_watermark ON raw.raw_orders (source_updated_at);
CREATE INDEX IF NOT EXISTS idx_raw_customers_watermark ON raw.raw_customers (source_updated_at);
CREATE INDEX IF NOT EXISTS idx_raw_events_watermark ON raw.raw_events (event_timestamp);
