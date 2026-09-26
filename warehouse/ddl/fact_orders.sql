-- ==============================================================================
-- Table: marts.fact_orders
-- Description: Core transaction fact table capturing ecommerce purchases.
-- Grain: Exactly one row per completed order transaction.
-- Partitioning: Declarative range partitioning by order_timestamp (monthly).
-- ==============================================================================

CREATE SCHEMA IF NOT EXISTS marts;

DROP TABLE IF EXISTS marts.fact_orders CASCADE;

CREATE TABLE marts.fact_orders (
    order_id                VARCHAR(64) NOT NULL,             -- Unique business order identifier
    order_timestamp         TIMESTAMP WITH TIME ZONE NOT NULL,-- Exact UTC transaction timestamp (Partition Key)
    order_date_key          INTEGER NOT NULL,                 -- Foreign key to dim_date (YYYYMMDD)
    customer_sk             BIGINT NOT NULL,                  -- Foreign key to dim_customer(customer_sk) at transaction time
    customer_id             VARCHAR(64) NOT NULL,             -- Degenerate dimension key for direct customer lookups
    order_status            VARCHAR(32) NOT NULL,             -- COMPLETED, REFUNDED, CANCELLED, PENDING
    payment_method          VARCHAR(32),                      -- CREDIT_CARD, PAYPAL, STRIPE, UPI
    shipping_country        VARCHAR(8),                       -- Shipping country code
    item_count              INTEGER NOT NULL DEFAULT 1,       -- Number of items in order
    gross_amount_usd        NUMERIC(12, 2) NOT NULL,          -- Subtotal before discounts & taxes
    discount_amount_usd     NUMERIC(12, 2) NOT NULL DEFAULT 0.00, -- Promotional discount
    net_amount_usd          NUMERIC(12, 2) NOT NULL,          -- gross_amount_usd - discount_amount_usd
    tax_amount_usd          NUMERIC(12, 2) NOT NULL DEFAULT 0.00, -- Sales tax / VAT
    total_amount_usd        NUMERIC(12, 2) NOT NULL,          -- net_amount_usd + tax_amount_usd
    created_at              TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,

    -- In PostgreSQL declarative partitioning, PRIMARY KEY must contain all partition columns
    CONSTRAINT pk_fact_orders PRIMARY KEY (order_id, order_timestamp),
    CONSTRAINT chk_fact_orders_gross_positive CHECK (gross_amount_usd >= 0),
    CONSTRAINT chk_fact_orders_net_calc CHECK (net_amount_usd = gross_amount_usd - discount_amount_usd)
) PARTITION BY RANGE (order_timestamp);

-- Comments on table and attributes
COMMENT ON TABLE marts.fact_orders IS 'Range-partitioned transactional fact table recording ecommerce order financial activity.';
COMMENT ON COLUMN marts.fact_orders.order_id IS 'Business key identifying the order.';
COMMENT ON COLUMN marts.fact_orders.order_timestamp IS 'Transaction timestamp in UTC; partition pruning column.';
COMMENT ON COLUMN marts.fact_orders.customer_sk IS 'Surrogate key linking to the point-in-time version in dim_customer.';

-- ------------------------------------------------------------------------------
-- Partitions: Default Catch-All + Monthly Ranges
-- ------------------------------------------------------------------------------

-- Default partition ensures unexpected dates or historical backfills never fail the ingestion pipeline
CREATE TABLE IF NOT EXISTS marts.fact_orders_default 
PARTITION OF marts.fact_orders DEFAULT;

-- Sample Monthly Partitions covering typical execution windows
CREATE TABLE IF NOT EXISTS marts.fact_orders_2025_h2 
PARTITION OF marts.fact_orders 
FOR VALUES FROM ('2025-07-01 00:00:00+00') TO ('2026-01-01 00:00:00+00');

CREATE TABLE IF NOT EXISTS marts.fact_orders_2026_01 
PARTITION OF marts.fact_orders 
FOR VALUES FROM ('2026-01-01 00:00:00+00') TO ('2026-02-01 00:00:00+00');

CREATE TABLE IF NOT EXISTS marts.fact_orders_2026_02 
PARTITION OF marts.fact_orders 
FOR VALUES FROM ('2026-02-01 00:00:00+00') TO ('2026-03-01 00:00:00+00');

CREATE TABLE IF NOT EXISTS marts.fact_orders_2026_03 
PARTITION OF marts.fact_orders 
FOR VALUES FROM ('2026-03-01 00:00:00+00') TO ('2026-04-01 00:00:00+00');

CREATE TABLE IF NOT EXISTS marts.fact_orders_2026_04 
PARTITION OF marts.fact_orders 
FOR VALUES FROM ('2026-04-01 00:00:00+00') TO ('2026-05-01 00:00:00+00');

CREATE TABLE IF NOT EXISTS marts.fact_orders_2026_05 
PARTITION OF marts.fact_orders 
FOR VALUES FROM ('2026-05-01 00:00:00+00') TO ('2026-06-01 00:00:00+00');

CREATE TABLE IF NOT EXISTS marts.fact_orders_2026_06 
PARTITION OF marts.fact_orders 
FOR VALUES FROM ('2026-06-01 00:00:00+00') TO ('2026-07-01 00:00:00+00');

-- ------------------------------------------------------------------------------
-- Indexes: Automatically propagated to all underlying partitions
-- ------------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_fact_orders_customer_sk ON marts.fact_orders (customer_sk, order_timestamp);
CREATE INDEX IF NOT EXISTS idx_fact_orders_date_key ON marts.fact_orders (order_date_key);
CREATE INDEX IF NOT EXISTS idx_fact_orders_status ON marts.fact_orders (order_status);
