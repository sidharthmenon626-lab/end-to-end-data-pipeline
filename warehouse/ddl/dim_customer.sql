-- ==============================================================================
-- Table: marts.dim_customer
-- Description: Slowly Changing Dimension (SCD Type II) tracking customer profiles.
-- Grain: Exactly one row per customer version for a contiguous validity window.
-- ==============================================================================

CREATE SCHEMA IF NOT EXISTS marts;

DROP TABLE IF EXISTS marts.dim_customer CASCADE;

CREATE TABLE marts.dim_customer (
    customer_sk             BIGINT NOT NULL,                  -- Surrogate key unique to each profile version (-1 for unknown)
    customer_id             VARCHAR(64) NOT NULL,             -- Natural / business key from source systems
    first_name              VARCHAR(64),                      -- Customer given name
    last_name               VARCHAR(64),                      -- Customer surname
    email                   VARCHAR(255),                     -- Contact email (hashed or raw)
    country_code            VARCHAR(8),                       -- ISO-3166-1 alpha-2 / alpha-3 country code
    plan_tier               VARCHAR(32) NOT NULL DEFAULT 'FREE', -- SaaS subscription tier (FREE, PRO, ENTERPRISE)
    account_status          VARCHAR(32) NOT NULL DEFAULT 'ACTIVE', -- ACTIVE, CHURNED, SUSPENDED, DELETED
    acquisition_channel     VARCHAR(64),                      -- Marketing channel (Organic, Paid Search, Referral, etc.)
    first_seen_timestamp    TIMESTAMP WITH TIME ZONE,         -- Initial touchpoint / registration timestamp
    valid_from              TIMESTAMP WITH TIME ZONE NOT NULL,-- Start of validity window for this record version
    valid_to                TIMESTAMP WITH TIME ZONE,         -- End of validity window (NULL indicates currently active)
    is_current              BOOLEAN NOT NULL DEFAULT TRUE,    -- Flag indicating the latest active customer state
    created_at              TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_dim_customer PRIMARY KEY (customer_sk),
    CONSTRAINT chk_dim_customer_validity_window CHECK (valid_to IS NULL OR valid_to > valid_from)
);

-- Comments on table and attributes
COMMENT ON TABLE marts.dim_customer IS 'SCD Type II customer dimension preserving full historical fidelity of profile and subscription tier migrations.';
COMMENT ON COLUMN marts.dim_customer.customer_sk IS 'Surrogate primary key generated per record version; -1 reserved for orphan / unassigned facts.';
COMMENT ON COLUMN marts.dim_customer.valid_from IS 'Point-in-time timestamp marking when this customer attribute state became active.';
COMMENT ON COLUMN marts.dim_customer.valid_to IS 'Point-in-time timestamp marking when this state was superseded; NULL if currently active.';
COMMENT ON COLUMN marts.dim_customer.is_current IS 'Boolean flag: TRUE indicates the single active version for this customer_id.';

-- Partial Unique Index: Guarantees database-level invariant that a customer can NEVER have >1 active record
CREATE UNIQUE INDEX IF NOT EXISTS uq_dim_customer_active_version 
ON marts.dim_customer (customer_id) 
WHERE is_current = TRUE;

-- B-Tree Index for Point-in-Time Fact Joins
CREATE INDEX IF NOT EXISTS idx_dim_customer_pit 
ON marts.dim_customer (customer_id, valid_from, valid_to);

-- Seed unknown / unassigned record to prevent orphan fact drops
INSERT INTO marts.dim_customer (
    customer_sk,
    customer_id,
    first_name,
    last_name,
    email,
    country_code,
    plan_tier,
    account_status,
    acquisition_channel,
    first_seen_timestamp,
    valid_from,
    valid_to,
    is_current
) VALUES (
    -1,
    'UNKNOWN',
    'Unassigned',
    'Customer',
    'unknown@example.com',
    'XX',
    'UNKNOWN',
    'UNKNOWN',
    'UNKNOWN',
    '1970-01-01 00:00:00+00',
    '1970-01-01 00:00:00+00',
    NULL,
    TRUE
) ON CONFLICT (customer_sk) DO NOTHING;
