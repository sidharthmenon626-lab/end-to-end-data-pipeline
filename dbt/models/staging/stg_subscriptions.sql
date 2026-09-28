WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_subscriptions') }}
),

renamed AS (
    SELECT
        subscription_event_id::VARCHAR(64) AS subscription_event_id,
        subscription_id::VARCHAR(64) AS subscription_id,
        customer_id::VARCHAR(64) AS customer_id,
        CASE 
            WHEN UPPER(TRIM(plan_tier)) IN ('PRO', 'PROFESSIONAL') THEN 'PRO'
            WHEN UPPER(TRIM(plan_tier)) IN ('ENTERPRISE') THEN 'ENTERPRISE'
            WHEN UPPER(TRIM(plan_tier)) IN ('STARTER') THEN 'STARTER'
            WHEN UPPER(TRIM(plan_tier)) IN ('FREE') THEN 'FREE'
            ELSE 'STARTER'
        END::VARCHAR(32) AS plan_tier,
        monthly_recurring_revenue::NUMERIC(12, 2) AS mrr_amount_usd,
        CASE 
            WHEN UPPER(TRIM(status)) IN ('ACTIVE') THEN 'ACTIVE'
            WHEN UPPER(TRIM(status)) IN ('PAST_DUE') THEN 'PAST_DUE'
            WHEN UPPER(TRIM(status)) IN ('TRIALING') THEN 'TRIALING'
            WHEN UPPER(TRIM(status)) IN ('CHURNED', 'CANCELLED') THEN 'CANCELLED'
            WHEN UPPER(TRIM(status)) IN ('PAUSED', 'EXPIRED') THEN 'EXPIRED'
            ELSE 'ACTIVE'
        END::VARCHAR(32) AS subscription_status,
        UPPER(TRIM(billing_frequency))::VARCHAR(32) AS billing_frequency,
        CASE 
            WHEN cancelled_at IS NOT NULL AND cancelled_at < started_at THEN cancelled_at 
            ELSE started_at 
        END::TIMESTAMPTZ AS started_at,
        cancelled_at::TIMESTAMPTZ AS cancelled_at,
        event_timestamp::TIMESTAMPTZ AS event_timestamp,
        DATE(event_timestamp) AS event_date,
        CASE 
            WHEN UPPER(TRIM(status)) = 'ACTIVE' AND (cancelled_at IS NULL OR cancelled_at > event_timestamp) THEN TRUE 
            ELSE FALSE 
        END AS is_active,
        _source_op::CHAR(1) AS source_op,
        _batch_id::VARCHAR(64) AS batch_id,
        _ingested_at::TIMESTAMPTZ AS ingested_at
    FROM source
)

SELECT * FROM renamed
