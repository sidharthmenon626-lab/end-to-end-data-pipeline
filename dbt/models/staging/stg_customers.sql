WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_customers') }}
),

renamed AS (
    SELECT
        customer_id::VARCHAR(64) AS customer_id,
        NULLIF(TRIM(first_name), '')::VARCHAR(128) AS first_name,
        NULLIF(TRIM(last_name), '')::VARCHAR(128) AS last_name,
        LOWER(TRIM(email))::VARCHAR(255) AS email,
        SUBSTRING(UPPER(TRIM(country_code)), 1, 8)::VARCHAR(8) AS country_code,
        CASE 
            WHEN UPPER(TRIM(plan_tier)) IN ('PRO', 'PROFESSIONAL') THEN 'PRO'
            WHEN UPPER(TRIM(plan_tier)) IN ('ENTERPRISE') THEN 'ENTERPRISE'
            WHEN UPPER(TRIM(plan_tier)) IN ('STARTER') THEN 'STARTER'
            WHEN UPPER(TRIM(plan_tier)) IN ('FREE') THEN 'FREE'
            ELSE 'STARTER'
        END::VARCHAR(32) AS plan_tier,
        CASE 
            WHEN UPPER(TRIM(account_status)) IN ('ACTIVE') THEN 'ACTIVE'
            WHEN UPPER(TRIM(account_status)) IN ('SUSPENDED', 'AT_RISK') THEN 'SUSPENDED'
            WHEN UPPER(TRIM(account_status)) IN ('CHURNED') THEN 'CHURNED'
            WHEN UPPER(TRIM(account_status)) IN ('CANCELLED') THEN 'CANCELLED'
            WHEN UPPER(TRIM(account_status)) IN ('PENDING', 'NEW') THEN 'PENDING'
            ELSE 'ACTIVE'
        END::VARCHAR(32) AS account_status,
        acquisition_channel::VARCHAR(64) AS acquisition_channel,
        source_updated_at::TIMESTAMPTZ AS source_updated_at,
        _source_op::CHAR(1) AS source_op,
        _is_deleted::BOOLEAN AS is_deleted,
        _batch_id::VARCHAR(64) AS batch_id,
        _ingested_at::TIMESTAMPTZ AS ingested_at
    FROM source
)

SELECT * FROM renamed
