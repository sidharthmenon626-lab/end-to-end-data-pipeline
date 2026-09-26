WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_customers') }}
),

renamed AS (
    SELECT
        customer_id::VARCHAR(64) AS customer_id,
        NULLIF(TRIM(first_name), '')::VARCHAR(128) AS first_name,
        NULLIF(TRIM(last_name), '')::VARCHAR(128) AS last_name,
        LOWER(TRIM(email))::VARCHAR(255) AS email,
        UPPER(TRIM(country_code))::VARCHAR(8) AS country_code,
        UPPER(TRIM(plan_tier))::VARCHAR(32) AS plan_tier,
        UPPER(TRIM(account_status))::VARCHAR(32) AS account_status,
        acquisition_channel::VARCHAR(64) AS acquisition_channel,
        source_updated_at::TIMESTAMPTZ AS source_updated_at,
        _source_op::CHAR(1) AS source_op,
        _is_deleted::BOOLEAN AS is_deleted,
        _batch_id::VARCHAR(64) AS batch_id,
        _ingested_at::TIMESTAMPTZ AS ingested_at
    FROM source
)

SELECT * FROM renamed
