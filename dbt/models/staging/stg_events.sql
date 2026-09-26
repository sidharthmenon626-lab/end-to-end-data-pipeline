WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_events') }}
),

renamed AS (
    SELECT
        event_id::VARCHAR(64) AS event_id,
        customer_id::VARCHAR(64) AS customer_id,
        session_id::VARCHAR(64) AS session_id,
        LOWER(TRIM(event_name))::VARCHAR(64) AS event_name,
        LOWER(TRIM(device_category))::VARCHAR(32) AS device_category,
        LOWER(TRIM(operating_system))::VARCHAR(32) AS operating_system,
        TRIM(page_path)::VARCHAR(255) AS page_path,
        event_timestamp::TIMESTAMPTZ AS event_timestamp,
        DATE(event_timestamp) AS event_date,
        _batch_id::VARCHAR(64) AS batch_id,
        _ingested_at::TIMESTAMPTZ AS ingested_at
    FROM source
)

SELECT * FROM renamed
