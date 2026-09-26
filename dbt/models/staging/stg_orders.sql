WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_orders') }}
),

renamed AS (
    SELECT
        order_id::VARCHAR(64) AS order_id,
        customer_id::VARCHAR(64) AS customer_id,
        UPPER(TRIM(order_status))::VARCHAR(32) AS order_status,
        order_timestamp::TIMESTAMPTZ AS order_timestamp,
        DATE(order_timestamp) AS order_date,
        order_amount_usd::NUMERIC(12, 2) AS gross_amount_usd,
        COALESCE(discount_usd, 0.00)::NUMERIC(12, 2) AS discount_usd,
        (order_amount_usd - COALESCE(discount_usd, 0.00))::NUMERIC(12, 2) AS net_amount_usd,
        UPPER(TRIM(payment_method))::VARCHAR(32) AS payment_method,
        UPPER(TRIM(shipping_country))::VARCHAR(8) AS shipping_country,
        source_updated_at::TIMESTAMPTZ AS source_updated_at,
        _batch_id::VARCHAR(64) AS batch_id,
        _ingested_at::TIMESTAMPTZ AS ingested_at
    FROM source
)

SELECT * FROM renamed
