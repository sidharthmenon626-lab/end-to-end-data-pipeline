WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_orders') }}
),

renamed AS (
    SELECT
        order_id::VARCHAR(64) AS order_id,
        customer_id::VARCHAR(64) AS customer_id,
        CASE 
            WHEN UPPER(TRIM(order_status)) IN ('DELIVERED', 'SHIPPED', 'COMPLETED') THEN 'COMPLETED'
            WHEN UPPER(TRIM(order_status)) IN ('PLACED', 'PACKED', 'PAID', 'PENDING') THEN 'PENDING'
            WHEN UPPER(TRIM(order_status)) IN ('CANCELLED') THEN 'CANCELLED'
            WHEN UPPER(TRIM(order_status)) IN ('REFUNDED') THEN 'REFUNDED'
            ELSE 'PENDING'
        END::VARCHAR(32) AS order_status,
        order_timestamp::TIMESTAMPTZ AS order_timestamp,
        DATE(order_timestamp) AS order_date,
        order_amount_usd::NUMERIC(12, 2) AS gross_amount_usd,
        COALESCE(discount_usd, 0.00)::NUMERIC(12, 2) AS discount_usd,
        (order_amount_usd - COALESCE(discount_usd, 0.00))::NUMERIC(12, 2) AS net_amount_usd,
        UPPER(TRIM(payment_method))::VARCHAR(32) AS payment_method,
        SUBSTRING(UPPER(TRIM(shipping_country)), 1, 8)::VARCHAR(8) AS shipping_country,
        source_updated_at::TIMESTAMPTZ AS source_updated_at,
        _batch_id::VARCHAR(64) AS batch_id,
        _ingested_at::TIMESTAMPTZ AS ingested_at
    FROM source
)

SELECT * FROM renamed
