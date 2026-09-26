{{ config(
    materialized = 'table'
) }}

WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),

final AS (
    SELECT
        order_id,
        customer_id,
        order_status,
        order_timestamp,
        order_date,
        (TO_CHAR(order_date, 'YYYYMMDD'))::INTEGER AS date_key,
        gross_amount_usd,
        discount_usd,
        net_amount_usd,
        payment_method,
        shipping_country
    FROM orders
)

SELECT * FROM final
