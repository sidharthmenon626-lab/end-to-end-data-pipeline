-- Returns any order where discount exceeds gross amount or net amount is negative
SELECT
    order_id,
    gross_amount_usd,
    discount_usd,
    net_amount_usd
FROM {{ ref('fact_orders') }}
WHERE net_amount_usd < 0
   OR discount_usd > gross_amount_usd
   OR gross_amount_usd < 0
