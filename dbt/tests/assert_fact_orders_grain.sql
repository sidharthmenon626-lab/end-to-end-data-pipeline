-- Returns duplicate order_id records violating the declared grain
SELECT
    order_id,
    COUNT(*) AS row_count
FROM {{ ref('fact_orders') }}
GROUP BY order_id
HAVING COUNT(*) > 1
