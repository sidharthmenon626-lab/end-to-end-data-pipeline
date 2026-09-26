-- Returns any order referencing a non-existent customer
SELECT
    o.order_id,
    o.customer_id
FROM {{ ref('fact_orders') }} o
LEFT JOIN (SELECT DISTINCT customer_id FROM {{ ref('dim_customer') }}) c
    ON o.customer_id = c.customer_id
WHERE c.customer_id IS NULL
