-- Returns duplicate customer versions for the exact same validity start point
SELECT
    customer_id,
    valid_from,
    COUNT(*) AS row_count
FROM {{ ref('dim_customer') }}
GROUP BY customer_id, valid_from
HAVING COUNT(*) > 1
