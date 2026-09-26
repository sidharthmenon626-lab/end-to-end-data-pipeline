-- Returns customers with more than one active/current record
SELECT
    customer_id,
    COUNT(*) AS active_records_count
FROM {{ ref('dim_customer') }}
WHERE is_current = TRUE
GROUP BY customer_id
HAVING COUNT(*) > 1
