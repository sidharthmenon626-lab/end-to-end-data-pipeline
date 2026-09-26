-- Returns duplicate date_day entries in dim_date
SELECT
    date_day,
    COUNT(*) AS row_count
FROM {{ ref('dim_date') }}
GROUP BY date_day
HAVING COUNT(*) > 1
