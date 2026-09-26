-- Returns missing calendar days in the date spine
WITH expected_dates AS (
    SELECT generate_series('2020-01-01'::DATE, '2030-12-31'::DATE, '1 day'::INTERVAL)::DATE AS expected_date
)
SELECT e.expected_date
FROM expected_dates e
LEFT JOIN {{ ref('dim_date') }} d
    ON e.expected_date = d.date_day
WHERE d.date_day IS NULL
