{{ config(
    materialized = 'table'
) }}

WITH date_spine AS (
    SELECT 
        datum::DATE AS date_day
    FROM GENERATE_SERIES(
        '2020-01-01'::DATE,
        '2030-12-31'::DATE,
        '1 day'::INTERVAL
    ) AS datum
),

calculated AS (
    SELECT
        (TO_CHAR(date_day, 'YYYYMMDD'))::INTEGER AS date_key,
        date_day,
        EXTRACT(YEAR FROM date_day)::INTEGER AS year,
        EXTRACT(QUARTER FROM date_day)::INTEGER AS quarter,
        EXTRACT(MONTH FROM date_day)::INTEGER AS month,
        TO_CHAR(date_day, 'FMMonth')::VARCHAR(16) AS month_name,
        EXTRACT(WEEK FROM date_day)::INTEGER AS week_of_year,
        EXTRACT(DAY FROM date_day)::INTEGER AS day_of_month,
        EXTRACT(ISODOW FROM date_day)::INTEGER AS day_of_week,
        TO_CHAR(date_day, 'FMDay')::VARCHAR(16) AS day_name,
        CASE WHEN EXTRACT(ISODOW FROM date_day) IN (6, 7) THEN TRUE ELSE FALSE END::BOOLEAN AS is_weekend
    FROM date_spine
)

SELECT * FROM calculated
