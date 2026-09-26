{{ config(
    materialized = 'table'
) }}

WITH stg AS (
    SELECT * FROM {{ ref('stg_customers') }}
),

windowed AS (
    SELECT
        customer_id,
        first_name,
        last_name,
        email,
        country_code,
        plan_tier,
        account_status,
        acquisition_channel,
        source_updated_at AS valid_from,
        COALESCE(
            LEAD(source_updated_at) OVER (PARTITION BY customer_id ORDER BY source_updated_at),
            '9999-12-31 23:59:59+00'::TIMESTAMPTZ
        ) AS valid_to,
        CASE 
            WHEN LEAD(source_updated_at) OVER (PARTITION BY customer_id ORDER BY source_updated_at) IS NULL 
                 AND is_deleted = FALSE 
            THEN TRUE 
            ELSE FALSE 
        END AS is_current
    FROM stg
),

final AS (
    SELECT
        MD5(customer_id || '|' || valid_from::TEXT)::VARCHAR(32) AS customer_sk,
        customer_id,
        first_name,
        last_name,
        email,
        country_code,
        plan_tier,
        account_status,
        acquisition_channel,
        valid_from,
        valid_to,
        is_current
    FROM windowed
)

SELECT * FROM final
