{{ config(
    materialized = 'table'
) }}

WITH subs AS (
    SELECT * FROM {{ ref('stg_subscriptions') }}
),

final AS (
    SELECT
        subscription_event_id,
        subscription_id,
        customer_id,
        plan_tier,
        mrr_amount_usd,
        subscription_status,
        billing_frequency,
        started_at,
        cancelled_at,
        event_timestamp,
        event_date,
        (TO_CHAR(event_date, 'YYYYMMDD'))::INTEGER AS date_key,
        is_active
    FROM subs
)

SELECT * FROM final
