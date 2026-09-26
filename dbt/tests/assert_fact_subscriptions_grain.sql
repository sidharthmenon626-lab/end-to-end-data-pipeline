-- Returns duplicate subscription_event_id records violating the declared grain
SELECT
    subscription_event_id,
    COUNT(*) AS row_count
FROM {{ ref('fact_subscription_events') }}
GROUP BY subscription_event_id
HAVING COUNT(*) > 1
