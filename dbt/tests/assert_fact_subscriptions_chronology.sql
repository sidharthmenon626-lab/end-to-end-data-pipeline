-- Returns subscriptions with invalid chronological timestamps
SELECT
    subscription_event_id,
    subscription_id,
    started_at,
    cancelled_at
FROM {{ ref('fact_subscription_events') }}
WHERE cancelled_at IS NOT NULL
  AND cancelled_at < started_at
