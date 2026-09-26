"""
Change Data Capture (CDC) and Upsert Handler.
Applies atomic merges to the raw landing schema with full mutation awareness (INSERT, UPDATE, DELETE)
and out-of-order sequence protection.
"""

import logging
from typing import Any

from sqlalchemy import text

from src.utils.db import get_engine

logger = logging.getLogger(__name__)


class CDCHandler:
    """
    Executes idempotent upserts and CDC tombstones in PostgreSQL.
    Protects against out-of-order event clobbering via monotonic timestamp guards.
    """

    @staticmethod
    def upsert_customers(records: list[dict[str, Any]]) -> int:
        """
        Upserts customers into raw.raw_customers.
        Handles UPDATE mutations and DELETE tombstones with out-of-order sequence guard.
        """
        if not records:
            return 0

        engine = get_engine()
        upsert_sql = text("""
            INSERT INTO raw.raw_customers (
                customer_id, first_name, last_name, email, country_code,
                plan_tier, account_status, acquisition_channel,
                source_updated_at, _source_op, _is_deleted, _batch_id, _ingested_at
            ) VALUES (
                :customer_id, :first_name, :last_name, :email, :country_code,
                :plan_tier, :account_status, :acquisition_channel,
                :source_updated_at, :_source_op, :_is_deleted, :_batch_id, CURRENT_TIMESTAMP
            )
            ON CONFLICT (customer_id) DO UPDATE SET
                first_name = EXCLUDED.first_name,
                last_name = EXCLUDED.last_name,
                email = EXCLUDED.email,
                country_code = EXCLUDED.country_code,
                plan_tier = EXCLUDED.plan_tier,
                account_status = EXCLUDED.account_status,
                acquisition_channel = EXCLUDED.acquisition_channel,
                source_updated_at = EXCLUDED.source_updated_at,
                _source_op = EXCLUDED._source_op,
                _is_deleted = EXCLUDED._is_deleted,
                _batch_id = EXCLUDED._batch_id,
                _ingested_at = CURRENT_TIMESTAMP
            WHERE EXCLUDED.source_updated_at >= raw.raw_customers.source_updated_at;
        """)

        with engine.begin() as conn:
            conn.execute(upsert_sql, records)
        return len(records)

    @staticmethod
    def upsert_orders(records: list[dict[str, Any]]) -> int:
        """
        Idempotent upsert of orders into raw.raw_orders with sequence protection.
        """
        if not records:
            return 0

        engine = get_engine()
        upsert_sql = text("""
            INSERT INTO raw.raw_orders (
                order_id, customer_id, order_status, order_timestamp,
                order_amount_usd, discount_usd, payment_method, shipping_country,
                source_updated_at, _source_op, _batch_id, _ingested_at
            ) VALUES (
                :order_id, :customer_id, :order_status, :order_timestamp,
                :order_amount_usd, :discount_usd, :payment_method, :shipping_country,
                :source_updated_at, :_source_op, :_batch_id, CURRENT_TIMESTAMP
            )
            ON CONFLICT (order_id) DO UPDATE SET
                order_status = EXCLUDED.order_status,
                order_amount_usd = EXCLUDED.order_amount_usd,
                discount_usd = EXCLUDED.discount_usd,
                source_updated_at = EXCLUDED.source_updated_at,
                _source_op = EXCLUDED._source_op,
                _batch_id = EXCLUDED._batch_id,
                _ingested_at = CURRENT_TIMESTAMP
            WHERE EXCLUDED.source_updated_at >= raw.raw_orders.source_updated_at;
        """)

        with engine.begin() as conn:
            conn.execute(upsert_sql, records)
        return len(records)

    @staticmethod
    def upsert_subscriptions(records: list[dict[str, Any]]) -> int:
        """
        Idempotent upsert of subscription events into raw.raw_subscriptions.
        """
        if not records:
            return 0

        engine = get_engine()
        upsert_sql = text("""
            INSERT INTO raw.raw_subscriptions (
                subscription_event_id, subscription_id, customer_id, plan_tier,
                monthly_recurring_revenue, status, billing_frequency, started_at,
                cancelled_at, event_timestamp, _source_op, _batch_id, _ingested_at
            ) VALUES (
                :subscription_event_id, :subscription_id, :customer_id, :plan_tier,
                :monthly_recurring_revenue, :status, :billing_frequency, :started_at,
                :cancelled_at, :event_timestamp, :_source_op, :_batch_id, CURRENT_TIMESTAMP
            )
            ON CONFLICT (subscription_event_id) DO UPDATE SET
                status = EXCLUDED.status,
                monthly_recurring_revenue = EXCLUDED.monthly_recurring_revenue,
                _source_op = EXCLUDED._source_op,
                _batch_id = EXCLUDED._batch_id,
                _ingested_at = CURRENT_TIMESTAMP
            WHERE EXCLUDED.event_timestamp >= raw.raw_subscriptions.event_timestamp;
        """)

        with engine.begin() as conn:
            conn.execute(upsert_sql, records)
        return len(records)

    @staticmethod
    def insert_events(records: list[dict[str, Any]]) -> int:
        """
        Append-only insert of telemetry events. Skips duplicates idempotently.
        """
        if not records:
            return 0

        engine = get_engine()
        insert_sql = text("""
            INSERT INTO raw.raw_events (
                event_id, customer_id, session_id, event_name,
                device_category, operating_system, page_path, event_timestamp,
                _batch_id, _ingested_at
            ) VALUES (
                :event_id, :customer_id, :session_id, :event_name,
                :device_category, :operating_system, :page_path, :event_timestamp,
                :_batch_id, CURRENT_TIMESTAMP
            )
            ON CONFLICT (event_id) DO NOTHING;
        """)

        with engine.begin() as conn:
            conn.execute(insert_sql, records)
        return len(records)
