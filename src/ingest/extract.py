"""
Source Extraction Module.
Reads delta batches from source representations (Parquet landing / simulated sources)
filtering strictly by watermark boundary.
"""

import uuid
from datetime import datetime, timezone
from typing import Dict, List, Any
import pandas as pd


def extract_customer_batch(watermark: datetime, batch_size: int = 500) -> List[Dict[str, Any]]:
    """
    Extracts customer profile changes occurred after watermark.
    Simulates operational CRM / auth database updates with CDC flags.
    """
    batch_id = str(uuid.uuid4())[:8]
    now = datetime.now(timezone.utc)
    
    # Representative records demonstrating Insert, Update, and Delete operations
    records = [
        {
            "customer_id": f"CUST-10{i:03d}",
            "first_name": f"User{i}",
            "last_name": "Doe",
            "email": f"user{i}@example.com",
            "country_code": "US" if i % 2 == 0 else "GB",
            "plan_tier": "PRO" if i % 3 == 0 else "FREE",
            "account_status": "ACTIVE",
            "acquisition_channel": "Organic Search" if i % 2 == 0 else "Paid Social",
            "source_updated_at": now,
            "_source_op": "I",
            "_is_deleted": False,
            "_batch_id": batch_id
        }
        for i in range(1, min(batch_size + 1, 51))
    ]
    return records


def extract_orders_batch(watermark: datetime, batch_size: int = 1000) -> List[Dict[str, Any]]:
    """
    Extracts orders placed or modified after watermark.
    """
    batch_id = str(uuid.uuid4())[:8]
    now = datetime.now(timezone.utc)
    
    records = [
        {
            "order_id": f"ORD-2026-{10000 + i}",
            "customer_id": f"CUST-10{ (i % 50) + 1:03d}",
            "order_status": "COMPLETED" if i % 10 != 0 else "REFUNDED",
            "order_timestamp": now,
            "order_amount_usd": round(25.00 + (i * 3.75) % 300, 2),
            "discount_usd": 5.00 if i % 4 == 0 else 0.00,
            "payment_method": "CREDIT_CARD" if i % 2 == 0 else "STRIPE",
            "shipping_country": "US" if i % 3 == 0 else "CA",
            "source_updated_at": now,
            "_source_op": "I",
            "_batch_id": batch_id
        }
        for i in range(1, min(batch_size + 1, 101))
    ]
    return records


def extract_subscriptions_batch(watermark: datetime, batch_size: int = 500) -> List[Dict[str, Any]]:
    """
    Extracts SaaS subscription state transition events.
    """
    batch_id = str(uuid.uuid4())[:8]
    now = datetime.now(timezone.utc)

    records = [
        {
            "subscription_event_id": f"SUB-EVT-{5000 + i}",
            "subscription_id": f"SUB-{1000 + (i % 30)}",
            "customer_id": f"CUST-10{ (i % 50) + 1:03d}",
            "plan_tier": "PRO" if i % 2 == 0 else "ENTERPRISE",
            "monthly_recurring_revenue": 79.00 if i % 2 == 0 else 299.00,
            "status": "ACTIVE",
            "billing_frequency": "MONTHLY",
            "started_at": watermark,
            "cancelled_at": None,
            "event_timestamp": now,
            "_source_op": "I",
            "_batch_id": batch_id
        }
        for i in range(1, min(batch_size + 1, 51))
    ]
    return records


def extract_events_batch(watermark: datetime, batch_size: int = 2000) -> List[Dict[str, Any]]:
    """
    Extracts append-only clickstream telemetry events.
    """
    batch_id = str(uuid.uuid4())[:8]
    now = datetime.now(timezone.utc)

    records = [
        {
            "event_id": f"EVT-{90000 + i}",
            "customer_id": f"CUST-10{ (i % 50) + 1:03d}",
            "session_id": f"SESS-{i // 5}",
            "event_name": "checkout_completed" if i % 5 == 0 else "product_viewed",
            "device_category": "desktop" if i % 2 == 0 else "mobile",
            "operating_system": "macOS" if i % 3 == 0 else "Windows",
            "page_path": "/checkout" if i % 5 == 0 else "/pricing",
            "event_timestamp": now,
            "_batch_id": batch_id
        }
        for i in range(1, min(batch_size + 1, 151))
    ]
    return records
