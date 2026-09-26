"""
High-Performance Vectorized Raw Data Generator.
Produces 1,050,000 synthetic operational and telemetry records stored as Parquet files
in data/raw/ conforming to data/manifest.json and raw.* schema DDL.
"""

import os
import time
import numpy as np
import pandas as pd


def generate_customers(n: int = 50_000, base_dir: str = "data/raw/customers") -> str:
    print(f"Generating {n:,} customer records...")
    os.makedirs(base_dir, exist_ok=True)
    out_file = os.path.join(base_dir, "customers.parquet")

    ids = [f"CUST-{i:06d}" for i in range(1, n + 1)]
    first_names = np.random.choice(["James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda", "William", "Elizabeth", "David", "Barbara", "Richard", "Susan", "Joseph", "Jessica"], size=n)
    last_names = np.random.choice(["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson", "Thomas"], size=n)
    emails = [f"user_{i}@{np.random.choice(['gmail.com', 'yahoo.com', 'enterprise.io', 'techcorp.net'])}" for i in range(1, n + 1)]
    countries = np.random.choice(["US", "GB", "DE", "FR", "CA", "AU", "IN", "JP"], size=n, p=[0.45, 0.15, 0.10, 0.08, 0.07, 0.05, 0.05, 0.05])
    plans = np.random.choice(["FREE", "STARTER", "PRO", "ENTERPRISE"], size=n, p=[0.40, 0.30, 0.20, 0.10])
    statuses = np.random.choice(["ACTIVE", "PENDING", "SUSPENDED", "CHURNED"], size=n, p=[0.75, 0.10, 0.05, 0.10])
    channels = np.random.choice(["Organic Search", "Paid Social", "Referral", "Email Campaign", "Direct"], size=n, p=[0.35, 0.25, 0.15, 0.15, 0.10])
    
    start_ts = pd.Timestamp("2025-01-01 00:00:00", tz="UTC").value
    end_ts = pd.Timestamp("2026-03-25 00:00:00", tz="UTC").value
    random_ts = np.random.randint(start_ts, end_ts, size=n, dtype=np.int64)
    timestamps = pd.to_datetime(random_ts, utc=True)

    df = pd.DataFrame({
        "customer_id": ids,
        "first_name": first_names,
        "last_name": last_names,
        "email": emails,
        "country_code": countries,
        "plan_tier": plans,
        "account_status": statuses,
        "acquisition_channel": channels,
        "source_updated_at": timestamps,
        "_source_op": "I",
        "_is_deleted": False,
        "_batch_id": "BATCH-INIT-CUST"
    })
    df.to_parquet(out_file, index=False, engine="pyarrow", compression="snappy")
    print(f"  [OK] Saved {len(df):,} customers -> {out_file} ({os.path.getsize(out_file) / 1024 / 1024:.2f} MB)")
    return out_file


def generate_orders(n: int = 400_000, n_cust: int = 50_000, base_dir: str = "data/raw/orders") -> str:
    print(f"Generating {n:,} order records...")
    os.makedirs(base_dir, exist_ok=True)
    out_file = os.path.join(base_dir, "orders.parquet")

    ids = [f"ORD-{i:07d}" for i in range(1, n + 1)]
    cust_idx = np.random.randint(1, n_cust + 1, size=n)
    cust_ids = [f"CUST-{i:06d}" for i in cust_idx]
    statuses = np.random.choice(["COMPLETED", "PENDING", "CANCELLED", "REFUNDED"], size=n, p=[0.82, 0.08, 0.05, 0.05])
    amounts = np.round(np.random.gamma(shape=3.0, scale=35.0, size=n) + 5.0, 2)
    has_discount = np.random.rand(n) < 0.25
    discounts = np.where(has_discount, np.round(amounts * np.random.uniform(0.05, 0.20, size=n), 2), 0.0)
    payments = np.random.choice(["CREDIT_CARD", "STRIPE", "PAYPAL", "APPLE_PAY"], size=n, p=[0.50, 0.30, 0.12, 0.08])
    countries = np.random.choice(["US", "GB", "DE", "FR", "CA", "AU", "IN", "JP"], size=n, p=[0.45, 0.15, 0.10, 0.08, 0.07, 0.05, 0.05, 0.05])

    start_ts = pd.Timestamp("2025-01-01 00:00:00", tz="UTC").value
    end_ts = pd.Timestamp("2026-03-25 00:00:00", tz="UTC").value
    random_ts = np.random.randint(start_ts, end_ts, size=n, dtype=np.int64)
    timestamps = pd.to_datetime(random_ts, utc=True)

    df = pd.DataFrame({
        "order_id": ids,
        "customer_id": cust_ids,
        "order_status": statuses,
        "order_timestamp": timestamps,
        "order_amount_usd": amounts,
        "discount_usd": discounts,
        "payment_method": payments,
        "shipping_country": countries,
        "source_updated_at": timestamps,
        "_source_op": "I",
        "_batch_id": "BATCH-INIT-ORD"
    })
    df.to_parquet(out_file, index=False, engine="pyarrow", compression="snappy")
    print(f"  [OK] Saved {len(df):,} orders -> {out_file} ({os.path.getsize(out_file) / 1024 / 1024:.2f} MB)")
    return out_file


def generate_subscriptions(n: int = 200_000, n_cust: int = 50_000, base_dir: str = "data/raw/subscriptions") -> str:
    print(f"Generating {n:,} subscription event records...")
    os.makedirs(base_dir, exist_ok=True)
    out_file = os.path.join(base_dir, "subscriptions.parquet")

    ids = [f"SUB-EVT-{i:07d}" for i in range(1, n + 1)]
    sub_ids = [f"SUB-{((i - 1) % 40000) + 1:06d}" for i in range(1, n + 1)]
    cust_idx = np.random.randint(1, n_cust + 1, size=n)
    cust_ids = [f"CUST-{i:06d}" for i in cust_idx]
    plans = np.random.choice(["STARTER", "PRO", "ENTERPRISE"], size=n, p=[0.50, 0.35, 0.15])
    mrr_map = {"STARTER": 29.00, "PRO": 79.00, "ENTERPRISE": 299.00}
    mrr = [mrr_map[p] for p in plans]
    statuses = np.random.choice(["ACTIVE", "TRIALING", "CANCELLED", "PAST_DUE"], size=n, p=[0.70, 0.15, 0.10, 0.05])
    billing_freq = np.random.choice(["MONTHLY", "ANNUAL"], size=n, p=[0.75, 0.25])

    start_ts = pd.Timestamp("2025-01-01 00:00:00", tz="UTC").value
    end_ts = pd.Timestamp("2026-03-25 00:00:00", tz="UTC").value
    random_ts = np.random.randint(start_ts, end_ts, size=n, dtype=np.int64)
    timestamps = pd.to_datetime(random_ts, utc=True)

    df = pd.DataFrame({
        "subscription_event_id": ids,
        "subscription_id": sub_ids,
        "customer_id": cust_ids,
        "plan_tier": plans,
        "monthly_recurring_revenue": mrr,
        "status": statuses,
        "billing_frequency": billing_freq,
        "started_at": timestamps,
        "cancelled_at": pd.Series([None] * n, dtype="datetime64[ns, UTC]"),
        "event_timestamp": timestamps,
        "_source_op": "I",
        "_batch_id": "BATCH-INIT-SUB"
    })
    df.to_parquet(out_file, index=False, engine="pyarrow", compression="snappy")
    print(f"  [OK] Saved {len(df):,} subscriptions -> {out_file} ({os.path.getsize(out_file) / 1024 / 1024:.2f} MB)")
    return out_file


def generate_events(n: int = 400_000, n_cust: int = 50_000, base_dir: str = "data/raw/events") -> str:
    print(f"Generating {n:,} telemetry event records...")
    os.makedirs(base_dir, exist_ok=True)
    out_file = os.path.join(base_dir, "events.parquet")

    ids = [f"EVT-{i:07d}" for i in range(1, n + 1)]
    cust_idx = np.random.randint(1, n_cust + 1, size=n)
    cust_ids = [f"CUST-{i:06d}" for i in cust_idx]
    sess_idx = np.random.randint(1, 150_001, size=n)
    sess_ids = [f"SESS-{i:07d}" for i in sess_idx]
    event_names = np.random.choice(
        ["page_view", "product_viewed", "cart_add", "checkout_started", "checkout_completed", "subscription_upgraded"],
        size=n,
        p=[0.40, 0.25, 0.15, 0.10, 0.07, 0.03]
    )
    devices = np.random.choice(["desktop", "mobile", "tablet"], size=n, p=[0.55, 0.38, 0.07])
    os_names = np.random.choice(["Windows", "macOS", "iOS", "Android", "Linux"], size=n, p=[0.35, 0.25, 0.20, 0.15, 0.05])
    pages = np.random.choice(["/", "/products", "/pricing", "/cart", "/checkout", "/dashboard"], size=n, p=[0.30, 0.25, 0.15, 0.12, 0.10, 0.08])

    start_ts = pd.Timestamp("2025-01-01 00:00:00", tz="UTC").value
    end_ts = pd.Timestamp("2026-03-25 00:00:00", tz="UTC").value
    random_ts = np.random.randint(start_ts, end_ts, size=n, dtype=np.int64)
    timestamps = pd.to_datetime(random_ts, utc=True)

    df = pd.DataFrame({
        "event_id": ids,
        "customer_id": cust_ids,
        "session_id": sess_ids,
        "event_name": event_names,
        "device_category": devices,
        "operating_system": os_names,
        "page_path": pages,
        "event_timestamp": timestamps,
        "_batch_id": "BATCH-INIT-EVT"
    })
    df.to_parquet(out_file, index=False, engine="pyarrow", compression="snappy")
    print(f"  [OK] Saved {len(df):,} events -> {out_file} ({os.path.getsize(out_file) / 1024 / 1024:.2f} MB)")
    return out_file


def generate_all():
    print("=" * 70)
    print("  RAW DATA LAKE GENERATOR: 1,050,000 RECORDS")
    print("=" * 70)
    t0 = time.time()
    generate_customers(50_000)
    generate_orders(400_000)
    generate_subscriptions(200_000)
    generate_events(400_000)
    elapsed = time.time() - t0
    print("=" * 70)
    print(f"[COMPLETE] Generated 1,050,000 records in {elapsed:.2f} seconds.")
    print("=" * 70)


if __name__ == "__main__":
    generate_all()
