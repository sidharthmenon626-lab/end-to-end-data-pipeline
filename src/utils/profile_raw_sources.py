import json
from sqlalchemy import text
from src.utils.db import get_engine

def profile_tables():
    engine = get_engine()
    tables = [
        ("raw_customers", "customer_id", "source_updated_at"),
        ("raw_orders", "order_id", "order_timestamp"),
        ("raw_subscriptions", "subscription_id", "started_at"),
        ("raw_events", "event_id", "event_timestamp")
    ]
    
    inventory = {}
    with engine.connect() as conn:
        for tbl, pk, ts in tables:
            print(f"\nProfiling raw.{tbl}...")
            # Columns and types
            cols_query = text(f"""
                SELECT column_name, data_type, is_nullable 
                FROM information_schema.columns 
                WHERE table_schema = 'raw' AND table_name = '{tbl}'
                ORDER BY ordinal_position;
            """)
            columns = [{"name": r[0], "type": r[1], "nullable": r[2]} for r in conn.execute(cols_query).fetchall()]
            
            # Row count and duplicate key check
            total_rows = conn.execute(text(f"SELECT COUNT(*) FROM raw.{tbl};")).scalar()
            distinct_pks = conn.execute(text(f"SELECT COUNT(DISTINCT {pk}) FROM raw.{tbl};")).scalar()
            duplicates = total_rows - distinct_pks
            
            # Timestamp range
            ts_min_max = conn.execute(text(f"SELECT MIN({ts}), MAX({ts}) FROM raw.{tbl};")).fetchone()
            
            # Null analysis across all columns
            null_counts = {}
            for col in columns:
                cname = col["name"]
                ncnt = conn.execute(text(f"SELECT COUNT(*) FROM raw.{tbl} WHERE {cname} IS NULL;")).scalar()
                null_counts[cname] = ncnt
                
            inventory[tbl] = {
                "total_rows": total_rows,
                "primary_key": pk,
                "duplicate_pks": duplicates,
                "timestamp_column": ts,
                "timestamp_min": str(ts_min_max[0]),
                "timestamp_max": str(ts_min_max[1]),
                "columns": columns,
                "null_counts": null_counts
            }
            
            print(f"  Total Rows: {total_rows:,} | PK '{pk}' Duplicates: {duplicates}")
            print(f"  Timestamp '{ts}': Min={ts_min_max[0]} | Max={ts_min_max[1]}")
            print(f"  Columns ({len(columns)}): {', '.join([c['name'] for c in columns])}")
            
    with open("data/source_inventory.json", "w", encoding="utf-8") as f:
        json.dump(inventory, f, indent=2)
    print("\nSource inventory saved to data/source_inventory.json")

if __name__ == "__main__":
    profile_tables()
