"""
Verification script for warehouse target connectivity and schema readiness.
Run via:
    python -m src.utils.test_connection
"""

import sys
from sqlalchemy import text
from src.utils.db import get_engine, get_db_url


def test_connection() -> bool:
    print("=" * 60)
    print("  Warehouse Connectivity Test")
    print("=" * 60)
    print(f"Target URI : {get_db_url(mask_password=True)}")
    
    try:
        engine = get_engine()
        with engine.connect() as conn:
            # 1. Check basic connection and server version
            version_res = conn.execute(text("SELECT version();")).scalar()
            print(f"[✓] Connected successfully to warehouse target!")
            print(f"    Version: {version_res.split(',')[0] if version_res else 'Unknown'}")
            
            # 2. Check schemas
            print("\nChecking required pipeline schemas:")
            required_schemas = ["raw", "staging", "marts"]
            query = text("""
                SELECT schema_name 
                FROM information_schema.schemata 
                WHERE schema_name IN :schemas;
            """)
            existing_schemas = [
                row[0] for row in conn.execute(query, {"schemas": tuple(required_schemas)})
            ]
            
            for schema in required_schemas:
                if schema in existing_schemas:
                    print(f"  [✓] Schema '{schema}' exists.")
                else:
                    print(f"  [✗] Schema '{schema}' is MISSING (will be created by init.sql).")
                    
        print("\n[SUCCESS] Warehouse environment is ready for ingestion.")
        print("=" * 60)
        return True
    except Exception as e:
        print(f"\n[ERROR] Connection failed: {e}")
        print("\nTroubleshooting tips:")
        print("  1. Verify the PostgreSQL service/container is running: docker compose up -d")
        print("  2. Check your local .env configuration values against .env.example")
        print("=" * 60)
        return False


if __name__ == "__main__":
    success = test_connection()
    sys.exit(0 if success else 1)
