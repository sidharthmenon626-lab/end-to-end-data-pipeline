"""
Warehouse Initialization Utility
Applies foundational DDL schemas and raw landing tables to PostgreSQL.
Safe to run idempotently (uses IF NOT EXISTS).
"""

from pathlib import Path

from sqlalchemy import text

from src.utils.db import get_engine


def init_warehouse():
    engine = get_engine()
    root_dir = Path(__file__).resolve().parent.parent.parent

    init_sql_path = root_dir / "warehouse" / "init.sql"
    raw_ddl_path = root_dir / "warehouse" / "ddl" / "raw_tables.sql"

    print("=" * 60)
    print("INITIALIZING WAREHOUSE SCHEMAS & TABLES")
    print("=" * 60)

    with engine.begin() as conn:
        if init_sql_path.exists():
            print(f"Applying: {init_sql_path.name}")
            init_sql = init_sql_path.read_text(encoding="utf-8")
            for statement in init_sql.split(";"):
                stmt = statement.strip()
                if stmt:
                    conn.execute(text(stmt))
            print("  [OK] Schemas 'raw', 'staging', 'marts' ready.")

        if raw_ddl_path.exists():
            print(f"Applying: {raw_ddl_path.name}")
            raw_sql = raw_ddl_path.read_text(encoding="utf-8")
            for statement in raw_sql.split(";"):
                stmt = statement.strip()
                if stmt:
                    conn.execute(text(stmt))
            print("  [OK] Landing tables and watermark store ready.")

    print("=" * 60)
    print("[SUCCESS] Warehouse initialized successfully.")
    print("=" * 60)


if __name__ == "__main__":
    init_warehouse()
