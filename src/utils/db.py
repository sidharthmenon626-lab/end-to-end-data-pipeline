"""
Database connection factory and configuration helper.
Safely extracts warehouse credentials from environment variables.
"""

import os
from urllib.parse import quote_plus

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import Connection, Engine

# Explicitly load project-level .env regardless of caller current working directory
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
load_dotenv(os.path.join(PROJECT_ROOT, ".env"))


def get_db_url(mask_password: bool = False) -> str:
    """
    Constructs the PostgreSQL connection URL from environment variables.

    Args:
        mask_password: If True, replaces the actual password with asterisks for safe logging.
    """
    user = os.getenv("WAREHOUSE_USER", "pipeline_user")
    raw_password = os.getenv("WAREHOUSE_PASSWORD", "pipeline_secure_pass")
    host = os.getenv("WAREHOUSE_HOST", "localhost")
    port = os.getenv("WAREHOUSE_PORT", "5433")
    db_name = os.getenv("WAREHOUSE_DB", "analytics_dw")

    password = "••••••••" if mask_password else quote_plus(raw_password)
    return f"postgresql://{user}:{password}@{host}:{port}/{db_name}"


def get_engine(**kwargs) -> Engine:
    """
    Creates and returns a SQLAlchemy Engine configured with connection pooling.
    """
    db_url = get_db_url(mask_password=False)
    return create_engine(
        db_url,
        pool_pre_ping=True,
        pool_size=kwargs.get("pool_size", 5),
        max_overflow=kwargs.get("max_overflow", 10),
    )


def get_connection() -> Connection:
    """
    Returns an active database connection.
    """
    engine = get_engine()
    return engine.connect()
