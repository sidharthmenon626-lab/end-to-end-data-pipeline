"""
Database connection factory and configuration helper.
Safely extracts warehouse and upstream source credentials from environment variables.
"""

import os
from urllib.parse import quote_plus, urlsplit, urlunsplit

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import Connection, Engine

# Explicitly load project-level .env regardless of caller current working directory
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
load_dotenv(os.path.join(PROJECT_ROOT, ".env"))


def get_db_url(mask_password: bool = False) -> str:
    """
    Constructs the PostgreSQL connection URL from environment variables for the analytics warehouse.

    Args:
        mask_password: If True, replaces the actual password with asterisks for safe logging.
    """
    user = os.getenv("WAREHOUSE_USER", "pipeline_user")
    raw_password = os.getenv("WAREHOUSE_PASSWORD", "pipeline_secure_pass")
    host = os.getenv("WAREHOUSE_HOST", "localhost")
    port = os.getenv("WAREHOUSE_PORT", "5433")
    db_name = os.getenv("WAREHOUSE_DB", "analytics_dw")

    password = "********" if mask_password else quote_plus(raw_password)
    return f"postgresql://{user}:{password}@{host}:{port}/{db_name}"


def get_source_url(source_name: str, mask_password: bool = False) -> str | None:
    """
    Retrieves the connection URL for upstream source databases (ecom or saas).
    """
    var_name = f"{source_name.upper()}_SOURCE_URL"
    raw_url = os.getenv(var_name)
    if not raw_url:
        return None
    if not mask_password:
        return raw_url
    
    parts = urlsplit(raw_url)
    if parts.password:
        netloc = parts.netloc.replace(f":{parts.password}@", ":********@")
        return urlunsplit((parts.scheme, netloc, parts.path, parts.query, parts.fragment))
    return raw_url


def get_engine(**kwargs) -> Engine:
    """
    Creates and returns a SQLAlchemy Engine configured with connection pooling for the warehouse.
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
    Returns an active database connection to the warehouse.
    """
    engine = get_engine()
    return engine.connect()


def get_source_engine(source_name: str, **kwargs) -> Engine | None:
    """
    Creates and returns a SQLAlchemy Engine for a remote source database (ecom or saas).
    """
    url = get_source_url(source_name, mask_password=False)
    if not url:
        return None
    return create_engine(
        url,
        pool_pre_ping=True,
        pool_size=kwargs.get("pool_size", 3),
        max_overflow=kwargs.get("max_overflow", 5),
    )
