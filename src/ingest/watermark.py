"""
Persistent Watermark Management Module.
Ensures high-water marks survive process restarts and integrate with database transactions.
"""

import json
import logging
import os
from datetime import datetime, timezone
from typing import Optional
from sqlalchemy import text
from src.utils.db import get_engine

logger = logging.getLogger(__name__)

# Fallback state file path if database is unreachable
FALLBACK_STATE_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
    "data",
    "watermarks.json"
)


class WatermarkStore:
    """
    Manages persistent extraction watermarks in PostgreSQL raw._pipeline_watermarks,
    with an automatic JSON fallback file on disk.
    """

    def __init__(self, use_db: bool = True):
        self.use_db = use_db
        if self.use_db:
            try:
                self._ensure_table()
            except Exception as e:
                logger.warning(f"Could not connect to DB for watermarks ({e}). Falling back to local file.")
                self.use_db = False

    def _ensure_table(self):
        engine = get_engine()
        with engine.begin() as conn:
            conn.execute(text("""
                CREATE SCHEMA IF NOT EXISTS raw;
                CREATE TABLE IF NOT EXISTS raw._pipeline_watermarks (
                    source_name         VARCHAR(64) PRIMARY KEY,
                    last_watermark      TIMESTAMP WITH TIME ZONE NOT NULL,
                    records_extracted   BIGINT NOT NULL DEFAULT 0,
                    last_success_at     TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                );
            """))

    def get_watermark(self, source_name: str, default: Optional[datetime] = None) -> datetime:
        """
        Retrieves the last committed watermark timestamp for a source.
        """
        if default is None:
            default = datetime(1970, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

        if self.use_db:
            try:
                engine = get_engine()
                with engine.connect() as conn:
                    result = conn.execute(
                        text("SELECT last_watermark FROM raw._pipeline_watermarks WHERE source_name = :s"),
                        {"s": source_name}
                    ).scalar()
                    if result:
                        return result if result.tzinfo else result.replace(tzinfo=timezone.utc)
            except Exception as e:
                logger.warning(f"Failed to read watermark from DB: {e}. Checking file fallback.")

        # File-based fallback
        if os.path.exists(FALLBACK_STATE_PATH):
            try:
                with open(FALLBACK_STATE_PATH, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    if source_name in data and "last_watermark" in data[source_name]:
                        return datetime.fromisoformat(data[source_name]["last_watermark"])
            except Exception as e:
                logger.error(f"Error reading file fallback: {e}")

        return default

    def update_watermark(self, source_name: str, new_watermark: datetime, records_count: int = 0) -> None:
        """
        Persists an updated watermark timestamp and batch count.
        """
        if not new_watermark.tzinfo:
            new_watermark = new_watermark.replace(tzinfo=timezone.utc)

        # 1. Update in Database
        if self.use_db:
            try:
                engine = get_engine()
                with engine.begin() as conn:
                    conn.execute(text("""
                        INSERT INTO raw._pipeline_watermarks (source_name, last_watermark, records_extracted, last_success_at)
                        VALUES (:s, :wm, :cnt, CURRENT_TIMESTAMP)
                        ON CONFLICT (source_name) DO UPDATE SET
                            last_watermark = EXCLUDED.last_watermark,
                            records_extracted = raw._pipeline_watermarks.records_extracted + EXCLUDED.records_extracted,
                            last_success_at = CURRENT_TIMESTAMP;
                    """), {"s": source_name, "wm": new_watermark, "cnt": records_count})
            except Exception as e:
                logger.error(f"Failed to update watermark in DB: {e}")

        # 2. Update File Fallback
        os.makedirs(os.path.dirname(FALLBACK_STATE_PATH), exist_ok=True)
        file_data = {}
        if os.path.exists(FALLBACK_STATE_PATH):
            try:
                with open(FALLBACK_STATE_PATH, "r", encoding="utf-8") as f:
                    file_data = json.load(f)
            except Exception:
                file_data = {}

        file_data[source_name] = {
            "last_watermark": new_watermark.isoformat(),
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "records_extracted": records_count
        }

        with open(FALLBACK_STATE_PATH, "w", encoding="utf-8") as f:
            json.dump(file_data, f, indent=2)

        logger.info(f"Committed watermark for {source_name}: {new_watermark.isoformat()} (+{records_count} rows)")
