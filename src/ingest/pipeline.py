"""
Master Ingestion Pipeline Orchestrator.
Coordinates extract -> CDC transformation -> upsert -> watermark persistence.
"""

import argparse
import logging
from datetime import datetime, timezone
from src.ingest.watermark import WatermarkStore
from src.ingest.extract import (
    extract_customer_batch,
    extract_orders_batch,
    extract_subscriptions_batch,
    extract_events_batch,
)
from src.ingest.cdc import CDCHandler

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


class IngestionPipeline:
    """
    Executes incremental loads across all source datasets.
    """

    def __init__(self, full_refresh: bool = False):
        self.full_refresh = full_refresh
        self.wm_store = WatermarkStore()

    def run_source(self, source_name: str) -> int:
        logger.info(f"--- Starting Ingestion for source: {source_name} ---")
        
        default_wm = datetime(1970, 1, 1, tzinfo=timezone.utc) if self.full_refresh else None
        current_wm = self.wm_store.get_watermark(source_name, default=default_wm)
        logger.info(f"Active Watermark for {source_name}: {current_wm.isoformat()}")

        records_processed = 0
        new_wm = datetime.now(timezone.utc)

        if source_name == "customers":
            records = extract_customer_batch(current_wm)
            records_processed = CDCHandler.upsert_customers(records)
        elif source_name == "orders":
            records = extract_orders_batch(current_wm)
            records_processed = CDCHandler.upsert_orders(records)
        elif source_name == "subscriptions":
            records = extract_subscriptions_batch(current_wm)
            records_processed = CDCHandler.upsert_subscriptions(records)
        elif source_name == "events":
            records = extract_events_batch(current_wm)
            records_processed = CDCHandler.insert_events(records)
        else:
            raise ValueError(f"Unknown source name: {source_name}")

        self.wm_store.update_watermark(source_name, new_wm, records_processed)
        logger.info(f"[SUCCESS] Ingested {records_processed} rows into raw.{source_name}")
        return records_processed

    def run_all(self) -> dict:
        sources = ["customers", "orders", "subscriptions", "events"]
        results = {}
        for s in sources:
            try:
                results[s] = self.run_source(s)
            except Exception as e:
                logger.error(f"Failed to ingest source {s}: {e}")
                results[s] = 0
        return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Ingestion Pipeline")
    parser.add_argument("--source", type=str, help="Specific source to ingest (customers, orders, subscriptions, events)")
    parser.add_argument("--full-refresh", action="store_true", help="Reset watermark and reload from epoch")
    args = parser.parse_args()

    pipeline = IngestionPipeline(full_refresh=args.full_refresh)
    if args.source:
        pipeline.run_source(args.source)
    else:
        pipeline.run_all()
