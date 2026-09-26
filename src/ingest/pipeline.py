"""
Master Ingestion Pipeline Orchestrator.
Coordinates extract -> CDC transformation -> upsert -> watermark persistence.
"""

import argparse
import logging
from datetime import datetime, timezone
from src.ingest.watermark import WatermarkStore
from src.ingest.extract import extract_batches
from src.ingest.cdc import CDCHandler

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


class IngestionPipeline:
    """
    Executes incremental loads across all source datasets.
    """

    def __init__(self, full_refresh: bool = False, batch_size: int = 25000):
        self.full_refresh = full_refresh
        self.batch_size = batch_size
        self.wm_store = WatermarkStore()

    def run_source(self, source_name: str) -> int:
        logger.info(f"--- Starting Ingestion for source: {source_name} ---")
        
        default_wm = datetime(1970, 1, 1, tzinfo=timezone.utc)
        current_wm = default_wm if self.full_refresh else self.wm_store.get_watermark(source_name, default=default_wm)
        logger.info(f"Active Watermark for {source_name}: {current_wm.isoformat()}")

        total_processed = 0
        max_ts_seen = None
        ts_col = "event_timestamp" if source_name in ["subscriptions", "events"] else "source_updated_at"

        for batch in extract_batches(source_name, current_wm, batch_size=self.batch_size):
            if not batch:
                continue

            for r in batch:
                ts = r.get(ts_col)
                if ts is not None:
                    if hasattr(ts, "to_pydatetime"):
                        ts = ts.to_pydatetime()
                    if ts.tzinfo is None:
                        ts = ts.replace(tzinfo=timezone.utc)
                    if max_ts_seen is None or ts > max_ts_seen:
                        max_ts_seen = ts

            if source_name == "customers":
                cnt = CDCHandler.upsert_customers(batch)
            elif source_name == "orders":
                cnt = CDCHandler.upsert_orders(batch)
            elif source_name == "subscriptions":
                cnt = CDCHandler.upsert_subscriptions(batch)
            elif source_name == "events":
                cnt = CDCHandler.insert_events(batch)
            else:
                raise ValueError(f"Unknown source name: {source_name}")

            total_processed += cnt
            logger.info(f"Ingested batch: {cnt:,} rows into raw.raw_{source_name} (Total: {total_processed:,})")

        new_wm = max_ts_seen if max_ts_seen is not None else current_wm
        self.wm_store.update_watermark(source_name, new_wm, total_processed)
        logger.info(f"[SUCCESS] Ingested {total_processed:,} rows into raw.raw_{source_name}. Committed Watermark: {new_wm.isoformat()}")
        return total_processed

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
