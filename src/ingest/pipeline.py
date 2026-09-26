"""
Master Ingestion Pipeline Orchestrator.
Coordinates extract -> CDC transformation -> upsert -> watermark persistence.
Supports late-arriving data lookback windows and detailed batch audit logging.
"""

import argparse
import logging
import uuid
from datetime import UTC, datetime, timedelta

from src.ingest.cdc import CDCHandler
from src.ingest.extract import extract_batches
from src.ingest.watermark import WatermarkStore

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


class IngestionPipeline:
    """
    Executes incremental loads across all source datasets with late-data lookback support.
    """

    def __init__(self, full_refresh: bool = False, batch_size: int = 25000, lookback_minutes: int = 0):
        self.full_refresh = full_refresh
        self.batch_size = batch_size
        self.lookback_window = timedelta(minutes=lookback_minutes) if lookback_minutes > 0 else None
        self.wm_store = WatermarkStore()
        self.run_batch_id = f"RUN-{uuid.uuid4().hex[:8].upper()}"

    def run_source(self, source_name: str) -> int:
        logger.info(f"--- Starting Ingestion for source: {source_name} [Batch ID: {self.run_batch_id}] ---")

        default_wm = datetime(1970, 1, 1, tzinfo=UTC)
        current_wm = default_wm if self.full_refresh else self.wm_store.get_watermark(source_name, default=default_wm)
        logger.info(f"Committed Watermark for {source_name}: {current_wm.isoformat()}")

        total_processed = 0
        total_inserts = 0
        total_updates = 0
        total_deletes = 0
        max_ts_seen = None

        try:
            for batch in extract_batches(
                source_name, current_wm, batch_size=self.batch_size, lookback_window=self.lookback_window
            ):
                if batch.row_count == 0:
                    continue

                # Inject run batch ID if record lacks one
                for r in batch.records:
                    if not r.get("_batch_id"):
                        r["_batch_id"] = self.run_batch_id

                # Route to appropriate CDC / upsert handler
                if source_name == "customers":
                    cnt = CDCHandler.upsert_customers(batch.records)
                elif source_name == "orders":
                    cnt = CDCHandler.upsert_orders(batch.records)
                elif source_name == "subscriptions":
                    cnt = CDCHandler.upsert_subscriptions(batch.records)
                elif source_name == "events":
                    cnt = CDCHandler.insert_events(batch.records)
                else:
                    raise ValueError(f"Unknown source name: {source_name}")

                total_processed += cnt
                total_inserts += batch.inserts_count
                total_updates += batch.updates_count
                total_deletes += batch.deletes_count

                if max_ts_seen is None or batch.end_watermark > max_ts_seen:
                    max_ts_seen = batch.end_watermark

                logger.info(
                    f"[{source_name}] Ingested batch of {cnt:,} rows "
                    f"(I: {batch.inserts_count:,}, U: {batch.updates_count:,}, D: {batch.deletes_count:,}) "
                    f"| Total so far: {total_processed:,}"
                )

            # Advance watermark strictly forward
            new_wm = max_ts_seen if (max_ts_seen and max_ts_seen > current_wm) else current_wm
            self.wm_store.update_watermark(
                source_name=source_name,
                new_watermark=new_wm,
                records_count=total_processed,
                batch_id=self.run_batch_id,
                status="SUCCESS",
                error_message=None,
            )
            logger.info(
                f"[SUCCESS] Source '{source_name}' completed. "
                f"Ingested {total_processed:,} rows (I: {total_inserts:,}, U: {total_updates:,}, D: {total_deletes:,}). "
                f"Committed Watermark: {new_wm.isoformat()}"
            )
            return total_processed

        except Exception as e:
            logger.error(f"[ERROR] Pipeline execution failed for source '{source_name}': {e}", exc_info=True)
            self.wm_store.update_watermark(
                source_name=source_name,
                new_watermark=current_wm,
                records_count=0,
                batch_id=self.run_batch_id,
                status="FAILED",
                error_message=str(e),
            )
            raise e

    def run_all(self) -> dict[str, int]:
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
    parser.add_argument(
        "--source", type=str, help="Specific source to ingest (customers, orders, subscriptions, events)"
    )
    parser.add_argument("--full-refresh", action="store_true", help="Reset watermark and reload from epoch")
    parser.add_argument(
        "--lookback-minutes", type=int, default=0, help="Lookback window in minutes for late-arriving data"
    )
    args = parser.parse_args()

    pipeline = IngestionPipeline(full_refresh=args.full_refresh, lookback_minutes=args.lookback_minutes)
    if args.source:
        pipeline.run_source(args.source)
    else:
        pipeline.run_all()
