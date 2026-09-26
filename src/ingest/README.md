# Ingestion Modules & CDC Handlers

This directory contains the Python ingestion pipelines responsible for extracting source records, managing persistent watermark states, and applying Change Data Capture (CDC) mutations.

## Key Components (Milestone 3)
* **Watermark Management**: Persisting state boundaries across runs to prevent duplicate extracts.
* **Idempotent Upserts**: Ensuring pipeline re-runs produce identical warehouse states.
* **CDC Processing**: Translating database transaction logs / mutation flags into analytical updates.
