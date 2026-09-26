# Raw Data Landing Zone

This directory serves as the landing area for immutable, raw data extracts before they are ingested into the `raw` schema of the warehouse.

---

## 1. Directory Structure

```text
data/raw/
├── orders/             # Raw ecommerce transaction extracts (.parquet / .csv)
├── subscriptions/      # SaaS subscription contract extracts (.parquet / .csv)
└── events/             # Web & app customer telemetry events (.parquet / .csv)
```

---

## 2. Ingestion Principles

1. **Immutability**: Raw files once placed in `data/raw/` are never modified in place.
2. **Format**: Compressed Parquet (`snappy` compression) is prioritized for high-volume telemetry (~400k rows) due to columnar efficiency and schema preservation; CSV extracts are supported with strict quoting.
3. **Partitioning**: Event data is partitioned chronologically by `dt=YYYY-MM-DD`.
4. **Git Hygiene**: Actual raw data files (*.csv, *.parquet) are excluded from Git via `.gitignore`. Staging manifests and schemas are tracked in `data/manifest.json`.
