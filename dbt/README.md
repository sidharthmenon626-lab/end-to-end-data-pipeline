# dbt Transformation Project

This directory contains the dbt transformation layer:

* **`models/staging/`**: Cleaned, standardized views over the `raw` schema.
* **`models/marts/`**: Dimensional star schema models (`dim_customer`, `fact_orders`, `fact_subscription_events`) enforcing schema contracts.
* **`tests/`**: Singular test queries verifying data integrity and business rules.
