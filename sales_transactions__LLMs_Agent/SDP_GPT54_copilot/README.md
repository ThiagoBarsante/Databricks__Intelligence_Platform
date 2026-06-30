# Sales Medallion Pipeline

This project contains a SQL-first, serverless Databricks Lakeflow Spark Declarative Pipeline that builds a bronze, silver, and gold medallion model from CSV files stored in `/Volumes/copilot_gpt54/dw_raw/raw_data`.

## Implemented Assets

- Pipeline name: `pipeline_sales_medallion_copilot_gpt54_v1`
- Catalog: `copilot_gpt54`
- Bronze schema: `bronze_sales_medallion_dev_v1`
- Silver schema: `silver_sales_medallion_dev_v1`
- Gold schema: `gold_sales_medallion_dev_v1`
- Bundle config: `databricks.yml`
- Pipeline resource: `resources/pipeline_sales_medallion_copilot_gpt54_v1.pipeline.yml`
- Transformations: `src/pipeline_sales_medallion_copilot_gpt54_v1/transformations`, named by medallion layer:
  - `01_bronze_sales_transactions_raw.sql`
  - `02_silver_sales_transactions_clean.sql`
  - `03_gold_dim_customer_current.sql`
  - `03_gold_dim_product_current.sql`
  - `03_gold_dim_location_history.sql`
  - `03_gold_dim_payment_type_history.sql`
  - `03_gold_dim_order_status_history.sql`
  - `03_gold_dim_order_calendar.sql`
  - `03_gold_fact_sales_transactions.sql`
  - `03_gold_agg_daily_sales_metrics.sql`
  - `03_gold_agg_monthly_customer_segment_metrics.sql`
  - `03_gold_agg_state_category_sales_metrics.sql`
- SDD docs: `docs/requirements.md`, `docs/design.md`, `docs/task.md`
- Admin SQL: `sql/admin/bootstrap_medallion_schemas.sql`, `sql/admin/cleanup_medallion_schemas.sql`

## Project Setup Executed

The following setup and validation steps were executed in Databricks:

1. Confirmed Databricks CLI connectivity and authenticated workspace profile.
2. Created and validated a standalone Databricks Asset Bundle with `databricks bundle validate`.
3. Implemented bronze ingestion as a streaming table with all source columns loaded as `STRING` plus ingest metadata and rescued data support.
4. Created the bronze, silver, and gold Unity Catalog schemas.
5. Deployed the bundle with `databricks bundle deploy --profile DEFAULT`.
6. Dry-ran the pipeline graph and resolved Lakeflow analysis issues until the full graph compiled successfully.
7. Executed the pipeline with `databricks bundle run pipeline_sales_medallion_copilot_gpt54_v1 --profile DEFAULT`.
8. Verified publication of the bronze, silver, and gold tables and materialized views in Unity Catalog.
9. Reran the deployed pipeline after source data upload and verified non-zero published table counts.

## Medallion Design Summary

### Bronze

- `bronze_sales_transactions_raw`
- Reads CSV files from `/Volumes/copilot_gpt54/dw_raw/raw_data`
- Stores every business column as `STRING`
- Adds `_ingested_at`, `_source_file`, `_source_file_modified_at`, `_source_file_size`, `_rescued_data`
- Uses an explicit CSV schema with `header => true`, `inferSchema => false`, `mode => 'PERMISSIVE'`, and `rescuedDataColumn => '_rescued_data'`
- Applies no business filtering in this layer so the raw feed is preserved as landed

### Silver

- `silver_sales_transactions_clean`
- Streaming table
- Applies no deduplication
- Casts `transaction_id`, `order_date`, `ship_date`, `customer_age`, `quantity`, `unit_price`, `discount_pct`, and `ingestion_date` into analytics-friendly types
- Normalizes customer age into bands, normalizes gender, defaults missing product/category/location/payment/status values, and derives `location_key`
- Calculates `gross_sales_amount`, `discount_amount`, `net_sales_amount`, `shipping_lead_days`, `is_cancelled`, and `is_returned`
- Filters rows with rescued data, invalid transaction IDs, invalid order or ship dates, blank customer or product IDs, invalid quantity, invalid unit price, or out-of-range discount percentages

### Gold

- SCD Type 1 dimensions: `dim_customer_current`, `dim_product_current`
- SCD Type 2 dimensions: `dim_location_history`, `dim_payment_type_history`, `dim_order_status_history`
- Additional materialized view: `dim_order_calendar`
- Fact table: `fact_sales_transactions`
- Aggregate tables: `agg_daily_sales_metrics`, `agg_monthly_customer_segment_metrics`, `agg_state_category_sales_metrics`

## Layer Rules Applied

### Bronze Rules Applied

1. Read source data from `/Volumes/copilot_gpt54/dw_raw/raw_data` as CSV.
2. Persist all business columns as `STRING` regardless of apparent source type.
3. Preserve ingestion lineage with `_ingested_at`, `_source_file`, `_source_file_modified_at`, and `_source_file_size`.
4. Route malformed or drifted data into `_rescued_data` instead of failing the landing layer.
5. Keep the bronze layer append-oriented with no cleansing or business-rule filtering.

### Silver Rules Applied

1. Do not deduplicate rows.
2. Accept only rows where `_rescued_data IS NULL`.
3. Require a parsable `transaction_id`, `order_date`, and `ship_date`.
4. Require non-blank `customer_id` and `product_id`.
5. Require `quantity` to parse as `INT` and be greater than or equal to `0`.
6. Require `unit_price` to parse as `DECIMAL(18,4)` and be greater than or equal to `0`.
7. Allow null `discount_pct`, but when present require it to be between `0` and `100`.
8. Normalize customer age, age band, gender, product category, city, state, payment type, and order status.
9. Derive `location_key`, `gross_sales_amount`, `discount_amount`, `net_sales_amount`, `shipping_lead_days`, `is_cancelled`, and `is_returned`.

### Gold Rules Applied

1. Publish current-state customer and product dimensions as SCD Type 1 using `AUTO CDC` keyed by `customer_id` and `product_id`.
2. Publish location, payment type, and order status dimensions as SCD Type 2 using `AUTO CDC` with history tracking on descriptive attributes.
3. Build `dim_order_calendar` from the union of silver `order_date` and non-null `ship_date`, adding year, quarter, month, week, weekday, and weekend flags.
4. Publish `fact_sales_transactions` as the star-schema fact from the validated silver dataset.
5. Publish `agg_daily_sales_metrics` grouped by `order_date`, `state`, and `product_category`.
6. Publish `agg_monthly_customer_segment_metrics` grouped by month, age band, gender, and payment type.
7. Publish `agg_state_category_sales_metrics` grouped by month, state, and product category.

## Post-Run Table Counts

Snapshot taken: `2026-06-27 17:48:46 -03:00`

Published project tables only are listed below. Internal event-log and materialization backing tables are excluded.

| Schema | Table | Rows |
| --- | --- | ---: |
| bronze_sales_medallion_dev_v1 | bronze_sales_transactions_raw | 100000 |
| silver_sales_medallion_dev_v1 | silver_sales_transactions_clean | 18302 |
| gold_sales_medallion_dev_v1 | dim_customer_current | 17492 |
| gold_sales_medallion_dev_v1 | dim_product_current | 15352 |
| gold_sales_medallion_dev_v1 | dim_location_history | 10 |
| gold_sales_medallion_dev_v1 | dim_payment_type_history | 5 |
| gold_sales_medallion_dev_v1 | dim_order_status_history | 3 |
| gold_sales_medallion_dev_v1 | dim_order_calendar | 1109 |
| gold_sales_medallion_dev_v1 | fact_sales_transactions | 18302 |
| gold_sales_medallion_dev_v1 | agg_daily_sales_metrics | 13183 |
| gold_sales_medallion_dev_v1 | agg_monthly_customer_segment_metrics | 3672 |
| gold_sales_medallion_dev_v1 | agg_state_category_sales_metrics | 1090 |

## Current State

The pipeline is deployed, the source volume is populated, and the published tables have been refreshed successfully. Bronze retained all `100000` raw rows, while silver published `18302` validated rows after applying the documented quality filters. Gold fact and aggregate tables were refreshed successfully from that silver output.

## Rerun Command

```powershell
databricks bundle run pipeline_sales_medallion_copilot_gpt54_v1 --profile DEFAULT
```
