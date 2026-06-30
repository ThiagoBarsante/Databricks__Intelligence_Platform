# Design

## Solution Overview

The solution is a standalone Databricks Asset Bundle that deploys a single serverless Lakeflow pipeline named `pipeline_sales_medallion_copilot_gpt54_v1`. The pipeline publishes datasets into three schemas in catalog `copilot_gpt54`:

1. `bronze_sales_medallion_dev_v1`
2. `silver_sales_medallion_dev_v1`
3. `gold_sales_medallion_dev_v1`

All transformations are implemented as plain SQL files under the pipeline source folder. Each file is prefixed by its medallion layer (`01_bronze_`, `02_silver_`, `03_gold_`) so the source folder reads top-to-bottom in layer order. Execution order itself is resolved by the Lakeflow dependency graph, not by the filename prefix.

## Layer Design

### Bronze

`bronze_sales_transactions_raw` reads CSV files from `/Volumes/copilot_gpt54/dw_raw/raw_data` using `STREAM read_files(...)`.

Design choices:

1. Explicit all-string schema to allow analysis even when the source directory is empty.
2. `rescuedDataColumn` plus permissive mode to preserve malformed or drifted records.
3. Operational metadata columns for ingest timestamp, file path, modified time, and file size.
4. No business filtering is applied in bronze; the layer preserves the landed feed as strings plus metadata.

Applied bronze rules:

1. `format => 'csv'`, `header => true`, and `inferSchema => false` are enforced.
2. Every source column is cast to `STRING` before publication.
3. `_rescued_data` is retained as a first-class audit column.
4. Bronze is append-style and does not reject or normalize business values.

### Silver

`silver_sales_transactions_clean` is a streaming table over bronze.

Design choices:

1. No deduplication.
2. Invalid numeric and date records are filtered out.
3. Business enrichments are derived inline: age band, normalized gender, normalized payment type, normalized order status, location key, sales amounts, and shipping lead days.
4. A stable `source_sequence_at` timestamp is retained for downstream SCD processing.

Applied silver rules:

1. Reject rows where `_rescued_data` is populated.
2. Reject rows where `transaction_id` does not parse to `BIGINT`.
3. Reject rows where `order_date` or `ship_date` does not parse to `DATE`.
4. Reject rows with blank `customer_id` or `product_id`.
5. Reject rows where `quantity` is null or negative.
6. Reject rows where `unit_price` is null or negative.
7. Reject rows where `discount_pct` is present but outside `0..100`.
8. Normalize age, age band, gender, category, location, payment type, and order status.
9. Derive `location_key`, `gross_sales_amount`, `discount_amount`, `net_sales_amount`, `shipping_lead_days`, `is_cancelled`, and `is_returned`.

### Gold

Gold uses a star-schema-oriented model.

Dimensions:

1. `dim_customer_current` as SCD Type 1.
2. `dim_product_current` as SCD Type 1.
3. `dim_location_history` as SCD Type 2.
4. `dim_payment_type_history` as SCD Type 2.
5. `dim_order_status_history` as SCD Type 2.

Fact:

1. `fact_sales_transactions` as the central sales fact table with dimension keys and measures.

Aggregates:

1. `agg_daily_sales_metrics` for daily sales metrics by state and product category.
2. `agg_monthly_customer_segment_metrics` for monthly metrics by customer segment and payment type.
3. `agg_state_category_sales_metrics` for monthly geography and merchandising rollups.

Applied gold rules:

1. `dim_customer_current` uses SCD Type 1 keyed by `customer_id`.
2. `dim_product_current` uses SCD Type 1 keyed by `product_id`.
3. `dim_location_history` uses SCD Type 2 keyed by `location_key` and tracks history on `city` and `state`.
4. `dim_payment_type_history` uses SCD Type 2 keyed by `payment_type_code` and tracks history on `payment_type_name`.
5. `dim_order_status_history` uses SCD Type 2 keyed by `order_status_code` and tracks history on `order_status_name`.
6. `dim_order_calendar` is generated from distinct silver order and ship dates and derives calendar attributes plus `is_weekend`.
7. `fact_sales_transactions` republishes the validated silver measures and business dimensions into the star-schema fact grain.
8. `agg_daily_sales_metrics` summarizes daily activity by date, state, and product category.
9. `agg_monthly_customer_segment_metrics` summarizes month-level performance by age band, gender, and payment type.
10. `agg_state_category_sales_metrics` summarizes month-level performance by state and product category.

## Deployment Design

1. Bundle root config is in `databricks.yml`.
2. Pipeline resource config is in `resources/pipeline_sales_medallion_copilot_gpt54_v1.pipeline.yml`.
3. Schema bootstrap and cleanup scripts are stored under `sql/admin`.
4. The pipeline uses fully qualified names so one pipeline can publish to multiple schemas.
5. Transformation files are named by layer (`01_bronze_`, `02_silver_`, `03_gold_`) and picked up via the `transformations/**` glob in the pipeline resource.

## Current Data Profile

The source volume is now populated and the latest successful pipeline rerun produced these results:

1. Bronze stored `100000` raw rows.
2. Silver published `18302` typed and validated rows after applying the quality filters.
3. Gold published the fact table, SCD dimensions, and aggregate views successfully from the silver layer.

This row reduction is expected because the silver layer intentionally filters invalid records rather than preserving them for analytics.
