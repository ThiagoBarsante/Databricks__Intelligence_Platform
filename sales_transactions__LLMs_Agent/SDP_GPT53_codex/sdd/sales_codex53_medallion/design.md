# Sales Codex53 Medallion Design

## Architecture Overview
Single Serverless Spark Declarative Pipeline with SQL transformations and fully qualified Unity Catalog targets.

- Source volume file:
  - `/Volumes/kiro_catalog/demo_dw_raw2/raw_vol2/sales_transactions.csv`
- Output catalog: `kiro_catalog`
- Output schemas:
  - Bronze: `sales_codex53_bronze_dev_20260601`
  - Silver: `sales_codex53_silver_dev_20260601`
  - Gold: `sales_codex53_gold_dev_20260601`

## Layer Design

### Bronze
Table: `kiro_catalog.sales_codex53_bronze_dev_20260601.bronze_sales_transactions` (streaming table)

Purpose:
- Raw append-only landing with schema evolution.
- Capture source and ingest metadata.

Key implementation:
- `FROM STREAM read_files(..., format => 'csv', header => 'true', inferSchema => 'true', schemaEvolutionMode => 'addNewColumns', rescuedDataColumn => '_rescued_data')`

### Silver
Table: `kiro_catalog.sales_codex53_silver_dev_20260601.silver_sales_transactions` (streaming table)

Purpose:
- Clean and enrich events for analytics.
- No deduplication at this layer by requirement.

Key implementation:
- Cast to strong data types.
- Normalize dimensions (`gender`, `payment_type`).
- Derive `gross_amount`, `discount_amount`, `net_amount`, `ship_lag_days`, and `order_month`.
- Data quality constraints drop records with missing core business keys.

### Gold
Purpose:
- Curated dimensions/facts and business aggregates.

Entities:
1. `gold_dim_customer_scd2` (materialized view): SCD Type 2 style history tracking for customer attributes.
2. `gold_dim_product_scd2` (materialized view): SCD Type 2 style history tracking for product attributes.
3. `gold_fact_sales_scd1` (materialized view): SCD Type 1 latest-state transaction fact.
4. `gold_mv_daily_sales_metrics` (materialized view): daily KPI rollup.
5. `gold_mv_category_state_metrics` (materialized view): category and geography KPI rollup.

## SCD Strategy

### SCD Type 2 (Small Tables)
Approach:
- Build dimensional history snapshots from silver using attribute hash change detection.
- For each business key, compute `effective_start_ts`, `effective_end_ts`, and `is_current`.

### SCD Type 1 (Large Table)
Approach:
- Use latest row per `transaction_id` based on processing timestamps.
- Replace prior state by keeping only most recent version in gold fact view.

## Deployment and Validation Strategy
1. Create bronze/silver/gold schemas.
2. Deploy bronze-only pipeline file first.
3. Run and validate bronze row count/schema/sample.
4. Extend pipeline with silver and gold files.
5. Run full refresh and validate all layers.

## Iteration and Cleanup
During retries, drop prior dev schemas and recreate them before rerun using provided cleanup script.
