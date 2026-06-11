# Sales Codex53 Serverless SDP Medallion Pipeline

## Overview
This project implements a SQL-first, Serverless Spark Declarative Pipeline (SDP) for a medallion architecture (bronze, silver, gold).

The pipeline ingests raw CSV sales data from Unity Catalog volume storage and publishes curated outputs into separate Unity Catalog schemas for each layer.

## Source and Target

### Source
- Volume file: `/Volumes/kiro_catalog/demo_dw_raw2/raw_vol2/sales_transactions.csv`
- Runtime ingestion pattern: Auto Loader directory ingestion from `/Volumes/kiro_catalog/demo_dw_raw2/raw_vol2/`

### Target Catalog and Schemas (Dev)
- Catalog: `kiro_catalog`
- Bronze schema: `sales_codex53_bronze_dev_20260601`
- Silver schema: `sales_codex53_silver_dev_20260601`
- Gold schema: `sales_codex53_gold_dev_20260601`

## Architecture Flow

1. Bronze streaming table reads CSV files with schema evolution enabled.
2. Bronze appends ingestion and source metadata columns.
3. Silver streaming table cleans and enriches data.
4. Silver does not deduplicate records (by requirement).
5. Gold publishes:
   - SCD Type 2 dimensions for small entities (customer, product)
   - SCD Type 1 fact for larger transactional entity (sales)
   - Two aggregate materialized views for business KPIs

Flow summary:

`Volume CSV -> Bronze (raw + metadata) -> Silver (clean + enrich, no dedup) -> Gold (SCD + metrics)`

## SQL Artifacts

### Setup and Cleanup
- `sql/setup_uc_schemas.sql`
- `sql/cleanup_previous_iteration.sql`

### Transformations
- `transformations/bronze_sales_transactions.sql`
- `transformations/silver_sales_transactions.sql`
- `transformations/gold_dim_customer_scd2.sql`
- `transformations/gold_dim_product_scd2.sql`
- `transformations/gold_fact_sales_scd1.sql`
- `transformations/gold_mv_daily_sales_metrics.sql`
- `transformations/gold_mv_category_state_metrics.sql`

## Initial Validation Results

## 1) Source Validation
- Source rows discovered: **100000**
- Source files: **1**

## 2) Bronze-First Validation (Executed Before Silver/Gold)
- Bronze table: `kiro_catalog.sales_codex53_bronze_dev_20260601.bronze_sales_transactions`
- Bronze rows: **100000**
- Metadata columns validated:
  - `_ingest_ts`
  - `_source_file`
  - `_source_file_modification_ts`
  - `_source_file_size_bytes`
- Sample query validated successfully against bronze.

## 3) Full Medallion Validation

### Silver
- `kiro_catalog.sales_codex53_silver_dev_20260601.silver_sales_transactions`: **100000** rows

### Gold
- `kiro_catalog.sales_codex53_gold_dev_20260601.gold_dim_customer_scd2`: **99934** rows
- `kiro_catalog.sales_codex53_gold_dev_20260601.gold_dim_product_scd2`: **95023** rows
- `kiro_catalog.sales_codex53_gold_dev_20260601.gold_fact_sales_scd1`: **43236** rows
- `kiro_catalog.sales_codex53_gold_dev_20260601.gold_mv_daily_sales_metrics`: **6483** rows
- `kiro_catalog.sales_codex53_gold_dev_20260601.gold_mv_category_state_metrics`: **450** rows

## Notes from Initial Run
- Bronze ingestion initially failed when `read_files` pointed to a single file path.
- The pipeline was corrected to use directory ingestion (`/Volumes/kiro_catalog/demo_dw_raw2/raw_vol2/`), after which bronze validation succeeded.
- A temporary bronze-only pipeline was removed to avoid table ownership conflict before running the consolidated medallion pipeline.

## SDD Documents
- `/sdd/sales_codex53_medallion/requirements.md`
- `/sdd/sales_codex53_medallion/design.md`
- `/sdd/sales_codex53_medallion/task.md`

## Iteration Guidance
When iterating with new schema versions:

1. Run cleanup script to drop previous dev schemas.
2. Run setup script to create fresh bronze/silver/gold schemas.
3. Redeploy and re-run bronze-first validation before executing the full medallion refresh.
