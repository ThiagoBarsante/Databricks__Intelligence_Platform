# Sales Codex53 Medallion Requirements

## Objective
Build a Serverless Spark Declarative Pipeline (SQL-first) that ingests raw CSV sales data from a Unity Catalog volume and publishes bronze, silver, and gold layers into separate Unity Catalog schemas.

## Scope
- Source: `/Volumes/kiro_catalog/demo_dw_raw2/raw_vol2/sales_transactions.csv`
- Target catalog: `kiro_catalog`
- Development schemas for this iteration:
  - Bronze: `sales_codex53_bronze_dev_20260601`
  - Silver: `sales_codex53_silver_dev_20260601`
  - Gold: `sales_codex53_gold_dev_20260601`

## Functional Requirements
1. Pipeline must use Spark Declarative Pipeline SQL syntax (`CREATE OR REFRESH ...`) and run on serverless compute.
2. Bronze layer must ingest CSV with schema evolution support and include ingestion metadata.
3. Bronze layer must append these metadata fields:
   - `_ingest_ts`
   - `_source_file`
   - `_source_file_modification_ts`
   - `_source_file_size_bytes`
4. Silver layer must be a streaming table that performs data cleaning and enrichment.
5. Silver layer must not deduplicate records.
6. Gold layer must implement:
   - SCD Type 2 for smaller dimensional tables.
   - SCD Type 1 for larger fact-style table.
7. Gold layer must include two aggregate tables with common sales metrics.
8. Build/validate/deploy/test sequence must run bronze first, with evidence collected before silver/gold rollout.

## Data Quality and Transformation Rules
1. Parse/cast known typed columns from bronze strings.
2. Normalize `gender` values into canonical values (`Male`, `Female`, `Unknown`).
3. Clamp out-of-range measures in silver:
   - negative `quantity` becomes `0` for analytics fields.
   - invalid or negative `unit_price` becomes `NULL` for cleaned price.
   - invalid `discount_pct` defaults to `0`.
4. Derive silver metrics: `gross_amount`, `discount_amount`, `net_amount`, `ship_lag_days`, `order_month`.

## Non-Functional Requirements
1. Keep transformations in plain `.sql` files.
2. Use fully qualified table names to support multi-schema medallion in one pipeline.
3. Preserve lineage by carrying source metadata through silver and fact tables.
4. Ensure artifacts are reproducible and versioned in repository.

## Deliverables
1. SDD documents:
   - `requirements.md`
   - `design.md`
   - `task.md`
2. SQL assets:
   - UC setup and cleanup SQL scripts
   - bronze, silver, gold transformation SQL files
3. Executed bronze deployment and validation evidence.
