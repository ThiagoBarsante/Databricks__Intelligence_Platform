# Tasks — Sales Medallion Pipeline (`pipeline_sales_medallion_fa5_v1`)

## Phase 0 — Setup
- [ ] T0.1 Profile source data in `/Volumes/cowork_fable5/dw_raw/raw_data` (row count, schema, quality issues)
- [ ] T0.2 Create UC schemas: `cowork_fable5.bronze_fa5_v1`, `cowork_fable5.silver_fa5_v1`, `cowork_fable5.gold_fa5_v1`

## Phase 1 — Bronze (deploy & validate FIRST)
- [ ] T1.1 Write `pipeline/01_bronze_sales.sql` — streaming table, Auto Loader CSV, all STRING, schema evolution, `_ingested_at` / `_source_file` / file metadata
- [ ] T1.2 Upload pipeline files to workspace
- [ ] T1.3 Create serverless pipeline `pipeline_sales_medallion_fa5_v1` (catalog `cowork_fable5`) with bronze only and run it
- [ ] T1.4 Validate: state COMPLETED, bronze row count = 100,000, all business columns STRING, metadata columns populated
- [ ] T1.5 Show bronze results (stats + sample rows) before continuing

## Phase 2 — Silver
- [ ] T2.1 Write `pipeline/02_silver_sales.sql` — streaming table from bronze: cast all columns, normalize gender, null-out invalid age/discount/ship_date, expectations (DROP on critical rules), enrichment columns, NO dedup
- [ ] T2.2 Re-upload, update pipeline, run
- [ ] T2.3 Validate: typed schema, row count vs bronze, expectation drop metrics, sample rows

## Phase 3 — Gold (star schema)
- [ ] T3.1 Write `pipeline/03_gold_dimensions.sql` — `dim_customer` (SCD1), `dim_product` (SCD1), `dim_location` (SCD2), `dim_payment_type` (SCD2) via AUTO CDC flows
- [ ] T3.2 Write `pipeline/04_gold_fact.sql` — `fact_sales` SCD1 keyed by `transaction_id` (gold-layer dedup)
- [ ] T3.3 Write `pipeline/05_gold_aggregates.sql` — `agg_monthly_sales`, `agg_customer_demographics` materialized views
- [ ] T3.4 Re-upload, update pipeline, run
- [ ] T3.5 Validate: fact grain = 1 row/transaction_id, SCD2 `__START_AT`/`__END_AT` present, dims populated, aggregates plausible

## Phase 4 — Final validation & handoff
- [ ] T4.1 `get_table_stats_and_schema` across all three schemas
- [ ] T4.2 Sample business queries (current-state SCD2 view, top categories, monthly trend)
- [ ] T4.3 Summarize results, pipeline ID/URL, and how to query SCD2 tables

## Iteration policy
If a layer must be redesigned: bump schema suffix to `_v2`, **drop the old `_v1` schemas**
(`DROP SCHEMA ... CASCADE`) before retrying, and update the pipeline default schema.
