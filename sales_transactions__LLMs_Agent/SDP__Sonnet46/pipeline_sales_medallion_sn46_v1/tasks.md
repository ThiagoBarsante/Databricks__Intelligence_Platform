# Tasks — Sales Transactions Medallion Pipeline

- [x] 1. Explore source volume `/Volumes/cowork_sn46/dw_raw/raw_data` and existing catalog/schemas
- [x] 2. Write SDD docs: `requirements.md`, `design.md`, `tasks.md`
- [x] 3. Create UC schemas `cowork_sn46.bronze`, `cowork_sn46.silver`, `cowork_sn46.gold`
- [x] 4. Write bronze transformation (`01_bronze_sales_transactions.sql`) — Auto Loader, all
      STRING, schema evolution, ingest metadata
- [x] 5. Upload files & create pipeline `pipeline_sales_medallion_sn46_v1` with **bronze only**;
      run; validate with `get_table_stats_and_schema`; **show results**
      → 100,000 rows, all columns STRING, `_ingested_at`/`_source_file` present. ✅
- [x] 6. Write silver transformation (`02_silver_sales_transactions.sql`) — cast/clean/enrich,
      no dedup; add to pipeline; run; validate
      → 100,000 rows (1:1 with bronze), correct types, gender normalized, `net_amount` +
      DQ flags computed. ✅
- [x] 7. Write gold dimension transformations:
      - `03_gold_dim_product.sql` (SCD Type 2 — smaller dimension) → 43,306 rows, all current
        (no category reassignments observed in source data)
      - `04_gold_dim_customer.sql` (SCD Type 1 — larger dimension) → 78,649 rows (deduped to
        current state per customer)
- [x] 8. Write gold fact transformation `05_gold_fact_sales_transactions.sql`
      (SCD Type 1 / dedup on `transaction_id`) → 43,236 rows (deduped from 100,000 silver rows
      down to 43,236 distinct `transaction_id`s — confirms the silver-deferred dedup strategy)
- [x] 9. Write gold aggregate transformations `06_gold_aggregates.sql`:
      - `agg_category_monthly_metrics` (materialized view) → 185 rows (5 categories × 37 months)
      - `agg_customer_segment_metrics` (materialized view) → 90 rows (3 genders × 5 age
        brackets × 6 states)
- [x] 10. Add gold files to pipeline; run full pipeline; validate end-to-end with
       `get_table_stats_and_schema` across bronze/silver/gold; spot-check aggregate metrics
       → All 8 tables created and populated correctly across bronze/silver/gold schemas. ✅
- [x] 11. Summarize final results for the user (row counts, sample metrics, table list)
