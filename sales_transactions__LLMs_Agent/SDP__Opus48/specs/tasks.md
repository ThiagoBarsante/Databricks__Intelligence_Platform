# Tasks — Sales Medallion Pipeline (`pipeline_sales_medallion_op48_v1`)

Status legend: [ ] todo · [~] in progress · [x] done

**STATUS: ✅ COMPLETE** — full bronze→silver→gold pipeline built, deployed (single serverless
pipeline), run, and validated. Automated one-shot deploy script delivered and verified.

## Phase 0 — Discovery
- [x] Profile source CSV (100k rows; dirty quantity/price/age/gender; dup transaction_ids)
- [x] Decide dimension sizing (customer/product = large→SCD1; location/category = small→SCD2)
- [x] Author SDD specs (requirements.md, design.md, tasks.md)

## Phase 1 — Schemas
- [x] Create `cowork_op48.bronze`
- [x] Create `cowork_op48.silver`
- [x] Create `cowork_op48.gold`

## Phase 2 — Bronze (built + validated + deployed + tested FIRST)
- [x] Write `bronze/bronze_sales_transactions.sql` (all STRING, schema evolution, metadata)
- [x] Upload files; create pipeline `pipeline_sales_medallion_op48_v1` (serverless, bronze only)
- [x] Run pipeline (full refresh)
- [x] Validate: **100,000 rows**, all-STRING schema, `_ingested_at`/`_source_file*` present
- [x] Showed sample results → paused for user confirmation before Silver

## Phase 3 — Silver
- [x] Write `silver/silver_sales_transactions.sql` (cast all, filter to valid sales, enrich, no dedup)
- [x] Add file to pipeline; run
- [x] Validate: **11,227 rows**, correct types, derived columns; dup transaction_ids retained

## Phase 4 — Gold (star schema)
- [x] `gold/dim_customer.sql` — SCD Type 1 (large) → **10,896 rows**
- [x] `gold/dim_product.sql` — SCD Type 1 (large) → **10,053 rows**
- [x] `gold/dim_location.sql` — SCD Type 2 (small) → **10 rows** (`__START_AT`/`__END_AT`)
- [x] `gold/dim_category.sql` — SCD Type 2 (small) → **5 rows** (fixed: `COLUMNS product_category`)
- [x] `gold/fact_sales.sql` — transaction-grain fact (MV) → **11,227 rows** (10,059 distinct txn)
- [x] `gold/agg_sales_by_category_month.sql` — aggregate #1 (MV) → **185 rows**
- [x] `gold/agg_sales_by_state_status.sql` — aggregate #2 (MV, fact⨝SCD2 dim) → **18 rows**
- [x] Run full pipeline; validate dims/fact/aggregates

## Phase 5 — Single-pipeline consolidation + automation
- [x] Confirm all 9 transformations run as ONE pipeline (single full-refresh update)
- [x] Add `databricks-sdk` to the uv project (`uv add databricks-sdk`)
- [x] Write `scripts/deploy_and_run.py` (ingest → schemas → upload → create/update → run → validate)
- [x] Verify end-to-end via `uv run scripts/deploy_and_run.py` (run COMPLETED, gold counts correct)
- [x] Update README; remove PowerShell variant (Python-only per user preference)

## Phase 6 — Final validation
- [x] `get_table_stats_and_schema` across all 3 schemas
- [x] Spot-check SCD2 `__START_AT`/`__END_AT`, fact↔silver row parity, aggregate sanity
- [x] Summarize results to user

## Issues encountered & resolved
- **`dim_category` SCD2 row explosion (4,818 → 5):** the sequencing column `seq_ts` leaked into
  tracked attributes; restricted the flow to `COLUMNS product_category`.
- **Deploy script — workspace upload:** used `ImportFormat.RAW` so `.sql` files land as FILES
  (matching `libraries.file.path`), not notebooks.
- **Deploy script — validation:** SQL warehouse cold-starts; added statement polling so gold
  row counts print reliably.

## Rollback / iteration notes
- Incompatible streaming change → drop affected table(s) or schema, re-run `full_refresh=True`.
- Keep pipeline name stable; never use `CREATE OR REPLACE` (use `CREATE OR REFRESH`).
