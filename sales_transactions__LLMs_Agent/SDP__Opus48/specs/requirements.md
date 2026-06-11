# Requirements — Sales Medallion Pipeline (`pipeline_sales_medallion_op48_v1`)

> **STATUS: ✅ DELIVERED.** All requirements implemented, deployed as a single serverless
> pipeline, run, and validated. Traceability matrix in §6.

## 1. Overview
Build a **Serverless Lakeflow Spark Declarative Pipeline (SDP)**, written in **SQL**, that
ingests raw sales-transaction CSV files and refines them through a **Bronze → Silver → Gold
medallion architecture** using **dimensional (star-schema) modelling** in the Gold layer.

- **Source:** `/Volumes/cowork_op48/dw_raw/raw_data` (CSV, header row present)
- **Catalog:** `cowork_op48`
- **Pipeline name:** `pipeline_sales_medallion_op48_v1`
- **Compute:** Serverless
- **Language:** SQL (Lakeflow `CREATE OR REFRESH` syntax)

## 2. Source data
CSV columns (one row = one sales transaction line):

| Column | Notes from profiling (100,000 rows) |
|--------|-------------------------------------|
| `transaction_id` | NOT unique — 43,236 distinct of 100,000 (duplicates exist) |
| `order_date` | all parseable as DATE |
| `ship_date` | sometimes earlier than `order_date` (dirty) |
| `customer_id` | 78,649 distinct → **large** dimension |
| `customer_age` | 61,592 invalid (negative, >120, or null) |
| `gender` | 4 variants: `M`, `F`, `Male`, `Female` (+ nulls) |
| `product_id` | 43,306 distinct → **large** dimension |
| `product_category` | 5 distinct → **small** dimension |
| `quantity` | 66,647 rows ≤ 0 (zero / negative) |
| `unit_price` | 33,576 null, 33,239 ≤ 0 |
| `discount_pct` | 33,170 null; some values > 100 (invalid) |
| `city` | 10 distinct → **small** dimension |
| `state` | 6 distinct → **small** dimension |
| `payment_type` | 4 distinct (+ nulls) |
| `order_status` | 3 distinct: `Cancelled`, `Delivered`, `Returned` |
| `ingestion_date` | source-supplied batch date |

**Only 11,227 rows are valid sales** (`quantity > 0 AND unit_price > 0`).

## 3. Functional requirements

### R1 — Schema isolation
Separate UC schema per layer inside `cowork_op48`: `bronze`, `silver`, `gold`.
One pipeline writes to all three using fully-qualified names.

### R2 — Bronze layer
- **R2.1** Load **all columns as STRING** (no casting).
- **R2.2** Enable **schema evolution** (`schemaEvolutionMode => 'addNewColumns'`).
- **R2.3** Add ingest metadata: `_ingested_at`, and source-file metadata
  `_source_file`, `_source_file_modified_at`, `_source_file_size`.
- **R2.4** Append-only streaming table via Auto Loader (`STREAM read_files`).
- **R2.5** Bronze must be **built, validated, deployed, and tested first**, with results
  shown, before Silver/Gold are added.

### R3 — Silver layer
- **R3.1** Streaming table(s) reading from Bronze.
- **R3.2** **Filter and cast all information** — drop records that cannot represent a real
  sale; cast every column to its proper type.
- **R3.3** **Enrich** with derived business columns (net/gross amount, shipping delay,
  normalized gender, age band).
- **R3.4** **No deduplication** at this layer (duplicate `transaction_id`s are retained).

### R4 — Gold layer (star schema)
- **R4.1** Dimensional model: fact table + dimension tables.
- **R4.2** **Small** dimensions → **SCD Type 2** (history tracked).
- **R4.3** **Large** dimensions → **SCD Type 1** (current state only).
- **R4.4** Provide **2 aggregate tables** demonstrating common metrics for sales data.
- **R4.5** Gold must follow fact / dimensional modelling.

### R5 — Lifecycle
- Deploy via MCP (`manage_pipeline`), serverless, full refresh during development.
- Use fresh schemas during iteration; clean up a failed schema before retrying.
- Validate every layer with `get_table_stats_and_schema` + sample queries.

## 4. Non-functional requirements
- Idempotent, declarative (`CREATE OR REFRESH`).
- Pipeline files are plain `.sql` files (no notebooks).
- All money columns use `DECIMAL` for precision.

## 5. Out of scope
- Real-time/continuous mode (batch full-refresh is sufficient for the demo).
- Data quality quarantine tables (records are filtered, not quarantined).
- Orchestration/scheduling jobs.

## 6. Requirement traceability (as-built)

| Req | Status | Evidence |
|-----|--------|----------|
| R1 — schema isolation | ✅ | `cowork_op48.bronze` / `.silver` / `.gold`, one pipeline, FQ names |
| R2.1 — bronze all STRING | ✅ | 16 source cols all `STRING` (`schemaHints`) |
| R2.2 — schema evolution | ✅ | `schemaEvolutionMode => 'addNewColumns'` |
| R2.3 — ingest + file metadata | ✅ | `_ingested_at`, `_source_file`, `_source_file_modified_at`, `_source_file_size` |
| R2.4 — append streaming | ✅ | `STREAM read_files(...)` streaming table |
| R2.5 — bronze first | ✅ | bronze built/validated (100,000 rows) and shown before silver/gold |
| R3.1 — silver streaming from bronze | ✅ | `STREAM cowork_op48.bronze.bronze_sales_transactions` |
| R3.2 — filter + cast all | ✅ | expectations drop invalid rows; every column cast; 11,227 valid rows |
| R3.3 — enrich | ✅ | gross/net amount, age_band, gender_clean, discount_pct_clean, shipping_delay_days |
| R3.4 — no dedup | ✅ | 11,227 rows / 9,426 distinct txn (dups retained) |
| R4.1 — fact + dimensions | ✅ | `fact_sales` + 4 dims |
| R4.2 — small → SCD2 | ✅ | `dim_location` (10), `dim_category` (5) with `__START_AT`/`__END_AT` |
| R4.3 — large → SCD1 | ✅ | `dim_customer` (10,896), `dim_product` (10,053) |
| R4.4 — 2 gold aggregates | ✅ | `agg_sales_by_category_month` (185), `agg_sales_by_state_status` (18) |
| R4.5 — fact/dimensional modelling | ✅ | star schema, FKs + degenerate dims, `location_id = md5(city\|state)` |
| R5 — lifecycle/automation | ✅ | `scripts/deploy_and_run.py` (`uv run`): ingest→schemas→upload→deploy→run→validate |
