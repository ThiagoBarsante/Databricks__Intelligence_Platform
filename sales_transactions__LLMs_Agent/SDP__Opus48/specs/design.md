# Design — Sales Medallion Pipeline (`pipeline_sales_medallion_op48_v1`)

> **STATUS: ✅ IMPLEMENTED & VERIFIED.** All layers run as a **single serverless pipeline**
> (one full-refresh update). Deployment is automated by `scripts/deploy_and_run.py` (run via
> `uv run`). See §8 for the as-built results.

## 1. Architecture

```
/Volumes/cowork_op48/dw_raw/raw_data (CSV)
        │  STREAM read_files (Auto Loader, all STRING, schema evolution)
        ▼
cowork_op48.bronze.bronze_sales_transactions      (streaming table, append)
        │  cast + filter + enrich (NO dedup)
        ▼
cowork_op48.silver.silver_sales_transactions      (streaming table)
        │
        ├─► dims (AUTO CDC)                         ├─► facts/aggregates
        ▼                                           ▼
cowork_op48.gold.dim_customer   (SCD1, large)   cowork_op48.gold.fact_sales (MV, star center)
cowork_op48.gold.dim_product    (SCD1, large)   cowork_op48.gold.agg_sales_by_category_month (MV)
cowork_op48.gold.dim_location   (SCD2, small)   cowork_op48.gold.agg_sales_by_state_status   (MV)
cowork_op48.gold.dim_category   (SCD2, small)
```

## 2. Bronze design
**Table:** `cowork_op48.bronze.bronze_sales_transactions`

- `STREAM read_files(..., format => 'csv', header => 'true')`.
- `schemaHints` forces **every** source column to `STRING`.
- `schemaEvolutionMode => 'addNewColumns'` → schema evolution.
- Added metadata columns:
  - `_ingested_at = current_timestamp()`
  - `_source_file = _metadata.file_path`
  - `_source_file_modified_at = _metadata.file_modification_time`
  - `_source_file_size = _metadata.file_size`
- Append-only, minimal transforms (medallion bronze rule).

## 3. Silver design
**Table:** `cowork_op48.silver.silver_sales_transactions` (streaming table from `STREAM bronze`).

### 3.1 Casting (R3.2)
| Target column | Source → cast |
|---------------|---------------|
| `transaction_id` | `CAST(... AS BIGINT)` |
| `order_date`, `ship_date` | `CAST(... AS DATE)` |
| `customer_id`, `product_id` | `STRING` (trimmed) |
| `customer_age` | `CAST(... AS INT)` (validated, see enrich) |
| `quantity` | `CAST(... AS INT)` |
| `unit_price` | `CAST(... AS DECIMAL(12,2))` |
| `discount_pct` | `CAST(... AS DECIMAL(6,2))` |
| `product_category`, `city`, `state`, `payment_type`, `order_status` | `STRING` (trimmed) |
| `ingestion_date` | `CAST(... AS DATE)` |

### 3.2 Filters (drop record) — keep only rows that can be a real sale
- `transaction_id` not null
- `order_date` not null (parseable)
- `quantity > 0`
- `unit_price > 0`
Expected survivors ≈ 11,227 rows.

### 3.3 Enrichment (keep record, derive/normalize)
- `gender_clean`: `M`/`Male`→`Male`, `F`/`Female`→`Female`, else `Unknown`.
- `customer_age_clean`: keep `customer_age` only when `BETWEEN 0 AND 120`, else `NULL`.
- `age_band`: `Unknown`/`<18`/`18-29`/`30-44`/`45-59`/`60+`.
- `discount_pct_clean`: keep when `BETWEEN 0 AND 100`, else `0`.
- `gross_amount = quantity * unit_price` `DECIMAL(14,2)`.
- `net_amount = quantity * unit_price * (1 - discount_pct_clean/100)` `DECIMAL(14,2)`.
- `shipping_delay_days = DATEDIFF(ship_date, order_date)` (can be negative = dirty, retained).
- `order_year`, `order_month` (for aggregates).
- Carries `_ingested_at`, `_source_file` lineage forward.

**No dedup** — duplicate `transaction_id`s remain.

## 4. Gold design (star schema)

### 4.1 Dimension sizing decision
| Dimension | Distinct keys | Size | SCD type | Rationale |
|-----------|---------------|------|----------|-----------|
| `dim_customer` | 78,649 | large | **Type 1** | R4.3 large → current state only |
| `dim_product`  | 43,306 | large | **Type 1** | R4.3 large → current state only |
| `dim_location` (city+state) | ≤10 | small | **Type 2** | R4.2 small → track history |
| `dim_category` | 5 | small | **Type 2** | R4.2 small → track history |

### 4.2 SCD implementation (AUTO CDC)
Each dimension has a **clean CDC source** (streaming view/table) selecting its attributes +
a `SEQUENCE BY` key, then an `AUTO CDC INTO` flow.

- **Sequence key:** `_ingested_at` (single batch → one version per key; demonstrates the
  pattern and dedups multi-row keys, e.g. duplicate customers/products).
- **dim_customer** — `KEYS(customer_id)`, attrs `customer_age_clean, gender_clean, age_band`,
  `STORED AS SCD TYPE 1`.
- **dim_product** — `KEYS(product_id)`, attrs `product_category`, `STORED AS SCD TYPE 1`.
- **dim_location** — `KEYS(location_id)` where `location_id = md5(city||'|'||state)`,
  attrs `city, state`, `STORED AS SCD TYPE 2` (`__START_AT`/`__END_AT`).
- **dim_category** — `KEYS(product_category)`, `STORED AS SCD TYPE 2`.

### 4.3 Fact table
**`cowork_op48.gold.fact_sales`** — materialized view at transaction-line grain
(duplicates retained, NOT a dimension). Columns:
- Degenerate dim: `transaction_id`, `order_status`, `payment_type`.
- FKs: `customer_id`, `product_id`, `product_category`, `location_id`.
- Dates: `order_date`, `ship_date`, `order_year`, `order_month`.
- Measures: `quantity`, `unit_price`, `discount_pct_clean`, `gross_amount`, `net_amount`,
  `shipping_delay_days`.
- `location_id = md5(city||'|'||state)` to join `dim_location`.

### 4.4 Aggregate tables (R4.4 — 2 required), provided by Gold layer as materialized views
1. **`agg_sales_by_category_month`** — monthly sales performance by category:
   `product_category, order_year, order_month` →
   `order_count`, `total_units`, `total_gross`, `total_net`, `avg_discount_pct`,
   `distinct_customers`.
2. **`agg_sales_by_state_status`** — geography × fulfilment outcome:
   `state, order_status` →
   `order_count`, `total_net`, `avg_order_value`, `total_units`, `distinct_customers`.

## 5. Pipeline file layout
```
pipeline/
  bronze/bronze_sales_transactions.sql
  silver/silver_sales_transactions.sql
  gold/dim_customer.sql
  gold/dim_product.sql
  gold/dim_location.sql
  gold/dim_category.sql
  gold/fact_sales.sql
  gold/agg_sales_by_category_month.sql
  gold/agg_sales_by_state_status.sql
```
Uploaded to `/Workspace/Users/XPTO--@Email.com/pipeline_sales_medallion_op48_v1/`.
Pipeline default catalog `cowork_op48`, default schema `bronze`; cross-schema tables use
fully-qualified names.

## 6. Validation strategy
- `get_table_stats_and_schema` after each layer (row counts, schema, nulls).
- Bronze: confirm 100,000 rows, all-STRING types, metadata columns present.
- Silver: confirm ≈11,227 rows, correct types, derived columns populated.
- Gold: dim row counts match distinct keys; SCD2 dims expose `__START_AT`/`__END_AT`;
  fact row count = silver count; aggregates non-empty and sane.

## 7. Iteration / cleanup
If a layer needs a structural change incompatible with streaming, drop the affected tables
(or the whole schema) and re-run with `full_refresh=True`. Keep pipeline name stable
(`pipeline_sales_medallion_op48_v1`); add transformation files incrementally bronze→silver→gold.

## 8. As-built — single pipeline, automation & results

### 8.1 One pipeline, one update
All 9 `.sql` files are registered as libraries of the single pipeline
`pipeline_sales_medallion_op48_v1` (serverless, catalog `cowork_op48`, default schema `bronze`).
SDP resolves the bronze→silver→gold→aggregate dependency DAG and executes it in **one
full-refresh update** (no per-layer pipelines). Cross-schema writes use fully-qualified names.

### 8.2 Deployment automation — `scripts/deploy_and_run.py`
Pure-Python (Databricks SDK), run with **uv**. Idempotent steps:
1. optional CSV ingestion to the volume (`--local-csv`),
2. create `bronze`/`silver`/`gold` schemas,
3. upload `pipeline/*.sql` to the workspace as raw FILES (`ImportFormat.RAW`),
4. create or update the serverless pipeline (libraries = the 9 files),
5. start a full-refresh run and poll to completion,
6. validate gold row counts (statement execution with polling for warehouse cold-start).

```bash
uv run scripts/deploy_and_run.py                       # deploy + run (data in volume)
uv run scripts/deploy_and_run.py --local-csv data.csv  # ingest then deploy + run
uv run scripts/deploy_and_run.py --no-run              # deploy only
```
Dependency added via `uv add databricks-sdk` (tracked in `pyproject.toml` / `uv.lock`).

### 8.3 As-built row counts (verified)
| Table | Layer | Rows | Notes |
|-------|-------|------|-------|
| `bronze.bronze_sales_transactions` | bronze | 100,000 | all STRING + metadata |
| `silver.silver_sales_transactions` | silver | 11,227 | valid sales; dups retained (9,426 distinct txn) |
| `gold.dim_customer` | gold (SCD1) | 10,896 | rows = distinct keys |
| `gold.dim_product` | gold (SCD1) | 10,053 | rows = distinct keys |
| `gold.dim_location` | gold (SCD2) | 10 | `__START_AT`/`__END_AT` present |
| `gold.dim_category` | gold (SCD2) | 5 | fixed via `COLUMNS product_category` |
| `gold.fact_sales` | gold (fact) | 11,227 | 10,059 distinct txn (dups retained) |
| `gold.agg_sales_by_category_month` | gold (agg #1) | 185 | category × year × month |
| `gold.agg_sales_by_state_status` | gold (agg #2) | 18 | fact ⨝ SCD2 location dim |

### 8.4 Design deltas vs. original plan
- **`dim_category` SCD2:** original flow omitted `COLUMNS`, so the sequencing column `seq_ts`
  was tracked as an attribute and every distinct order_date created a new version (4,818 rows).
  Fixed by scoping to `COLUMNS product_category` → 5 clean rows.
- **Sequencing key:** dims sequence by `order_date` (latest order's attributes win) rather than
  `_ingested_at`; single batch still yields one SCD2 version per key as intended.
- **Deploy tooling:** standardized on a Python script run via `uv` (no PowerShell), per user preference.
