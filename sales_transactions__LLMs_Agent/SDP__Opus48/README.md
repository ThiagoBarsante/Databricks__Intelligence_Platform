# Sales Medallion Pipeline — `pipeline_sales_medallion_op48_v1`

A **Serverless Lakeflow Spark Declarative Pipeline (SDP)**, written in **SQL**, that ingests raw
sales-transaction CSV files and refines them through a **Bronze → Silver → Gold medallion
architecture** with **dimensional (star-schema) modelling** in the Gold layer.

Built using **Spec-Driven Development (SDD)** — see [specs/](specs/) for requirements, design, and tasks.

---

## 1. At a glance

| Item | Value |
|------|-------|
| **Source** | `/Volumes/cowork_op48/dw_raw/raw_data/sales_transactions.csv` (CSV w/ header, ~11.7 MB) |
| **Catalog** | `cowork_op48` |
| **Schemas** | `bronze`, `silver`, `gold` (one schema per layer) |
| **Pipeline** | `pipeline_sales_medallion_op48_v1` (serverless, SQL) |
| **Compute** | Serverless |
| **Language** | SQL (`CREATE OR REFRESH` Lakeflow syntax) |
| **Last full run** | COMPLETED in ~74s |

---

## 2. Architecture

```
/Volumes/cowork_op48/dw_raw/raw_data  (CSV)
        │  STREAM read_files  (Auto Loader · all STRING · schema evolution)
        ▼
cowork_op48.bronze.bronze_sales_transactions   (streaming table · append · 100,000 rows)
        │  cast + filter + enrich  (NO dedup)
        ▼
cowork_op48.silver.silver_sales_transactions   (streaming table · 11,227 rows)
        │
        ├─► dimensions (AUTO CDC)                   ├─► fact + aggregates
        ▼                                           ▼
  dim_customer  (SCD1, large, 10,896)         fact_sales (MV, 11,227 — star center)
  dim_product   (SCD1, large, 10,053)         agg_sales_by_category_month (MV, 185)
  dim_location  (SCD2, small, 10)             agg_sales_by_state_status   (MV, 18)
  dim_category  (SCD2, small, 5)
```

---

## 3. Layer details

### Bronze — `cowork_op48.bronze.bronze_sales_transactions`
- Loads **all columns as STRING** (no casting) via `schemaHints`.
- **Schema evolution** enabled (`schemaEvolutionMode => 'addNewColumns'`).
- Adds ingest + source-file metadata: `_ingested_at`, `_source_file`, `_source_file_modified_at`, `_source_file_size`.
- Append-only streaming table (Auto Loader `STREAM read_files`).
- **Result:** 100,000 rows, all 16 source columns `STRING`.

### Silver — `cowork_op48.silver.silver_sales_transactions`
- **Casts** every column to its proper type (`BIGINT`, `DATE`, `INT`, `DECIMAL`).
- **Filters** (via expectations `ON VIOLATION DROP ROW`) to rows that can be a real sale:
  `transaction_id` not null, `order_date` not null, `quantity > 0`, `unit_price > 0`.
- **Enriches**: `gender_clean`, `customer_age_clean`, `age_band`, `discount_pct_clean`,
  `gross_amount`, `net_amount`, `shipping_delay_days`, `order_year`, `order_month`.
- **No deduplication** — duplicate `transaction_id`s are retained.
- **Result:** 11,227 valid rows (≈11% of raw — the data is intentionally dirty).

### Gold — `cowork_op48.gold.*` (star schema)
Dimension SCD type chosen by size (per profiling):

| Dimension | Keys | Size | SCD | Rows |
|-----------|------|------|-----|------|
| `dim_customer` | `customer_id` | large | **Type 1** | 10,896 |
| `dim_product`  | `product_id`  | large | **Type 1** | 10,053 |
| `dim_location` | `location_id` (`md5(city\|state)`) | small | **Type 2** | 10 |
| `dim_category` | `product_category` | small | **Type 2** | 5 |

- **`fact_sales`** (materialized view) — transaction-line grain (duplicates retained), FKs to all
  dimensions + degenerate dims (`transaction_id`, `order_status`, `payment_type`) + measures.
- **`agg_sales_by_category_month`** (Aggregate #1) — monthly performance by product category.
- **`agg_sales_by_state_status`** (Aggregate #2) — geography × fulfilment outcome, built via a
  star-join `fact_sales ⨝ dim_location` (current SCD2 rows, `__END_AT IS NULL`).

---

## 4. Key insights from this build

### Data profiling (100,000 source rows)
The raw data is **deliberately dirty** — profiling drove every downstream design decision:

| Issue | Magnitude | Handling |
|-------|-----------|----------|
| `quantity` ≤ 0 (zero/negative) | 66,647 rows | filtered out in Silver |
| `unit_price` null or ≤ 0 | ~33,500 rows | filtered out in Silver |
| `customer_age` invalid (negative / >120 / null) | 61,592 rows | nulled + bucketed to `age_band='Unknown'` |
| `gender` in 4 variants (`M`/`F`/`Male`/`Female`) | — | normalized to Male/Female/Unknown |
| `discount_pct` null or >100% | ~33,000 + outliers | floored to 0 when out of `[0,100]` |
| `transaction_id` **not unique** | 43K distinct of 100K | **kept** (no dedup rule at Silver) |
| `ship_date` before `order_date` | some rows | kept; surfaced as negative `shipping_delay_days` |

➡️ **Only 11,227 rows (≈11%) are valid sales.** This is the single most important insight — it
explains the Silver row drop and validates the filtering logic.

### Dimension sizing → SCD strategy
Profiling distinct counts decided SCD types: customer (≈79K raw / 10.9K valid) and product
(≈43K / 10K) are **large → SCD Type 1**; category (5) and location (10 cities / 6 states) are
**small → SCD Type 2**.

### Sample business results
- **Top category-month:** Electronics, Jul-2025 — **$395,164 net**, 468 units, 19.6% avg discount.
- **Top geography × status:** CA Returned — **$2.88M net** across 1,162 customers.
- Electronics dominates the top revenue months; CA and TX dominate revenue overall.

### Engineering notes / lessons
- **SCD2 column scoping matters:** initially `dim_category` exploded to 4,818 rows because the
  `seq_ts` sequencing column leaked into the tracked attributes — every distinct date created a new
  version. Fixed by restricting the flow to `COLUMNS product_category` → 5 clean rows. SCD1 dims
  were unaffected because they used an explicit `COLUMNS` list.
- **`get_table_stats_and_schema` `unique_count` is approximate** (HLL). For SCD1 tables the exact
  distinct-key count equals `total_rows` by construction — verified with an exact `COUNT(DISTINCT)`.
- **SCD2 history accumulates over batches:** a single full-refresh run shows one version per key.
  History (`__START_AT`/`__END_AT`) builds up automatically as future batches bring changed attributes.
- Use `CREATE OR REFRESH` (never `CREATE OR REPLACE`); facts via materialized views, dimensions via
  `AUTO CDC INTO ... STORED AS SCD TYPE 1|2`.

---

## 5. Repository layout

```
.
├── README.md                         ← this file
├── Instrunctions.txt                 ← original task brief
├── pyproject.toml / uv.lock          ← uv project (deps: pandas, databricks-sdk)
├── specs/                            ← Spec-Driven Development artifacts
│   ├── requirements.md
│   ├── design.md
│   └── tasks.md
├── data/                             ← local CSV samples & generated ingest files
│   ├── silver_sales_transactions_sample.csv
│   └── ingestion/
│       └── sales_transactions_ingest_<yyyy_mm_dd_hh24miss>.csv
├── scripts/
│   ├── deploy_and_run.py             ← one-shot deploy + run (Python, run via `uv run`)
│   ├── download_silver_sample.py     ← download a 1k-row silver sample to data/*.csv
│   ├── generate_sample_data.py       ← generate 100 synthetic ingest records from the sample
│   ├── upload_data_and_run_pipeline.py ← generate → upload → deploy → run, end to end
│   └── audit.sql                     ← full-project audit: every object + row count
└── pipeline/                         ← SDP transformation files (plain .sql)
    ├── bronze/
    │   └── bronze_sales_transactions.sql
    ├── silver/
    │   └── silver_sales_transactions.sql
    └── gold/
        ├── dim_customer.sql              (SCD1)
        ├── dim_product.sql               (SCD1)
        ├── dim_location.sql              (SCD2)
        ├── dim_category.sql              (SCD2)
        ├── fact_sales.sql                (fact MV)
        ├── agg_sales_by_category_month.sql   (aggregate #1)
        └── agg_sales_by_state_status.sql     (aggregate #2)
```

---

## 6. How to deploy / run

The whole solution is **one pipeline**: all 9 `.sql` files are registered as libraries of the
single pipeline `pipeline_sales_medallion_op48_v1`, and SDP resolves the bronze→silver→gold→aggregate
DAG and runs it in **one update**. The files are plain `.sql` (no notebooks) on serverless compute.

### One-shot script (recommended) — `scripts/deploy_and_run.py`

Run with **uv** (the project manages `databricks-sdk` as a dependency):

```bash
# Deploy + run using data already in the volume:
uv run scripts/deploy_and_run.py

# Ingest a local CSV into the volume first, then deploy + run:
uv run scripts/deploy_and_run.py --local-csv path/to/sales_transactions.csv

# Deploy only (no run):
uv run scripts/deploy_and_run.py --no-run
```

The script (idempotent, uses the `DEFAULT` CLI profile by default) performs, in order:
1. optional CSV **ingestion** into `/Volumes/cowork_op48/dw_raw/raw_data`,
2. **create schemas** `bronze` / `silver` / `gold`,
3. **upload** all `pipeline/*.sql` files to the workspace as raw FILES,
4. **create or update** the single serverless pipeline,
5. **start a full-refresh run** and poll to completion,
6. **validate** gold row counts.

Verified end-to-end output (gold row counts):

```
dim_customer                     10896
dim_product                      10053
dim_location                     10
dim_category                     5
fact_sales                       11227
agg_sales_by_category_month      185
agg_sales_by_state_status        18
```

> Prereqs: a configured Databricks CLI/SDK profile (`~/.databrickscfg`) and the source CSV present
> in the volume (or pass `--local-csv`). Use `--profile <name>` to target a non-default profile.

### Validation queries
```sql
-- Row counts / key uniqueness across gold
SELECT 'dim_category'  t, COUNT(*) rows, COUNT(DISTINCT product_category) keys FROM cowork_op48.gold.dim_category
UNION ALL SELECT 'dim_customer', COUNT(*), COUNT(DISTINCT customer_id)   FROM cowork_op48.gold.dim_customer
UNION ALL SELECT 'dim_product',  COUNT(*), COUNT(DISTINCT product_id)    FROM cowork_op48.gold.dim_product
UNION ALL SELECT 'dim_location', COUNT(*), COUNT(DISTINCT location_id)   FROM cowork_op48.gold.dim_location
UNION ALL SELECT 'fact_sales',   COUNT(*), COUNT(DISTINCT transaction_id) FROM cowork_op48.gold.fact_sales;

-- SCD2 current rows
SELECT * FROM cowork_op48.gold.dim_location WHERE `__END_AT` IS NULL;
```

### Full-project audit — `scripts/audit.sql`

Lists **every** medallion object (bronze → silver → gold) with its layer, object type, and
`COUNT(*)`. Run it in the DBSQL editor or any SQL warehouse to confirm the whole project at a glance.

Verified output:

| layer | object_type | object_name | row_count |
|-------|-------------|-------------|-----------|
| bronze | streaming_table | `cowork_op48.bronze.bronze_sales_transactions` | 100,000 |
| silver | streaming_table | `cowork_op48.silver.silver_sales_transactions` | 11,227 |
| gold | aggregate | `cowork_op48.gold.agg_sales_by_category_month` | 185 |
| gold | aggregate | `cowork_op48.gold.agg_sales_by_state_status` | 18 |
| gold | dimension_scd2 | `cowork_op48.gold.dim_category` | 5 |
| gold | dimension_scd1 | `cowork_op48.gold.dim_customer` | 10,896 |
| gold | dimension_scd2 | `cowork_op48.gold.dim_location` | 10 |
| gold | dimension_scd1 | `cowork_op48.gold.dim_product` | 10,053 |
| gold | fact | `cowork_op48.gold.fact_sales` | 11,227 |

---

## 7. Generate new sample data, upload it & run the pipeline (Python)

Three small scripts (all run via **uv**) let you produce fresh "new arrival" data and push it
through the whole medallion pipeline without touching the Databricks UI:

| Script | What it does |
|--------|--------------|
| `download_silver_sample.py` | Downloads a 1,000-row sample of `cowork_op48.silver.silver_sales_transactions` to `data/silver_sales_transactions_sample.csv` (used as the reference/template for generation). |
| `generate_sample_data.py` | Reads that sample and writes **100 new synthetic records** in the **raw ingest schema** (matching bronze's `read_files` `schemaHints`) to `data/ingestion/sales_transactions_ingest_<yyyy_mm_dd_hh24miss>.csv`. `transaction_id` increments from the existing max and `customer_id` (plus other attributes) are freshly generated; every row is checked against the silver-layer expectations — `transaction_id`/`order_date` not null, `quantity > 0`, `unit_price > 0` — before being written, so nothing would be dropped on ingest. |
| `upload_data_and_run_pipeline.py` | **Recommended one-shot.** Chains the two steps above into `deploy_and_run.py --local-csv <generated file>`: generates a fresh ingest CSV, **uploads** it to `/Volumes/cowork_op48/dw_raw/raw_data`, then deploys/updates and **runs** the full bronze → silver → gold pipeline end to end (and validates gold row counts). |

```bash
# Generate + upload + deploy + run, all in one go:
uv run scripts/upload_data_and_run_pipeline.py

# Or run the steps individually:
uv run scripts/download_silver_sample.py     # refresh the reference sample (optional)
uv run scripts/generate_sample_data.py       # generate data/ingestion/sales_transactions_ingest_*.csv
uv run scripts/deploy_and_run.py --local-csv data/ingestion/sales_transactions_ingest_<ts>.csv
```

> Same prereqs as `deploy_and_run.py` above (configured CLI/SDK profile + a running/startable SQL
> warehouse for `download_silver_sample.py` and gold-count validation).

---

## 8. Possible next steps
- Package as a **Databricks Asset Bundle (DAB)** for multi-environment CI/CD.
- Build an **AI/BI dashboard** on the gold aggregates.
- Add **data-quality monitoring** / expectations reporting on the Silver constraints.
