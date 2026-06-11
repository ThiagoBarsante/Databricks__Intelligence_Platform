# Requirements — Sales Transactions Medallion Pipeline

## 1. Goal
Build a Lakeflow Spark Declarative Pipeline (SDP) named `pipeline_sales_medallion_sn46_v1` that
implements a bronze → silver → gold medallion architecture for the raw sales transactions CSV
located at `/Volumes/cowork_sn46/dw_raw/raw_data/sales_transactions.csv`.

## 2. Source Data
Single CSV file with header, columns:

| Column | Example | Notes |
|---|---|---|
| transaction_id | 3316 | numeric, unique-ish business key |
| order_date | 2025-10-28 | date |
| ship_date | 2025-10-26 | date, can be before order_date (data quality issue) |
| customer_id | CUST34172 | string business key |
| customer_age | 7 / -2 / 133 | numeric, contains invalid values (negative, >120) and nulls |
| gender | M / Male / Female / (blank) | inconsistent categorical values, contains nulls |
| product_id | PROD20926 | string business key |
| product_category | Furniture | string |
| quantity | 0 / -4 | numeric, contains invalid negative/zero values |
| unit_price | 9.71 / -21.00 / (blank) | decimal, contains negative and null values (data quality issue) |
| discount_pct | 9.71 / (blank) | decimal percentage, contains nulls |
| city | Chicago | string |
| state | IL | string |
| payment_type | COD / Card / Crypto / (blank) | string, contains nulls |
| order_status | Cancelled / Delivered / Returned | string |
| ingestion_date | 2026-05-31 | date supplied by source extract |

Data quality observed: nulls across many columns, negative ages/quantities/prices, inconsistent
gender labels (`M`, `Male`, `Female`, blank). These must be handled in the silver layer.

## 3. Functional Requirements

### 3.1 Catalog / Schema layout
- All tables live in Unity Catalog catalog **`cowork_sn46`**.
- Use **one schema per medallion layer**: `cowork_sn46.bronze`, `cowork_sn46.silver`, `cowork_sn46.gold`.
- Single pipeline (`pipeline_sales_medallion_sn46_v1`) writes to all three schemas using fully
  qualified table names.

### 3.2 Bronze layer
- Ingest the raw CSV with Auto Loader (`STREAM read_files`).
- **All columns loaded as STRING** (no type casting at this layer).
- Must use **schema evolution** (so new source columns are picked up automatically).
- Must capture **ingestion metadata**: an ingest timestamp and the source file path.

### 3.3 Silver layer
- Streaming table(s) that **clean, validate, cast, and enrich** the bronze data.
- Cast every column to its proper type (dates, integers, decimals, strings).
- Normalize inconsistent categorical data (e.g. gender values).
- Flag/quarantine data-quality problems (negative quantity/age/price, nulls in key columns)
  rather than silently dropping — **no deduplication at this layer** (dedup happens downstream).
- Add enrichment columns useful to downstream consumers (e.g. computed net amount, data-quality
  flags, normalized gender).

### 3.4 Gold layer (star schema — facts & dimensions)
- Model the data as a **star schema**: dimension tables + a central fact table.
- **Small dimension → SCD Type 2** (`dim_product`, fewer distinct members): keep full history of
  attribute changes using `AUTO CDC ... STORED AS SCD TYPE 2`.
- **Large dimension/fact → SCD Type 1** (`dim_customer` and `fact_sales_transactions`, many more
  distinct members / rows): keep current state only and use `AUTO CDC ... STORED AS SCD TYPE 1`,
  which also performs the deduplication that was deliberately skipped in silver (merge on the
  natural key).
- Provide **two aggregate tables** (materialized views) that demonstrate common metrics for retail
  sales analytics, built on top of the fact + dimension tables:
  1. Sales performance by product category and month (revenue, units, discounts, order mix).
  2. Sales performance by geography (state/city) with customer reach and order-status mix.

## 4. Non-Functional Requirements
- **Serverless** SDP pipeline (no classic clusters).
- SQL-first implementation (per skill guidance — simple, declarative, tabular transforms).
- Re-runnable / idempotent: re-running the pipeline should not duplicate data (handled by
  Auto Loader checkpointing in bronze/silver and `AUTO CDC` merge semantics in gold).
- Build, validate, deploy and test the **bronze layer first**, show results, then proceed to
  silver and gold.

## 5. Out of Scope
- Orchestration/scheduling (job triggers) — pipeline is run on demand for this exercise.
- Historical backfills beyond the single CSV file provided.
