# Requirements — Sales Medallion Pipeline (`pipeline_sales_medallion_fa5_v1`)

## 1. Overview
Build a **serverless Lakeflow Spark Declarative Pipeline (SDP)** in **SQL** that ingests raw sales
transaction CSV files and produces a medallion architecture (Bronze → Silver → Gold) in Unity Catalog.

## 2. Source Data
| Item | Value |
|---|---|
| Location | `/Volumes/cowork_fable5/dw_raw/raw_data` |
| Format | CSV with header, 1 file, ~100,000 rows (~11.7 MB) |
| Columns | transaction_id, order_date, ship_date, customer_id, customer_age, gender, product_id, product_category, quantity, unit_price, discount_pct, city, state, payment_type, order_status, ingestion_date |

### Known data-quality issues (profiled)
- `transaction_id`: duplicated (~40,455 unique of 100,000 rows)
- `customer_age`: nulls (~25%), negatives, implausible values (e.g. -10, 133)
- `gender`: nulls (~20%), mixed encodings (`M`, `F`, `Male`, `Female`)
- `quantity`: zeros and negatives
- `unit_price`: ~34% null, negative values
- `discount_pct`: ~33% null, values > 100
- `ship_date`: sometimes earlier than `order_date`
- `payment_type`: ~20% null

## 3. Functional Requirements

### FR-1 Pipeline
- FR-1.1 Pipeline name: **`pipeline_sales_medallion_fa5_v1`**
- FR-1.2 Serverless compute; SQL syntax for all transformations
- FR-1.3 Single pipeline writing to three separate UC schemas in catalog **`cowork_fable5`**
- FR-1.4 Schemas are versioned for development (`*_fa5_v1`); on iteration, the previous schema set is cleaned up before retry

### FR-2 Bronze Layer (`cowork_fable5.bronze_fa5_v1`)
- FR-2.1 Streaming table ingesting CSV via Auto Loader (`STREAM read_files`)
- FR-2.2 **All columns loaded as STRING** (no type inference)
- FR-2.3 Schema evolution enabled (new columns added automatically; rescued data captured)
- FR-2.4 Ingestion metadata added: `_ingested_at`, `_source_file`, `_file_modification_time`, `_file_size`
- FR-2.5 Bronze is built, deployed, and validated **first**, with results shown before continuing

### FR-3 Silver Layer (`cowork_fable5.silver_fa5_v1`)
- FR-3.1 Streaming table reading from Bronze
- FR-3.2 **All columns cast** to proper types (INT, DATE, DECIMAL, …)
- FR-3.3 Cleaned: gender normalized, invalid ages/discounts/ship dates nulled, critical-rule violations filtered via expectations
- FR-3.4 Enriched: monetary measures (gross/discount/net amount), shipping delay, date parts
- FR-3.5 **No deduplication** at this layer

### FR-4 Gold Layer (`cowork_fable5.gold_fa5_v1`)
- FR-4.1 Star schema with dimension (`dim_*`) and fact (`fact_*`) tables
- FR-4.2 **Small dimensions → SCD Type 2** (history tracked): `dim_location`, `dim_payment_type`
- FR-4.3 **Large tables → SCD Type 1** (current state only): `dim_customer`, `dim_product`, `fact_sales`
- FR-4.4 Deduplication happens here via `AUTO CDC` keyed flows
- FR-4.5 **Two aggregate tables** in Gold demonstrating common retail metrics:
  - `agg_monthly_sales` — revenue/orders/quantity/discount by month × category × state
  - `agg_customer_demographics` — customer count, revenue, AOV, return & cancellation rates by state × gender × age band

## 4. Non-Functional Requirements
- NFR-1 Unity Catalog managed tables only; Liquid Clustering (`CLUSTER BY`), no PARTITION BY/ZORDER
- NFR-2 Data-quality expectations declared in pipeline (visible in pipeline UI)
- NFR-3 Pipeline files are plain `.sql` files (no notebooks)
- NFR-4 Each layer validated (row counts + schema + sample data) after deployment

## 5. Acceptance Criteria
- AC-1 Pipeline `pipeline_sales_medallion_fa5_v1` runs to COMPLETED state
- AC-2 Bronze row count = source row count (100,000); all business columns STRING
- AC-3 Silver row count ≤ Bronze (expectations drop invalid rows); all columns typed
- AC-4 `fact_sales` has 1 row per `transaction_id` (deduplicated)
- AC-5 SCD2 dims expose `__START_AT` / `__END_AT`; current rows queryable with `__END_AT IS NULL`
- AC-6 Both aggregate tables return plausible business metrics
