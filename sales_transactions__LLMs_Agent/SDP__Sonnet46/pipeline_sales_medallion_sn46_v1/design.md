# Design — Sales Transactions Medallion Pipeline

## 1. Overview

```
/Volumes/cowork_sn46/dw_raw/raw_data/sales_transactions.csv
                │  (Auto Loader, STREAM read_files, schema evolution, all STRING)
                ▼
cowork_sn46.bronze.brz_sales_transactions          (streaming table)
                │  (cast types, normalize, enrich, flag DQ issues — no dedup)
                ▼
cowork_sn46.silver.slv_sales_transactions          (streaming table)
                │
     ┌──────────┴────────────────────────┬─────────────────────────────┐
     ▼                                    ▼                             ▼
gold.dim_product (SCD2)          gold.dim_customer (SCD1)     gold.fact_sales_transactions (SCD1 / dedup by transaction_id)
     │                                    │                             │
     └────────────────────────────────────┴─────────────────────────────┘
                                          │
                       ┌──────────────────┴───────────────────┐
                       ▼                                       ▼
        gold.agg_category_monthly_metrics          gold.agg_geo_sales_metrics
            (materialized view)                       (materialized view)
```

Single pipeline `pipeline_sales_medallion_sn46_v1`, SQL-based, serverless, writing to three UC
schemas (`bronze`, `silver`, `gold`) inside catalog `cowork_sn46` using fully-qualified table names.

## 2. Bronze Layer — `cowork_sn46.bronze`

### `brz_sales_transactions` (streaming table)
Reads the CSV folder with `STREAM read_files(...)`:
- `format => 'csv'`, `header => true`
- `schemaHints` forcing **every** business column to `STRING` (satisfies "load all information as
  STRING" regardless of Auto Loader's type inference)
- `schemaEvolutionMode => 'addNewColumns'` — new columns in future files are added automatically
  (schema evolution requirement)
- Adds `_ingested_at` (`current_timestamp()`) and `_source_file` (`_metadata.file_path`) metadata
  columns
- Clustered by `ingestion_date` (Liquid Clustering) for efficient downstream pruning

All columns land as STRING; no business-rule transformations happen here (append-only, raw mirror
of the source plus ingestion lineage).

## 3. Silver Layer — `cowork_sn46.silver`

### `slv_sales_transactions` (streaming table, reads `STREAM` from bronze)
Cleans, casts and enriches every record (1:1 with bronze — **no deduplication**):
- Type casts: `transaction_id BIGINT`, `order_date/ship_date/ingestion_date DATE`,
  `customer_age INT`, `quantity INT`, `unit_price/discount_pct DECIMAL(10,2)`
- Normalizes `gender` to `M` / `F` / `Unknown` (collapses `Male`→`M`, `Female`→`F`,
  null/blank→`Unknown`)
- Trims/upper-cases `state`, trims `city`
- Enrichment columns:
  - `net_amount = ROUND(quantity * unit_price * (1 - discount_pct/100), 2)` (NULL-safe)
  - `is_valid_amount` — FALSE when `unit_price` is NULL or negative or `quantity <= 0`
  - `is_valid_age` — FALSE when `customer_age` is NULL or not in `[0, 110]`
  - `has_data_quality_issue` — convenience flag combining the checks above plus missing
    `customer_id`/`product_id`
- Carries `_ingested_at`, `_source_file` lineage columns through from bronze

This keeps every bronze row (good and bad) so gold can decide how to treat duplicates/quality
issues — matches the "do not deduplicate at this layer" instruction.

## 4. Gold Layer — `cowork_sn46.gold` (Star Schema)

Star schema centered on `fact_sales_transactions`, surrounded by `dim_product` and `dim_customer`.
Date/geography are kept as degenerate attributes on the fact table (small dataset, single source);
a full `dim_date`/`dim_geography` would be over-engineering for this volume.

### Dimension sizing → SCD strategy
Distinct-member counts decide SCD type, per the instruction *"small tables → SCD2, large tables →
SCD1"*:
- `dim_product` — keyed by `product_id` (~thousands of distinct products) → **smaller** dimension
  → **SCD Type 2** (`AUTO CDC ... STORED AS SCD TYPE 2`), preserving full history of
  `product_category` reassignments via `__START_AT`/`__END_AT`.
- `dim_customer` — keyed by `customer_id` (tens of thousands of distinct customers, the larger
  dimension) → **SCD Type 1** (`AUTO CDC ... STORED AS SCD TYPE 1`), keeping only the latest known
  attributes (age, gender, city, state) per customer.
- `fact_sales_transactions` — the largest table (one row per transaction) → also **SCD Type 1**.
  `AUTO CDC ... KEYS (transaction_id) STORED AS SCD TYPE 1` both keeps only the latest version of
  each transaction *and* performs the deduplication on `transaction_id` that was intentionally
  deferred from the silver layer (sequenced by `_ingested_at` so the most recently ingested record
  for a given key wins).

### `dim_product` (SCD Type 2)
Columns: `product_id` (key), `product_category`, `__START_AT`, `__END_AT`.
Source: distinct `(product_id, product_category)` combinations from silver, sequenced by
`_ingested_at`.

### `dim_customer` (SCD Type 1)
Columns: `customer_id` (key), `customer_age`, `gender`, `city`, `state`, `__START_AT`, `__END_AT`
(maintained by `AUTO CDC`, but only the latest version is retained for SCD1).
Source: latest known attributes per `customer_id` from silver, sequenced by `_ingested_at`.

### `fact_sales_transactions` (SCD Type 1 / dedup)
Grain: one row per `transaction_id`. Columns: `transaction_id` (key), `customer_id` FK,
`product_id` FK, `order_date`, `ship_date`, `quantity`, `unit_price`, `discount_pct`,
`net_amount`, `payment_type`, `order_status`, `city`, `state`, `is_valid_amount`, `is_valid_age`.
Source: `slv_sales_transactions`, sequenced by `_ingested_at` so re-ingests of the same
`transaction_id` converge to a single current row.

## 5. Gold Aggregate Tables (materialized views, demonstrating common retail metrics)

### `agg_category_monthly_metrics`
Grain: `product_category` × `order_month`. Joins fact ⋈ `dim_product` (current row,
`__END_AT IS NULL`). Metrics: `order_count`, `total_quantity`, `total_revenue`,
`avg_discount_pct`, `cancelled_orders`, `returned_orders`, `delivered_orders`.
Answers: "which categories perform best each month, and how much do they get returned/cancelled?"

### `agg_geo_sales_metrics`
Grain: `state` × `city`. Joins fact ⋈ `dim_customer` (current row, `__END_AT IS NULL`).
Metrics: `order_count`, `distinct_customers`, `total_revenue`, `avg_order_value`,
`cancelled_orders`, `returned_orders`, `delivered_orders`.
Answers: "where are our customers and revenue concentrated, and how does fulfillment vary by
geography?"

Both are `MATERIALIZED VIEW`s (full-table aggregations, recomputed on refresh — per skill guidance
"use MVs for simple aggregations that recompute fully").

## 6. File Layout

```
pipeline_sales_medallion_sn46_v1/
├── requirements.md
├── design.md
├── tasks.md
└── transformations/
    ├── 01_bronze_sales_transactions.sql
    ├── 02_silver_sales_transactions.sql
    ├── 03_gold_dim_product.sql
    ├── 04_gold_dim_customer.sql
    ├── 05_gold_fact_sales_transactions.sql
    └── 06_gold_aggregates.sql
```

## 7. Deployment Strategy
1. Create UC schemas `cowork_sn46.{bronze,silver,gold}`.
2. Upload `transformations/` to the workspace, create the pipeline with **only the bronze file**,
   run it, validate output with `get_table_stats_and_schema` — show results.
3. Add the silver file to the pipeline, update + run, validate.
4. Add the gold files (dimensions, fact, aggregates), update + run, validate end to end.
