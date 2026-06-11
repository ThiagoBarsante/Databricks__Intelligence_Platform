# Sales Medallion Pipeline Design

## Architecture

The solution uses one Serverless Spark Declarative Pipeline with SQL
transformation files. Tables are written to separate Unity Catalog schemas by
fully qualified table names.

## Bronze Layer

`gpt55_codex.sales_bronze.sales_transactions_raw`

- Streaming table over `read_files`.
- Reads from `/Volumes/gpt55_codex/dw_raw/raw_data`.
- Keeps source columns as `STRING`.
- Adds `_ingested_at`, `_source_file`, `_source_file_modification_time`, and
  `_source_file_size`.

## Silver Layer

`gpt55_codex.sales_silver.sales_transactions_clean`

- Streaming table from bronze.
- Trims string identifiers and descriptive fields.
- Casts dates, integers, and decimal measures.
- Standardizes gender and missing categorical values.
- Filters records with invalid transaction, customer, product, date, quantity,
  price, discount, and age values.
- Calculates `gross_amount`, `discount_amount`, and `net_amount`.
- Keeps duplicate transactions because deduplication is deferred to gold.

## Gold Layer

Star-schema objects in `gpt55_codex.sales_gold`:

- `fact_sales`: SCD Type 1 streaming table keyed by `transaction_id`.
- `dim_customer`: SCD Type 2 streaming dimension keyed by `customer_id`.
- `dim_product`: SCD Type 2 streaming dimension keyed by `product_id`.
- `dim_location`: SCD Type 2 streaming dimension keyed by `location_key`.
- `dim_payment_type`: SCD Type 2 streaming dimension keyed by `payment_type`.
- `dim_order_status`: SCD Type 2 streaming dimension keyed by `order_status`.
- `mv_daily_sales_metrics`: daily sales aggregate.
- `mv_category_state_metrics`: category/state aggregate.

The transaction event sequence is `_sequence_timestamp`, derived from
`order_date` plus a deterministic microsecond offset from row contents so CDC
flows can order multiple events for the same business key.
