# Sales Medallion Pipeline Requirements

## Source

- Read CSV files from `/Volumes/gpt55_codex/dw_raw/raw_data`.
- Source fields:
  `transaction_id`, `order_date`, `ship_date`, `customer_id`, `customer_age`,
  `gender`, `product_id`, `product_category`, `quantity`, `unit_price`,
  `discount_pct`, `city`, `state`, `payment_type`, `order_status`,
  `ingestion_date`.
- Use a Serverless Spark Declarative Pipeline named `pipeline_sales_medallion_gpt55_v1`.
- Prefer SQL syntax.

## Unity Catalog Layout

- Catalog: `gpt55_codex`.
- Bronze schema: `sales_bronze`.
- Silver schema: `sales_silver`.
- Gold schema: `sales_gold`.

## Bronze

- Load raw CSV data with schema evolution.
- Store all source fields as `STRING`.
- Add ingestion timestamp and source file metadata.
- Validate and test bronze before continuing with silver and gold.

## Silver

- Create streaming cleaned/enriched tables.
- Cast fields to appropriate business types.
- Filter invalid records.
- Do not deduplicate at the silver layer.

## Gold

- Publish a dimensional star schema.
- Treat large fact-like data as SCD Type 1.
- Treat small dimensions as SCD Type 2.
- Provide at least two aggregate gold tables with common sales metrics.
