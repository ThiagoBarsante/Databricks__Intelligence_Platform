CREATE OR REFRESH STREAMING TABLE gpt55_codex.sales_gold.fact_sales
COMMENT 'SCD Type 1 fact table for sales transactions keyed by transaction_id.'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true',
  'quality' = 'gold'
);

CREATE TEMPORARY VIEW fact_sales_source AS
SELECT
  transaction_id,
  order_date,
  ship_date,
  customer_id,
  product_id,
  location_key,
  payment_type,
  order_status,
  quantity,
  unit_price,
  discount_pct,
  gross_amount,
  discount_amount,
  net_amount,
  ingestion_date,
  _ingested_at,
  _source_file,
  _sequence_timestamp
FROM STREAM gpt55_codex.sales_silver.sales_transactions_clean;

CREATE FLOW fact_sales_scd1_flow AS
AUTO CDC INTO gpt55_codex.sales_gold.fact_sales
FROM stream(fact_sales_source)
KEYS (transaction_id)
SEQUENCE BY _sequence_timestamp
COLUMNS * EXCEPT (_sequence_timestamp)
STORED AS SCD TYPE 1;
