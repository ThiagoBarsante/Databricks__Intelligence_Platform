CREATE OR REFRESH STREAMING TABLE gpt55_codex.sales_gold.dim_customer
COMMENT 'SCD Type 2 customer dimension.'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true',
  'quality' = 'gold'
);

CREATE TEMPORARY VIEW dim_customer_source AS
SELECT
  customer_id,
  customer_age,
  gender,
  _ingested_at,
  _source_file,
  _sequence_timestamp
FROM STREAM gpt55_codex.sales_silver.sales_transactions_clean;

CREATE FLOW dim_customer_scd2_flow AS
AUTO CDC INTO gpt55_codex.sales_gold.dim_customer
FROM stream(dim_customer_source)
KEYS (customer_id)
SEQUENCE BY _sequence_timestamp
COLUMNS * EXCEPT (_sequence_timestamp)
STORED AS SCD TYPE 2;
