CREATE OR REFRESH STREAMING TABLE gpt55_codex.sales_gold.dim_product
COMMENT 'SCD Type 2 product dimension.'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true',
  'quality' = 'gold'
);

CREATE TEMPORARY VIEW dim_product_source AS
SELECT
  product_id,
  product_category,
  _ingested_at,
  _source_file,
  _sequence_timestamp
FROM STREAM gpt55_codex.sales_silver.sales_transactions_clean;

CREATE FLOW dim_product_scd2_flow AS
AUTO CDC INTO gpt55_codex.sales_gold.dim_product
FROM stream(dim_product_source)
KEYS (product_id)
SEQUENCE BY _sequence_timestamp
COLUMNS * EXCEPT (_sequence_timestamp)
STORED AS SCD TYPE 2;
