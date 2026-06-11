CREATE OR REFRESH STREAMING TABLE gpt55_codex.sales_gold.dim_order_status
COMMENT 'SCD Type 2 order status dimension.'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true',
  'quality' = 'gold'
);

CREATE TEMPORARY VIEW dim_order_status_source AS
SELECT
  order_status,
  _ingested_at,
  _source_file,
  _sequence_timestamp
FROM STREAM gpt55_codex.sales_silver.sales_transactions_clean;

CREATE FLOW dim_order_status_scd2_flow AS
AUTO CDC INTO gpt55_codex.sales_gold.dim_order_status
FROM stream(dim_order_status_source)
KEYS (order_status)
SEQUENCE BY _sequence_timestamp
COLUMNS * EXCEPT (_sequence_timestamp)
STORED AS SCD TYPE 2;
