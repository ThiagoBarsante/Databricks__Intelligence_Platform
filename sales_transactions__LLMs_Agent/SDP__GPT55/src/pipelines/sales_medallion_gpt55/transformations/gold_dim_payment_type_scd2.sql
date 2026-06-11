CREATE OR REFRESH STREAMING TABLE gpt55_codex.sales_gold.dim_payment_type
COMMENT 'SCD Type 2 payment type dimension.'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true',
  'quality' = 'gold'
);

CREATE TEMPORARY VIEW dim_payment_type_source AS
SELECT
  payment_type,
  _ingested_at,
  _source_file,
  _sequence_timestamp
FROM STREAM gpt55_codex.sales_silver.sales_transactions_clean;

CREATE FLOW dim_payment_type_scd2_flow AS
AUTO CDC INTO gpt55_codex.sales_gold.dim_payment_type
FROM stream(dim_payment_type_source)
KEYS (payment_type)
SEQUENCE BY _sequence_timestamp
COLUMNS * EXCEPT (_sequence_timestamp)
STORED AS SCD TYPE 2;
