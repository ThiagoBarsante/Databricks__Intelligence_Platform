CREATE OR REFRESH STREAMING TABLE gpt55_codex.sales_gold.dim_location
COMMENT 'SCD Type 2 location dimension.'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true',
  'quality' = 'gold'
);

CREATE TEMPORARY VIEW dim_location_source AS
SELECT
  location_key,
  city,
  state,
  _ingested_at,
  _source_file,
  _sequence_timestamp
FROM STREAM gpt55_codex.sales_silver.sales_transactions_clean;

CREATE FLOW dim_location_scd2_flow AS
AUTO CDC INTO gpt55_codex.sales_gold.dim_location
FROM stream(dim_location_source)
KEYS (location_key)
SEQUENCE BY _sequence_timestamp
COLUMNS * EXCEPT (_sequence_timestamp)
STORED AS SCD TYPE 2;
