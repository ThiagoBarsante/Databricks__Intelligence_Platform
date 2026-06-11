-- Gold dimension: dim_location (city + state)
-- SMALL dimension (≤10 keys) -> SCD TYPE 2 (history tracked with __START_AT/__END_AT, R4.2).
CREATE TEMPORARY VIEW v_location_cdc AS
SELECT
  md5(concat_ws('|', city, state)) AS location_id,
  city,
  state,
  order_date AS seq_ts
FROM STREAM cowork_op48.silver.silver_sales_transactions
WHERE city IS NOT NULL AND state IS NOT NULL;

CREATE OR REFRESH STREAMING TABLE cowork_op48.gold.dim_location
COMMENT 'Location dimension - SCD Type 2 (small dimension, history tracked)';

CREATE FLOW dim_location_scd2 AS
AUTO CDC INTO cowork_op48.gold.dim_location
FROM stream(v_location_cdc)
KEYS (location_id)
SEQUENCE BY seq_ts
COLUMNS location_id, city, state
STORED AS SCD TYPE 2;
