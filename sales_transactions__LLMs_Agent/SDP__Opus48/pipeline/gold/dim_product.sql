-- Gold dimension: dim_product
-- LARGE dimension (≈43k keys) -> SCD TYPE 1 (current state only, R4.3).
CREATE TEMPORARY VIEW v_product_cdc AS
SELECT
  product_id,
  product_category,
  order_date AS seq_ts
FROM STREAM cowork_op48.silver.silver_sales_transactions
WHERE product_id IS NOT NULL;

CREATE OR REFRESH STREAMING TABLE cowork_op48.gold.dim_product
COMMENT 'Product dimension - SCD Type 1 (large dimension, current state only)';

CREATE FLOW dim_product_scd1 AS
AUTO CDC INTO cowork_op48.gold.dim_product
FROM stream(v_product_cdc)
KEYS (product_id)
SEQUENCE BY seq_ts
COLUMNS product_id, product_category
STORED AS SCD TYPE 1;
