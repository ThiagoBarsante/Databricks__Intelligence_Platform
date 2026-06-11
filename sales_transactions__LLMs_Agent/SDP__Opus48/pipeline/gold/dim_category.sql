-- Gold dimension: dim_category
-- SMALL dimension (5 keys) -> SCD TYPE 2 (history tracked with __START_AT/__END_AT, R4.2).
CREATE TEMPORARY VIEW v_category_cdc AS
SELECT
  product_category,
  order_date AS seq_ts
FROM STREAM cowork_op48.silver.silver_sales_transactions
WHERE product_category IS NOT NULL;

CREATE OR REFRESH STREAMING TABLE cowork_op48.gold.dim_category
COMMENT 'Product-category dimension - SCD Type 2 (small dimension, history tracked)';

CREATE FLOW dim_category_scd2 AS
AUTO CDC INTO cowork_op48.gold.dim_category
FROM stream(v_category_cdc)
KEYS (product_category)
SEQUENCE BY seq_ts
COLUMNS product_category
STORED AS SCD TYPE 2;
