-- Gold dimension: dim_customer
-- LARGE dimension (≈79k keys) -> SCD TYPE 1 (current state only, R4.3).
-- AUTO CDC also collapses duplicate customer rows from silver (keeps latest by order_date).
CREATE TEMPORARY VIEW v_customer_cdc AS
SELECT
  customer_id,
  customer_age_clean,
  gender_clean,
  age_band,
  order_date AS seq_ts
FROM STREAM cowork_op48.silver.silver_sales_transactions
WHERE customer_id IS NOT NULL;

CREATE OR REFRESH STREAMING TABLE cowork_op48.gold.dim_customer
COMMENT 'Customer dimension - SCD Type 1 (large dimension, current state only)';

CREATE FLOW dim_customer_scd1 AS
AUTO CDC INTO cowork_op48.gold.dim_customer
FROM stream(v_customer_cdc)
KEYS (customer_id)
SEQUENCE BY seq_ts
COLUMNS customer_id, customer_age_clean, gender_clean, age_band
STORED AS SCD TYPE 1;
