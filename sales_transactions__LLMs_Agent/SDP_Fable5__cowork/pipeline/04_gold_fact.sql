-- ============================================================
-- GOLD LAYER — fact table (star schema center)
-- fact_sales: LARGE -> SCD TYPE 1 keyed by transaction_id.
-- This is where deduplication happens (silver keeps duplicates):
-- AUTO CDC keeps the latest version of each transaction.
-- FK columns (customer_id, product_id, city/state, payment_type)
-- reference the dimensions by natural key.
-- ============================================================

CREATE TEMPORARY VIEW fact_sales_src AS
SELECT
  transaction_id,
  order_date,
  ship_date,
  customer_id,
  product_id,
  product_category,
  city,
  state,
  payment_type,
  order_status,
  quantity,
  unit_price,
  discount_pct,
  gross_amount,
  discount_amount,
  net_amount,
  days_to_ship,
  order_year,
  order_month,
  uuid() AS _seq_uid
FROM STREAM cowork_fable5.silver_fa5_v1.silver_sales_transactions;

CREATE OR REFRESH STREAMING TABLE cowork_fable5.gold_fa5_v1.fact_sales
COMMENT 'Sales fact table - SCD Type 1, one row per transaction_id (deduplicated at gold)'
CLUSTER BY (order_date, product_category)
TBLPROPERTIES ('quality' = 'gold');

CREATE FLOW fact_sales_scd1 AS
AUTO CDC INTO cowork_fable5.gold_fa5_v1.fact_sales
FROM stream(fact_sales_src)
KEYS (transaction_id)
SEQUENCE BY struct(order_date, _seq_uid)
COLUMNS * EXCEPT (_seq_uid)
STORED AS SCD TYPE 1;
