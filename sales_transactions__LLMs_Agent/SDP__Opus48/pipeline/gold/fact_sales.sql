-- Gold fact: fact_sales (star-schema center)
-- Transaction-line grain (duplicates retained - facts are not deduped).
-- FKs to dims: customer_id, product_id, product_category, location_id.
CREATE OR REFRESH MATERIALIZED VIEW cowork_op48.gold.fact_sales
COMMENT 'Sales fact at transaction-line grain - star-schema center'
CLUSTER BY (order_date, product_category)
AS
SELECT
  -- degenerate dimensions
  transaction_id,
  order_status,
  payment_type,
  -- foreign keys to dimensions
  customer_id,
  product_id,
  product_category,
  md5(concat_ws('|', city, state)) AS location_id,
  -- date attributes
  order_date,
  ship_date,
  order_year,
  order_month,
  -- measures
  quantity,
  unit_price,
  discount_pct_clean,
  gross_amount,
  net_amount,
  shipping_delay_days
FROM cowork_op48.silver.silver_sales_transactions;
