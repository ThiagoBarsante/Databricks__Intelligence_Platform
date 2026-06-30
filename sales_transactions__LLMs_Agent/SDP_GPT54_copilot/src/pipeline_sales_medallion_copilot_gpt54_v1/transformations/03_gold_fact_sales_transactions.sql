CREATE OR REFRESH MATERIALIZED VIEW ${catalog_name}.${gold_schema}.fact_sales_transactions
COMMENT 'Gold fact table for sales transactions in star-schema form.'
CLUSTER BY (order_date, state, product_category)
AS
SELECT
  transaction_id AS sales_transaction_key,
  transaction_id,
  order_date,
  ship_date,
  customer_id,
  product_id,
  location_key,
  payment_type AS payment_type_code,
  order_status AS order_status_code,
  customer_age_band,
  gender_normalized,
  product_category,
  city,
  state,
  quantity,
  unit_price,
  discount_pct,
  gross_sales_amount,
  discount_amount,
  net_sales_amount,
  shipping_lead_days,
  is_cancelled,
  is_returned,
  ingestion_date,
  _ingested_at,
  _source_file
FROM ${catalog_name}.${silver_schema}.silver_sales_transactions_clean;
