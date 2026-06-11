CREATE OR REFRESH MATERIALIZED VIEW kiro_catalog.sales_codex53_gold_dev_20260601.gold_fact_sales_scd1
COMMENT 'Latest-state sales fact (SCD Type 1 semantics) keyed by transaction_id.'
TBLPROPERTIES (
  'quality' = 'gold',
  'scd_type' = '1'
)
AS
WITH ranked AS (
  SELECT
    s.*,
    ROW_NUMBER() OVER (
      PARTITION BY transaction_id
      ORDER BY _silver_processed_ts DESC, _ingest_ts DESC
    ) AS rn
  FROM kiro_catalog.sales_codex53_silver_dev_20260601.silver_sales_transactions AS s
)
SELECT
  transaction_id,
  order_date,
  ship_date,
  customer_id,
  customer_age,
  gender,
  product_id,
  product_category,
  quantity,
  unit_price,
  discount_pct,
  city,
  state,
  payment_type,
  order_status,
  ingestion_date,
  gross_amount,
  discount_amount,
  net_amount,
  ship_lag_days,
  order_month,
  _ingest_ts,
  _source_file,
  _source_file_modification_ts,
  _source_file_size_bytes,
  _silver_processed_ts
FROM ranked
WHERE rn = 1;
