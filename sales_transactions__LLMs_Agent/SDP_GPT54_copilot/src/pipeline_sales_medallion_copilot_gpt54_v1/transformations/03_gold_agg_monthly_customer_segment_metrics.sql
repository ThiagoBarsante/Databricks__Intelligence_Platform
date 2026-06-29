CREATE OR REFRESH MATERIALIZED VIEW ${catalog_name}.${gold_schema}.agg_monthly_customer_segment_metrics
COMMENT 'Monthly sales metrics by customer segment and payment type.'
CLUSTER BY (order_month, payment_type_code)
AS
SELECT
  DATE_TRUNC('month', order_date) AS order_month,
  customer_age_band,
  gender_normalized,
  payment_type_code,
  COUNT(*) AS transaction_count,
  COUNT(DISTINCT customer_id) AS distinct_customer_count,
  CAST(SUM(net_sales_amount) AS DECIMAL(18,4)) AS net_sales_amount,
  CAST(AVG(net_sales_amount) AS DECIMAL(18,4)) AS avg_transaction_value,
  CAST(AVG(discount_pct) AS DECIMAL(9,2)) AS avg_discount_pct
FROM ${catalog_name}.${gold_schema}.fact_sales_transactions
GROUP BY DATE_TRUNC('month', order_date), customer_age_band, gender_normalized, payment_type_code;
