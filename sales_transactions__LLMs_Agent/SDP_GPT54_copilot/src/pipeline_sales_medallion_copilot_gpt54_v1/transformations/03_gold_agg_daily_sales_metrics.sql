CREATE OR REFRESH MATERIALIZED VIEW ${catalog_name}.${gold_schema}.agg_daily_sales_metrics
COMMENT 'Daily sales metrics by state and product category.'
CLUSTER BY (order_date, state)
AS
SELECT
  order_date,
  state,
  product_category,
  COUNT(*) AS transaction_count,
  COUNT(DISTINCT customer_id) AS distinct_customer_count,
  SUM(quantity) AS units_sold,
  CAST(SUM(gross_sales_amount) AS DECIMAL(18,4)) AS gross_sales_amount,
  CAST(SUM(discount_amount) AS DECIMAL(18,4)) AS discount_amount,
  CAST(SUM(net_sales_amount) AS DECIMAL(18,4)) AS net_sales_amount,
  SUM(CASE WHEN is_cancelled THEN 1 ELSE 0 END) AS cancelled_transaction_count,
  SUM(CASE WHEN is_returned THEN 1 ELSE 0 END) AS returned_transaction_count
FROM ${catalog_name}.${gold_schema}.fact_sales_transactions
GROUP BY order_date, state, product_category;
