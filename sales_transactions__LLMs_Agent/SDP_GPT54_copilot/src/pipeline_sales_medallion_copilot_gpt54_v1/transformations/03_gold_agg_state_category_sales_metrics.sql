CREATE OR REFRESH MATERIALIZED VIEW ${catalog_name}.${gold_schema}.agg_state_category_sales_metrics
COMMENT 'Monthly state and product category aggregate for common merchandising and geography analysis.'
AS
SELECT
  DATE_TRUNC('MONTH', fact.order_date) AS sales_month,
  fact.state,
  prod.product_category,
  COUNT(*) AS transaction_count,
  COUNT(DISTINCT fact.customer_id) AS distinct_customer_count,
  SUM(fact.quantity) AS total_units,
  CAST(SUM(fact.net_sales_amount) AS DECIMAL(18,2)) AS net_sales_amount,
  CAST(AVG(fact.net_sales_amount) AS DECIMAL(18,2)) AS avg_transaction_amount
FROM ${catalog_name}.${gold_schema}.fact_sales_transactions AS fact
INNER JOIN ${catalog_name}.${gold_schema}.dim_product_current AS prod
  ON fact.product_id = prod.product_id
GROUP BY
  DATE_TRUNC('MONTH', fact.order_date),
  fact.state,
  prod.product_category;
