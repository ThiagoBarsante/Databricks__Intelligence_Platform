CREATE OR REFRESH MATERIALIZED VIEW gpt55_codex.sales_gold.mv_daily_sales_metrics
COMMENT 'Gold daily sales metrics by order date and order status.'
TBLPROPERTIES (
  'quality' = 'gold'
)
AS
SELECT
  order_date,
  order_status,
  COUNT(*) AS transaction_count,
  COUNT(DISTINCT customer_id) AS customer_count,
  COUNT(DISTINCT product_id) AS product_count,
  SUM(quantity) AS units_sold,
  SUM(gross_amount) AS gross_sales_amount,
  SUM(discount_amount) AS discount_amount,
  SUM(net_amount) AS net_sales_amount,
  AVG(net_amount) AS average_transaction_amount
FROM gpt55_codex.sales_gold.fact_sales
GROUP BY order_date, order_status;
