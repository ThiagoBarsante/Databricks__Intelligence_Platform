-- Gold aggregate #1: monthly sales performance by product category.
-- Provided by the Gold layer, sourced from the gold fact table.
CREATE OR REFRESH MATERIALIZED VIEW cowork_op48.gold.agg_sales_by_category_month
COMMENT 'Aggregate #1 - monthly sales performance by product category'
AS
SELECT
  product_category,
  order_year,
  order_month,
  COUNT(*)                          AS order_count,
  SUM(quantity)                     AS total_units,
  SUM(gross_amount)                 AS total_gross,
  SUM(net_amount)                   AS total_net,
  ROUND(AVG(discount_pct_clean), 2) AS avg_discount_pct,
  ROUND(AVG(net_amount), 2)         AS avg_order_value,
  COUNT(DISTINCT customer_id)       AS distinct_customers
FROM cowork_op48.gold.fact_sales
GROUP BY product_category, order_year, order_month;
