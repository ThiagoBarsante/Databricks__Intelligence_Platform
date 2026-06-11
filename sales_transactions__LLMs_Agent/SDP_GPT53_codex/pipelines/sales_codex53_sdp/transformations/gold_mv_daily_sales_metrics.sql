CREATE OR REFRESH MATERIALIZED VIEW kiro_catalog.sales_codex53_gold_dev_20260601.gold_mv_daily_sales_metrics
COMMENT 'Daily sales KPIs by order date and state.'
TBLPROPERTIES (
  'quality' = 'gold',
  'table_role' = 'aggregate'
)
AS
SELECT
  order_date,
  state,
  COUNT(*) AS total_orders,
  SUM(quantity) AS total_units,
  ROUND(SUM(gross_amount), 2) AS gross_revenue,
  ROUND(SUM(discount_amount), 2) AS total_discount,
  ROUND(SUM(net_amount), 2) AS net_revenue,
  ROUND(AVG(net_amount), 2) AS avg_order_value,
  ROUND(AVG(ship_lag_days), 2) AS avg_ship_lag_days
FROM kiro_catalog.sales_codex53_gold_dev_20260601.gold_fact_sales_scd1
GROUP BY order_date, state;
