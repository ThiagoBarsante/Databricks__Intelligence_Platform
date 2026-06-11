CREATE OR REFRESH MATERIALIZED VIEW kiro_catalog.sales_codex53_gold_dev_20260601.gold_mv_category_state_metrics
COMMENT 'Category and payment-level sales KPIs by state.'
TBLPROPERTIES (
  'quality' = 'gold',
  'table_role' = 'aggregate'
)
AS
SELECT
  state,
  product_category,
  payment_type,
  order_status,
  COUNT(*) AS txn_count,
  SUM(quantity) AS units_sold,
  ROUND(SUM(net_amount), 2) AS net_revenue,
  ROUND(AVG(discount_pct), 2) AS avg_discount_pct,
  ROUND(AVG(unit_price), 2) AS avg_unit_price
FROM kiro_catalog.sales_codex53_gold_dev_20260601.gold_fact_sales_scd1
GROUP BY state, product_category, payment_type, order_status;
