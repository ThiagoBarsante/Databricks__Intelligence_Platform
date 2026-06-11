CREATE OR REFRESH MATERIALIZED VIEW gpt55_codex.sales_gold.mv_category_state_metrics
COMMENT 'Gold sales metrics by product category, state, payment type, and order status.'
TBLPROPERTIES (
  'quality' = 'gold'
)
AS
SELECT
  p.product_category,
  l.state,
  f.payment_type,
  f.order_status,
  COUNT(*) AS transaction_count,
  COUNT(DISTINCT f.customer_id) AS customer_count,
  COUNT(DISTINCT f.product_id) AS product_count,
  SUM(f.quantity) AS units_sold,
  SUM(f.gross_amount) AS gross_sales_amount,
  SUM(f.discount_amount) AS discount_amount,
  SUM(f.net_amount) AS net_sales_amount
FROM gpt55_codex.sales_gold.fact_sales AS f
LEFT JOIN gpt55_codex.sales_gold.dim_product AS p
  ON f.product_id = p.product_id
  AND p.__END_AT IS NULL
LEFT JOIN gpt55_codex.sales_gold.dim_location AS l
  ON f.location_key = l.location_key
  AND l.__END_AT IS NULL
GROUP BY
  p.product_category,
  l.state,
  f.payment_type,
  f.order_status;
