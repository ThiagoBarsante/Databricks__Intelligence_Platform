-- Gold aggregate tables: common retail-analytics metrics, built as star-schema rollups
-- (fact_sales_transactions joined to its dimensions). Materialized views = full recompute
-- on refresh, appropriate for these whole-table aggregations.

-- 1) Monthly performance by product category (fact ⋈ dim_product, current rows only)
CREATE OR REFRESH MATERIALIZED VIEW cowork_sn46.gold.agg_category_monthly_metrics
CLUSTER BY (order_month)
COMMENT 'Monthly sales metrics per product category: revenue, units, avg discount, fulfillment mix'
AS
SELECT
  date_trunc('MONTH', f.order_date)                                AS order_month,
  p.product_category,
  COUNT(*)                                                         AS order_count,
  SUM(f.quantity)                                                  AS total_quantity,
  SUM(f.net_amount)                                                AS total_revenue,
  ROUND(AVG(f.discount_pct), 2)                                    AS avg_discount_pct,
  SUM(CASE WHEN f.order_status = 'Cancelled' THEN 1 ELSE 0 END)    AS cancelled_orders,
  SUM(CASE WHEN f.order_status = 'Returned'  THEN 1 ELSE 0 END)    AS returned_orders,
  SUM(CASE WHEN f.order_status = 'Delivered' THEN 1 ELSE 0 END)    AS delivered_orders
FROM cowork_sn46.gold.fact_sales_transactions f
INNER JOIN cowork_sn46.gold.dim_product p
  ON f.product_id = p.product_id
 AND p.__END_AT IS NULL
GROUP BY date_trunc('MONTH', f.order_date), p.product_category;

-- 2) Performance by customer segment (fact ⋈ dim_customer): demographics x geography
CREATE OR REFRESH MATERIALIZED VIEW cowork_sn46.gold.agg_customer_segment_metrics
CLUSTER BY (state)
COMMENT 'Sales metrics per customer segment (gender, age bracket, state): reach, revenue, order value, returns'
AS
SELECT
  c.gender,
  CASE
    WHEN c.customer_age IS NULL  THEN 'Unknown'
    WHEN c.customer_age < 25     THEN 'Under 25'
    WHEN c.customer_age < 40     THEN '25-39'
    WHEN c.customer_age < 60     THEN '40-59'
    ELSE '60+'
  END                                                              AS age_bracket,
  c.state,
  COUNT(*)                                                         AS order_count,
  COUNT(DISTINCT f.customer_id)                                    AS distinct_customers,
  SUM(f.net_amount)                                                AS total_revenue,
  ROUND(AVG(f.net_amount), 2)                                      AS avg_order_value,
  SUM(CASE WHEN f.order_status = 'Returned' THEN 1 ELSE 0 END)     AS returned_orders
FROM cowork_sn46.gold.fact_sales_transactions f
INNER JOIN cowork_sn46.gold.dim_customer c
  ON f.customer_id = c.customer_id
GROUP BY c.gender, age_bracket, c.state;
