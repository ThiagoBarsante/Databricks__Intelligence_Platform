-- ============================================================
-- GOLD AGGREGATE: Customer Lifetime Metrics
-- ============================================================
-- Purpose: Pre-aggregated customer lifetime value and behavior
-- Type: Materialized View (refreshed on pipeline update)
-- Grain: One row per customer
-- ============================================================

CREATE OR REFRESH MATERIALIZED VIEW gold.agg_customer_metrics
COMMENT 'Customer lifetime metrics including purchase behavior and value'
AS
SELECT
  -- Customer attributes (current state from SCD Type 2)
  c.customer_id,
  c.customer_age,
  c.gender,
  c.city,
  c.state,
  
  -- Aggregated metrics
  COUNT(*) AS total_transactions,
  SUM(f.quantity) AS total_items_purchased,
  SUM(f.net_amount) AS lifetime_value,
  AVG(f.net_amount) AS avg_order_value,
  
  -- Temporal metrics
  MIN(f.order_date) AS first_purchase_date,
  MAX(f.order_date) AS last_purchase_date,
  DATEDIFF(MAX(f.order_date), MIN(f.order_date)) AS customer_tenure_days
  
FROM gold.fact_sales f

-- Join with customer dimension to get current customer attributes
INNER JOIN gold.dim_customers c
  ON f.customer_key = c.customer_key
  AND c.__END_AT IS NULL

GROUP BY ALL
ORDER BY lifetime_value DESC
