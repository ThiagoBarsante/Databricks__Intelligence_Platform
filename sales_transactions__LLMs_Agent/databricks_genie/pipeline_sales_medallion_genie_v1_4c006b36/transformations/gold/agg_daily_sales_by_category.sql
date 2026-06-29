-- ============================================================
-- GOLD AGGREGATE: Daily Sales by Product Category
-- ============================================================
-- Purpose: Pre-aggregated daily sales metrics by category
-- Type: Materialized View (refreshed on pipeline update)
-- Grain: One row per date and product category
-- ============================================================

CREATE OR REFRESH MATERIALIZED VIEW gold.agg_daily_sales_by_category
COMMENT 'Daily sales metrics aggregated by product category for performance'
AS
SELECT
  -- Grouping dimensions
  od.date_value,
  p.product_category,
  
  -- Aggregated metrics
  COUNT(*) AS transaction_count,
  SUM(f.quantity) AS total_quantity,
  SUM(f.gross_amount) AS total_gross_sales,
  SUM(f.discount_amount) AS total_discounts,
  SUM(f.net_amount) AS total_net_sales,
  AVG(f.net_amount) AS avg_transaction_value
  
FROM gold.fact_sales f

-- Join with product dimension to get category
INNER JOIN gold.dim_products p
  ON f.product_key = p.product_key
  AND p.__END_AT IS NULL

-- Join with date dimension to get date attributes
INNER JOIN gold.dim_dates od
  ON f.order_date_key = od.date_key

GROUP BY ALL
ORDER BY date_value DESC, product_category
