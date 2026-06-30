-- ============================================================
-- GOLD FACT: Sales Fact Table (SCD Type 1)
-- ============================================================
-- Purpose: Central fact table for sales analytics
-- Type: SCD Type 1 (latest values only)
-- Grain: One row per transaction
-- ============================================================

CREATE OR REFRESH MATERIALIZED VIEW gold.fact_sales
COMMENT 'Sales fact table with foreign keys to all dimensions and transaction measures'
AS
SELECT
  -- Transaction identifier
  s.transaction_id,
  
  -- Foreign keys to dimensions
  c.customer_key,
  p.product_key,
  od.date_key AS order_date_key,
  sd.date_key AS ship_date_key,
  l.location_key,
  
  -- Measures (numeric facts)
  s.quantity,
  s.unit_price,
  s.discount_pct,
  s.gross_amount,
  s.discount_amount,
  s.net_amount,
  s.days_to_ship,
  
  -- Degenerate dimensions (descriptive attributes stored in fact)
  s.payment_type,
  s.order_status,
  
  -- Audit columns
  s.order_date,
  s.ship_date,
  s._silver_timestamp
  
FROM silver.sales_transactions_clean s

-- Join with Customer dimension (SCD Type 2 - current records only)
INNER JOIN gold.dim_customers c
  ON s.customer_id = c.customer_id
  AND c.__END_AT IS NULL

-- Join with Product dimension (SCD Type 2 - current records only)
INNER JOIN gold.dim_products p
  ON s.product_id = p.product_id
  AND p.__END_AT IS NULL

-- Join with Date dimension for order_date
INNER JOIN gold.dim_dates od
  ON CAST(date_format(s.order_date, 'yyyyMMdd') AS INT) = od.date_key

-- Join with Date dimension for ship_date
INNER JOIN gold.dim_dates sd
  ON CAST(date_format(s.ship_date, 'yyyyMMdd') AS INT) = sd.date_key

-- Join with Location dimension
INNER JOIN gold.dim_locations l
  ON s.city = l.city
  AND s.state = l.state
