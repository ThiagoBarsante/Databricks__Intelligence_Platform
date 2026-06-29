-- ============================================================
-- GOLD DIMENSION: Date Dimension (SCD Type 1)
-- ============================================================
-- Purpose: Date dimension for temporal analysis and reporting
-- Type: SCD Type 1 (replace on change, no history)
-- Source: Extract from order_date and ship_date
-- ============================================================

CREATE OR REFRESH MATERIALIZED VIEW gold.dim_dates
COMMENT 'Date dimension with calendar attributes for temporal analysis'
AS
WITH all_dates AS (
  -- Extract unique dates from order_date
  SELECT DISTINCT order_date AS date_value
  FROM silver.sales_transactions_clean
  WHERE order_date IS NOT NULL
  
  UNION
  
  -- Extract unique dates from ship_date
  SELECT DISTINCT ship_date AS date_value
  FROM silver.sales_transactions_clean
  WHERE ship_date IS NOT NULL
)
SELECT
  -- Date key (yyyyMMdd format as INT for efficient joins)
  CAST(date_format(date_value, 'yyyyMMdd') AS INT) AS date_key,
  
  -- Date value
  date_value,
  
  -- Year attributes
  YEAR(date_value) AS year,
  QUARTER(date_value) AS quarter,
  
  -- Month attributes
  MONTH(date_value) AS month,
  date_format(date_value, 'MMMM') AS month_name,
  
  -- Day attributes
  DAY(date_value) AS day,
  DAYOFWEEK(date_value) AS day_of_week,
  date_format(date_value, 'EEEE') AS day_name,
  
  -- Weekend flag
  CASE WHEN DAYOFWEEK(date_value) IN (1, 7) THEN TRUE ELSE FALSE END AS is_weekend
  
FROM all_dates
ORDER BY date_value
