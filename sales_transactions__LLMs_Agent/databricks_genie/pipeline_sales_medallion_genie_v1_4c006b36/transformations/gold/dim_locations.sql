-- ============================================================
-- GOLD DIMENSION: Location Dimension (SCD Type 1)
-- ============================================================
-- Purpose: Location dimension for geographic analysis
-- Type: SCD Type 1 (replace on change, no history)
-- Source: Distinct city and state combinations
-- ============================================================

CREATE OR REFRESH MATERIALIZED VIEW gold.dim_locations
COMMENT 'Location dimension with city and state for geographic analysis'
AS
SELECT
  -- Surrogate key (generated from city and state)
  ROW_NUMBER() OVER (ORDER BY state, city) AS location_key,
  
  -- Location attributes
  city,
  state
  
FROM (
  SELECT DISTINCT
    city,
    state
  FROM silver.sales_transactions_clean
  WHERE city IS NOT NULL
    AND state IS NOT NULL
) AS unique_locations
ORDER BY state, city
