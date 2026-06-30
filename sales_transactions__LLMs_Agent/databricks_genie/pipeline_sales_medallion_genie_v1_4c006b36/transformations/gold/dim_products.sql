-- ============================================================
-- GOLD DIMENSION: Product Dimension (SCD Type 2)
-- ============================================================
-- Purpose: Product dimension with historical tracking
-- Type: SCD Type 2 (track changes with __START_AT/__END_AT)
-- Source: Silver sales transactions
-- ============================================================

-- Step 1: Create target streaming table with SCD Type 2 columns
CREATE OR REFRESH STREAMING TABLE gold.dim_products (
  product_key BIGINT GENERATED ALWAYS AS IDENTITY,
  product_id STRING NOT NULL,
  product_category STRING,
  __START_AT DATE,
  __END_AT DATE
)
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true'
)
COMMENT 'Product dimension with SCD Type 2 history tracking';

-- Step 2: Create CDC flow to populate the dimension
CREATE FLOW dim_products_cdc AS AUTO CDC INTO gold.dim_products
FROM STREAM(silver.sales_transactions_clean)
KEYS (product_id)
SEQUENCE BY order_date
COLUMNS product_id, product_category
STORED AS SCD TYPE 2
