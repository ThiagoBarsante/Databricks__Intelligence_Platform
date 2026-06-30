-- ============================================================
-- GOLD DIMENSION: Customer Dimension (SCD Type 2)
-- ============================================================
-- Purpose: Customer dimension with historical tracking
-- Type: SCD Type 2 (track changes with __START_AT/__END_AT)
-- Source: Silver sales transactions
-- ============================================================

-- Step 1: Create target streaming table with SCD Type 2 columns
CREATE OR REFRESH STREAMING TABLE gold.dim_customers (
  customer_key BIGINT GENERATED ALWAYS AS IDENTITY,
  customer_id STRING NOT NULL,
  customer_age INT,
  gender STRING,
  city STRING,
  state STRING,
  __START_AT DATE,
  __END_AT DATE
)
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true'
)
COMMENT 'Customer dimension with SCD Type 2 history tracking';

-- Step 2: Create CDC flow to populate the dimension
CREATE FLOW dim_customers_cdc AS AUTO CDC INTO gold.dim_customers
FROM STREAM(silver.sales_transactions_clean)
KEYS (customer_id)
SEQUENCE BY order_date
COLUMNS customer_id, customer_age, gender, city, state
STORED AS SCD TYPE 2
