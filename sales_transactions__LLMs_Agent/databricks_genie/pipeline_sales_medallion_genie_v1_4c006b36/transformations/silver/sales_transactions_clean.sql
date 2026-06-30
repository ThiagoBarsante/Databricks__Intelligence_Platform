-- ============================================================
-- SILVER LAYER: Cleansed and Typed Sales Transactions
-- ============================================================
-- Purpose: Transform Bronze data with proper types, data quality,
--          gender normalization, and derived business metrics
-- Features: Type casting, data validation, business calculations
-- ============================================================

CREATE OR REFRESH STREAMING TABLE silver.sales_transactions_clean (
  CONSTRAINT valid_transaction_id EXPECT (transaction_id IS NOT NULL),
  CONSTRAINT valid_customer_id EXPECT (customer_id IS NOT NULL),
  CONSTRAINT valid_product_id EXPECT (product_id IS NOT NULL),
  CONSTRAINT valid_quantity EXPECT (quantity >= 0),
  CONSTRAINT valid_unit_price EXPECT (unit_price > 0),
  CONSTRAINT valid_customer_age EXPECT (customer_age BETWEEN 0 AND 120),
  CONSTRAINT valid_order_date EXPECT (order_date IS NOT NULL),
  CONSTRAINT valid_ship_date EXPECT (ship_date IS NOT NULL)
)
TBLPROPERTIES (
  'pipelines.autoOptimize.managed' = 'true',
  'delta.enableChangeDataFeed' = 'true'
)
COMMENT 'Cleansed sales transactions with proper data types, quality filters, and derived business metrics'
AS SELECT
  -- Primary identifiers (cast to proper types)
  CAST(transaction_id AS BIGINT) AS transaction_id,
  
  -- Date fields (cast from STRING to DATE)
  TRY_TO_DATE(order_date, 'yyyy-MM-dd') AS order_date,
  TRY_TO_DATE(ship_date, 'yyyy-MM-dd') AS ship_date,
  
  -- Customer fields
  customer_id,
  CAST(customer_age AS INT) AS customer_age,
  
  -- Gender normalization (M/Male -> 'Male', F/Female -> 'Female', else -> 'Unknown')
  CASE
    WHEN UPPER(TRIM(gender)) IN ('M', 'MALE') THEN 'Male'
    WHEN UPPER(TRIM(gender)) IN ('F', 'FEMALE') THEN 'Female'
    ELSE 'Unknown'
  END AS gender,
  
  -- Product fields
  product_id,
  product_category,
  
  -- Transaction metrics (cast to proper numeric types)
  CAST(quantity AS INT) AS quantity,
  CAST(unit_price AS DECIMAL(10, 2)) AS unit_price,
  CAST(discount_pct AS DECIMAL(5, 2)) AS discount_pct,
  
  -- Derived business metrics
  CAST(quantity AS INT) * CAST(unit_price AS DECIMAL(10, 2)) AS gross_amount,
  (CAST(quantity AS INT) * CAST(unit_price AS DECIMAL(10, 2))) * (CAST(discount_pct AS DECIMAL(5, 2)) / 100) AS discount_amount,
  (CAST(quantity AS INT) * CAST(unit_price AS DECIMAL(10, 2))) - 
    ((CAST(quantity AS INT) * CAST(unit_price AS DECIMAL(10, 2))) * (CAST(discount_pct AS DECIMAL(5, 2)) / 100)) AS net_amount,
  
  -- Days to ship calculation
  DATEDIFF(TRY_TO_DATE(ship_date, 'yyyy-MM-dd'), TRY_TO_DATE(order_date, 'yyyy-MM-dd')) AS days_to_ship,
  
  -- Location fields
  city,
  state,
  
  -- Payment and order status (handle nulls)
  COALESCE(payment_type, 'Unknown') AS payment_type,
  order_status,
  
  -- Original ingestion metadata
  TRY_TO_DATE(ingestion_date, 'yyyy-MM-dd') AS ingestion_date,
  _ingest_timestamp,
  _source_file,
  
  -- Silver layer timestamp
  current_timestamp() AS _silver_timestamp
  
FROM STREAM bronze.sales_transactions_raw
WHERE
  -- Data quality filters: only include valid records
  transaction_id IS NOT NULL
  AND customer_id IS NOT NULL
  AND product_id IS NOT NULL
  AND TRY_TO_DATE(order_date, 'yyyy-MM-dd') IS NOT NULL
  AND TRY_TO_DATE(ship_date, 'yyyy-MM-dd') IS NOT NULL
  AND CAST(quantity AS INT) >= 0
  AND CAST(unit_price AS DECIMAL(10, 2)) > 0
  AND CAST(customer_age AS INT) BETWEEN 0 AND 120
