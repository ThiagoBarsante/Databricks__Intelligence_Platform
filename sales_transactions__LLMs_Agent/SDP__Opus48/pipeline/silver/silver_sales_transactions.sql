-- Silver: cleaned, cast & enriched sales transactions.
-- R3: cast ALL columns to proper types, FILTER to records that can be a real sale,
--     ENRICH with derived business columns. NO deduplication (dup transaction_ids retained).
CREATE OR REFRESH STREAMING TABLE cowork_op48.silver.silver_sales_transactions (
  CONSTRAINT valid_transaction  EXPECT (transaction_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_order_date   EXPECT (order_date IS NOT NULL)     ON VIOLATION DROP ROW,
  CONSTRAINT positive_quantity  EXPECT (quantity > 0)               ON VIOLATION DROP ROW,
  CONSTRAINT positive_price     EXPECT (unit_price > 0)             ON VIOLATION DROP ROW
)
COMMENT 'Silver cleaned/cast/enriched sales (valid sales only, no dedup)'
TBLPROPERTIES ('quality' = 'silver')
AS
WITH cast_cols AS (
  SELECT
    -- ---- casts (R3.2) ----
    CAST(transaction_id AS BIGINT)            AS transaction_id,
    CAST(order_date     AS DATE)              AS order_date,
    CAST(ship_date      AS DATE)              AS ship_date,
    TRIM(customer_id)                         AS customer_id,
    CAST(customer_age   AS INT)               AS customer_age_raw,
    TRIM(gender)                              AS gender_raw,
    TRIM(product_id)                          AS product_id,
    TRIM(product_category)                    AS product_category,
    CAST(quantity       AS INT)               AS quantity,
    CAST(unit_price     AS DECIMAL(12,2))     AS unit_price,
    CAST(discount_pct   AS DECIMAL(6,2))      AS discount_pct_raw,
    TRIM(city)                                AS city,
    TRIM(state)                               AS state,
    TRIM(payment_type)                        AS payment_type,
    TRIM(order_status)                        AS order_status,
    CAST(ingestion_date AS DATE)              AS ingestion_date,
    _ingested_at,
    _source_file
  FROM STREAM cowork_op48.bronze.bronze_sales_transactions
)
SELECT
  transaction_id,
  order_date,
  ship_date,
  customer_id,
  product_id,
  product_category,
  quantity,
  unit_price,
  city,
  state,
  payment_type,
  order_status,
  ingestion_date,
  -- ---- enrichment (R3.3) ----
  CASE WHEN gender_raw IN ('M','Male')   THEN 'Male'
       WHEN gender_raw IN ('F','Female') THEN 'Female'
       ELSE 'Unknown' END                                          AS gender_clean,
  CASE WHEN customer_age_raw BETWEEN 0 AND 120 THEN customer_age_raw END AS customer_age_clean,
  CASE WHEN customer_age_raw NOT BETWEEN 0 AND 120 OR customer_age_raw IS NULL THEN 'Unknown'
       WHEN customer_age_raw < 18 THEN '<18'
       WHEN customer_age_raw < 30 THEN '18-29'
       WHEN customer_age_raw < 45 THEN '30-44'
       WHEN customer_age_raw < 60 THEN '45-59'
       ELSE '60+' END                                              AS age_band,
  CASE WHEN discount_pct_raw BETWEEN 0 AND 100 THEN discount_pct_raw ELSE 0 END AS discount_pct_clean,
  CAST(quantity * unit_price AS DECIMAL(14,2))                     AS gross_amount,
  CAST(quantity * unit_price *
       (1 - CASE WHEN discount_pct_raw BETWEEN 0 AND 100 THEN discount_pct_raw ELSE 0 END / 100)
       AS DECIMAL(14,2))                                           AS net_amount,
  DATEDIFF(ship_date, order_date)                                 AS shipping_delay_days,
  YEAR(order_date)                                                AS order_year,
  MONTH(order_date)                                               AS order_month,
  -- ---- lineage carried forward ----
  _ingested_at,
  _source_file
FROM cast_cols;
