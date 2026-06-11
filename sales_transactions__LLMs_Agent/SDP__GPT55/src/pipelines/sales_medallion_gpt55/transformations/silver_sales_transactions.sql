CREATE OR REFRESH STREAMING TABLE gpt55_codex.sales_silver.sales_transactions_clean (
  CONSTRAINT valid_transaction EXPECT (transaction_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_customer EXPECT (customer_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_product EXPECT (product_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_order_date EXPECT (order_date IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_quantity EXPECT (quantity > 0) ON VIOLATION DROP ROW,
  CONSTRAINT valid_unit_price EXPECT (unit_price >= 0) ON VIOLATION DROP ROW,
  CONSTRAINT valid_discount EXPECT (discount_pct BETWEEN 0 AND 100) ON VIOLATION DROP ROW,
  CONSTRAINT valid_age EXPECT (customer_age BETWEEN 0 AND 120 OR customer_age IS NULL) ON VIOLATION DROP ROW
)
COMMENT 'Cleaned and typed sales transactions. Duplicates are intentionally preserved for gold-layer SCD handling.'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true',
  'quality' = 'silver'
)
AS
SELECT
  CAST(TRIM(transaction_id) AS BIGINT) AS transaction_id,
  TO_DATE(TRIM(order_date)) AS order_date,
  TO_DATE(TRIM(ship_date)) AS ship_date,
  NULLIF(TRIM(customer_id), '') AS customer_id,
  CAST(TRIM(customer_age) AS INT) AS customer_age,
  CASE
    WHEN UPPER(TRIM(gender)) IN ('M', 'MALE') THEN 'Male'
    WHEN UPPER(TRIM(gender)) IN ('F', 'FEMALE') THEN 'Female'
    WHEN NULLIF(TRIM(gender), '') IS NULL THEN 'Unknown'
    ELSE INITCAP(TRIM(gender))
  END AS gender,
  NULLIF(TRIM(product_id), '') AS product_id,
  COALESCE(NULLIF(INITCAP(TRIM(product_category)), ''), 'Unknown') AS product_category,
  CAST(TRIM(quantity) AS INT) AS quantity,
  CAST(TRIM(unit_price) AS DECIMAL(18, 4)) AS unit_price,
  COALESCE(CAST(TRIM(discount_pct) AS DECIMAL(9, 4)), CAST(0 AS DECIMAL(9, 4))) AS discount_pct,
  COALESCE(NULLIF(INITCAP(TRIM(city)), ''), 'Unknown') AS city,
  COALESCE(NULLIF(UPPER(TRIM(state)), ''), 'Unknown') AS state,
  COALESCE(NULLIF(INITCAP(TRIM(payment_type)), ''), 'Unknown') AS payment_type,
  COALESCE(NULLIF(INITCAP(TRIM(order_status)), ''), 'Unknown') AS order_status,
  TO_DATE(TRIM(ingestion_date)) AS ingestion_date,
  CAST(TRIM(quantity) AS INT) * CAST(TRIM(unit_price) AS DECIMAL(18, 4)) AS gross_amount,
  CAST(TRIM(quantity) AS INT) * CAST(TRIM(unit_price) AS DECIMAL(18, 4))
    * COALESCE(CAST(TRIM(discount_pct) AS DECIMAL(9, 4)), CAST(0 AS DECIMAL(9, 4))) / 100 AS discount_amount,
  CAST(TRIM(quantity) AS INT) * CAST(TRIM(unit_price) AS DECIMAL(18, 4))
    * (1 - COALESCE(CAST(TRIM(discount_pct) AS DECIMAL(9, 4)), CAST(0 AS DECIMAL(9, 4))) / 100) AS net_amount,
  SHA2(CONCAT_WS('|',
    COALESCE(INITCAP(TRIM(city)), 'Unknown'),
    COALESCE(UPPER(TRIM(state)), 'Unknown')
  ), 256) AS location_key,
  TIMESTAMPADD(
    MICROSECOND,
    CAST(PMOD(XXHASH64(CONCAT_WS('|',
      TRIM(transaction_id),
      TRIM(order_date),
      TRIM(ship_date),
      TRIM(customer_id),
      TRIM(customer_age),
      TRIM(gender),
      TRIM(product_id),
      TRIM(product_category),
      TRIM(quantity),
      TRIM(unit_price),
      TRIM(discount_pct),
      TRIM(city),
      TRIM(state),
      TRIM(payment_type),
      TRIM(order_status),
      TRIM(ingestion_date)
    )), 86400000000) AS BIGINT),
    CAST(TO_TIMESTAMP(TRIM(order_date)) AS TIMESTAMP)
  ) AS _sequence_timestamp,
  _ingested_at,
  _source_file,
  _source_file_modification_time,
  _source_file_size
FROM STREAM gpt55_codex.sales_bronze.sales_transactions_raw
WHERE TRY_CAST(TRIM(transaction_id) AS BIGINT) IS NOT NULL
  AND NULLIF(TRIM(customer_id), '') IS NOT NULL
  AND NULLIF(TRIM(product_id), '') IS NOT NULL
  AND TRY_CAST(TRIM(order_date) AS DATE) IS NOT NULL
  AND TRY_CAST(TRIM(quantity) AS INT) > 0
  AND TRY_CAST(TRIM(unit_price) AS DECIMAL(18, 4)) >= 0
  AND COALESCE(TRY_CAST(TRIM(discount_pct) AS DECIMAL(9, 4)), CAST(0 AS DECIMAL(9, 4))) BETWEEN 0 AND 100
  AND (
    TRY_CAST(TRIM(customer_age) AS INT) BETWEEN 0 AND 120
    OR NULLIF(TRIM(customer_age), '') IS NULL
  );
