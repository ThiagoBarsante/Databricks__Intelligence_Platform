CREATE OR REFRESH STREAMING TABLE ${catalog_name}.${silver_schema}.silver_sales_transactions_clean
COMMENT 'Silver sales transactions with typed cleansing and business enrichments. No deduplication is applied at this layer.'
CLUSTER BY (order_date, state, product_category)
AS
SELECT
  TRY_CAST(transaction_id AS BIGINT) AS transaction_id,
  TRY_CAST(order_date AS DATE) AS order_date,
  TRY_CAST(ship_date AS DATE) AS ship_date,
  TRIM(customer_id) AS customer_id,
  CASE
    WHEN TRY_CAST(customer_age AS INT) BETWEEN 0 AND 120 THEN TRY_CAST(customer_age AS INT)
    ELSE NULL
  END AS customer_age,
  CASE
    WHEN TRY_CAST(customer_age AS INT) BETWEEN 0 AND 17 THEN 'UNDER_18'
    WHEN TRY_CAST(customer_age AS INT) BETWEEN 18 AND 24 THEN '18_24'
    WHEN TRY_CAST(customer_age AS INT) BETWEEN 25 AND 34 THEN '25_34'
    WHEN TRY_CAST(customer_age AS INT) BETWEEN 35 AND 44 THEN '35_44'
    WHEN TRY_CAST(customer_age AS INT) BETWEEN 45 AND 54 THEN '45_54'
    WHEN TRY_CAST(customer_age AS INT) BETWEEN 55 AND 64 THEN '55_64'
    WHEN TRY_CAST(customer_age AS INT) >= 65 THEN '65_PLUS'
    ELSE 'UNKNOWN'
  END AS customer_age_band,
  CASE
    WHEN UPPER(TRIM(gender)) IN ('M', 'MALE') THEN 'MALE'
    WHEN UPPER(TRIM(gender)) IN ('F', 'FEMALE') THEN 'FEMALE'
    ELSE 'UNKNOWN'
  END AS gender_normalized,
  TRIM(product_id) AS product_id,
  COALESCE(NULLIF(TRIM(product_category), ''), 'UNKNOWN') AS product_category,
  TRY_CAST(quantity AS INT) AS quantity,
  TRY_CAST(unit_price AS DECIMAL(18,4)) AS unit_price,
  COALESCE(TRY_CAST(discount_pct AS DECIMAL(9,2)), CAST(0 AS DECIMAL(9,2))) AS discount_pct,
  COALESCE(NULLIF(TRIM(city), ''), 'UNKNOWN') AS city,
  COALESCE(NULLIF(TRIM(state), ''), 'UNKNOWN') AS state,
  CONCAT_WS('|', COALESCE(NULLIF(TRIM(city), ''), 'UNKNOWN'), COALESCE(NULLIF(TRIM(state), ''), 'UNKNOWN')) AS location_key,
  COALESCE(NULLIF(UPPER(TRIM(payment_type)), ''), 'UNKNOWN') AS payment_type,
  COALESCE(NULLIF(UPPER(TRIM(order_status)), ''), 'UNKNOWN') AS order_status,
  TRY_CAST(ingestion_date AS DATE) AS ingestion_date,
  CAST(TRY_CAST(quantity AS DECIMAL(18,4)) * TRY_CAST(unit_price AS DECIMAL(18,4)) AS DECIMAL(18,4)) AS gross_sales_amount,
  CAST(
    (TRY_CAST(quantity AS DECIMAL(18,4)) * TRY_CAST(unit_price AS DECIMAL(18,4))) *
    (COALESCE(TRY_CAST(discount_pct AS DECIMAL(9,4)), CAST(0 AS DECIMAL(9,4))) / CAST(100 AS DECIMAL(9,4)))
    AS DECIMAL(18,4)
  ) AS discount_amount,
  CAST(
    (TRY_CAST(quantity AS DECIMAL(18,4)) * TRY_CAST(unit_price AS DECIMAL(18,4))) -
    ((TRY_CAST(quantity AS DECIMAL(18,4)) * TRY_CAST(unit_price AS DECIMAL(18,4))) *
      (COALESCE(TRY_CAST(discount_pct AS DECIMAL(9,4)), CAST(0 AS DECIMAL(9,4))) / CAST(100 AS DECIMAL(9,4))))
    AS DECIMAL(18,4)
  ) AS net_sales_amount,
  DATEDIFF(TRY_CAST(ship_date AS DATE), TRY_CAST(order_date AS DATE)) AS shipping_lead_days,
  COALESCE(NULLIF(UPPER(TRIM(order_status)), ''), 'UNKNOWN') = 'CANCELLED' AS is_cancelled,
  COALESCE(NULLIF(UPPER(TRIM(order_status)), ''), 'UNKNOWN') = 'RETURNED' AS is_returned,
  _ingested_at AS source_sequence_at,
  _ingested_at,
  _source_file,
  _source_file_modified_at,
  _source_file_size,
  _rescued_data
FROM STREAM ${catalog_name}.${bronze_schema}.bronze_sales_transactions_raw
WHERE _rescued_data IS NULL
  AND TRY_CAST(transaction_id AS BIGINT) IS NOT NULL
  AND TRY_CAST(order_date AS DATE) IS NOT NULL
  AND TRY_CAST(ship_date AS DATE) IS NOT NULL
  AND NULLIF(TRIM(customer_id), '') IS NOT NULL
  AND NULLIF(TRIM(product_id), '') IS NOT NULL
  AND TRY_CAST(quantity AS INT) IS NOT NULL
  AND TRY_CAST(quantity AS INT) >= 0
  AND TRY_CAST(unit_price AS DECIMAL(18,4)) IS NOT NULL
  AND TRY_CAST(unit_price AS DECIMAL(18,4)) >= 0
  AND (
    discount_pct IS NULL OR TRY_CAST(discount_pct AS DECIMAL(9,2)) BETWEEN 0 AND 100
  );
