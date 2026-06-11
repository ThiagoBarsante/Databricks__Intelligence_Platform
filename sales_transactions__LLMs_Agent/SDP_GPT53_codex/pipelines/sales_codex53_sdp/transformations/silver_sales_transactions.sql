CREATE OR REFRESH STREAMING TABLE kiro_catalog.sales_codex53_silver_dev_20260601.silver_sales_transactions
(
  CONSTRAINT valid_transaction_id EXPECT (transaction_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_customer_id EXPECT (customer_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_product_id EXPECT (product_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_ingestion_date EXPECT (ingestion_date IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Cleansed and enriched sales stream. Deduplication intentionally excluded from silver.'
TBLPROPERTIES (
  'quality' = 'silver'
)
AS
SELECT
  CAST(transaction_id AS BIGINT) AS transaction_id,
  TO_DATE(order_date) AS order_date,
  TO_DATE(ship_date) AS ship_date,
  customer_id,
  CASE
    WHEN CAST(customer_age AS INT) BETWEEN 0 AND 120 THEN CAST(customer_age AS INT)
    ELSE NULL
  END AS customer_age,
  CASE
    WHEN lower(trim(gender)) IN ('m', 'male') THEN 'Male'
    WHEN lower(trim(gender)) IN ('f', 'female') THEN 'Female'
    ELSE 'Unknown'
  END AS gender,
  product_id,
  INITCAP(product_category) AS product_category,
  CAST(quantity AS INT) AS quantity_raw,
  GREATEST(COALESCE(CAST(quantity AS INT), 0), 0) AS quantity,
  CAST(unit_price AS DECIMAL(18, 4)) AS unit_price_raw,
  CASE
    WHEN CAST(unit_price AS DECIMAL(18, 4)) >= 0 THEN CAST(unit_price AS DECIMAL(18, 4))
    ELSE NULL
  END AS unit_price,
  CAST(discount_pct AS DECIMAL(9, 4)) AS discount_pct_raw,
  CASE
    WHEN CAST(discount_pct AS DECIMAL(9, 4)) BETWEEN 0 AND 100 THEN CAST(discount_pct AS DECIMAL(9, 4))
    ELSE 0
  END AS discount_pct,
  city,
  state,
  COALESCE(payment_type, 'Unknown') AS payment_type,
  order_status,
  TO_DATE(ingestion_date) AS ingestion_date,
  _ingest_ts,
  _source_file,
  _source_file_modification_ts,
  _source_file_size_bytes,
  GREATEST(COALESCE(CAST(quantity AS DECIMAL(18, 4)), 0), 0)
    * COALESCE(
      CASE
        WHEN CAST(unit_price AS DECIMAL(18, 4)) >= 0 THEN CAST(unit_price AS DECIMAL(18, 4))
        ELSE NULL
      END,
      0
    ) AS gross_amount,
  GREATEST(COALESCE(CAST(quantity AS DECIMAL(18, 4)), 0), 0)
    * COALESCE(
      CASE
        WHEN CAST(unit_price AS DECIMAL(18, 4)) >= 0 THEN CAST(unit_price AS DECIMAL(18, 4))
        ELSE NULL
      END,
      0
    )
    * (
      CASE
        WHEN CAST(discount_pct AS DECIMAL(9, 4)) BETWEEN 0 AND 100 THEN CAST(discount_pct AS DECIMAL(9, 4))
        ELSE 0
      END
    ) / 100 AS discount_amount,
  (
    GREATEST(COALESCE(CAST(quantity AS DECIMAL(18, 4)), 0), 0)
      * COALESCE(
        CASE
          WHEN CAST(unit_price AS DECIMAL(18, 4)) >= 0 THEN CAST(unit_price AS DECIMAL(18, 4))
          ELSE NULL
        END,
        0
      )
  ) - (
    GREATEST(COALESCE(CAST(quantity AS DECIMAL(18, 4)), 0), 0)
      * COALESCE(
        CASE
          WHEN CAST(unit_price AS DECIMAL(18, 4)) >= 0 THEN CAST(unit_price AS DECIMAL(18, 4))
          ELSE NULL
        END,
        0
      )
      * (
        CASE
          WHEN CAST(discount_pct AS DECIMAL(9, 4)) BETWEEN 0 AND 100 THEN CAST(discount_pct AS DECIMAL(9, 4))
          ELSE 0
        END
      ) / 100
  ) AS net_amount,
  CASE
    WHEN TO_DATE(ship_date) IS NOT NULL AND TO_DATE(order_date) IS NOT NULL
      THEN DATEDIFF(TO_DATE(ship_date), TO_DATE(order_date))
    ELSE NULL
  END AS ship_lag_days,
  CASE
    WHEN TO_DATE(order_date) IS NOT NULL THEN DATE_TRUNC('MONTH', TO_DATE(order_date))
    ELSE NULL
  END AS order_month,
  current_timestamp() AS _silver_processed_ts
FROM STREAM kiro_catalog.sales_codex53_bronze_dev_20260601.bronze_sales_transactions;
