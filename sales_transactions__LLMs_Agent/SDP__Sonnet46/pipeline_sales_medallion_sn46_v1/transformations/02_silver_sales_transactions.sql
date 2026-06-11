-- Silver layer: clean, cast, and enrich bronze sales transactions.
-- 1:1 with bronze (no deduplication here — dedup happens in gold via AUTO CDC SCD Type 1).

CREATE OR REFRESH STREAMING TABLE cowork_sn46.silver.slv_sales_transactions
CLUSTER BY (order_date)
AS
SELECT
  CAST(transaction_id AS BIGINT)                          AS transaction_id,
  CAST(order_date AS DATE)                                AS order_date,
  CAST(ship_date AS DATE)                                 AS ship_date,
  customer_id,
  CAST(customer_age AS INT)                               AS customer_age,
  CASE
    WHEN UPPER(TRIM(gender)) IN ('M', 'MALE')   THEN 'M'
    WHEN UPPER(TRIM(gender)) IN ('F', 'FEMALE') THEN 'F'
    ELSE 'Unknown'
  END                                                      AS gender,
  product_id,
  product_category,
  CAST(quantity AS INT)                                   AS quantity,
  CAST(unit_price AS DECIMAL(10,2))                       AS unit_price,
  CAST(discount_pct AS DECIMAL(5,2))                      AS discount_pct,
  TRIM(city)                                              AS city,
  UPPER(TRIM(state))                                      AS state,
  payment_type,
  order_status,
  CAST(ingestion_date AS DATE)                            AS ingestion_date,
  -- Enrichment: net amount after discount (NULL-safe; NULL when price/discount unknown)
  ROUND(
    CAST(quantity AS INT) * CAST(unit_price AS DECIMAL(10,2))
      * (1 - COALESCE(CAST(discount_pct AS DECIMAL(5,2)), 0) / 100),
    2
  )                                                        AS net_amount,
  -- Data-quality flags (records are KEPT, just flagged — no rows dropped/deduped here)
  CASE
    WHEN CAST(unit_price AS DECIMAL(10,2)) IS NULL
      OR CAST(unit_price AS DECIMAL(10,2)) < 0
      OR CAST(quantity AS INT) <= 0
    THEN FALSE ELSE TRUE
  END                                                      AS is_valid_amount,
  CASE
    WHEN CAST(customer_age AS INT) IS NULL
      OR CAST(customer_age AS INT) NOT BETWEEN 0 AND 110
    THEN FALSE ELSE TRUE
  END                                                      AS is_valid_age,
  CASE
    WHEN CAST(unit_price AS DECIMAL(10,2)) IS NULL
      OR CAST(unit_price AS DECIMAL(10,2)) < 0
      OR CAST(quantity AS INT) <= 0
      OR CAST(customer_age AS INT) IS NULL
      OR CAST(customer_age AS INT) NOT BETWEEN 0 AND 110
      OR customer_id IS NULL
      OR product_id IS NULL
    THEN TRUE ELSE FALSE
  END                                                      AS has_data_quality_issue,
  _ingested_at,
  _source_file
FROM STREAM cowork_sn46.bronze.brz_sales_transactions;
