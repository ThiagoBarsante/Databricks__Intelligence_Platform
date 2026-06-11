-- ============================================================
-- SILVER LAYER — cleaned, typed, enriched sales transactions
-- All columns cast from STRING to proper types.
-- Critical rule violations DROP the row (expectations below);
-- non-critical bad values are nulled/normalized instead.
-- NO deduplication at this layer (happens in gold via AUTO CDC).
-- ============================================================

CREATE OR REFRESH STREAMING TABLE cowork_fable5.silver_fa5_v1.silver_sales_transactions (
  CONSTRAINT valid_transaction_id EXPECT (transaction_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_customer_id    EXPECT (customer_id IS NOT NULL AND customer_id != '') ON VIOLATION DROP ROW,
  CONSTRAINT valid_product_id     EXPECT (product_id IS NOT NULL AND product_id != '') ON VIOLATION DROP ROW,
  CONSTRAINT valid_order_date     EXPECT (order_date IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT positive_quantity    EXPECT (quantity > 0) ON VIOLATION DROP ROW,
  CONSTRAINT positive_unit_price  EXPECT (unit_price > 0) ON VIOLATION DROP ROW,
  CONSTRAINT plausible_age        EXPECT (customer_age IS NULL OR customer_age BETWEEN 18 AND 95),
  CONSTRAINT known_gender         EXPECT (gender IS NOT NULL),
  CONSTRAINT valid_ship_sequence  EXPECT (ship_date IS NULL OR ship_date >= order_date)
)
COMMENT 'Cleaned and enriched sales transactions: fully typed, normalized, NOT deduplicated'
CLUSTER BY (order_date, product_category)
TBLPROPERTIES ('quality' = 'silver')
AS
WITH typed AS (
  SELECT
    CAST(transaction_id AS BIGINT)                 AS transaction_id,
    CAST(order_date AS DATE)                       AS order_date,
    CAST(ship_date AS DATE)                        AS ship_date_raw,
    TRIM(customer_id)                              AS customer_id,
    CAST(customer_age AS INT)                      AS customer_age_raw,
    UPPER(TRIM(gender))                            AS gender_raw,
    TRIM(product_id)                               AS product_id,
    TRIM(product_category)                         AS product_category,
    CAST(quantity AS INT)                          AS quantity,
    CAST(unit_price AS DECIMAL(12,2))              AS unit_price,
    CAST(discount_pct AS DECIMAL(5,2))             AS discount_pct_raw,
    TRIM(city)                                     AS city,
    TRIM(state)                                    AS state,
    TRIM(payment_type)                             AS payment_type_raw,
    TRIM(order_status)                             AS order_status,
    CAST(ingestion_date AS DATE)                   AS ingestion_date,
    _ingested_at,
    _source_file
  FROM STREAM cowork_fable5.bronze_fa5_v1.bronze_sales_transactions
)
SELECT
  transaction_id,
  order_date,
  -- ship_date earlier than order_date is impossible -> null it
  CASE WHEN ship_date_raw >= order_date THEN ship_date_raw END        AS ship_date,
  customer_id,
  -- implausible ages -> null
  CASE WHEN customer_age_raw BETWEEN 18 AND 95 THEN customer_age_raw END AS customer_age,
  -- normalize gender encodings
  CASE
    WHEN gender_raw IN ('M', 'MALE')   THEN 'M'
    WHEN gender_raw IN ('F', 'FEMALE') THEN 'F'
  END                                                                 AS gender,
  product_id,
  product_category,
  quantity,
  unit_price,
  -- missing discount = no discount; out-of-range discount -> null
  CASE
    WHEN discount_pct_raw IS NULL THEN CAST(0 AS DECIMAL(5,2))
    WHEN discount_pct_raw BETWEEN 0 AND 100 THEN discount_pct_raw
  END                                                                 AS discount_pct,
  city,
  state,
  COALESCE(payment_type_raw, 'Unknown')                               AS payment_type,
  order_status,
  ingestion_date,
  -- enrichment: monetary measures
  CAST(quantity * unit_price AS DECIMAL(14,2))                        AS gross_amount,
  CAST(quantity * unit_price
       * COALESCE(CASE WHEN discount_pct_raw BETWEEN 0 AND 100
                       THEN discount_pct_raw END, 0) / 100
       AS DECIMAL(14,2))                                              AS discount_amount,
  CAST(quantity * unit_price
       * (1 - COALESCE(CASE WHEN discount_pct_raw BETWEEN 0 AND 100
                            THEN discount_pct_raw END, 0) / 100)
       AS DECIMAL(14,2))                                              AS net_amount,
  -- enrichment: shipping & calendar attributes
  CASE WHEN ship_date_raw >= order_date
       THEN datediff(ship_date_raw, order_date) END                   AS days_to_ship,
  year(order_date)                                                    AS order_year,
  month(order_date)                                                   AS order_month,
  _ingested_at,
  _source_file
FROM typed;
