CREATE OR REFRESH MATERIALIZED VIEW kiro_catalog.sales_codex53_gold_dev_20260601.gold_dim_customer_scd2
COMMENT 'Customer dimension with SCD Type 2 style effective dating.'
TBLPROPERTIES (
  'quality' = 'gold',
  'scd_type' = '2'
)
AS
WITH base AS (
  SELECT
    customer_id,
    customer_age,
    gender,
    city,
    state,
    payment_type,
    transaction_id,
    CAST(_silver_processed_ts AS TIMESTAMP) AS event_ts,
    SHA2(
      CONCAT_WS(
        '||',
        COALESCE(CAST(customer_age AS STRING), 'NULL'),
        COALESCE(gender, 'NULL'),
        COALESCE(city, 'NULL'),
        COALESCE(state, 'NULL'),
        COALESCE(payment_type, 'NULL')
      ),
      256
    ) AS attribute_hash
  FROM kiro_catalog.sales_codex53_silver_dev_20260601.silver_sales_transactions
  WHERE customer_id IS NOT NULL
),
changes AS (
  SELECT
    *,
    LAG(attribute_hash) OVER (PARTITION BY customer_id ORDER BY event_ts, transaction_id) AS prev_hash
  FROM base
),
versioned AS (
  SELECT
    customer_id,
    customer_age,
    gender,
    city,
    state,
    payment_type,
    event_ts AS effective_start_ts,
    LEAD(event_ts) OVER (PARTITION BY customer_id ORDER BY event_ts, transaction_id) AS effective_end_ts
  FROM changes
  WHERE prev_hash IS NULL OR prev_hash <> attribute_hash
)
SELECT
  customer_id,
  customer_age,
  gender,
  city,
  state,
  payment_type,
  effective_start_ts,
  effective_end_ts,
  CASE WHEN effective_end_ts IS NULL THEN true ELSE false END AS is_current
FROM versioned;
