CREATE OR REFRESH MATERIALIZED VIEW kiro_catalog.sales_codex53_gold_dev_20260601.gold_dim_product_scd2
COMMENT 'Product dimension with SCD Type 2 style effective dating.'
TBLPROPERTIES (
  'quality' = 'gold',
  'scd_type' = '2'
)
AS
WITH base AS (
  SELECT
    product_id,
    product_category,
    CAST(unit_price AS DECIMAL(18, 4)) AS unit_price,
    transaction_id,
    CAST(_silver_processed_ts AS TIMESTAMP) AS event_ts,
    SHA2(
      CONCAT_WS(
        '||',
        COALESCE(product_category, 'NULL'),
        COALESCE(CAST(CAST(unit_price AS DECIMAL(18, 4)) AS STRING), 'NULL')
      ),
      256
    ) AS attribute_hash
  FROM kiro_catalog.sales_codex53_silver_dev_20260601.silver_sales_transactions
  WHERE product_id IS NOT NULL
),
changes AS (
  SELECT
    *,
    LAG(attribute_hash) OVER (PARTITION BY product_id ORDER BY event_ts, transaction_id) AS prev_hash
  FROM base
),
versioned AS (
  SELECT
    product_id,
    product_category,
    unit_price,
    event_ts AS effective_start_ts,
    LEAD(event_ts) OVER (PARTITION BY product_id ORDER BY event_ts, transaction_id) AS effective_end_ts
  FROM changes
  WHERE prev_hash IS NULL OR prev_hash <> attribute_hash
)
SELECT
  product_id,
  product_category,
  unit_price,
  effective_start_ts,
  effective_end_ts,
  CASE WHEN effective_end_ts IS NULL THEN true ELSE false END AS is_current
FROM versioned;
