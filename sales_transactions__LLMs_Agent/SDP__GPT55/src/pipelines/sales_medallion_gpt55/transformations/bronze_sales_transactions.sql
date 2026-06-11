CREATE OR REFRESH STREAMING TABLE gpt55_codex.sales_bronze.sales_transactions_raw
COMMENT 'Raw sales transactions loaded from CSV with all source fields preserved as strings.'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true',
  'quality' = 'bronze'
)
AS
SELECT
  CAST(transaction_id AS STRING) AS transaction_id,
  CAST(order_date AS STRING) AS order_date,
  CAST(ship_date AS STRING) AS ship_date,
  CAST(customer_id AS STRING) AS customer_id,
  CAST(customer_age AS STRING) AS customer_age,
  CAST(gender AS STRING) AS gender,
  CAST(product_id AS STRING) AS product_id,
  CAST(product_category AS STRING) AS product_category,
  CAST(quantity AS STRING) AS quantity,
  CAST(unit_price AS STRING) AS unit_price,
  CAST(discount_pct AS STRING) AS discount_pct,
  CAST(city AS STRING) AS city,
  CAST(state AS STRING) AS state,
  CAST(payment_type AS STRING) AS payment_type,
  CAST(order_status AS STRING) AS order_status,
  CAST(ingestion_date AS STRING) AS ingestion_date,
  current_timestamp() AS _ingested_at,
  _metadata.file_path AS _source_file,
  _metadata.file_modification_time AS _source_file_modification_time,
  _metadata.file_size AS _source_file_size
FROM STREAM read_files(
  '${source_path}',
  format => 'csv',
  header => true,
  inferColumnTypes => false,
  schemaEvolutionMode => 'addNewColumns',
  schemaHints => 'transaction_id STRING, order_date STRING, ship_date STRING, customer_id STRING, customer_age STRING, gender STRING, product_id STRING, product_category STRING, quantity STRING, unit_price STRING, discount_pct STRING, city STRING, state STRING, payment_type STRING, order_status STRING, ingestion_date STRING',
  mode => 'PERMISSIVE'
);
