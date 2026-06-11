-- Bronze: raw sales transactions
-- R2: load ALL columns as STRING, schema evolution enabled, add ingest + source-file metadata.
-- Append-only streaming table via Auto Loader (STREAM read_files).
CREATE OR REFRESH STREAMING TABLE cowork_op48.bronze.bronze_sales_transactions
COMMENT 'Bronze raw sales transactions - all columns STRING, schema evolution, ingest & source-file metadata'
TBLPROPERTIES ('quality' = 'bronze', 'pipelines.reset.allowed' = 'true')
AS
SELECT
  *,
  current_timestamp()                AS _ingested_at,
  _metadata.file_path                AS _source_file,
  _metadata.file_modification_time   AS _source_file_modified_at,
  _metadata.file_size                AS _source_file_size
FROM STREAM read_files(
  '/Volumes/cowork_op48/dw_raw/raw_data',
  format             => 'csv',
  header             => 'true',
  schemaEvolutionMode => 'addNewColumns',
  -- Force every known source column to STRING (Bronze loads all info as STRING)
  schemaHints        => '
    transaction_id STRING, order_date STRING, ship_date STRING,
    customer_id STRING, customer_age STRING, gender STRING,
    product_id STRING, product_category STRING, quantity STRING,
    unit_price STRING, discount_pct STRING, city STRING, state STRING,
    payment_type STRING, order_status STRING, ingestion_date STRING'
);
