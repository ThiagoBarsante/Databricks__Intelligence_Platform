-- ============================================================
-- BRONZE LAYER: Raw Data Ingestion with Auto Loader
-- ============================================================
-- Purpose: Ingest raw sales transaction data from CSV files
-- Schema: All columns loaded as STRING for maximum flexibility
-- Features: Schema evolution, metadata tracking, rescued data
-- ============================================================

CREATE OR REFRESH STREAMING TABLE bronze.sales_transactions_raw (
  CONSTRAINT valid_source_file EXPECT (_source_file IS NOT NULL)
)
TBLPROPERTIES (
  'pipelines.autoOptimize.managed' = 'true',
  'delta.enableChangeDataFeed' = 'true'
)
COMMENT 'Raw sales transactions ingested from CSV files with schema evolution and metadata tracking'
AS SELECT
  -- Source data columns (all as STRING for Bronze layer)
  transaction_id,
  order_date,
  ship_date,
  customer_id,
  customer_age,
  gender,
  product_id,
  product_category,
  quantity,
  unit_price,
  discount_pct,
  city,
  state,
  payment_type,
  order_status,
  ingestion_date,
  
  -- Metadata columns for tracking and auditing
  current_timestamp() AS _ingest_timestamp,
  _metadata.file_path AS _source_file,
  
  -- Rescued data column for schema evolution
  _rescued_data
  
FROM STREAM(read_files(
  '/Volumes/genie_dw/dw_raw/raw_data',
  format => 'csv',
  header => true,
  mode => 'PERMISSIVE',
  inferColumnTypes => false,
  schemaEvolutionMode => 'addNewColumns',
  rescuedDataColumn => '_rescued_data'
))
