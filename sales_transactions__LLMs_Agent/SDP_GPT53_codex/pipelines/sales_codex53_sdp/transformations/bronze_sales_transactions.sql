CREATE OR REFRESH STREAMING TABLE kiro_catalog.sales_codex53_bronze_dev_20260601.bronze_sales_transactions
COMMENT 'Raw CSV ingestion with schema evolution and source metadata for sales transactions.'
TBLPROPERTIES (
  'quality' = 'bronze'
)
AS
SELECT
  *,
  current_timestamp() AS _ingest_ts,
  _metadata.file_path AS _source_file,
  _metadata.file_modification_time AS _source_file_modification_ts,
  _metadata.file_size AS _source_file_size_bytes
FROM STREAM read_files(
  '/Volumes/kiro_catalog/demo_dw_raw2/raw_vol2/',
  format => 'csv',
  header => 'true',
  inferSchema => 'true',
  schemaEvolutionMode => 'addNewColumns',
  rescuedDataColumn => '_rescued_data'
);
