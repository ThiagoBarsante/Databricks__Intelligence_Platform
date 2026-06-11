-- ============================================================
-- BRONZE LAYER — raw sales transactions
-- All business columns ingested as STRING (no type inference).
-- Schema evolution enabled (Auto Loader addNewColumns + rescued data).
-- Adds ingestion-time and source-file metadata.
-- ============================================================

CREATE OR REFRESH STREAMING TABLE cowork_fable5.bronze_fa5_v1.bronze_sales_transactions
COMMENT 'Raw sales transactions from CSV, all columns as STRING, append-only with ingest/file metadata'
CLUSTER BY (order_date)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true',
  'quality' = 'bronze'
)
AS
SELECT
  *,
  current_timestamp()              AS _ingested_at,
  _metadata.file_path              AS _source_file,
  _metadata.file_modification_time AS _file_modification_time,
  _metadata.file_size              AS _file_size
FROM STREAM read_files(
  '/Volumes/cowork_fable5/dw_raw/raw_data',
  format => 'csv',
  header => true,
  inferColumnTypes => false,
  schemaEvolutionMode => 'addNewColumns'
);
