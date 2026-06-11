-- Bronze layer: raw ingestion of sales_transactions.csv
-- - Auto Loader (STREAM read_files) with schema evolution
-- - Every business column forced to STRING via schemaHints (bronze must load all info as STRING)
-- - Adds ingestion metadata: _ingested_at, _source_file

CREATE OR REFRESH STREAMING TABLE cowork_sn46.bronze.brz_sales_transactions
CLUSTER BY (ingestion_date)
AS
SELECT
  *,
  current_timestamp() AS _ingested_at,
  _metadata.file_path AS _source_file
FROM STREAM read_files(
  '/Volumes/cowork_sn46/dw_raw/raw_data/',
  format => 'csv',
  header => true,
  schemaHints => '
    transaction_id   STRING,
    order_date       STRING,
    ship_date        STRING,
    customer_id      STRING,
    customer_age     STRING,
    gender           STRING,
    product_id       STRING,
    product_category STRING,
    quantity         STRING,
    unit_price       STRING,
    discount_pct     STRING,
    city             STRING,
    state            STRING,
    payment_type     STRING,
    order_status     STRING,
    ingestion_date   STRING
  ',
  schemaEvolutionMode => 'addNewColumns'
);
