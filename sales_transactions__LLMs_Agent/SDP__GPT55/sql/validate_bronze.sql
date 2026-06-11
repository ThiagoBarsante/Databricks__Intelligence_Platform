SELECT
  COUNT(*) AS row_count,
  COUNT(DISTINCT _source_file) AS source_file_count,
  MIN(_ingested_at) AS first_ingested_at,
  MAX(_ingested_at) AS last_ingested_at
FROM gpt55_codex.sales_bronze.sales_transactions_raw;

SELECT *
FROM gpt55_codex.sales_bronze.sales_transactions_raw
LIMIT 10;
