-- audit.sql
-- Full-project audit for pipeline_sales_medallion_op48_v1.
-- Lists every medallion object (bronze -> silver -> gold) with its layer, object type and row count.
-- Run on a SQL warehouse, e.g.:
--   databricks sql query (via UI) / DBSQL editor, or
--   from the deploy script's validate step.
SELECT layer, object_type, object_name, row_count
FROM (
  SELECT 'bronze' AS layer, 'streaming_table' AS object_type,
         'cowork_op48.bronze.bronze_sales_transactions' AS object_name,
         COUNT(*) AS row_count
  FROM cowork_op48.bronze.bronze_sales_transactions

  UNION ALL
  SELECT 'silver', 'streaming_table',
         'cowork_op48.silver.silver_sales_transactions', COUNT(*)
  FROM cowork_op48.silver.silver_sales_transactions

  UNION ALL
  SELECT 'gold', 'dimension_scd1', 'cowork_op48.gold.dim_customer', COUNT(*)
  FROM cowork_op48.gold.dim_customer

  UNION ALL
  SELECT 'gold', 'dimension_scd1', 'cowork_op48.gold.dim_product', COUNT(*)
  FROM cowork_op48.gold.dim_product

  UNION ALL
  SELECT 'gold', 'dimension_scd2', 'cowork_op48.gold.dim_location', COUNT(*)
  FROM cowork_op48.gold.dim_location

  UNION ALL
  SELECT 'gold', 'dimension_scd2', 'cowork_op48.gold.dim_category', COUNT(*)
  FROM cowork_op48.gold.dim_category

  UNION ALL
  SELECT 'gold', 'fact', 'cowork_op48.gold.fact_sales', COUNT(*)
  FROM cowork_op48.gold.fact_sales

  UNION ALL
  SELECT 'gold', 'aggregate', 'cowork_op48.gold.agg_sales_by_category_month', COUNT(*)
  FROM cowork_op48.gold.agg_sales_by_category_month

  UNION ALL
  SELECT 'gold', 'aggregate', 'cowork_op48.gold.agg_sales_by_state_status', COUNT(*)
  FROM cowork_op48.gold.agg_sales_by_state_status
) audit
ORDER BY CASE layer WHEN 'bronze' THEN 1 WHEN 'silver' THEN 2 ELSE 3 END, object_name;
