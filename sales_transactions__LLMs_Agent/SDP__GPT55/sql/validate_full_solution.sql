SELECT 'bronze.sales_transactions_raw' AS table_name, COUNT(*) AS row_count
FROM gpt55_codex.sales_bronze.sales_transactions_raw
UNION ALL
SELECT 'silver.sales_transactions_clean', COUNT(*)
FROM gpt55_codex.sales_silver.sales_transactions_clean
UNION ALL
SELECT 'gold.fact_sales', COUNT(*)
FROM gpt55_codex.sales_gold.fact_sales
UNION ALL
SELECT 'gold.dim_customer', COUNT(*)
FROM gpt55_codex.sales_gold.dim_customer
WHERE __END_AT IS NULL
UNION ALL
SELECT 'gold.dim_product', COUNT(*)
FROM gpt55_codex.sales_gold.dim_product
WHERE __END_AT IS NULL
UNION ALL
SELECT 'gold.dim_location', COUNT(*)
FROM gpt55_codex.sales_gold.dim_location
WHERE __END_AT IS NULL
UNION ALL
SELECT 'gold.mv_daily_sales_metrics', COUNT(*)
FROM gpt55_codex.sales_gold.mv_daily_sales_metrics
UNION ALL
SELECT 'gold.mv_category_state_metrics', COUNT(*)
FROM gpt55_codex.sales_gold.mv_category_state_metrics;
