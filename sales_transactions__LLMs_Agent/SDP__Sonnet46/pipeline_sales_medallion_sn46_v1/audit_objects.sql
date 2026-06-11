-- Audit: row counts for all gold/silver/bronze objects created by pipeline_sales_medallion_sn46_v1
-- Streaming tables -> table_name; materialized views -> view_name. Internal SDP objects
-- (__materialization_*, event_log_*) are excluded as implementation details, not deliverables.

SELECT 'brz_sales_transactions' AS table_name, CAST(NULL AS STRING) AS view_name, COUNT(*) AS records FROM cowork_sn46.bronze.brz_sales_transactions
UNION ALL
SELECT 'slv_sales_transactions', NULL, COUNT(*) FROM cowork_sn46.silver.slv_sales_transactions
UNION ALL
SELECT 'dim_product', NULL, COUNT(*) FROM cowork_sn46.gold.dim_product
UNION ALL
SELECT 'dim_customer', NULL, COUNT(*) FROM cowork_sn46.gold.dim_customer
UNION ALL
SELECT 'fact_sales_transactions', NULL, COUNT(*) FROM cowork_sn46.gold.fact_sales_transactions
UNION ALL
SELECT NULL, 'agg_category_monthly_metrics', COUNT(*) FROM cowork_sn46.gold.agg_category_monthly_metrics
UNION ALL
SELECT NULL, 'agg_customer_segment_metrics', COUNT(*) FROM cowork_sn46.gold.agg_customer_segment_metrics
ORDER BY table_name NULLS LAST, view_name NULLS LAST
