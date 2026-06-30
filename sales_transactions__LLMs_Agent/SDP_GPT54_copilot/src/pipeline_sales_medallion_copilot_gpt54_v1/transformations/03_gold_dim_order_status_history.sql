CREATE TEMPORARY VIEW order_status_dim_source AS
SELECT
  order_status AS order_status_code,
  INITCAP(LOWER(order_status)) AS order_status_name,
  source_sequence_at
FROM STREAM ${catalog_name}.${silver_schema}.silver_sales_transactions_clean;

CREATE OR REFRESH STREAMING TABLE ${catalog_name}.${gold_schema}.dim_order_status_history;

CREATE FLOW dim_order_status_history_scd2_flow AS
AUTO CDC INTO ${catalog_name}.${gold_schema}.dim_order_status_history
FROM stream(order_status_dim_source)
KEYS (order_status_code)
SEQUENCE BY source_sequence_at
COLUMNS * EXCEPT (source_sequence_at)
STORED AS SCD TYPE 2
TRACK HISTORY ON order_status_name;
