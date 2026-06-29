CREATE TEMPORARY VIEW payment_type_dim_source AS
SELECT
  payment_type AS payment_type_code,
  INITCAP(LOWER(payment_type)) AS payment_type_name,
  source_sequence_at
FROM STREAM ${catalog_name}.${silver_schema}.silver_sales_transactions_clean;

CREATE OR REFRESH STREAMING TABLE ${catalog_name}.${gold_schema}.dim_payment_type_history;

CREATE FLOW dim_payment_type_history_scd2_flow AS
AUTO CDC INTO ${catalog_name}.${gold_schema}.dim_payment_type_history
FROM stream(payment_type_dim_source)
KEYS (payment_type_code)
SEQUENCE BY source_sequence_at
COLUMNS * EXCEPT (source_sequence_at)
STORED AS SCD TYPE 2
TRACK HISTORY ON payment_type_name;
