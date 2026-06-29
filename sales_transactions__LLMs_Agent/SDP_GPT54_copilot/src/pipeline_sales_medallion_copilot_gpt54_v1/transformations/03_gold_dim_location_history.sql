CREATE TEMPORARY VIEW location_dim_source AS
SELECT
  location_key,
  city,
  state,
  source_sequence_at
FROM STREAM ${catalog_name}.${silver_schema}.silver_sales_transactions_clean;

CREATE OR REFRESH STREAMING TABLE ${catalog_name}.${gold_schema}.dim_location_history;

CREATE FLOW dim_location_history_scd2_flow AS
AUTO CDC INTO ${catalog_name}.${gold_schema}.dim_location_history
FROM stream(location_dim_source)
KEYS (location_key)
SEQUENCE BY source_sequence_at
COLUMNS * EXCEPT (source_sequence_at)
STORED AS SCD TYPE 2
TRACK HISTORY ON city, state;
