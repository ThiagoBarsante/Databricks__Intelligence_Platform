CREATE TEMPORARY VIEW customer_dim_source AS
SELECT
  customer_id,
  customer_age,
  customer_age_band,
  gender_normalized,
  city,
  state,
  location_key,
  source_sequence_at
FROM STREAM ${catalog_name}.${silver_schema}.silver_sales_transactions_clean;

CREATE OR REFRESH STREAMING TABLE ${catalog_name}.${gold_schema}.dim_customer_current;

CREATE FLOW dim_customer_current_scd1_flow AS
AUTO CDC INTO ${catalog_name}.${gold_schema}.dim_customer_current
FROM stream(customer_dim_source)
KEYS (customer_id)
SEQUENCE BY source_sequence_at
COLUMNS * EXCEPT (source_sequence_at)
STORED AS SCD TYPE 1;
