CREATE TEMPORARY VIEW product_dim_source AS
SELECT
  product_id,
  product_category,
  unit_price AS last_known_unit_price,
  discount_pct AS last_known_discount_pct,
  source_sequence_at
FROM STREAM ${catalog_name}.${silver_schema}.silver_sales_transactions_clean;

CREATE OR REFRESH STREAMING TABLE ${catalog_name}.${gold_schema}.dim_product_current;

CREATE FLOW dim_product_current_scd1_flow AS
AUTO CDC INTO ${catalog_name}.${gold_schema}.dim_product_current
FROM stream(product_dim_source)
KEYS (product_id)
SEQUENCE BY source_sequence_at
COLUMNS * EXCEPT (source_sequence_at)
STORED AS SCD TYPE 1;
