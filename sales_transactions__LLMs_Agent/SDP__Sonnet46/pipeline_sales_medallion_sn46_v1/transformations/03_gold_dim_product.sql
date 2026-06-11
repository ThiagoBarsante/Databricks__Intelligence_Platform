-- Gold dimension: dim_product
-- Smaller dimension (fewer distinct product_id values than customers) -> SCD TYPE 2,
-- preserving full history of product_category reassignments via __START_AT / __END_AT.

CREATE TEMPORARY VIEW product_cdc_source AS
SELECT
  product_id,
  product_category,
  _ingested_at AS event_timestamp
FROM STREAM cowork_sn46.silver.slv_sales_transactions
WHERE product_id IS NOT NULL;

CREATE OR REFRESH STREAMING TABLE cowork_sn46.gold.dim_product
CLUSTER BY (product_id)
COMMENT 'Product dimension - SCD Type 2 (smaller dimension; tracks category history with __START_AT/__END_AT)';

CREATE FLOW dim_product_scd2_flow AS
AUTO CDC INTO cowork_sn46.gold.dim_product
FROM stream(product_cdc_source)
KEYS (product_id)
SEQUENCE BY event_timestamp
COLUMNS * EXCEPT (event_timestamp)
STORED AS SCD TYPE 2;
