-- Gold dimension: dim_customer
-- Larger dimension (tens of thousands of distinct customer_id values) -> SCD TYPE 1,
-- keeping only the latest known attributes per customer (also dedupes repeat sightings).
-- Out-of-range ages (the bronze/silver data contains negative and >110 values) are
-- nullified here so the dimension only carries business-valid attributes.

CREATE TEMPORARY VIEW customer_cdc_source AS
SELECT
  customer_id,
  CASE WHEN customer_age BETWEEN 0 AND 110 THEN customer_age ELSE NULL END AS customer_age,
  gender,
  city,
  state,
  _ingested_at AS event_timestamp
FROM STREAM cowork_sn46.silver.slv_sales_transactions
WHERE customer_id IS NOT NULL;

CREATE OR REFRESH STREAMING TABLE cowork_sn46.gold.dim_customer
CLUSTER BY (customer_id)
COMMENT 'Customer dimension - SCD Type 1 (larger dimension; current attributes only, deduped by customer_id)';

CREATE FLOW dim_customer_scd1_flow AS
AUTO CDC INTO cowork_sn46.gold.dim_customer
FROM stream(customer_cdc_source)
KEYS (customer_id)
SEQUENCE BY event_timestamp
COLUMNS * EXCEPT (event_timestamp)
STORED AS SCD TYPE 1;
