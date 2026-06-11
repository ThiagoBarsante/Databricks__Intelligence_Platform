-- Gold fact: fact_sales_transactions
-- Largest table (one row per transaction) -> SCD TYPE 1.
-- AUTO CDC merging on transaction_id both keeps the latest version of each transaction AND
-- performs the deduplication that was deliberately deferred from the silver layer
-- (silver showed 100,000 rows but only ~42,749 distinct transaction_id values).

CREATE TEMPORARY VIEW fact_sales_cdc_source AS
SELECT
  transaction_id,
  customer_id,
  product_id,
  order_date,
  ship_date,
  quantity,
  unit_price,
  discount_pct,
  net_amount,
  payment_type,
  order_status,
  city,
  state,
  is_valid_amount,
  is_valid_age,
  _ingested_at AS event_timestamp
FROM STREAM cowork_sn46.silver.slv_sales_transactions
WHERE transaction_id IS NOT NULL;

CREATE OR REFRESH STREAMING TABLE cowork_sn46.gold.fact_sales_transactions
CLUSTER BY (order_date)
COMMENT 'Sales fact table - grain: one row per transaction_id. SCD Type 1 merge dedupes transaction_id (deferred from silver) and keeps FKs to dim_product / dim_customer plus measures for star-schema rollups.';

CREATE FLOW fact_sales_scd1_flow AS
AUTO CDC INTO cowork_sn46.gold.fact_sales_transactions
FROM stream(fact_sales_cdc_source)
KEYS (transaction_id)
SEQUENCE BY event_timestamp
COLUMNS * EXCEPT (event_timestamp)
STORED AS SCD TYPE 1;
