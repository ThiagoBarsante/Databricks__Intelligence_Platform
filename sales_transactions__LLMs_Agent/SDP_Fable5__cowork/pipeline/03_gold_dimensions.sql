-- ============================================================
-- GOLD LAYER — star schema dimensions
-- Large dimensions  -> SCD TYPE 1 (current state only)
-- Small dimensions  -> SCD TYPE 2 (full history, __START_AT/__END_AT)
-- AUTO CDC deduplicates by KEYS; SEQUENCE uses a composite struct
-- (order_date, transaction_id, row uid) to avoid ordering ties.
-- ============================================================

-- ─────────────────────────────────────────────────────────────
-- dim_customer — LARGE (~79K keys) -> SCD TYPE 1
-- ─────────────────────────────────────────────────────────────
CREATE TEMPORARY VIEW dim_customer_src AS
SELECT
  customer_id,
  customer_age,
  gender,
  city,
  state,
  order_date     AS last_order_date,
  transaction_id AS _seq_txn,
  uuid()         AS _seq_uid
FROM STREAM cowork_fable5.silver_fa5_v1.silver_sales_transactions;

CREATE OR REFRESH STREAMING TABLE cowork_fable5.gold_fa5_v1.dim_customer
COMMENT 'Customer dimension - SCD Type 1 (large table, current state only)'
TBLPROPERTIES ('quality' = 'gold');

CREATE FLOW dim_customer_scd1 AS
AUTO CDC INTO cowork_fable5.gold_fa5_v1.dim_customer
FROM stream(dim_customer_src)
KEYS (customer_id)
SEQUENCE BY struct(last_order_date, _seq_txn, _seq_uid)
COLUMNS * EXCEPT (_seq_txn, _seq_uid)
STORED AS SCD TYPE 1;

-- ─────────────────────────────────────────────────────────────
-- dim_product — LARGE (~42K keys) -> SCD TYPE 1
-- ─────────────────────────────────────────────────────────────
CREATE TEMPORARY VIEW dim_product_src AS
SELECT
  product_id,
  product_category,
  unit_price     AS latest_unit_price,
  order_date     AS last_sold_date,
  transaction_id AS _seq_txn,
  uuid()         AS _seq_uid
FROM STREAM cowork_fable5.silver_fa5_v1.silver_sales_transactions;

CREATE OR REFRESH STREAMING TABLE cowork_fable5.gold_fa5_v1.dim_product
COMMENT 'Product dimension - SCD Type 1 (large table, current state only)'
TBLPROPERTIES ('quality' = 'gold');

CREATE FLOW dim_product_scd1 AS
AUTO CDC INTO cowork_fable5.gold_fa5_v1.dim_product
FROM stream(dim_product_src)
KEYS (product_id)
SEQUENCE BY struct(last_sold_date, _seq_txn, _seq_uid)
COLUMNS * EXCEPT (_seq_txn, _seq_uid)
STORED AS SCD TYPE 1;

-- ─────────────────────────────────────────────────────────────
-- dim_location — SMALL (~10 keys) -> SCD TYPE 2
-- ─────────────────────────────────────────────────────────────
CREATE TEMPORARY VIEW dim_location_src AS
SELECT
  city,
  state,
  CASE
    WHEN state IN ('CA', 'AZ') THEN 'West'
    WHEN state = 'TX'          THEN 'South'
    WHEN state = 'IL'          THEN 'Midwest'
    WHEN state IN ('NY', 'PA') THEN 'Northeast'
    ELSE 'Other'
  END            AS region,
  order_date     AS _seq_date,
  transaction_id AS _seq_txn,
  uuid()         AS _seq_uid
FROM STREAM cowork_fable5.silver_fa5_v1.silver_sales_transactions;

CREATE OR REFRESH STREAMING TABLE cowork_fable5.gold_fa5_v1.dim_location
COMMENT 'Location dimension - SCD Type 2 (small table, history tracked via __START_AT/__END_AT)'
TBLPROPERTIES ('quality' = 'gold');

CREATE FLOW dim_location_scd2 AS
AUTO CDC INTO cowork_fable5.gold_fa5_v1.dim_location
FROM stream(dim_location_src)
KEYS (city, state)
SEQUENCE BY struct(_seq_date, _seq_txn, _seq_uid)
COLUMNS * EXCEPT (_seq_date, _seq_txn, _seq_uid)
STORED AS SCD TYPE 2;

-- ─────────────────────────────────────────────────────────────
-- dim_payment_type — SMALL (~5 keys) -> SCD TYPE 2
-- ─────────────────────────────────────────────────────────────
CREATE TEMPORARY VIEW dim_payment_type_src AS
SELECT
  payment_type,
  CASE
    WHEN payment_type IN ('Card', 'UPI', 'Crypto') THEN 'Digital'
    WHEN payment_type = 'COD'                      THEN 'Cash on Delivery'
    ELSE 'Unknown'
  END            AS payment_group,
  order_date     AS _seq_date,
  transaction_id AS _seq_txn,
  uuid()         AS _seq_uid
FROM STREAM cowork_fable5.silver_fa5_v1.silver_sales_transactions;

CREATE OR REFRESH STREAMING TABLE cowork_fable5.gold_fa5_v1.dim_payment_type
COMMENT 'Payment type dimension - SCD Type 2 (small table, history tracked via __START_AT/__END_AT)'
TBLPROPERTIES ('quality' = 'gold');

CREATE FLOW dim_payment_type_scd2 AS
AUTO CDC INTO cowork_fable5.gold_fa5_v1.dim_payment_type
FROM stream(dim_payment_type_src)
KEYS (payment_type)
SEQUENCE BY struct(_seq_date, _seq_txn, _seq_uid)
COLUMNS * EXCEPT (_seq_date, _seq_txn, _seq_uid)
STORED AS SCD TYPE 2;
