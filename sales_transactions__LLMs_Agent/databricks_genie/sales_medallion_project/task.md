# Sales Medallion Architecture - Implementation Tasks

## Project Information
**Pipeline Name:** pipeline_sales_medallion_genie_v1  
**Implementation Date:** June 25, 2026  
**Assigned To:** Data Engineering Team  

---

## Pre-Implementation Checklist

- [ ] Verify access to volume: `/Volumes/genie_dw/dw_raw/raw_data`
- [ ] Confirm catalog name for implementation
- [ ] Verify serverless compute availability
- [ ] Review requirements and design documents
- [ ] Set up development environment

---

## Phase 1: Bronze Layer Implementation

### Task 1.1: Create Bronze Schema
**Priority:** High  
**Estimated Time:** 5 minutes

**Steps:**
1. Create Unity Catalog schema for Bronze layer
```sql
CREATE SCHEMA IF NOT EXISTS <CATALOG_NAME>.bronze
COMMENT 'Bronze layer: Raw ingestion of sales transaction data';
```

**Acceptance Criteria:**
- [ ] Schema created successfully
- [ ] Schema visible in Catalog Explorer

---

### Task 1.2: Create Bronze Table (sales_transactions_raw)
**Priority:** High  
**Estimated Time:** 15 minutes

**Steps:**
1. Add notebook/SQL file to pipeline
2. Implement streaming table with Auto Loader
3. Configure schema evolution
4. Add metadata columns

**SQL Code:**
```sql
CREATE OR REFRESH STREAMING TABLE bronze.sales_transactions_raw
(
  transaction_id STRING,
  order_date STRING,
  ship_date STRING,
  customer_id STRING,
  customer_age STRING,
  gender STRING,
  product_id STRING,
  product_category STRING,
  quantity STRING,
  unit_price STRING,
  discount_pct STRING,
  city STRING,
  state STRING,
  payment_type STRING,
  order_status STRING,
  ingestion_date STRING,
  _ingest_timestamp TIMESTAMP GENERATED ALWAYS AS (current_timestamp()),
  _source_file STRING,
  _rescued_data STRING
)
TBLPROPERTIES (
  'pipelines.autoOptimize.managed' = 'true'
)
COMMENT 'Bronze layer: Raw sales transaction data from CSV files'
AS SELECT 
  *,
  _metadata.file_path as _source_file,
  _rescued_data
FROM STREAM read_files(
  '/Volumes/genie_dw/dw_raw/raw_data',
  format => 'csv',
  header => true,
  mode => 'PERMISSIVE',
  rescuedDataColumn => '_rescued_data'
);
```

**Acceptance Criteria:**
- [ ] Table created with all STRING columns
- [ ] Metadata columns populated correctly
- [ ] Auto Loader configured
- [ ] Schema evolution enabled
- [ ] Data loaded from volume

---

### Task 1.3: Deploy and Test Bronze Layer
**Priority:** High  
**Estimated Time:** 10 minutes

**Steps:**
1. Start pipeline
2. Monitor pipeline execution
3. Validate data load

**Validation Queries:**
```sql
-- Check record count
SELECT COUNT(*) as total_records 
FROM <CATALOG_NAME>.bronze.sales_transactions_raw;

-- Check metadata columns
SELECT 
  COUNT(DISTINCT _source_file) as file_count,
  MIN(_ingest_timestamp) as first_ingest,
  MAX(_ingest_timestamp) as last_ingest
FROM <CATALOG_NAME>.bronze.sales_transactions_raw;

-- Check for rescued data
SELECT COUNT(*) as rescued_count
FROM <CATALOG_NAME>.bronze.sales_transactions_raw
WHERE _rescued_data IS NOT NULL;

-- Sample data
SELECT * FROM <CATALOG_NAME>.bronze.sales_transactions_raw LIMIT 10;
```

**Acceptance Criteria:**
- [ ] Pipeline runs successfully
- [ ] All CSV records loaded
- [ ] Metadata columns have values
- [ ] No pipeline errors
- [ ] **STOP and validate before proceeding to Silver**

---

## Phase 2: Silver Layer Implementation

### Task 2.1: Create Silver Schema
**Priority:** High  
**Estimated Time:** 5 minutes

**Steps:**
1. Create Unity Catalog schema for Silver layer
```sql
CREATE SCHEMA IF NOT EXISTS <CATALOG_NAME>.silver
COMMENT 'Silver layer: Cleansed and typed sales transaction data';
```

**Acceptance Criteria:**
- [ ] Schema created successfully

---

### Task 2.2: Create Silver Table (sales_transactions_clean)
**Priority:** High  
**Estimated Time:** 30 minutes

**Steps:**
1. Add SQL file to pipeline
2. Implement data quality filters
3. Implement type casting
4. Add derived fields
5. Implement gender normalization

**SQL Code:**
```sql
CREATE OR REFRESH STREAMING TABLE silver.sales_transactions_clean
(
  transaction_id BIGINT,
  order_date DATE,
  ship_date DATE,
  customer_id STRING,
  customer_age INT,
  gender STRING,
  product_id STRING,
  product_category STRING,
  quantity INT,
  unit_price DECIMAL(10,2),
  discount_pct DECIMAL(5,2),
  city STRING,
  state STRING,
  payment_type STRING,
  order_status STRING,
  gross_amount DECIMAL(12,2),
  discount_amount DECIMAL(12,2),
  net_amount DECIMAL(12,2),
  days_to_ship INT,
  _ingest_timestamp TIMESTAMP,
  _source_file STRING,
  _silver_timestamp TIMESTAMP GENERATED ALWAYS AS (current_timestamp())
)
COMMENT 'Silver layer: Cleansed and typed sales transactions'
AS SELECT 
  CAST(transaction_id AS BIGINT) as transaction_id,
  TO_DATE(order_date, 'yyyy-MM-dd') as order_date,
  TO_DATE(ship_date, 'yyyy-MM-dd') as ship_date,
  customer_id,
  CAST(customer_age AS INT) as customer_age,
  CASE 
    WHEN UPPER(gender) IN ('M', 'MALE') THEN 'Male'
    WHEN UPPER(gender) IN ('F', 'FEMALE') THEN 'Female'
    ELSE 'Unknown'
  END as gender,
  product_id,
  product_category,
  CAST(quantity AS INT) as quantity,
  CAST(unit_price AS DECIMAL(10,2)) as unit_price,
  CAST(discount_pct AS DECIMAL(5,2)) as discount_pct,
  city,
  state,
  COALESCE(payment_type, 'Unknown') as payment_type,
  order_status,
  CAST(quantity AS INT) * CAST(unit_price AS DECIMAL(10,2)) as gross_amount,
  CAST(quantity AS INT) * CAST(unit_price AS DECIMAL(10,2)) * CAST(discount_pct AS DECIMAL(5,2)) / 100 as discount_amount,
  (CAST(quantity AS INT) * CAST(unit_price AS DECIMAL(10,2))) - 
    (CAST(quantity AS INT) * CAST(unit_price AS DECIMAL(10,2)) * CAST(discount_pct AS DECIMAL(5,2)) / 100) as net_amount,
  DATEDIFF(TO_DATE(ship_date, 'yyyy-MM-dd'), TO_DATE(order_date, 'yyyy-MM-dd')) as days_to_ship,
  _ingest_timestamp,
  _source_file
FROM STREAM(bronze.sales_transactions_raw)
WHERE 
  transaction_id IS NOT NULL
  AND customer_id IS NOT NULL
  AND product_id IS NOT NULL
  AND TRY_TO_DATE(order_date, 'yyyy-MM-dd') IS NOT NULL
  AND TRY_TO_DATE(ship_date, 'yyyy-MM-dd') IS NOT NULL
  AND TRY_CAST(quantity AS INT) >= 0
  AND TRY_CAST(unit_price AS DECIMAL(10,2)) > 0
  AND TRY_CAST(customer_age AS INT) BETWEEN 0 AND 120;
```

**Acceptance Criteria:**
- [ ] Table created with correct data types
- [ ] Data quality filters applied
- [ ] Derived fields calculated
- [ ] Gender normalized
- [ ] No deduplication performed

---

### Task 2.3: Deploy and Test Silver Layer
**Priority:** High  
**Estimated Time:** 15 minutes

**Validation Queries:**
```sql
-- Check record count and rejection rate
SELECT 
  (SELECT COUNT(*) FROM bronze.sales_transactions_raw) as bronze_count,
  (SELECT COUNT(*) FROM silver.sales_transactions_clean) as silver_count,
  ROUND(100.0 * (SELECT COUNT(*) FROM silver.sales_transactions_clean) / 
        (SELECT COUNT(*) FROM bronze.sales_transactions_raw), 2) as pass_rate_pct;

-- Validate data types
DESCRIBE TABLE silver.sales_transactions_clean;

-- Check derived fields
SELECT 
  transaction_id,
  quantity,
  unit_price,
  discount_pct,
  gross_amount,
  discount_amount,
  net_amount
FROM silver.sales_transactions_clean
LIMIT 10;

-- Validate gender normalization
SELECT gender, COUNT(*) as count
FROM silver.sales_transactions_clean
GROUP BY gender;

-- Check for nulls in critical fields
SELECT 
  SUM(CASE WHEN transaction_id IS NULL THEN 1 ELSE 0 END) as null_transaction_id,
  SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) as null_customer_id,
  SUM(CASE WHEN product_id IS NULL THEN 1 ELSE 0 END) as null_product_id
FROM silver.sales_transactions_clean;
```

**Acceptance Criteria:**
- [ ] Pipeline runs successfully
- [ ] Data quality filters working
- [ ] Derived fields calculated correctly
- [ ] Gender values normalized (Male/Female/Unknown only)
- [ ] Pass rate documented

---

## Phase 3: Gold Layer Implementation

### Task 3.1: Create Gold Schema
**Priority:** High  
**Estimated Time:** 5 minutes

**Steps:**
```sql
CREATE SCHEMA IF NOT EXISTS <CATALOG_NAME>.gold
COMMENT 'Gold layer: Star schema with fact and dimension tables';
```

---

### Task 3.2: Create Dimension Tables

#### Task 3.2.1: dim_dates (SCD Type 1)
**Priority:** High  
**Estimated Time:** 15 minutes

**SQL Code:**
```sql
CREATE OR REFRESH STREAMING TABLE gold.dim_dates
AS SELECT DISTINCT
  CAST(DATE_FORMAT(order_date, 'yyyyMMdd') AS INT) as date_key,
  order_date as date_value,
  YEAR(order_date) as year,
  QUARTER(order_date) as quarter,
  MONTH(order_date) as month,
  DAY(order_date) as day,
  DAYOFWEEK(order_date) as day_of_week,
  DATE_FORMAT(order_date, 'EEEE') as day_name,
  DATE_FORMAT(order_date, 'MMMM') as month_name,
  CASE WHEN DAYOFWEEK(order_date) IN (1, 7) THEN true ELSE false END as is_weekend,
  current_timestamp() as _created_at
FROM STREAM(silver.sales_transactions_clean)
UNION
SELECT DISTINCT
  CAST(DATE_FORMAT(ship_date, 'yyyyMMdd') AS INT) as date_key,
  ship_date as date_value,
  YEAR(ship_date) as year,
  QUARTER(ship_date) as quarter,
  MONTH(ship_date) as month,
  DAY(ship_date) as day,
  DAYOFWEEK(ship_date) as day_of_week,
  DATE_FORMAT(ship_date, 'EEEE') as day_name,
  DATE_FORMAT(ship_date, 'MMMM') as month_name,
  CASE WHEN DAYOFWEEK(ship_date) IN (1, 7) THEN true ELSE false END as is_weekend,
  current_timestamp() as _created_at
FROM STREAM(silver.sales_transactions_clean);
```

---

#### Task 3.2.2: dim_locations (SCD Type 1)
**Priority:** High  
**Estimated Time:** 10 minutes

**SQL Code:**
```sql
CREATE OR REFRESH STREAMING TABLE gold.dim_locations
AS SELECT DISTINCT
  ROW_NUMBER() OVER (ORDER BY state, city) as location_key,
  city,
  state,
  current_timestamp() as _created_at
FROM STREAM(silver.sales_transactions_clean)
WHERE city IS NOT NULL AND state IS NOT NULL;
```

---

#### Task 3.2.3: dim_customers (SCD Type 2)
**Priority:** High  
**Estimated Time:** 30 minutes

**SQL Code:**
```sql
-- Initial load using APPLY CHANGES INTO for SCD Type 2
CREATE OR REFRESH STREAMING TABLE gold.dim_customers;

APPLY CHANGES INTO
  LIVE.gold.dim_customers
FROM
  STREAM(silver.sales_transactions_clean)
KEYS
  (customer_id)
SEQUENCE BY
  order_date
COLUMNS * EXCEPT (transaction_id, order_date, ship_date, product_id, product_category,
                  quantity, unit_price, discount_pct, payment_type, order_status,
                  gross_amount, discount_amount, net_amount, days_to_ship,
                  _ingest_timestamp, _source_file, _silver_timestamp)
STORED AS
  SCD TYPE 2;
```

---

#### Task 3.2.4: dim_products (SCD Type 2)
**Priority:** High  
**Estimated Time:** 30 minutes

**SQL Code:**
```sql
CREATE OR REFRESH STREAMING TABLE gold.dim_products;

APPLY CHANGES INTO
  LIVE.gold.dim_products
FROM
  STREAM(silver.sales_transactions_clean)
KEYS
  (product_id)
SEQUENCE BY
  order_date
COLUMNS
  product_id,
  product_category
STORED AS
  SCD TYPE 2;
```

---

### Task 3.3: Create Fact Table

#### Task 3.3.1: fact_sales (SCD Type 1)
**Priority:** High  
**Estimated Time:** 30 minutes

**SQL Code:**
```sql
CREATE OR REFRESH STREAMING TABLE gold.fact_sales
AS SELECT
  s.transaction_id,
  c.customer_key,
  p.product_key,
  CAST(DATE_FORMAT(s.order_date, 'yyyyMMdd') AS INT) as order_date_key,
  CAST(DATE_FORMAT(s.ship_date, 'yyyyMMdd') AS INT) as ship_date_key,
  l.location_key,
  s.quantity,
  s.unit_price,
  s.discount_pct,
  s.gross_amount,
  s.discount_amount,
  s.net_amount,
  s.days_to_ship,
  s.payment_type,
  s.order_status,
  s._ingest_timestamp,
  current_timestamp() as _created_at
FROM STREAM(silver.sales_transactions_clean) s
LEFT JOIN STREAM(gold.dim_customers) c 
  ON s.customer_id = c.customer_id 
  AND c.__END_AT IS NULL
LEFT JOIN STREAM(gold.dim_products) p 
  ON s.product_id = p.product_id 
  AND p.__END_AT IS NULL
LEFT JOIN STREAM(gold.dim_locations) l 
  ON s.city = l.city AND s.state = l.state;
```

---

### Task 3.4: Create Aggregate Tables

#### Task 3.4.1: agg_daily_sales_by_category
**Priority:** Medium  
**Estimated Time:** 20 minutes

**SQL Code:**
```sql
CREATE OR REFRESH MATERIALIZED VIEW gold.agg_daily_sales_by_category
AS SELECT
  d.date_value as sale_date,
  p.product_category,
  COUNT(DISTINCT f.transaction_id) as transaction_count,
  SUM(f.quantity) as total_quantity,
  SUM(f.gross_amount) as total_gross_sales,
  SUM(f.discount_amount) as total_discounts,
  SUM(f.net_amount) as total_net_sales,
  AVG(f.net_amount) as avg_transaction_value,
  current_timestamp() as _refreshed_at
FROM gold.fact_sales f
JOIN gold.dim_dates d ON f.order_date_key = d.date_key
JOIN gold.dim_products p ON f.product_key = p.product_key AND p.__END_AT IS NULL
GROUP BY d.date_value, p.product_category;
```

---

#### Task 3.4.2: agg_customer_metrics
**Priority:** Medium  
**Estimated Time:** 20 minutes

**SQL Code:**
```sql
CREATE OR REFRESH MATERIALIZED VIEW gold.agg_customer_metrics
AS SELECT
  c.customer_id,
  c.customer_age,
  c.gender,
  l.city,
  l.state,
  COUNT(DISTINCT f.transaction_id) as total_transactions,
  SUM(f.quantity) as total_items_purchased,
  SUM(f.net_amount) as lifetime_value,
  AVG(f.net_amount) as avg_order_value,
  MIN(d.date_value) as first_purchase_date,
  MAX(d.date_value) as last_purchase_date,
  DATEDIFF(MAX(d.date_value), MIN(d.date_value)) as customer_tenure_days,
  current_timestamp() as _refreshed_at
FROM gold.fact_sales f
JOIN gold.dim_customers c ON f.customer_key = c.customer_key AND c.__END_AT IS NULL
JOIN gold.dim_dates d ON f.order_date_key = d.date_key
JOIN gold.dim_locations l ON f.location_key = l.location_key
GROUP BY c.customer_id, c.customer_age, c.gender, l.city, l.state;
```

---

### Task 3.5: Deploy and Test Gold Layer
**Priority:** High  
**Estimated Time:** 20 minutes

**Validation Queries:**
```sql
-- Check dimension record counts
SELECT 'dim_dates' as table_name, COUNT(*) as record_count FROM gold.dim_dates
UNION ALL
SELECT 'dim_locations', COUNT(*) FROM gold.dim_locations
UNION ALL
SELECT 'dim_customers', COUNT(*) FROM gold.dim_customers
UNION ALL
SELECT 'dim_products', COUNT(*) FROM gold.dim_products
UNION ALL
SELECT 'fact_sales', COUNT(*) FROM gold.fact_sales;

-- Validate SCD Type 2 for customers
SELECT 
  customer_id,
  COUNT(*) as version_count
FROM gold.dim_customers
GROUP BY customer_id
HAVING COUNT(*) > 1
ORDER BY version_count DESC
LIMIT 10;

-- Check current vs historical records in dim_customers
SELECT 
  CASE WHEN __END_AT IS NULL THEN 'Current' ELSE 'Historical' END as status,
  COUNT(*) as record_count
FROM gold.dim_customers
GROUP BY CASE WHEN __END_AT IS NULL THEN 'Current' ELSE 'Historical' END;

-- Validate fact table joins
SELECT 
  COUNT(*) as total_facts,
  SUM(CASE WHEN customer_key IS NULL THEN 1 ELSE 0 END) as missing_customer,
  SUM(CASE WHEN product_key IS NULL THEN 1 ELSE 0 END) as missing_product,
  SUM(CASE WHEN location_key IS NULL THEN 1 ELSE 0 END) as missing_location
FROM gold.fact_sales;

-- Test aggregate tables
SELECT * FROM gold.agg_daily_sales_by_category ORDER BY sale_date DESC, total_net_sales DESC LIMIT 20;

SELECT * FROM gold.agg_customer_metrics ORDER BY lifetime_value DESC LIMIT 20;

-- Star schema query test
SELECT 
  d.year,
  d.month_name,
  p.product_category,
  SUM(f.net_amount) as total_sales,
  COUNT(DISTINCT f.transaction_id) as transaction_count
FROM gold.fact_sales f
JOIN gold.dim_dates d ON f.order_date_key = d.date_key
JOIN gold.dim_products p ON f.product_key = p.product_key AND p.__END_AT IS NULL
GROUP BY d.year, d.month_name, p.product_category
ORDER BY d.year DESC, total_sales DESC;
```

**Acceptance Criteria:**
- [ ] All dimension tables populated
- [ ] Fact table created with valid foreign keys
- [ ] SCD Type 2 working for customers and products
- [ ] Aggregate tables returning correct metrics
- [ ] No orphan records in fact table
- [ ] Star schema queries performing well

---

## Phase 4: Final Validation and Documentation

### Task 4.1: End-to-End Testing
**Priority:** High  
**Estimated Time:** 30 minutes

**Tests:**
1. Add new CSV file to volume
2. Verify pipeline processes incrementally
3. Verify Bronze → Silver → Gold flow
4. Test schema evolution
5. Validate SCD Type 2 versioning with data changes

---

### Task 4.2: Performance Optimization
**Priority:** Medium  
**Estimated Time:** 20 minutes

**Tasks:**
- [ ] Review query plans for aggregate tables
- [ ] Add table properties for optimization
- [ ] Document query performance

---

### Task 4.3: Documentation
**Priority:** Medium  
**Estimated Time:** 30 minutes

**Deliverables:**
- [ ] Pipeline configuration documented
- [ ] Data lineage diagram
- [ ] Known issues and workarounds
- [ ] Operational runbook

---

## Rollback Procedures

### If Bronze Layer Fails:
1. Check volume permissions
2. Verify CSV file format
3. Review Auto Loader configuration
4. Check pipeline event logs

### If Silver Layer Fails:
1. Review data quality filters
2. Check type casting logic
3. Validate source data from Bronze
4. Adjust rejection thresholds if needed

### If Gold Layer Fails:
1. Review dimension table creation
2. Check SCD Type 2 logic
3. Validate join keys
4. Review aggregate query syntax

### Schema Cleanup (if iteration needed):
```sql
-- Cleanup previous attempt (use with caution!)
DROP SCHEMA IF EXISTS <CATALOG_NAME>.bronze CASCADE;
DROP SCHEMA IF EXISTS <CATALOG_NAME>.silver CASCADE;
DROP SCHEMA IF EXISTS <CATALOG_NAME>.gold CASCADE;
```

---

## Post-Implementation

### Monitoring
- [ ] Set up pipeline monitoring alerts
- [ ] Configure data quality checks
- [ ] Schedule regular validation queries

### Handoff
- [ ] Train team on pipeline operations
- [ ] Document support procedures
- [ ] Schedule knowledge transfer session

---

## Task Completion Summary

| Phase | Task Count | Estimated Time |
|-------|-----------|----------------|
| Phase 1: Bronze | 3 | 30 minutes |
| Phase 2: Silver | 3 | 50 minutes |
| Phase 3: Gold | 9 | 2.5 hours |
| Phase 4: Final | 3 | 1.5 hours |
| **Total** | **18** | **~4.5 hours** |

---

**Document Status:** Ready for Implementation  
**Version:** 1.0  
**Last Updated:** June 25, 2026