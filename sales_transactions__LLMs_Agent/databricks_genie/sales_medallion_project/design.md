# Sales Medallion Architecture - Design Document

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                     DATA FLOW ARCHITECTURE                       │
└─────────────────────────────────────────────────────────────────┘

┌──────────────┐      ┌──────────────┐      ┌──────────────┐
│   RAW DATA   │      │    BRONZE    │      │    SILVER    │
│   CSV Files  │─────▶│  Raw Ingest  │─────▶│   Cleansed   │
│   (Volume)   │      │  (STRING)    │      │   (Typed)    │
└──────────────┘      └──────────────┘      └──────────────┘
                                                     │
                                                     ▼
                                             ┌──────────────┐
                                             │     GOLD     │
                                             │ Star Schema  │
                                             │  + Aggregates│
                                             └──────────────┘
```

## Layer Designs

### 1. Bronze Layer Design

#### Purpose
Preserve raw data exactly as received, with minimal transformation.

#### Implementation
**Table:** `bronze.sales_transactions_raw`

**Technology:** Streaming Table with Auto Loader

**Schema Strategy:**
- All source columns as STRING
- Enable schema evolution (`RESCUE DATA` column)
- Add metadata columns

```sql
CREATE OR REFRESH STREAMING TABLE bronze.sales_transactions_raw
(
  -- Source columns (all STRING)
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
  
  -- Metadata columns
  _ingest_timestamp TIMESTAMP GENERATED ALWAYS AS (current_timestamp()),
  _source_file STRING,
  _rescued_data STRING  -- For schema evolution
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

**Key Features:**
- **Schema Evolution:** `rescuedDataColumn` captures unexpected columns
- **Source Tracking:** `_source_file` tracks origin
- **Timestamp:** `_ingest_timestamp` records processing time
- **Permissive Mode:** Never fails on bad data

---

### 2. Silver Layer Design

#### Purpose
Cleanse, validate, and type-cast data. Filter out invalid records but preserve all valid records without deduplication.

#### Implementation
**Table:** `silver.sales_transactions_clean`

**Technology:** Streaming Table

**Data Quality Rules:**

| Rule | Logic | Action |
|------|-------|--------|
| Valid dates | order_date and ship_date castable to DATE | Filter |
| Valid quantity | quantity >= 0 | Filter |
| Valid price | unit_price > 0 | Filter |
| Valid age | customer_age between 0 and 120 | Filter |
| Gender standardization | Map M/Male→Male, F/Female→Female | Transform |
| Missing critical fields | product_id, customer_id not null | Filter |

**Schema:**
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
  
  -- Derived fields
  gross_amount DECIMAL(12,2),
  discount_amount DECIMAL(12,2),
  net_amount DECIMAL(12,2),
  days_to_ship INT,
  
  -- Metadata
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
  
  -- Derived calculations
  CAST(quantity AS INT) * CAST(unit_price AS DECIMAL(10,2)) as gross_amount,
  CAST(quantity AS INT) * CAST(unit_price AS DECIMAL(10,2)) * CAST(discount_pct AS DECIMAL(5,2)) / 100 as discount_amount,
  (CAST(quantity AS INT) * CAST(unit_price AS DECIMAL(10,2))) - 
    (CAST(quantity AS INT) * CAST(unit_price AS DECIMAL(10,2)) * CAST(discount_pct AS DECIMAL(5,2)) / 100) as net_amount,
  DATEDIFF(TO_DATE(ship_date, 'yyyy-MM-dd'), TO_DATE(order_date, 'yyyy-MM-dd')) as days_to_ship,
  
  _ingest_timestamp,
  _source_file
FROM STREAM(bronze.sales_transactions_raw)
WHERE 
  -- Data quality filters
  transaction_id IS NOT NULL
  AND customer_id IS NOT NULL
  AND product_id IS NOT NULL
  AND TRY_TO_DATE(order_date, 'yyyy-MM-dd') IS NOT NULL
  AND TRY_TO_DATE(ship_date, 'yyyy-MM-dd') IS NOT NULL
  AND TRY_CAST(quantity AS INT) >= 0
  AND TRY_CAST(unit_price AS DECIMAL(10,2)) > 0
  AND TRY_CAST(customer_age AS INT) BETWEEN 0 AND 120;
```

**Key Features:**
- Type conversion with validation
- Gender normalization
- Calculated fields (amounts, shipping time)
- No deduplication - preserves all valid records

---

### 3. Gold Layer Design

#### Purpose
Implement star schema for analytical queries with optimized dimension and fact tables.

#### Star Schema Design

```
       ┌──────────────┐
       │ dim_dates    │
       │ (SCD Type 1) │
       └──────┬───────┘
              │
┌─────────────┼─────────────┐
│             │             │
▼             ▼             ▼
┌──────────┐  ┌──────────┐  ┌──────────┐
│dim_cust  │  │fact_sales│  │dim_prod  │
│(SCD-2)   │◀─┤(Type 1)  │─▶│(SCD-2)   │
└──────────┘  └──────────┘  └──────────┘
              │
              ▼
       ┌──────────────┐
       │ dim_location │
       │ (SCD Type 1) │
       └──────────────┘
```

#### 3.1 Dimension Tables

**A. dim_customers (SCD Type 2)**
```sql
CREATE OR REFRESH STREAMING TABLE gold.dim_customers
(
  customer_key BIGINT GENERATED ALWAYS AS IDENTITY,
  customer_id STRING NOT NULL,
  customer_age INT,
  gender STRING,
  
  -- SCD Type 2 columns
  effective_date DATE,
  end_date DATE,
  is_current BOOLEAN,
  
  -- Metadata
  _created_at TIMESTAMP
)
COMMENT 'Gold layer: Customer dimension with SCD Type 2';
```

**B. dim_products (SCD Type 2)**
```sql
CREATE OR REFRESH STREAMING TABLE gold.dim_products
(
  product_key BIGINT GENERATED ALWAYS AS IDENTITY,
  product_id STRING NOT NULL,
  product_category STRING,
  
  -- SCD Type 2 columns
  effective_date DATE,
  end_date DATE,
  is_current BOOLEAN,
  
  -- Metadata
  _created_at TIMESTAMP
)
COMMENT 'Gold layer: Product dimension with SCD Type 2';
```

**C. dim_dates (SCD Type 1)**
```sql
CREATE OR REFRESH STREAMING TABLE gold.dim_dates
(
  date_key INT,
  date_value DATE NOT NULL,
  year INT,
  quarter INT,
  month INT,
  day INT,
  day_of_week INT,
  day_name STRING,
  month_name STRING,
  is_weekend BOOLEAN,
  
  -- Metadata
  _created_at TIMESTAMP
)
COMMENT 'Gold layer: Date dimension';
```

**D. dim_locations (SCD Type 1)**
```sql
CREATE OR REFRESH STREAMING TABLE gold.dim_locations
(
  location_key BIGINT GENERATED ALWAYS AS IDENTITY,
  city STRING,
  state STRING,
  
  -- Metadata
  _created_at TIMESTAMP
)
COMMENT 'Gold layer: Location dimension';
```

#### 3.2 Fact Table

**fact_sales (SCD Type 1)**
```sql
CREATE OR REFRESH STREAMING TABLE gold.fact_sales
(
  sales_key BIGINT GENERATED ALWAYS AS IDENTITY,
  transaction_id BIGINT NOT NULL,
  
  -- Foreign keys
  customer_key BIGINT,
  product_key BIGINT,
  order_date_key INT,
  ship_date_key INT,
  location_key BIGINT,
  
  -- Measures
  quantity INT,
  unit_price DECIMAL(10,2),
  discount_pct DECIMAL(5,2),
  gross_amount DECIMAL(12,2),
  discount_amount DECIMAL(12,2),
  net_amount DECIMAL(12,2),
  days_to_ship INT,
  
  -- Degenerate dimensions
  payment_type STRING,
  order_status STRING,
  
  -- Metadata
  _ingest_timestamp TIMESTAMP,
  _created_at TIMESTAMP
)
COMMENT 'Gold layer: Sales fact table';
```

#### 3.3 Aggregate Tables

**A. agg_daily_sales_by_category**
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
JOIN gold.dim_products p ON f.product_key = p.product_key AND p.is_current = true
GROUP BY d.date_value, p.product_category;
```

**B. agg_customer_metrics**
```sql
CREATE OR REFRESH MATERIALIZED VIEW gold.agg_customer_metrics
AS SELECT
  c.customer_id,
  c.customer_age,
  c.gender,
  COUNT(DISTINCT f.transaction_id) as total_transactions,
  SUM(f.quantity) as total_items_purchased,
  SUM(f.net_amount) as lifetime_value,
  AVG(f.net_amount) as avg_order_value,
  MIN(d.date_value) as first_purchase_date,
  MAX(d.date_value) as last_purchase_date,
  DATEDIFF(MAX(d.date_value), MIN(d.date_value)) as customer_tenure_days,
  current_timestamp() as _refreshed_at
FROM gold.fact_sales f
JOIN gold.dim_customers c ON f.customer_key = c.customer_key AND c.is_current = true
JOIN gold.dim_dates d ON f.order_date_key = d.date_key
GROUP BY c.customer_id, c.customer_age, c.gender;
```

---

## SCD Type 2 Implementation

### Strategy for dim_customers

**Logic:**
1. Detect changes in customer attributes (age, gender)
2. Close old record: set `end_date` and `is_current = false`
3. Insert new record with new `effective_date` and `is_current = true`

**MERGE Statement Pattern:**
```sql
MERGE INTO gold.dim_customers as target
USING (
  SELECT DISTINCT customer_id, customer_age, gender, order_date
  FROM silver.sales_transactions_clean
) as source
ON target.customer_id = source.customer_id 
   AND target.is_current = true
WHEN MATCHED AND (
  target.customer_age != source.customer_age OR
  target.gender != source.gender
) THEN UPDATE SET
  end_date = source.order_date,
  is_current = false
WHEN NOT MATCHED THEN INSERT (
  customer_id, customer_age, gender,
  effective_date, end_date, is_current, _created_at
) VALUES (
  source.customer_id, source.customer_age, source.gender,
  source.order_date, NULL, true, current_timestamp()
);

-- Insert new version for changed records
INSERT INTO gold.dim_customers (
  customer_id, customer_age, gender,
  effective_date, end_date, is_current, _created_at
)
SELECT ...
```

---

## Performance Optimizations

### Bronze Layer
- **Auto Loader**: Efficient incremental file processing
- **Schema Evolution**: Automatic handling of new columns

### Silver Layer
- **Z-Order**: Cluster by `order_date`, `customer_id`
- **Liquid Clustering**: Enable for frequently filtered columns

### Gold Layer
- **Partitioning**: Partition fact table by `order_date_key`
- **Materialized Views**: Pre-compute aggregates
- **Bloom Filters**: On high-cardinality join keys

---

## Data Quality Monitoring

### Metrics to Track

| Layer | Metric | Threshold |
|-------|--------|----------|
| Bronze | Record count | > 0 |
| Silver | Invalid record % | < 10% |
| Silver | Null critical fields | < 1% |
| Gold | Orphan records | = 0 |
| Gold | SCD integrity | 100% |

---

## Security and Governance

### Unity Catalog Integration
- **Catalog:** `<CATALOG_NAME>`
- **Schemas:** `bronze`, `silver`, `gold`
- **Permissions:**
  - Bronze: Read-only for data engineers
  - Silver: Read for analysts
  - Gold: Read for business users

### Data Lineage
- Automatic lineage tracking through Delta Lake
- Source file tracking in metadata columns

---

## Technology Stack

| Component | Technology |
|-----------|------------|
| Pipeline | Spark Declarative Pipeline (Serverless) |
| Storage | Unity Catalog + Delta Lake |
| File Format | Parquet (managed by Delta) |
| Ingestion | Auto Loader |
| Language | SQL (primary), Python (if needed) |

---

## Deployment Strategy

### Phase 1: Bronze
1. Create schema and tables
2. Configure Auto Loader
3. Run and validate

### Phase 2: Silver
1. Implement cleansing logic
2. Test data quality rules
3. Validate derived fields

### Phase 3: Gold
1. Create dimension tables
2. Implement SCD Type 2
3. Create fact table
4. Build aggregate views
5. Validate star schema joins

---

## Rollback Plan

1. Pipeline failures: Review event logs
2. Data quality issues: Adjust filters in Silver
3. Performance issues: Add optimizations
4. Schema changes: Use separate dev schema for testing

---

## Monitoring and Alerting

### Pipeline Health
- Monitor pipeline run status
- Track processing latency
- Alert on failures

### Data Quality
- Track rejected record counts
- Monitor null percentages
- Validate SCD integrity

---

**Document Version:** 1.0  
**Last Updated:** June 25, 2026  
**Status:** Ready for Implementation