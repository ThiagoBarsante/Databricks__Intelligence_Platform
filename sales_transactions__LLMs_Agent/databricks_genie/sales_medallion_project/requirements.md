# Sales Medallion Architecture - Requirements Document

## Project Overview
**Project Name:** Sales Transaction Data Pipeline  
**Pipeline Name:** pipeline_sales_medallion_genie_v1  
**Version:** 1.0  
**Date:** June 25, 2026  

## Business Context
This project implements a medallion architecture (Bronze-Silver-Gold) for sales transaction data processing. The pipeline will ingest raw CSV files containing e-commerce transaction data and progressively refine it through multiple layers to support analytics and reporting.

## Data Source
**Location:** `/Volumes/genie_dw/dw_raw/raw_data`  
**Format:** CSV files  
**Update Frequency:** Batch ingestion with incremental processing

### Source Schema
```
transaction_id      : Transaction identifier
order_date         : Date order was placed
ship_date          : Date order was shipped
customer_id        : Customer identifier
customer_age       : Customer age
gender             : Customer gender (M/F/Male/Female)
product_id         : Product identifier
product_category   : Product category
quantity           : Order quantity
unit_price         : Price per unit
discount_pct       : Discount percentage
city               : Customer city
state              : Customer state
payment_type       : Payment method (Card/COD/Crypto)
order_status       : Order status (Delivered/Cancelled/Returned)
ingestion_date     : Raw file ingestion date
```

### Known Data Quality Issues
- Negative ages and quantities
- Negative unit prices
- Missing values in critical fields (gender, payment_type)
- Inconsistent gender values (M/F vs Male/Female)
- Invalid customer ages (e.g., -10, 133, 7)
- Zero quantities with non-zero prices
- Future order dates (2025 dates in 2026 file)

## Architecture Requirements

### Bronze Layer
**Schema:** `<CATALOG_NAME>.bronze`

**Requirements:**
1. Load all CSV columns as STRING data type
2. Enable schema evolution for new columns
3. Add metadata columns:
   - `_ingest_timestamp`: Processing timestamp
   - `_source_file`: Source file path
4. Preserve all raw data, including invalid records
5. Use streaming table for continuous processing

### Silver Layer
**Schema:** `<CATALOG_NAME>.silver`

**Requirements:**
1. Implement as streaming tables
2. Data cleansing:
   - Filter out invalid records
   - Standardize data values (e.g., gender normalization)
   - Remove records with critical missing values
3. Data type casting:
   - Convert dates to DATE type
   - Convert numeric fields to appropriate types (INT, DECIMAL)
4. Data enrichment:
   - Calculate derived fields (total_amount, net_amount)
   - Add data quality flags
5. **No deduplication at this layer** - preserve all valid records

### Gold Layer
**Schema:** `<CATALOG_NAME>.gold`

**Requirements:**
1. Implement Star Schema with fact and dimension tables
2. Apply Slowly Changing Dimension (SCD) strategies:
   - **SCD Type 2** for small dimension tables (customers, products)
   - **SCD Type 1** for large fact tables (transactions)
3. Create 2 aggregate tables for common business metrics
4. Optimize for analytical query performance

**Star Schema Components:**
- **Fact Table:** `fact_sales` - transaction-level data
- **Dimension Tables:**
  - `dim_customers` (SCD Type 2)
  - `dim_products` (SCD Type 2)
  - `dim_dates` (SCD Type 1)
  - `dim_locations` (SCD Type 1)
- **Aggregate Tables:**
  - `agg_daily_sales_by_category`
  - `agg_customer_metrics`

## Technical Requirements

### Pipeline Configuration
1. Use Serverless Spark Declarative Pipeline
2. Prefer SQL syntax when possible
3. Use separate Unity Catalog schemas for each layer
4. Enable Auto Loader for CSV ingestion
5. Implement incremental processing where applicable

### Development Approach
1. **Iterative development with cleanup:**
   - Create new schemas for each iteration
   - Clean up previous schemas before retrying
2. **Phased deployment:**
   - Phase 1: Build and validate Bronze layer
   - Phase 2: Build and validate Silver layer
   - Phase 3: Build and validate Gold layer
3. **Testing requirements:**
   - Validate data quality at each layer
   - Verify record counts and transformations
   - Test schema evolution
   - Validate SCD Type 2 versioning

## Success Criteria

### Bronze Layer
- [ ] All raw CSV data loaded successfully
- [ ] Metadata columns populated correctly
- [ ] Schema evolution enabled and tested
- [ ] No data loss from source files

### Silver Layer
- [ ] Data quality rules applied correctly
- [ ] All data types cast appropriately
- [ ] Invalid records filtered out
- [ ] Derived fields calculated accurately
- [ ] No deduplication performed

### Gold Layer
- [ ] Star schema implemented correctly
- [ ] SCD Type 2 working for dimension tables
- [ ] Aggregate tables provide accurate metrics
- [ ] Query performance optimized
- [ ] Business logic validated

## Constraints and Assumptions

### Constraints
- Must use Serverless compute (R and Scala not supported)
- Must use Unity Catalog for governance
- Pipeline name fixed as: `pipeline_sales_medallion_genie_v1`

### Assumptions
- Source files are incrementally added to the volume
- Catalog name will be specified during implementation
- Transaction_id is unique per transaction
- Customer_id and product_id are consistent across files
- Ingestion_date represents the file arrival date

## Dependencies
- Unity Catalog with appropriate schemas
- Volume access: `/Volumes/genie_dw/dw_raw/raw_data`
- Serverless compute availability

## Risks and Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| Poor source data quality | High | Implement robust cleansing in Silver layer |
| Schema changes in source | Medium | Enable schema evolution in Bronze |
| Performance issues on large data | Medium | Use streaming and optimize Gold aggregates |
| SCD Type 2 complexity | Medium | Thorough testing and validation |

## Approval

**Document Status:** Draft  
**Review Required By:** Data Engineering Team, Business Stakeholders  
**Implementation Start Date:** TBD