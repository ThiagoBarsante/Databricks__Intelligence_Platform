# Task Tracking

- [x] Confirm Databricks CLI and workspace connectivity.
- [x] Confirm target catalog from the source volume path.
- [x] Scaffold the Databricks Asset Bundle.
- [x] Implement bronze SQL ingestion.
- [x] Validate bronze bundle configuration.
- [x] Deploy bronze and fix analyzer issues for empty input handling.
- [x] Publish bronze successfully and verify the runtime result.
- [x] Implement silver streaming table and validate end-to-end publish.
- [x] Implement gold dimensions, fact, and aggregates and validate end-to-end publish.
- [x] Populate the source volume with CSV files and rerun data validation with non-zero rows.

## Applied Layer Rules Checklist

- [x] Bronze reads CSV from the target volume with an explicit schema.
- [x] Bronze stores all business columns as `STRING`.
- [x] Bronze preserves `_ingested_at`, file metadata, and `_rescued_data`.
- [x] Bronze applies no business-rule filtering.
- [x] Silver keeps the layer non-deduplicated.
- [x] Silver filters rescued rows and invalid keys, dates, quantities, prices, and discounts.
- [x] Silver standardizes age band, gender, product category, location, payment type, and order status.
- [x] Silver derives analytical measures and operational flags.
- [x] Gold publishes SCD Type 1 customer and product dimensions.
- [x] Gold publishes SCD Type 2 location, payment type, and order status dimensions.
- [x] Gold publishes a calendar dimension from order and ship dates.
- [x] Gold publishes the sales fact table and three aggregate reporting tables.