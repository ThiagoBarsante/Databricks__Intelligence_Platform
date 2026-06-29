# Requirements

## Goal

Build a Serverless Lakeflow Spark Declarative Pipeline, using SQL syntax where possible, that reads sales CSV files from `/Volumes/copilot_gpt54/dw_raw/raw_data` and publishes a bronze, silver, and gold medallion model inside the `copilot_gpt54` catalog.

## Functional Requirements

1. Create a standalone Databricks Asset Bundle for pipeline `pipeline_sales_medallion_copilot_gpt54_v1`.
2. Use separate Unity Catalog schemas for bronze, silver, and gold.
3. Bronze must ingest CSV files with every business column stored as `STRING`.
4. Bronze must append ingestion metadata and source file metadata.
5. Bronze must support schema drift handling through permissive ingestion and a rescue column.
6. Silver must be implemented as streaming tables.
7. Silver must cast fields to analytics-friendly types, filter invalid records, and enrich the records.
8. Silver must not deduplicate records.
9. Gold must follow dimensional modeling with fact and dimension tables.
10. Larger dimensions must be implemented as SCD Type 1.
11. Smaller dimensions must be implemented as SCD Type 2.
12. Gold must include two aggregate tables exposing business metrics.
13. The solution must include bootstrap and cleanup SQL for iterative development.
14. The solution must include SDD artifacts: requirements, design, and task tracking.

## Non-Functional Requirements

1. Use serverless compute.
2. Keep the solution SQL-first.
3. Keep the initial implementation simple enough to validate bronze independently before widening scope.
4. Support re-deployment through `databricks bundle validate`, `deploy`, and `run`.

## Validation Notes

1. Bronze bundle validation succeeded locally.
2. Bronze pipeline deployment and runtime validation succeeded in Databricks.
3. Full silver and gold graph dry-run validation succeeded in Databricks after resolving duplicate definitions and an aggregate column mismatch.
4. Full pipeline runtime execution published bronze, silver, dimensions, fact, and aggregate objects successfully.
5. After source data upload, the rerun completed successfully and produced the following published counts:
	- Bronze: `100000`
	- Silver: `18302`
	- Gold fact: `18302`
	- Gold aggregates: `13183`, `3672`, and `1090`
