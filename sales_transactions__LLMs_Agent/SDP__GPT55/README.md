# Sales Medallion Pipeline - gpt55_codex

This project builds a Databricks Serverless Spark Declarative Pipeline for sales
transactions using a bronze, silver, and gold medallion architecture.

The source CSV is expected in this Unity Catalog volume path:

```text
/Volumes/gpt55_codex/dw_raw/raw_data
```

The deployed pipeline is:

```text
pipeline_sales_medallion_gpt55_v1
```

Pipeline ID from the deployment in this chat:

```text
XPTO-43d4-8285-1f1a8f2b62ec
```

Final successful update ID:

```text
XPTO--43f5-9389-5db835501f14
```

## What Was Built

The solution uses one Databricks Asset Bundle and one Serverless Spark
Declarative Pipeline. SQL transformation files are plain `.sql` files under:

```text
src/pipelines/sales_medallion_gpt55/transformations
```

Unity Catalog layout:

| Layer | Schema | Main objects |
|---|---|---|
| Bronze | `gpt55_codex.sales_bronze` | `sales_transactions_raw` |
| Silver | `gpt55_codex.sales_silver` | `sales_transactions_clean` |
| Gold | `gpt55_codex.sales_gold` | `fact_sales`, dimensions, aggregate materialized views |

Gold star schema objects:

| Object | Type |
|---|---|
| `fact_sales` | SCD Type 1 fact table |
| `dim_customer` | SCD Type 2 dimension |
| `dim_product` | SCD Type 2 dimension |
| `dim_location` | SCD Type 2 dimension |
| `dim_payment_type` | SCD Type 2 dimension |
| `dim_order_status` | SCD Type 2 dimension |
| `mv_daily_sales_metrics` | Gold aggregate materialized view |
| `mv_category_state_metrics` | Gold aggregate materialized view |

SDD files:

```text
sdd/sales_medallion_gpt55/requirements.md
sdd/sales_medallion_gpt55/design.md
sdd/sales_medallion_gpt55/task.md
```

Deployment files:

```text
databricks.yml
resources/pipeline_sales_medallion_gpt55.yml
sql/setup_uc_schemas.sql
sql/validate_bronze.sql
sql/validate_full_solution.sql
```

## Chat Deployment Result

The pipeline was built, deployed, triggered, and validated.

Bronze-first validation completed before silver and gold were added:

| Check | Result |
|---|---:|
| `gpt55_codex.sales_bronze.sales_transactions_raw` rows | `100000` |
| Source file count | `1` |

Final validation after the full pipeline run:

| Table | Rows |
|---|---:|
| `bronze.sales_transactions_raw` | `100000` |
| `silver.sales_transactions_clean` | `5896` |
| `gold.fact_sales` | `5583` |
| `gold.dim_customer` current rows | `5794` |
| `gold.dim_product` current rows | `5565` |
| `gold.dim_location` current rows | `10` |
| `gold.dim_payment_type` current rows | `5` |
| `gold.dim_order_status` current rows | `3` |
| `gold.mv_daily_sales_metrics` | `2716` |
| `gold.mv_category_state_metrics` | `450` |

## Prerequisites

- Databricks CLI authenticated to the target workspace.
- Access to catalog `gpt55_codex`.
- Access to volume path `/Volumes/gpt55_codex/dw_raw/raw_data`.
- A SQL warehouse for validation queries.

The warehouse used in this chat was:

```text
XPTO-ac5
```

Check the current Databricks user:

```bash
databricks current-user me --output json
```

List SQL warehouses:

```bash
databricks warehouses list --output json
```

## Manual Runbook - Bash and CLI

Run commands from the repository root.

### 1. Upload Source Files To The Volume

Create the volume directory if needed:

```bash
databricks fs mkdirs dbfs:/Volumes/gpt55_codex/dw_raw/raw_data
```

Upload a local CSV:

```bash
databricks fs cp ./sales_transactions.csv \
  dbfs:/Volumes/gpt55_codex/dw_raw/raw_data/sales_transactions.csv \
  --overwrite
```

List uploaded files:

```bash
databricks fs ls dbfs:/Volumes/gpt55_codex/dw_raw/raw_data
```

### 2. Create Unity Catalog Schemas

```bash
databricks schemas create sales_bronze gpt55_codex \
  --comment "Bronze layer for raw sales transaction ingestion"

databricks schemas create sales_silver gpt55_codex \
  --comment "Silver layer for cleaned and enriched sales transactions"

databricks schemas create sales_gold gpt55_codex \
  --comment "Gold layer for dimensional sales marts and aggregates"
```

If the schemas already exist, the create commands may return an error. That is
fine; continue after confirming they exist:

```bash
databricks schemas get gpt55_codex.sales_bronze --output json
databricks schemas get gpt55_codex.sales_silver --output json
databricks schemas get gpt55_codex.sales_gold --output json
```

### 3. Validate The Bundle

```bash
databricks bundle validate -t dev
```

### 4. Deploy The Pipeline

```bash
databricks bundle deploy -t dev --auto-approve
```

### 5. Trigger The Pipeline

```bash
databricks bundle run pipeline_sales_medallion_gpt55_v1 -t dev
```

Or trigger by pipeline ID:

```bash
databricks pipelines start-update XPTO--1f1a8f2b62ec
```

Check the pipeline:

```bash
databricks pipelines get XPTO--1f1a8f2b62ec --output json
```

## Strict Bronze-First Reproduction

The final repository contains the full bronze, silver, and gold pipeline. To
reproduce the exact bronze-first build gate manually, temporarily exclude the
silver and gold SQL files from the pipeline glob, deploy and run bronze, then
restore them.

```bash
mkdir -p .manual_hold
mv src/pipelines/sales_medallion_gpt55/transformations/silver_*.sql .manual_hold/
mv src/pipelines/sales_medallion_gpt55/transformations/gold_*.sql .manual_hold/

databricks bundle validate -t dev
databricks bundle deploy -t dev --auto-approve
databricks bundle run pipeline_sales_medallion_gpt55_v1 -t dev
```

Validate bronze:

```bash
databricks api post /api/2.0/sql/statements \
  --json '{
    "warehouse_id": "XPTO-ac5",
    "statement": "SELECT COUNT(*) AS row_count, COUNT(DISTINCT _source_file) AS source_file_count FROM gpt55_codex.sales_bronze.sales_transactions_raw",
    "wait_timeout": "30s"
  }' \
  --output json
```

Restore the full pipeline:

```bash
mv .manual_hold/*.sql src/pipelines/sales_medallion_gpt55/transformations/
rmdir .manual_hold

databricks bundle validate -t dev
databricks bundle deploy -t dev --auto-approve
databricks bundle run pipeline_sales_medallion_gpt55_v1 -t dev
```

## Validate The Full Solution

Use the SQL in `sql/validate_full_solution.sql`, or run this statement through
the Statement Execution API:

```bash
cat > /tmp/sales_medallion_validate.json <<'JSON'
{
  "warehouse_id": "XPTO-c18ac5",
  "statement": "SELECT 'bronze.sales_transactions_raw' AS table_name, COUNT(*) AS row_count FROM gpt55_codex.sales_bronze.sales_transactions_raw UNION ALL SELECT 'silver.sales_transactions_clean', COUNT(*) FROM gpt55_codex.sales_silver.sales_transactions_clean UNION ALL SELECT 'gold.fact_sales', COUNT(*) FROM gpt55_codex.sales_gold.fact_sales UNION ALL SELECT 'gold.dim_customer_current', COUNT(*) FROM gpt55_codex.sales_gold.dim_customer WHERE __END_AT IS NULL UNION ALL SELECT 'gold.dim_product_current', COUNT(*) FROM gpt55_codex.sales_gold.dim_product WHERE __END_AT IS NULL UNION ALL SELECT 'gold.dim_location_current', COUNT(*) FROM gpt55_codex.sales_gold.dim_location WHERE __END_AT IS NULL UNION ALL SELECT 'gold.dim_payment_type_current', COUNT(*) FROM gpt55_codex.sales_gold.dim_payment_type WHERE __END_AT IS NULL UNION ALL SELECT 'gold.dim_order_status_current', COUNT(*) FROM gpt55_codex.sales_gold.dim_order_status WHERE __END_AT IS NULL UNION ALL SELECT 'gold.mv_daily_sales_metrics', COUNT(*) FROM gpt55_codex.sales_gold.mv_daily_sales_metrics UNION ALL SELECT 'gold.mv_category_state_metrics', COUNT(*) FROM gpt55_codex.sales_gold.mv_category_state_metrics",
  "wait_timeout": "30s"
}
JSON

databricks api post /api/2.0/sql/statements \
  --json @/tmp/sales_medallion_validate.json \
  --output json
```

On PowerShell, put the JSON body in a file and pass it as `"@file.json"` to
avoid shell quoting issues.

## Manual Runbook - Python SDK

Install the SDK if needed:

```bash
pip install databricks-sdk
```

The examples below use the default Databricks authentication profile or
environment variables.

### Upload A CSV To The Volume

```python
from pathlib import Path

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

local_file = Path("sales_transactions.csv")
volume_file = "/Volumes/gpt55_codex/dw_raw/raw_data/sales_transactions.csv"

with local_file.open("rb") as f:
    w.files.upload(file_path=volume_file, contents=f, overwrite=True)

for entry in w.files.list_directory_contents(
    "/Volumes/gpt55_codex/dw_raw/raw_data"
):
    print(entry.name)
```

### Create Schemas

```python
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import ResourceAlreadyExists

w = WorkspaceClient()

schemas = [
    ("sales_bronze", "Bronze layer for raw sales transaction ingestion"),
    ("sales_silver", "Silver layer for cleaned and enriched sales transactions"),
    ("sales_gold", "Gold layer for dimensional sales marts and aggregates"),
]

for name, comment in schemas:
    try:
        w.schemas.create(
            name=name,
            catalog_name="gpt55_codex",
            comment=comment,
        )
        print(f"created gpt55_codex.{name}")
    except ResourceAlreadyExists:
        print(f"exists gpt55_codex.{name}")
```

### Trigger The Pipeline And Poll For Completion

```python
import time

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

pipeline_id = "XPTO-8285-1f1a8f2b62ec"

started = w.api_client.do(
    method="POST",
    path=f"/api/2.0/pipelines/{pipeline_id}/updates",
    body={},
)

update_id = started["update_id"]
print(f"started update {update_id}")

terminal_states = {"COMPLETED", "FAILED", "CANCELED"}

while True:
    update = w.api_client.do(
        method="GET",
        path=f"/api/2.0/pipelines/{pipeline_id}/updates/{update_id}",
    )
    state = update["update"]["state"]
    print(f"state={state}")

    if state in terminal_states:
        break

    time.sleep(15)

if state != "COMPLETED":
    raise RuntimeError(f"pipeline update ended in {state}")
```

### Validate Row Counts

```python
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

warehouse_id = "XPTO-18ac5"

statement = """
SELECT 'bronze.sales_transactions_raw' AS table_name, COUNT(*) AS row_count
FROM gpt55_codex.sales_bronze.sales_transactions_raw
UNION ALL
SELECT 'silver.sales_transactions_clean', COUNT(*)
FROM gpt55_codex.sales_silver.sales_transactions_clean
UNION ALL
SELECT 'gold.fact_sales', COUNT(*)
FROM gpt55_codex.sales_gold.fact_sales
UNION ALL
SELECT 'gold.dim_customer_current', COUNT(*)
FROM gpt55_codex.sales_gold.dim_customer
WHERE __END_AT IS NULL
UNION ALL
SELECT 'gold.dim_product_current', COUNT(*)
FROM gpt55_codex.sales_gold.dim_product
WHERE __END_AT IS NULL
UNION ALL
SELECT 'gold.dim_location_current', COUNT(*)
FROM gpt55_codex.sales_gold.dim_location
WHERE __END_AT IS NULL
UNION ALL
SELECT 'gold.dim_payment_type_current', COUNT(*)
FROM gpt55_codex.sales_gold.dim_payment_type
WHERE __END_AT IS NULL
UNION ALL
SELECT 'gold.dim_order_status_current', COUNT(*)
FROM gpt55_codex.sales_gold.dim_order_status
WHERE __END_AT IS NULL
UNION ALL
SELECT 'gold.mv_daily_sales_metrics', COUNT(*)
FROM gpt55_codex.sales_gold.mv_daily_sales_metrics
UNION ALL
SELECT 'gold.mv_category_state_metrics', COUNT(*)
FROM gpt55_codex.sales_gold.mv_category_state_metrics
"""

result = w.statement_execution.execute_statement(
    warehouse_id=warehouse_id,
    statement=statement,
    wait_timeout="30s",
)

state = getattr(result.status.state, "value", result.status.state)
if str(state).upper() != "SUCCEEDED":
    raise RuntimeError(f"validation query state: {result.status.state}")

for table_name, row_count in result.result.data_array:
    print(f"{table_name}: {row_count}")
```

## Common Operations

List files in the source volume:

```bash
databricks fs ls dbfs:/Volumes/gpt55_codex/dw_raw/raw_data
```

Inspect the pipeline:

```bash
databricks pipelines get XPTO--1f1a8f2b62ec --output json
```

Redeploy after SQL changes:

```bash
databricks bundle validate -t dev
databricks bundle deploy -t dev --auto-approve
databricks bundle run pipeline_sales_medallion_gpt55_v1 -t dev
```

## Notes

- Bronze loads all source columns as `STRING`.
- Silver casts, cleans, enriches, and filters invalid records.
- Silver intentionally does not deduplicate.
- Gold performs SCD Type 1 for the fact table and SCD Type 2 for dimensions.
- Gold aggregate tables are materialized views supplied by the gold layer.
- The pipeline uses fully qualified UC table names so one pipeline can write to
  separate bronze, silver, and gold schemas.
