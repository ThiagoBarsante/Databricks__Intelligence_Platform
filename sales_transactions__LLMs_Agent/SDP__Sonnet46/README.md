# Sales Transactions Medallion Pipeline (`pipeline_sales_medallion_sn46_v1`)

A Lakeflow Spark Declarative Pipeline (SDP) implementing a bronze → silver → gold medallion
architecture for the raw sales transactions CSV in
`/Volumes/cowork_sn46/dw_raw/raw_data/sales_transactions.csv`. Built using the SDD workflow —
see [requirements.md](pipeline_sales_medallion_sn46_v1/requirements.md), [design.md](pipeline_sales_medallion_sn46_v1/design.md), [tasks.md](pipeline_sales_medallion_sn46_v1/tasks.md).

## What was built

```
/Volumes/cowork_sn46/dw_raw/raw_data/sales_transactions.csv
        │  Auto Loader (STREAM read_files), schema evolution, all columns STRING
        ▼
cowork_sn46.bronze.brz_sales_transactions          (streaming table, 100,000 rows)
        │  cast types, normalize, enrich (net_amount, DQ flags) — no dedup
        ▼
cowork_sn46.silver.slv_sales_transactions          (streaming table, 100,000 rows)
        │
   ┌────┴───────────────────┬─────────────────────────────┐
   ▼                        ▼                             ▼
gold.dim_product       gold.dim_customer         gold.fact_sales_transactions
(SCD Type 2)           (SCD Type 1)              (SCD Type 1 — also dedupes
43,306 rows            78,649 rows                transaction_id: 100,000 → 43,236)
   │                        │                             │
   └────────────────────────┴─────────────┬───────────────┘
                                           ▼
                  gold.agg_category_monthly_metrics   (materialized view, 185 rows)
                  gold.agg_customer_segment_metrics   (materialized view, 90 rows)
```

| Layer | Table | Type | Strategy |
|---|---|---|---|
| Bronze | `cowork_sn46.bronze.brz_sales_transactions` | Streaming table | Auto Loader, all-STRING, schema evolution, `_ingested_at`/`_source_file` |
| Silver | `cowork_sn46.silver.slv_sales_transactions` | Streaming table | Cast/clean/enrich, no dedup |
| Gold | `cowork_sn46.gold.dim_product` | Streaming table | `AUTO CDC … STORED AS SCD TYPE 2` (smaller dimension, history tracked) |
| Gold | `cowork_sn46.gold.dim_customer` | Streaming table | `AUTO CDC … STORED AS SCD TYPE 1` (larger dimension, current state) |
| Gold | `cowork_sn46.gold.fact_sales_transactions` | Streaming table | `AUTO CDC … STORED AS SCD TYPE 1` (largest table; merge on `transaction_id` performs the dedup deferred from silver) |
| Gold | `cowork_sn46.gold.agg_category_monthly_metrics` | Materialized view | Star-schema rollup: revenue/units/discount/fulfillment by category × month |
| Gold | `cowork_sn46.gold.agg_customer_segment_metrics` | Materialized view | Star-schema rollup: reach/revenue/order-value/returns by gender × age bracket × state |

**Deployed objects:**
- Pipeline name: `pipeline_sales_medallion_sn46_v1`
- Pipeline ID: `XPTO--8233-ff2ee5f65044`
- Pipeline root path (workspace): `/Workspace/Users/XPTO--@EMAIL.com/pipeline_sales_medallion_sn46_v1`
- Transformation files (workspace): `…/pipeline_sales_medallion_sn46_v1/transformations/transformations/*.sql`
- SQL warehouse used for validation queries: `Serverless Starter Warehouse` (`XPTO--ID`)
- All examples below assume the CLI profile **`DEFAULT`** (`databricks auth login --profile DEFAULT` if not yet configured) and SDK auth via the same profile (`DATABRICKS_CONFIG_PROFILE=DEFAULT`).

---

## Manual run-through (bash CLI + Python SDK)

The steps below reproduce exactly what the assisted run did: upload source data to the Volume,
push transformation files to the Workspace, create/update the pipeline, trigger a run, and
validate the output. Each step shows the **Databricks CLI** (`databricks …`) and the **Databricks
SDK for Python** (`databricks-sdk`, `pip install databricks-sdk`) equivalents.

### Step 1 — Upload the source CSV to the Unity Catalog Volume

**Bash (Databricks CLI)**
```bash
# Copy a local CSV into the raw Volume that bronze reads from
databricks fs cp ./sales_transactions.csv \
  "dbfs:/Volumes/cowork_sn46/dw_raw/raw_data/sales_transactions.csv" \
  --overwrite

# Verify it landed
databricks fs ls "dbfs:/Volumes/cowork_sn46/dw_raw/raw_data" -l
```

**Python (Databricks SDK)**
```python
from databricks.sdk import WorkspaceClient

w = WorkspaceClient(profile="DEFAULT")

local_path = "./sales_transactions.csv"
volume_path = "/Volumes/cowork_sn46/dw_raw/raw_data/sales_transactions.csv"

with open(local_path, "rb") as f:
    w.files.upload(volume_path, f, overwrite=True)

# Verify
for entry in w.files.list_directory_contents("/Volumes/cowork_sn46/dw_raw/raw_data"):
    print(entry.path, entry.file_size)
```

> Bronze ingests the whole **folder** (`STREAM read_files('/Volumes/cowork_sn46/dw_raw/raw_data/', …)`),
> so dropping additional CSV files with the same schema into that Volume folder is enough for
> Auto Loader to pick them up incrementally on the next pipeline run.

### Step 2 — Upload the pipeline transformation files to the Workspace

**Bash (Databricks CLI)**
```bash
WS_ROOT="/Workspace/Users/XPTO--@Email.com/pipeline_sales_medallion_sn46_v1"

# Import the whole transformations/ folder in SOURCE format (one call per file shown for clarity;
# `databricks workspace import-dir` uploads a whole local directory recursively)
databricks workspace import-dir ./transformations "$WS_ROOT/transformations" --overwrite

# Or upload a single file:
databricks workspace import "$WS_ROOT/transformations/01_bronze_sales_transactions.sql" \
  --file ./transformations/01_bronze_sales_transactions.sql \
  --language SQL --format SOURCE --overwrite
```

**Python (Databricks SDK)**
```python
import pathlib
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ImportFormat, Language

w = WorkspaceClient(profile="DEFAULT")
ws_root = "/Workspace/Users/XPTO--@Email.com/pipeline_sales_medallion_sn46_v1/transformations"
local_dir = pathlib.Path("./transformations")

w.workspace.mkdirs(ws_root)
for sql_file in sorted(local_dir.glob("*.sql")):
    with open(sql_file, "rb") as f:
        w.workspace.upload(
            path=f"{ws_root}/{sql_file.name}",
            content=f.read(),
            format=ImportFormat.SOURCE,
            language=Language.SQL,
            overwrite=True,
        )
    print("uploaded", sql_file.name)
```

### Step 3 — Create or update the pipeline

**Bash (Databricks CLI)** — create once, then edit on subsequent changes:
```bash
WS_ROOT="/Workspace/Users/XPTO--@Email.com/pipeline_sales_medallion_sn46_v1"

cat > pipeline_spec.json << 'EOF'
{
  "name": "pipeline_sales_medallion_sn46_v1",
  "catalog": "cowork_sn46",
  "schema": "bronze",
  "serverless": true,
  "continuous": false,
  "root_path": "/Workspace/Users/XPTO--@Email.com/pipeline_sales_medallion_sn46_v1",
  "libraries": [
    {"file": {"path": "/Workspace/Users/XPTO--@Email.com/pipeline_sales_medallion_sn46_v1/transformations/01_bronze_sales_transactions.sql"}},
    {"file": {"path": "/Workspace/Users/XPTO--@Email.com/pipeline_sales_medallion_sn46_v1/transformations/02_silver_sales_transactions.sql"}},
    {"file": {"path": "/Workspace/Users/XPTO--@Email.com/pipeline_sales_medallion_sn46_v1/transformations/03_gold_dim_product.sql"}},
    {"file": {"path": "/Workspace/Users/XPTO--@Email.com/pipeline_sales_medallion_sn46_v1/transformations/04_gold_dim_customer.sql"}},
    {"file": {"path": "/Workspace/Users/XPTO--@Email.com/pipeline_sales_medallion_sn46_v1/transformations/05_gold_fact_sales_transactions.sql"}},
    {"file": {"path": "/Workspace/Users/XPTO--@Email.com/pipeline_sales_medallion_sn46_v1/transformations/06_gold_aggregates.sql"}}
  ]
}
EOF

# First time: create
databricks pipelines create --json @pipeline_spec.json

# Subsequent changes: edit the existing pipeline by ID
databricks pipelines update XPTO-ff2ee5f65044 --json @pipeline_spec.json

# Look up the ID by name at any time
databricks pipelines list-pipelines | grep pipeline_sales_medallion_sn46_v1
```

**Python (Databricks SDK)**
```python
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.pipelines import PipelineLibrary, FileLibrary

w = WorkspaceClient(profile="DEFAULT")
ws_root = "/Workspace/Users/XPTO--@Email.com/pipeline_sales_medallion_sn46_v1"
sql_files = [
    "01_bronze_sales_transactions.sql",
    "02_silver_sales_transactions.sql",
    "03_gold_dim_product.sql",
    "04_gold_dim_customer.sql",
    "05_gold_fact_sales_transactions.sql",
    "06_gold_aggregates.sql",
]
libraries = [
    PipelineLibrary(file=FileLibrary(path=f"{ws_root}/transformations/{name}"))
    for name in sql_files
]

# Idempotent create-or-update by name
existing = next(
    (p for p in w.pipelines.list_pipelines(filter="name LIKE 'pipeline_sales_medallion_sn46_v1'")),
    None,
)
if existing:
    w.pipelines.update(
        pipeline_id=existing.pipeline_id,
        name="pipeline_sales_medallion_sn46_v1",
        catalog="cowork_sn46",
        schema="bronze",
        serverless=True,
        continuous=False,
        root_path=ws_root,
        libraries=libraries,
    )
    pipeline_id = existing.pipeline_id
else:
    created = w.pipelines.create(
        name="pipeline_sales_medallion_sn46_v1",
        catalog="cowork_sn46",
        schema="bronze",
        serverless=True,
        continuous=False,
        root_path=ws_root,
        libraries=libraries,
    )
    pipeline_id = created.pipeline_id

print("pipeline_id:", pipeline_id)
```

### Step 4 — Trigger a pipeline run

**Bash (Databricks CLI)**
```bash
PIPELINE_ID="XPTO-ff2ee5f65044"

# Incremental run (process new/changed files only)
databricks pipelines start-update "$PIPELINE_ID"

# Full refresh (reprocess everything — useful while iterating on transformation logic)
databricks pipelines start-update "$PIPELINE_ID" --full-refresh

# Poll status
databricks pipelines get "$PIPELINE_ID"

# Tail recent events / errors
databricks pipelines list-pipeline-events "$PIPELINE_ID" --max-results 10
```

**Python (Databricks SDK)**
```python
import time
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.pipelines import UpdateInfoState

w = WorkspaceClient(profile="DEFAULT")
pipeline_id = "XPTO-ff2ee5f65044"

update = w.pipelines.start_update(pipeline_id=pipeline_id, full_refresh=False)
update_id = update.update_id

terminal_states = {
    UpdateInfoState.COMPLETED,
    UpdateInfoState.FAILED,
    UpdateInfoState.CANCELED,
}
while True:
    info = w.pipelines.get_update(pipeline_id=pipeline_id, update_id=update_id).update
    print("state:", info.state)
    if info.state in terminal_states:
        break
    time.sleep(15)

assert info.state == UpdateInfoState.COMPLETED, f"Pipeline run ended in {info.state}"
```

### Step 5 — Validate the results

**Bash (Databricks CLI — run SQL through a warehouse)**
```bash
WAREHOUSE_ID="XPTO--c18ac5"

databricks api post /api/2.0/sql/statements --json '{
  "warehouse_id": "'"$WAREHOUSE_ID"'",
  "statement": "SELECT '\''bronze'\'' AS layer, COUNT(*) AS rows FROM cowork_sn46.bronze.brz_sales_transactions
                UNION ALL SELECT '\''silver'\'', COUNT(*) FROM cowork_sn46.silver.slv_sales_transactions
                UNION ALL SELECT '\''gold.fact'\'', COUNT(*) FROM cowork_sn46.gold.fact_sales_transactions
                UNION ALL SELECT '\''gold.dim_customer'\'', COUNT(*) FROM cowork_sn46.gold.dim_customer
                UNION ALL SELECT '\''gold.dim_product'\'', COUNT(*) FROM cowork_sn46.gold.dim_product",
  "wait_timeout": "30s"
}'
```

**Python (Databricks SDK — Statement Execution API)**
```python
from databricks.sdk import WorkspaceClient

w = WorkspaceClient(profile="DEFAULT")
warehouse_id = "XPTO--c18ac5"

resp = w.statement_execution.execute_statement(
    warehouse_id=warehouse_id,
    statement="""
        SELECT 'bronze' AS layer, COUNT(*) AS rows FROM cowork_sn46.bronze.brz_sales_transactions
        UNION ALL SELECT 'silver', COUNT(*) FROM cowork_sn46.silver.slv_sales_transactions
        UNION ALL SELECT 'gold.fact', COUNT(*) FROM cowork_sn46.gold.fact_sales_transactions
        UNION ALL SELECT 'gold.dim_customer', COUNT(*) FROM cowork_sn46.gold.dim_customer
        UNION ALL SELECT 'gold.dim_product', COUNT(*) FROM cowork_sn46.gold.dim_product
    """,
    wait_timeout="30s",
)
for row in resp.result.data_array:
    print(row)
```

**Python (alternative — `databricks-sql-connector`)**
```python
from databricks import sql

with sql.connect(
    server_hostname="<your-workspace-host>.cloud.databricks.com",
    http_path="/sql/1.0/warehouses/XPTO--c18ac5",
    auth_type="databricks-oauth",  # or access_token=<PAT>
) as conn, conn.cursor() as cur:
    cur.execute("SELECT * FROM cowork_sn46.gold.agg_category_monthly_metrics ORDER BY order_month, product_category")
    for row in cur.fetchall():
        print(row)
```

---

## Expected results (from the validated run)

| Check | Expected |
|---|---|
| `brz_sales_transactions` row count | 100,000 (1:1 with source CSV; all columns `STRING`) |
| `slv_sales_transactions` row count | 100,000 (1:1 with bronze — silver does **not** dedup) |
| `dim_product` row count | 43,306 (one row per `(product_id, product_category)` version; SCD2 `__START_AT`/`__END_AT`) |
| `dim_customer` row count | 78,649 (one row per distinct `customer_id`; SCD1 current-state) |
| `fact_sales_transactions` row count | 43,236 (deduped from 100,000 silver rows down to distinct `transaction_id`s via SCD1 `AUTO CDC` merge) |
| `agg_category_monthly_metrics` row count | 185 (5 categories × 37 months) |
| `agg_customer_segment_metrics` row count | 90 (3 genders × 5 age brackets × 6 states) |

If counts don't match, re-run with `--full-refresh` (bash) / `full_refresh=True` (SDK) and inspect
`databricks pipelines list-pipeline-events <id> --event-type error` for root cause — see the
**Common Issues** table in the `databricks-spark-declarative-pipelines` skill reference.
