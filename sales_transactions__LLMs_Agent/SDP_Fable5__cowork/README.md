# Sales Medallion Pipeline — `pipeline_sales_medallion_fa5_v1`

A **serverless Lakeflow Spark Declarative Pipeline (SDP)** on Databricks that ingests raw sales
transaction CSV files and builds a full **medallion architecture (Bronze → Silver → Gold)** in
Unity Catalog, ending in a **star schema** with SCD Type 1 / Type 2 dimensions and two aggregate
tables.

Built using **SDD (Spec-Driven Development)** — see the [sdd/](sdd) folder for the
[requirements](sdd/requirements.md), [design](sdd/design.md), and [task plan](sdd/tasks.md).

---

## Architecture

```
/Volumes/cowork_fable5/dw_raw/raw_data   (CSV, Auto Loader)
        │  STREAM read_files — all STRING, schema evolution, file metadata
        ▼
BRONZE   cowork_fable5.bronze_fa5_v1.bronze_sales_transactions
        │  cast + clean + enrich + expectations (NO dedup)
        ▼
SILVER   cowork_fable5.silver_fa5_v1.silver_sales_transactions
        │  AUTO CDC flows (dedup + SCD)          │ materialized views
        ▼                                         ▼
GOLD     cowork_fable5.gold_fa5_v1   — star schema + aggregates
          dim_customer       SCD1 (large)
          dim_product        SCD1 (large)
          dim_location       SCD2 (small, history)
          dim_payment_type   SCD2 (small, history)
          fact_sales         SCD1 (1 row per transaction_id)
          agg_monthly_sales            (MV)
          agg_customer_demographics    (MV)
```

| Item | Value |
|---|---|
| Pipeline name | `pipeline_sales_medallion_fa5_v1` |
| Pipeline ID | `5e9e99ec-24c1-4822-bed2-9255bfed0398` |
| Compute | Serverless |
| Language | SQL (5 plain `.sql` files, no notebooks) |
| Catalog | `cowork_fable5` |
| Schemas | `bronze_fa5_v1`, `silver_fa5_v1`, `gold_fa5_v1` |
| Source | `/Volumes/cowork_fable5/dw_raw/raw_data` (CSV with header) |
| Workspace files | `/Workspace/Users/<masked-user>@<masked-domain>/pipeline_sales_medallion_fa5_v1/pipeline/` |

## Repository layout

```
sdd/
  requirements.md          functional & non-functional requirements, acceptance criteria
  design.md                architecture, table designs, SCD strategy, risks
  tasks.md                 phased task plan (bronze-first)
pipeline/
  01_bronze_sales.sql      bronze streaming table (Auto Loader, all STRING)
  02_silver_sales.sql      silver streaming table (cast/clean/enrich + expectations)
  03_gold_dimensions.sql   4 dimensions via AUTO CDC (SCD1 + SCD2)
  04_gold_fact.sql         fact_sales via AUTO CDC SCD1 (gold-layer dedup)
  05_gold_aggregates.sql   2 aggregate materialized views
README.md
```

---

## Deployed results (validated run)

The pipeline ran to **COMPLETED** state (bronze ~42s, bronze+silver ~48s, full medallion ~90s).
Layer-by-layer numbers from the deployment:

| Layer | Table | Type | Rows |
|---|---|---|---|
| Bronze | `bronze_sales_transactions` | Streaming table — all 16 business columns STRING + `_rescued_data` + 4 metadata cols | **100,000** |
| Silver | `silver_sales_transactions` | Streaming table — fully typed, cleaned, enriched, NOT deduplicated | **11,227** |
| Gold | `dim_customer` | SCD Type 1 (large dimension) | **10,896** |
| Gold | `dim_product` | SCD Type 1 (large dimension) | **10,053** |
| Gold | `dim_location` | SCD Type 2 (small dimension) | **10** |
| Gold | `dim_payment_type` | SCD Type 2 (small dimension) | **5** |
| Gold | `fact_sales` | SCD Type 1 — exactly 1 row per `transaction_id` | **10,059** |
| Gold | `agg_monthly_sales` | Materialized view (year × month × category × state) | **1,089** |
| Gold | `agg_customer_demographics` | Materialized view (state × gender × age band) | **108** |

### Why 100,000 → 11,227 in silver
The synthetic source data is intentionally dirty. Six **critical expectations DROP rows**:
non-null `transaction_id` / `customer_id` / `product_id` / `order_date`, `quantity > 0`
(~67% of rows fail: zeros and negatives), and `unit_price > 0` (~34% null plus negatives).
Non-critical issues are **nulled instead of dropped**: age outside 18–95, gender normalized
(`Male/M → M`, `Female/F → F`), discount outside 0–100, ship date earlier than order date.

### Gold dedup verification (exact counts)
- Silver: 11,227 rows containing **10,059 distinct** `transaction_id`s
- `fact_sales`: **10,059 rows = 10,059 distinct keys** — deduplication happens in gold via AUTO CDC
- `dim_customer` / `dim_product`: exactly 1 row per key (10,896 / 10,053)

### Sample business results
- Top revenue segment: **Electronics in CA — $146,330.19 net revenue (Jul 2025, 23 orders)**
- Demographic AOV ranges ~$1,929–$3,173 with return rates ~27–37% and cancellation rates ~30–40%

---

## How to trigger the pipeline after new raw data arrives

Bronze uses **Auto Loader** (`STREAM read_files`), so a normal pipeline update processes
**only new files** dropped into the volume — no full refresh needed.

### 1. Land the new file(s) in the raw volume

```bash
databricks fs cp ./new_sales_batch.csv dbfs:/Volumes/cowork_fable5/dw_raw/raw_data/
```
(Any method works: CLI, UI upload, external writer — files must match the CSV layout/header.)

### 2. Trigger an incremental pipeline update

**Option A — Databricks UI**
1. Workspace → **Jobs & Pipelines** → `pipeline_sales_medallion_fa5_v1`
2. Click **Start** (a normal update is incremental; do NOT pick "Full refresh all")

**Option B — Databricks CLI**
```bash
databricks pipelines start-update 5e9e99ec-24c1-4822-bed2-9255bfed0398
```

**Option C — REST API**
```bash
curl -X POST "https://<your-workspace-host>/api/2.0/pipelines/5e9e99ec-24c1-4822-bed2-9255bfed0398/updates" \
  -H "Authorization: Bearer <your-access-token>" \
  -d '{}'
```

### 3. (Optional) Automate — file-arrival trigger or schedule
Create a job that runs the pipeline whenever new files land in the volume:

```bash
databricks jobs create --json '{
  "name": "job_trigger_sales_medallion",
  "tasks": [{
    "task_key": "run_pipeline",
    "pipeline_task": { "pipeline_id": "5e9e99ec-24c1-4822-bed2-9255bfed0398" }
  }],
  "trigger": {
    "file_arrival": { "url": "/Volumes/cowork_fable5/dw_raw/raw_data/" }
  }
}'
```
(Swap `trigger` for a `schedule` block with a cron expression for time-based runs.)

### 4. Validate after the run
```sql
-- Bronze should grow by the new file's row count; downstream layers update incrementally
SELECT COUNT(*) FROM cowork_fable5.bronze_fa5_v1.bronze_sales_transactions;
SELECT COUNT(*) FROM cowork_fable5.silver_fa5_v1.silver_sales_transactions;
SELECT COUNT(*), COUNT(DISTINCT transaction_id) FROM cowork_fable5.gold_fa5_v1.fact_sales;
```

> **When IS a full refresh needed?** Only after changing table definitions incompatibly
> (e.g. new cast rules in silver). Then run with *Full refresh all* — Auto Loader will
> re-ingest everything and AUTO CDC rebuilds the gold tables.

---

## Querying the SCD Type 2 dimensions

SCD2 tables track history with `__START_AT` / `__END_AT`. Because the CDC flows use a
composite tie-breaking sequence, these columns are **structs** — use `__START_AT._seq_date`
for the effective date.

```sql
-- Current version of every location
SELECT city, state, region, __START_AT._seq_date AS valid_from
FROM cowork_fable5.gold_fa5_v1.dim_location
WHERE __END_AT IS NULL;

-- Full change history for one payment type
SELECT payment_type, payment_group,
       __START_AT._seq_date AS valid_from,
       __END_AT._seq_date   AS valid_to
FROM cowork_fable5.gold_fa5_v1.dim_payment_type
ORDER BY payment_type, valid_from;
```

## Iteration / cleanup policy

Schemas are versioned (`*_fa5_v1`). To redesign a layer: create `*_fa5_v2` schemas, update the
pipeline default schema, and drop the old set first:

```sql
DROP SCHEMA cowork_fable5.bronze_fa5_v1 CASCADE;
DROP SCHEMA cowork_fable5.silver_fa5_v1 CASCADE;
DROP SCHEMA cowork_fable5.gold_fa5_v1 CASCADE;
```

---

*Personal information (user emails / identities) is masked in this document as
`<masked-user>@<masked-domain>`. Replace with your own workspace user where paths require it.*
