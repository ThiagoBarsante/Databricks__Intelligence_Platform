# Design — Sales Medallion Pipeline (`pipeline_sales_medallion_fa5_v1`)

## 1. Architecture

```
/Volumes/cowork_fable5/dw_raw/raw_data  (CSV, Auto Loader)
        │  STREAM read_files (all STRING, schema evolution, file metadata)
        ▼
┌─ BRONZE ── cowork_fable5.bronze_fa5_v1 ─────────────────────────────┐
│  bronze_sales_transactions   (streaming table, append-only)         │
└──────────────────────────────────────────────────────────────────────┘
        │  STREAM(bronze) — cast, clean, enrich, expectations, NO dedup
        ▼
┌─ SILVER ── cowork_fable5.silver_fa5_v1 ─────────────────────────────┐
│  silver_sales_transactions   (streaming table, typed + enriched)     │
└──────────────────────────────────────────────────────────────────────┘
        │  AUTO CDC flows (dedup happens here)            │ MV reads
        ▼                                                  ▼
┌─ GOLD ── cowork_fable5.gold_fa5_v1 ─────────────────────────────────┐
│  STAR SCHEMA                                                         │
│   dim_customer       SCD1  (large ~79K keys)                         │
│   dim_product        SCD1  (large ~42K keys)                         │
│   dim_location       SCD2  (small ~10 city/state pairs)              │
│   dim_payment_type   SCD2  (small ~4 values)                         │
│   fact_sales         SCD1  (1 row per transaction_id)                │
│  AGGREGATES (materialized views)                                     │
│   agg_monthly_sales            month × category × state              │
│   agg_customer_demographics    state × gender × age band             │
└──────────────────────────────────────────────────────────────────────┘
```

One serverless SQL pipeline writes to all three schemas using fully-qualified
table names (`catalog.schema.table`). Default catalog/schema of the pipeline:
`cowork_fable5.bronze_fa5_v1`.

## 2. Bronze Design
**Table:** `bronze_fa5_v1.bronze_sales_transactions` (streaming table)

- `STREAM read_files(..., format => 'csv', header => true, inferColumnTypes => false)`
  → every source column lands as **STRING** (FR-2.2)
- Auto Loader default schema evolution (`addNewColumns`) + `_rescued_data` column (FR-2.3)
- Metadata columns: `_ingested_at` (current_timestamp), `_source_file` (_metadata.file_path),
  `_file_modification_time`, `_file_size` (FR-2.4)
- `CLUSTER BY (order_date)` — most common pruning column downstream

## 3. Silver Design
**Table:** `silver_fa5_v1.silver_sales_transactions` (streaming table from `STREAM(bronze)`)

### Casting (FR-3.2)
| Column | Type | Rule |
|---|---|---|
| transaction_id | BIGINT | cast |
| order_date / ship_date / ingestion_date | DATE | cast; `ship_date < order_date` → NULL |
| customer_id / product_id / product_category / city / state / order_status | STRING | trimmed |
| customer_age | INT | outside 18–95 → NULL |
| gender | STRING | `M/Male→M`, `F/Female→F`, else NULL |
| quantity | INT | cast |
| unit_price | DECIMAL(12,2) | cast |
| discount_pct | DECIMAL(5,2) | NULL→0; outside 0–100 → NULL |
| payment_type | STRING | NULL→'Unknown' |

### Expectations — critical rules DROP the row
- `transaction_id IS NOT NULL`, `customer_id IS NOT NULL`, `product_id IS NOT NULL`
- `order_date IS NOT NULL`
- `quantity > 0`
- `unit_price > 0`

Non-critical issues (age, gender, discount, ship_date) are **nulled, not dropped**, to
preserve volume. **No deduplication** (FR-3.5) — duplicate transaction_ids flow through.

### Enrichment (FR-3.4)
`gross_amount = quantity*unit_price`, `discount_amount`, `net_amount`,
`days_to_ship`, `order_year`, `order_month`.

## 4. Gold Design (Star Schema)

### Dedup & SCD via AUTO CDC
Silver is not deduplicated, so every Gold core table is fed by an `AUTO CDC INTO` flow,
which deduplicates by `KEYS` and orders by `SEQUENCE BY struct(order_date, transaction_id)`
(composite sequence avoids ties when one key appears many times per day).

| Table | Grain / KEYS | SCD | Why |
|---|---|---|---|
| `dim_customer` | customer_id | **Type 1** | large (~79K) — current state only |
| `dim_product` | product_id | **Type 1** | large (~42K) |
| `dim_location` | city, state | **Type 2** | small (~10) — history kept |
| `dim_payment_type` | payment_type | **Type 2** | small (~4) |
| `fact_sales` | transaction_id | **Type 1** | dedup of repeated transactions; latest version wins |

`fact_sales` keeps degenerate/natural FKs (`customer_id`, `product_id`, `city`,
`state`, `payment_type`, `order_date`) referencing the dimensions, plus all measures
(`quantity`, `unit_price`, `discount_pct`, `gross_amount`, `discount_amount`,
`net_amount`, `days_to_ship`) and `order_status`.

### Aggregates (materialized views, FR-4.5)
1. **`agg_monthly_sales`** — GROUP BY order_year, order_month, product_category, state:
   total_orders, total_quantity, gross/discount/net revenue, avg_order_value,
   avg_discount_pct, delivered/cancelled/returned counts.
2. **`agg_customer_demographics`** — joins `fact_sales` × `dim_customer`,
   GROUP BY state, gender, age_band: distinct customers, orders, net_revenue,
   avg_order_value, return_rate, cancellation_rate.

## 5. File Layout
```
pipeline/
  01_bronze_sales.sql        bronze streaming table
  02_silver_sales.sql        silver streaming table + expectations
  03_gold_dimensions.sql     4 dims + AUTO CDC flows
  04_gold_fact.sql           fact_sales + AUTO CDC flow
  05_gold_aggregates.sql     2 aggregate MVs
```
Uploaded to `/Workspace/Users/devcodecli@gmail.com/pipeline_sales_medallion_fa5_v1/`.

## 6. Deployment Strategy (bronze-first, FR-2.5)
1. Create schemas `bronze_fa5_v1`, `silver_fa5_v1`, `gold_fa5_v1`
2. Deploy pipeline with **bronze file only** → run → validate → show results
3. Add silver file → run → validate
4. Add gold files → run → validate (SCD2 columns, dedup, aggregates)
5. Iteration policy: if a redesign is needed, drop the `*_fa5_v1` schemas (or bump to `_v2`
   and clean up `_v1`) before retrying.

## 7. Key Risks & Mitigations
| Risk | Mitigation |
|---|---|
| CSV type inference breaking STRING requirement | `inferColumnTypes => false` |
| AUTO CDC sequence ties (same key, same day) | composite `SEQUENCE BY struct(order_date, transaction_id)` |
| Aggressive filtering emptying silver | only 6 critical DROP rules; the rest null-out |
| SCD2 query confusion | document `__START_AT`/`__END_AT` usage in validation queries |
