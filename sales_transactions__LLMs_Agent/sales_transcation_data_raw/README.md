# Sales Transactions — Raw Data Ingestion Layer

## Purpose

This repository provides a synthetic, intentionally flawed sales transaction dataset designed to test **Medallion Architecture** data pipelines on Databricks. It is intended to be consumed by LLM agents (e.g., Claude Opus 4.8, GPT-5.5, and others) that must design, implement, and validate Bronze → Silver → Gold pipeline transformations including data quality enforcement, cleansing, and aggregation.

---

## Repository Structure

```
.
├── data/
│   └── raw/
│       ├── sales_transactions.csv      # Primary raw dataset (100,000 rows, 16 columns)
│       └── sales_transactions.7z       # Compressed archive of the same dataset
├── reports/
│   └── sales_transactions_data_profiling_report.html   # ydata-profiling HTML report
├── Sales_transaction_evaluation.ipynb  # Exploratory profiling notebook
└── README.md
```

---

## Dataset: `data/raw/sales_transactions.csv`

- **Rows**: 100,000 (+ 1 header)
- **Columns**: 16
- **Date range**: 2023-01-01 to 2026-01-01 (order dates)
- **Ingestion date**: All records stamped `2026-05-31`

### Schema

| Column | Type | Description |
|---|---|---|
| `transaction_id` | int | Transaction identifier (range 1–50,000; NOT unique across 100K rows) |
| `order_date` | date | Date the order was placed |
| `ship_date` | date | Date the order was shipped |
| `customer_id` | string | Customer identifier (format: `CUST<number>`) |
| `customer_age` | float | Customer age in years |
| `gender` | string | Customer gender |
| `product_id` | string | Product identifier (format: `PROD<number>`) |
| `product_category` | string | One of: `Electronics`, `Fashion`, `Furniture`, `Grocery`, `Sports` |
| `quantity` | int | Number of units sold |
| `unit_price` | float | Price per unit in USD |
| `discount_pct` | float | Discount percentage applied |
| `city` | string | Destination city (10 distinct US cities) |
| `state` | string | Destination state abbreviation (6 states) |
| `payment_type` | string | One of: `COD`, `Card`, `Crypto`, `UPI` |
| `order_status` | string | One of: `Delivered`, `Cancelled`, `Returned` |
| `ingestion_date` | date | Pipeline ingestion timestamp |

---

## Embedded Data Quality Issues

The dataset was synthetically generated with the following defects. Pipeline agents **must detect and handle** each category:

### Structural / Uniqueness
| Issue | Detail |
|---|---|
| Duplicate `transaction_id` | Range is 1–50,000 across 100,000 rows — IDs repeat |
| Non-unique `customer_id` | 78,649 distinct IDs across 100,000 rows; one ID appears up to 7 times |

### Nulls / Missing Values
| Column | Approx. Null Rate |
|---|---|
| `customer_age` | ~25% |
| `gender` | ~20% |
| `unit_price` | ~33.6% |
| `discount_pct` | ~33.2% |
| `payment_type` | ~20% |

### Invalid / Out-of-Range Values
| Column | Issue |
|---|---|
| `customer_age` | Negative values (min = -10) and impossible ages (max = 200) |
| `quantity` | Negative values (min = -5) and zeros |
| `unit_price` | Negative values (min ≈ -100) |
| `discount_pct` | Values exceeding 100% (max ≈ 150%) |

### Inconsistent Encoding
| Column | Issue |
|---|---|
| `gender` | Mixed representations — `"M"` / `"Male"` and `"F"` / `"Female"` coexist |

### Temporal Anomalies
| Issue | Detail |
|---|---|
| `ship_date` before `order_date` | Multiple rows where shipment precedes the order |
| Future `order_date` | Dates up to 2026-01-01, beyond reasonable ingestion window |

---

## Medallion Architecture Target

Agents consuming this dataset should implement or evaluate the following pipeline:

```
Bronze (raw ingest, no transforms)
  └─ Silver (validated, cleansed, typed, deduplicated)
       └─ Gold (aggregated, business-ready metrics)
```

**Expected Silver-layer transformations** include:
- Deduplication on `transaction_id`
- Null imputation or rejection per business rules
- Gender normalization (`M`/`Male` → `Male`, `F`/`Female` → `Female`)
- Range enforcement (age 0–120, quantity ≥ 1, price > 0, discount 0–100%)
- Temporal validation (`ship_date` ≥ `order_date`)
- Schema enforcement with explicit casting

**Expected Gold-layer outputs** may include revenue aggregations, return/cancellation rates, category/payment-method breakdowns, and customer cohort metrics.

---

## Tooling

- Profiling: [`ydata-profiling`](https://github.com/ydataai/ydata-profiling) — see `reports/sales_transactions_data_profiling_report.html`
- Notebook: `Sales_transaction_evaluation.ipynb` (pandas-based EDA)
- Target platform: **Databricks** (Delta Lake, Unity Catalog, DLT or standard Spark pipelines)
