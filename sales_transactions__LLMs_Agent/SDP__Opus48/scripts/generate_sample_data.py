"""Generate 100 synthetic 'new arrival' sales-transaction records for ingestion.

Reads the sampled silver table (data/silver_sales_transactions_sample.csv), derives
realistic categorical values from it, and writes a fresh CSV in the RAW ingest
schema (the format expected by cowork_op48.bronze.bronze_sales_transactions's
read_files schemaHints) to data/ingestion/sales_transactions_ingest_<timestamp>.csv.

Generated rows satisfy the silver-layer row expectations so none would be dropped
on ingest:
    transaction_id IS NOT NULL, order_date IS NOT NULL, quantity > 0, unit_price > 0

Run with uv:
    uv run scripts/generate_sample_data.py
"""
from __future__ import annotations

import random
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd

SOURCE_PATH = Path(__file__).resolve().parent.parent / "data" / "silver_sales_transactions_sample.csv"
OUTPUT_DIR = Path(__file__).resolve().parent.parent / "data" / "ingestion"
RECORD_COUNT = 100

# Raw ingest schema, matching bronze's read_files schemaHints column order.
RAW_COLUMNS = [
    "transaction_id", "order_date", "ship_date", "customer_id", "customer_age",
    "gender", "product_id", "product_category", "quantity", "unit_price",
    "discount_pct", "city", "state", "payment_type", "order_status", "ingestion_date",
]

GENDERS = ["Male", "Female", "M", "F"]


def load_reference(path: Path) -> dict:
    df = pd.read_csv(path)
    return {
        "max_transaction_id": int(df["transaction_id"].max()),
        "product_pairs": list(df[["product_id", "product_category"]].drop_duplicates().itertuples(index=False, name=None)),
        "location_pairs": list(df[["city", "state"]].drop_duplicates().itertuples(index=False, name=None)),
        "payment_types": df["payment_type"].dropna().unique().tolist(),
        "order_statuses": df["order_status"].dropna().unique().tolist(),
    }


def random_order_dates() -> tuple[date, date]:
    order_date = date.today() - timedelta(days=random.randint(0, 90))
    ship_date = order_date + timedelta(days=random.randint(0, 10))
    return order_date, ship_date


def generate_record(seq: int, ref: dict, today: date) -> dict:
    product_id, product_category = random.choice(ref["product_pairs"])
    city, state = random.choice(ref["location_pairs"])
    order_date, ship_date = random_order_dates()
    return {
        "transaction_id": ref["max_transaction_id"] + seq,
        "order_date": order_date.isoformat(),
        "ship_date": ship_date.isoformat(),
        "customer_id": f"CUST{random.randint(100000, 999999)}",
        "customer_age": random.randint(18, 75),
        "gender": random.choice(GENDERS),
        "product_id": product_id,
        "product_category": product_category,
        "quantity": random.randint(1, 10),
        "unit_price": round(random.uniform(5.0, 1200.0), 2),
        "discount_pct": round(random.uniform(0.0, 35.0), 2),
        "city": city,
        "state": state,
        "payment_type": random.choice(ref["payment_types"]),
        "order_status": random.choice(ref["order_statuses"]),
        "ingestion_date": today.isoformat(),
    }


def validate(df: pd.DataFrame) -> pd.DataFrame:
    """Apply the silver-layer row expectations; drop anything that would be dropped on ingest."""
    valid = (
        df["transaction_id"].notna()
        & df["order_date"].notna()
        & (df["quantity"] > 0)
        & (df["unit_price"] > 0)
    )
    dropped = int((~valid).sum())
    if dropped:
        print(f"      dropped {dropped} record(s) failing silver expectations")
    return df[valid]


def main() -> Path:
    if not SOURCE_PATH.is_file():
        raise SystemExit(f"Source sample not found: {SOURCE_PATH}")

    ref = load_reference(SOURCE_PATH)
    today = date.today()
    print(f"==> Source:   {SOURCE_PATH.name}")
    print(f"==> Records:  {RECORD_COUNT}")
    print(f"==> Next id:  {ref['max_transaction_id'] + 1}")

    records = [generate_record(i, ref, today) for i in range(1, RECORD_COUNT + 1)]
    df = pd.DataFrame(records, columns=RAW_COLUMNS)
    df = validate(df)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_name = f"sales_transactions_ingest_{datetime.now().strftime('%Y_%m_%d_%H%M%S')}.csv"
    out_path = OUTPUT_DIR / out_name
    df.to_csv(out_path, index=False)
    print(f"\n[OK] Wrote {len(df)} records -> {out_path}")
    return out_path


if __name__ == "__main__":
    main()
