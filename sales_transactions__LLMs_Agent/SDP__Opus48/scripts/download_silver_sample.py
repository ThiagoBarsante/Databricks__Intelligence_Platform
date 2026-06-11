"""Download a 1,000-row sample of cowork_op48.silver.silver_sales_transactions to CSV.

Run with uv:
    uv run scripts/download_silver_sample.py
"""
from __future__ import annotations

import time
from pathlib import Path

import pandas as pd
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import Disposition, Format, StatementState

TABLE = "cowork_op48.silver.silver_sales_transactions"
ROW_LIMIT = 1000
OUTPUT_PATH = Path(__file__).resolve().parent.parent / "data" / "silver_sales_transactions_sample.csv"
TERMINAL_STATES = {StatementState.SUCCEEDED, StatementState.FAILED, StatementState.CANCELED, StatementState.CLOSED}


def main() -> None:
    w = WorkspaceClient(profile="DEFAULT")
    warehouse = next(iter(w.warehouses.list()), None)
    if warehouse is None:
        raise SystemExit("No SQL warehouse available in this workspace.")

    print(f"==> Warehouse: {warehouse.name} ({warehouse.id})")
    print(f"==> Table:     {TABLE}")
    print(f"==> Row limit: {ROW_LIMIT}")

    resp = w.statement_execution.execute_statement(
        warehouse_id=warehouse.id,
        statement=f"SELECT * FROM {TABLE} LIMIT {ROW_LIMIT}",
        wait_timeout="50s",
        disposition=Disposition.INLINE,
        format=Format.JSON_ARRAY,
    )

    deadline = time.time() + 180
    while resp.status and resp.status.state in (StatementState.PENDING, StatementState.RUNNING) and time.time() < deadline:
        time.sleep(5)
        resp = w.statement_execution.get_statement(resp.statement_id)

    if not resp.status or resp.status.state != StatementState.SUCCEEDED:
        state = resp.status.state.value if resp.status and resp.status.state else "UNKNOWN"
        raise SystemExit(f"Query did not succeed (state: {state})")

    columns = [c.name for c in resp.manifest.schema.columns]
    rows = resp.result.data_array if resp.result else []
    df = pd.DataFrame(rows, columns=columns)

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUTPUT_PATH, index=False)
    print(f"\n[OK] Wrote {len(df)} rows -> {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
