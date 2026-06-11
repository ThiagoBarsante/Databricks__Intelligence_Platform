"""One-shot deploy + run of the Sales Medallion SDP as a SINGLE pipeline.

Implements the entire solution (bronze -> silver -> gold dims/fact -> aggregates)
as one Lakeflow Spark Declarative Pipeline and runs it end-to-end:

    1. (optional) Ingest a local CSV file into the UC volume.
    2. Create the bronze / silver / gold schemas (idempotent).
    3. Upload all pipeline .sql files to the workspace (as raw FILES).
    4. Create or update the single pipeline `pipeline_sales_medallion_op48_v1` (serverless).
    5. Trigger a full-refresh run and poll until it completes.
    6. (optional) Validate gold row counts.

The pipeline contains ALL transformations; SDP resolves the bronze->gold DAG and
runs it in one update.

Run with uv:
    uv run scripts/deploy_and_run.py                       # data already in volume
    uv run scripts/deploy_and_run.py --local-csv data.csv  # ingest then deploy+run
    uv run scripts/deploy_and_run.py --no-run              # deploy only
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.pipelines import FileLibrary, PipelineLibrary
from databricks.sdk.service.workspace import ImportFormat

# Ordered list of transformation files (bronze -> silver -> gold).
# SDP derives the real dependency DAG; order here is for readability only.
PIPELINE_FILES = [
    "bronze/bronze_sales_transactions.sql",
    "silver/silver_sales_transactions.sql",
    "gold/dim_customer.sql",
    "gold/dim_product.sql",
    "gold/dim_location.sql",
    "gold/dim_category.sql",
    "gold/fact_sales.sql",
    "gold/agg_sales_by_category_month.sql",
    "gold/agg_sales_by_state_status.sql",
]

TERMINAL_STATES = {"COMPLETED", "FAILED", "CANCELED"}


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Deploy + run the Sales Medallion SDP.")
    p.add_argument("--profile", default="DEFAULT", help="databricks CLI profile (default: DEFAULT)")
    p.add_argument("--catalog", default="cowork_op48")
    p.add_argument("--volume-path", default="/Volumes/cowork_op48/dw_raw/raw_data")
    p.add_argument("--pipeline-name", default="pipeline_sales_medallion_op48_v1")
    p.add_argument("--local-csv", default=None, help="optional local CSV to ingest into the volume")
    p.add_argument("--no-run", action="store_true", help="deploy only; do not trigger a run")
    p.add_argument("--no-validate", action="store_true", help="skip gold row-count validation")
    return p.parse_args()


def ensure_schemas(w: WorkspaceClient, catalog: str) -> None:
    print("\n[2/6] Ensuring schemas exist: bronze, silver, gold")
    existing = {s.name for s in w.schemas.list(catalog_name=catalog)}
    for name in ("bronze", "silver", "gold"):
        if name in existing:
            print(f"      {catalog}.{name} already exists (ok)")
        else:
            w.schemas.create(name=name, catalog_name=catalog)
            print(f"      created {catalog}.{name}")


def ingest_csv(w: WorkspaceClient, local_csv: str, volume_path: str) -> None:
    src = Path(local_csv)
    if not src.is_file():
        sys.exit(f"--local-csv not found: {local_csv}")
    target = f"{volume_path}/{src.name}"
    print(f"\n[1/6] Ingesting {src.name} -> {target}")
    with src.open("rb") as f:
        w.files.upload(target, f, overwrite=True)


def upload_files(w: WorkspaceClient, project_root: Path, ws_pipeline_dir: str) -> list[str]:
    print("\n[3/6] Uploading pipeline files to workspace (as raw FILES)")
    ws_paths: list[str] = []
    for rel in PIPELINE_FILES:
        local = project_root / "pipeline" / rel
        ws_path = f"{ws_pipeline_dir}/{rel}"
        w.workspace.mkdirs(ws_path.rsplit("/", 1)[0])
        with local.open("rb") as f:
            w.workspace.upload(ws_path, f, format=ImportFormat.RAW, overwrite=True)
        ws_paths.append(ws_path)
    print(f"      uploaded {len(ws_paths)} files to {ws_pipeline_dir}")
    return ws_paths


def create_or_update_pipeline(
    w: WorkspaceClient, name: str, catalog: str, ws_paths: list[str]
) -> str:
    print("\n[4/6] Create/update pipeline (serverless, all layers in one pipeline)")
    libraries = [PipelineLibrary(file=FileLibrary(path=p)) for p in ws_paths]
    common = dict(
        name=name,
        catalog=catalog,
        schema="bronze",          # default schema; cross-schema tables use fully-qualified names
        serverless=True,
        channel="CURRENT",
        development=True,
        continuous=False,
        libraries=libraries,
    )
    existing = next((p for p in w.pipelines.list_pipelines() if p.name == name), None)
    if existing:
        pipeline_id = existing.pipeline_id
        w.pipelines.update(pipeline_id=pipeline_id, **common)
        print(f"      updated existing pipeline: {pipeline_id}")
    else:
        pipeline_id = w.pipelines.create(**common).pipeline_id
        print(f"      created new pipeline: {pipeline_id}")
    return pipeline_id


def run_pipeline(w: WorkspaceClient, pipeline_id: str) -> str:
    print("\n[5/6] Starting full-refresh run...")
    update_id = w.pipelines.start_update(pipeline_id, full_refresh=True).update_id
    print(f"      update_id: {update_id}")
    state = None
    while state not in TERMINAL_STATES:
        time.sleep(10)
        info = w.pipelines.get_update(pipeline_id, update_id).update
        state = info.state.value if info and info.state else "UNKNOWN"
        print(f"      state: {state}")
    if state != "COMPLETED":
        sys.exit(f"Pipeline run ended in state: {state}. Inspect events in the Databricks UI.")
    return state


def validate(w: WorkspaceClient, catalog: str) -> None:
    print("\n[6/6] Validating gold row counts")
    wh = next((x for x in w.warehouses.list()), None)
    if not wh:
        print("      no SQL warehouse available; skipping validation")
        return
    sql = f"""
        SELECT 'dim_customer'  AS tbl, COUNT(*) AS rows FROM {catalog}.gold.dim_customer
        UNION ALL SELECT 'dim_product',  COUNT(*) FROM {catalog}.gold.dim_product
        UNION ALL SELECT 'dim_location', COUNT(*) FROM {catalog}.gold.dim_location
        UNION ALL SELECT 'dim_category', COUNT(*) FROM {catalog}.gold.dim_category
        UNION ALL SELECT 'fact_sales',   COUNT(*) FROM {catalog}.gold.fact_sales
        UNION ALL SELECT 'agg_sales_by_category_month', COUNT(*) FROM {catalog}.gold.agg_sales_by_category_month
        UNION ALL SELECT 'agg_sales_by_state_status',   COUNT(*) FROM {catalog}.gold.agg_sales_by_state_status
    """
    resp = w.statement_execution.execute_statement(
        warehouse_id=wh.id, statement=sql, wait_timeout="50s"
    )
    # Warehouse may be cold-starting; poll the statement until it finishes.
    deadline = time.time() + 180
    while resp.status and resp.status.state.value in ("PENDING", "RUNNING") and time.time() < deadline:
        time.sleep(5)
        resp = w.statement_execution.get_statement(resp.statement_id)
    rows = resp.result.data_array if resp.result else []
    if not rows:
        print(f"      no results (statement state: {resp.status.state.value if resp.status else 'UNKNOWN'})")
    for tbl, cnt in rows:
        print(f"      {tbl:<32} {cnt}")


def main() -> None:
    args = parse_args()
    project_root = Path(__file__).resolve().parent.parent
    w = WorkspaceClient(profile=args.profile)
    me = w.current_user.me().user_name
    ws_root = f"/Workspace/Users/{me}/{args.pipeline_name}"
    ws_pipeline_dir = f"{ws_root}/pipeline"

    print(f"==> User:     {me}")
    print(f"==> Catalog:  {args.catalog}")
    print(f"==> Pipeline: {args.pipeline_name}")
    print(f"==> WS dir:   {ws_pipeline_dir}")

    if args.local_csv:
        ingest_csv(w, args.local_csv, args.volume_path)
    else:
        print(f"\n[1/6] Skipping ingestion (no --local-csv); using data already in {args.volume_path}")

    ensure_schemas(w, args.catalog)
    ws_paths = upload_files(w, project_root, ws_pipeline_dir)
    pipeline_id = create_or_update_pipeline(w, args.pipeline_name, args.catalog, ws_paths)

    if args.no_run:
        print(f"\n[done] --no-run set; deployment complete. Pipeline id: {pipeline_id}")
        return

    run_pipeline(w, pipeline_id)
    print(f"\n[OK] Pipeline run COMPLETED. Tables written to {args.catalog}.bronze / .silver / .gold")

    if not args.no_validate:
        try:
            validate(w, args.catalog)
        except Exception as exc:  # validation is best-effort
            print(f"      validation skipped: {exc}")


if __name__ == "__main__":
    main()
