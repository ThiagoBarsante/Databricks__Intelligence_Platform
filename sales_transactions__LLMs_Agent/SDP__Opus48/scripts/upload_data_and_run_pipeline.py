"""Generate fresh ingest data, then deploy + run the pipeline against it.

Chains the two existing scripts end to end:
    1. generate_sample_data.py        -> writes data/ingestion/sales_transactions_ingest_<ts>.csv
    2. deploy_and_run.py --local-csv  -> uploads that file to the UC volume and runs the
                                          full bronze -> silver -> gold pipeline

Run with uv:
    uv run scripts/upload_data_and_run_pipeline.py
"""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import generate_sample_data

SCRIPTS_DIR = Path(__file__).resolve().parent


def main() -> None:
    print("=" * 60)
    print("[1/2] Generating new ingest sample data")
    print("=" * 60)
    csv_path = generate_sample_data.main()

    print()
    print("=" * 60)
    print("[2/2] Deploying + running pipeline with the generated file")
    print("=" * 60)
    subprocess.run(
        [sys.executable, str(SCRIPTS_DIR / "deploy_and_run.py"), "--local-csv", str(csv_path)],
        check=True,
    )


if __name__ == "__main__":
    main()
