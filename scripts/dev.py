"""Quick-start dev environment: Oracle -> raw -> stg -> silver demo.

This script automates the full pipeline setup for development:
1. Start Docker containers (Oracle source + target DB)
2. Wait for Oracle to be ready
3. Load synthetic data to Oracle
4. Run dlt pipeline to extract to raw layer
5. Run SQLMesh to transform raw -> stg -> silver

Uses direct Python imports (no subprocess) for dlt and SQLMesh steps
for maximum compatibility across environments.

Usage:
    uv run dev                    # Uses default Postgres target
    uv run dev --dest mssql       # Use MSSQL as target
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from pathlib import Path

# Add project root and dlt folder to path for imports
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "dlt"))

# Import constants (from dlt/constants.py)
from constants import DESTINATION_TO_GATEWAY  # noqa: E402


def wait_for_oracle(timeout: int = 300) -> bool:
    """Wait for Oracle to be ready using direct Python connection."""
    print("[dev] Waiting for Oracle to be ready (can take 2-3 min)...")
    start = time.time()
    while time.time() - start < timeout:
        try:
            import oracledb

            oracledb.connect(
                user="appuser", password="apppass", dsn="localhost:1521/ggm"
            )
            return True
        except Exception:
            time.sleep(5)
    return False


def start_docker(destination: str) -> None:
    """Start Docker containers for Oracle source and target DB."""
    print(f"[dev] Starting Docker (Oracle + {destination})...")
    subprocess.run(
        [
            "docker",
            "compose",
            "-f",
            "docker/docker-compose.yml",
            "up",
            "-d",
            "oracle",
            destination,
        ],
        check=True,
    )


def load_synthetic_data() -> None:
    """Load synthetic CSV data to Oracle using direct import."""
    print("[dev] Loading synthetic data to Oracle...")

    # Import and run directly (no subprocess)
    from synthetic.load_to_oracle import load_csvs_to_oracle  # noqa: E402

    csv_dir = PROJECT_ROOT / "data" / "synthetic"
    load_csvs_to_oracle(
        csv_dir=csv_dir,
        host="localhost",
        port=1521,
        service_name="ggm",
        user="appuser",
        password="apppass",
    )


def run_dlt_pipeline(destination: str) -> None:
    """Run dlt pipeline using direct import."""
    print(f"[dev] dlt: Oracle -> {destination} raw layer...")

    # Set credentials matching docker-compose.yml
    os.environ.update(
        {
            # Oracle source
            "SOURCES__SQL_DATABASE__CREDENTIALS": "oracle+oracledb://appuser:apppass@localhost:1521/?service_name=ggm",
            # Postgres destination
            "DESTINATION__POSTGRES__CREDENTIALS": "postgresql://ggm:ggm_dev@localhost:5432/ggm_dev",
            # MSSQL destination
            "DESTINATION__MSSQL__CREDENTIALS": "mssql+pyodbc://sa:GGM_Dev123!@localhost:1433/ggm_dev?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes",
            # MySQL destination (via sqlalchemy destination)
            "DESTINATION__SQLALCHEMY__CREDENTIALS": "mysql+pymysql://root:ggm_dev@localhost:3306/ggm_dev",
            # DuckDB
            "DESTINATION__DUCKDB__CREDENTIALS": "ggm_dev.db",
            # Set DLT_PROJECT_DIR so dlt finds its .dlt/ config
            "DLT_PROJECT_DIR": str(PROJECT_ROOT / "dlt"),
        }
    )

    # Import and run dlt pipeline directly (no subprocess)
    from pipeline import run_pipeline as dlt_run_pipeline  # noqa: E402

    dlt_run_pipeline(destination=destination)


def run_sqlmesh_plan(gateway: str) -> None:
    """Run SQLMesh plan using direct Python API."""
    print("[dev] SQLMesh: raw -> stg -> silver...")

    # Import SQLMesh and run directly (no subprocess)
    from sqlmesh import Context

    sqlmesh_dir = PROJECT_ROOT / "sqlmesh"
    ctx = Context(paths=str(sqlmesh_dir), gateway=gateway)
    ctx.plan(auto_apply=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Quick-start dev environment")
    parser.add_argument(
        "--dest",
        "--destination",
        default="postgres",
        choices=["postgres", "mssql", "mysql"],
        help="Target database type",
    )
    parser.add_argument(
        "--skip-docker",
        action="store_true",
        help="Skip starting Docker containers",
    )
    args = parser.parse_args()

    # Step 1: Start Docker (if not skipped)
    if not args.skip_docker:
        start_docker(args.dest)
        if not wait_for_oracle():
            print("[dev] ERROR: Oracle did not become ready in time")
            sys.exit(1)
        print("[dev] Oracle is ready.")

    # Step 2: Load synthetic data to Oracle
    load_synthetic_data()

    # Step 3: Run dlt pipeline
    run_dlt_pipeline(args.dest)

    # Step 4: Run SQLMesh transformations
    gateway = DESTINATION_TO_GATEWAY.get(args.dest, "local")
    run_sqlmesh_plan(gateway)

    print(f"[dev] Done! Run 'sqlmesh -p sqlmesh --gateway {gateway} ui' to explore.")


if __name__ == "__main__":
    main()
