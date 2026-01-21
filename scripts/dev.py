"""Quick-start dev environment: Oracle -> raw -> stg -> silver demo.

This script automates the full pipeline setup for development:
1. Start Docker containers (Oracle source + target DB)
2. Wait for Oracle to be ready
3. Load synthetic data to Oracle
4. Run dlt pipeline to extract to raw layer
5. Run SQLMesh to transform raw -> stg -> silver

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

# Import constants from dlt folder
sys.path.insert(0, str(Path(__file__).parent.parent / "dlt"))
from constants import DESTINATION_TO_GATEWAY


def wait_for_oracle(timeout: int = 300) -> bool:
    """Wait for Oracle to be ready."""
    start = time.time()
    while time.time() - start < timeout:
        result = subprocess.run(
            [sys.executable, "-c", 
             "import oracledb; oracledb.connect(user='appuser', password='apppass', dsn='localhost:1521/ggm')"],
            capture_output=True,
        )
        if result.returncode == 0:
            return True
        time.sleep(5)
    return False


def main() -> None:
    parser = argparse.ArgumentParser(description="Quick-start dev environment")
    parser.add_argument(
        "--dest", "--destination",
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
    
    if not args.skip_docker:
        print("[dev] Starting Docker (Oracle + %s)..." % args.dest)
        subprocess.run(
            ["docker", "compose", "-f", "docker/docker-compose.yml", 
             "up", "-d", "oracle", args.dest],
            check=True,
        )
        
        print("[dev] Waiting for Oracle to be ready (can take 2-3 min)...")
        if not wait_for_oracle():
            print("[dev] ERROR: Oracle did not become ready in time")
            sys.exit(1)
        print("[dev] Oracle is ready.")
    
    print("[dev] Loading synthetic data to Oracle...")
    subprocess.run([
        sys.executable, "synthetic/load_to_oracle.py",
        "--user", "appuser", "--password", "apppass",
        "--service-name", "ggm"
    ], check=True)
    
    print("[dev] dlt: Oracle -> %s raw layer..." % args.dest)
    
    # Set credentials matching docker-compose.yml
    env = {
        **__import__('os').environ,
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
    }
    # Set DLT_PROJECT_DIR so dlt finds its .dlt/ config
    env["DLT_PROJECT_DIR"] = "dlt"
    subprocess.run(
        [sys.executable, "dlt/pipeline.py", "--dest", args.dest],
        check=True,
        env=env,
    )
    
    print("[dev] SQLMesh: raw -> stg -> silver...")
    gateway = DESTINATION_TO_GATEWAY.get(args.dest, "local")
    
    # Add SQLMesh gateway env vars matching docker-compose
    sqlmesh_env = {
        **__import__('os').environ,
        # MSSQL gateway (uses master database like dlt)
        "MSSQL_DATABASE": "master",
        "MSSQL_PASSWORD": "GGM_Dev123!",
    }
    subprocess.run(
        ["sqlmesh", "-p", "sqlmesh", "--gateway", gateway, "plan", "--auto-apply"],
        check=True,
        env=sqlmesh_env,
    )
    
    print("[dev] Done! Run 'sqlmesh -p sqlmesh --gateway %s ui' to explore." % gateway)


if __name__ == "__main__":
    main()
