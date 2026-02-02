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
import shutil
import subprocess
import sys
import time
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

# Load unified config (reads .env)
from config import load_config

load_config()

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


def _get_sqlmesh_command() -> list[str]:
    """Get the best SQLMesh command for spawning subprocesses."""
    uv = shutil.which("uv")
    if uv:
        return [uv, "run", "sqlmesh"]

    sqlmesh = shutil.which("sqlmesh")
    if sqlmesh:
        return [sqlmesh]

    return ["sqlmesh"]


def main() -> None:
    project_root = Path(__file__).parent.parent
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
            cwd=project_root,
            check=True,
        )
        
        print("[dev] Waiting for Oracle to be ready (can take 2-3 min)...")
        if not wait_for_oracle():
            print("[dev] ERROR: Oracle did not become ready in time")
            sys.exit(1)
        print("[dev] Oracle is ready.")
    
    print("[dev] Loading synthetic data to Oracle...")
    subprocess.run(
        [
            sys.executable,
            str(project_root / "synthetic" / "load_to_oracle.py"),
            "--user",
            "appuser",
            "--password",
            "apppass",
            "--service-name",
            "ggm",
        ],
        cwd=project_root,
        check=True,
    )
    
    print("[dev] dlt: Oracle -> %s raw layer..." % args.dest)
    
    # Set credentials matching docker-compose.yml
    # These override anything in .env for the dev environment
    env = {
        **os.environ,
        # Oracle source
        "SOURCES__SQL_DATABASE__CREDENTIALS": "oracle+oracledb://appuser:apppass@localhost:1521/?service_name=ggm",
        # Postgres destination
        "DESTINATION__POSTGRES__CREDENTIALS__HOST": "localhost",
        "DESTINATION__POSTGRES__CREDENTIALS__PORT": "5432",
        "DESTINATION__POSTGRES__CREDENTIALS__DATABASE": "ggm_dev",
        "DESTINATION__POSTGRES__CREDENTIALS__USERNAME": "ggm",
        "DESTINATION__POSTGRES__CREDENTIALS__PASSWORD": "ggm_dev",
        # MSSQL destination
        "DESTINATION__MSSQL__CREDENTIALS__HOST": "localhost",
        "DESTINATION__MSSQL__CREDENTIALS__PORT": "1433",
        "DESTINATION__MSSQL__CREDENTIALS__DATABASE": "ggm_dev",
        "DESTINATION__MSSQL__CREDENTIALS__USERNAME": "sa",
        "DESTINATION__MSSQL__CREDENTIALS__PASSWORD": "GGM_Dev123!",
        "DESTINATION__MSSQL__CREDENTIALS__DRIVER": "ODBC Driver 18 for SQL Server",
        "GGM_TRUST_SERVER_CERTIFICATE": "true",
        # MySQL destination (via sqlalchemy)
        "DESTINATION__SQLALCHEMY__CREDENTIALS__HOST": "localhost",
        "DESTINATION__SQLALCHEMY__CREDENTIALS__PORT": "3306",
        "DESTINATION__SQLALCHEMY__CREDENTIALS__DATABASE": "ggm_dev",
        "DESTINATION__SQLALCHEMY__CREDENTIALS__USERNAME": "root",
        "DESTINATION__SQLALCHEMY__CREDENTIALS__PASSWORD": "ggm_dev",
        "DESTINATION__SQLALCHEMY__CREDENTIALS__DRIVERNAME": "mysql+pymysql",
        # DuckDB
        "DESTINATION__DUCKDB__CREDENTIALS": "ggm_dev.db",
    }
    # Set DLT_PROJECT_DIR so dlt finds its .dlt/ config
    env["DLT_PROJECT_DIR"] = str(project_root / "dlt")
    subprocess.run(
        [sys.executable, str(project_root / "dlt" / "pipeline.py"), "--dest", args.dest],
        cwd=project_root,
        check=True,
        env=env,
    )
    
    print("[dev] SQLMesh: raw -> stg -> silver...")
    gateway = DESTINATION_TO_GATEWAY.get(args.dest, "local")
    
    sqlmesh_env = {**os.environ}
    # Pass through destination credentials for SQLMesh
    for key, value in env.items():
        if key.startswith("DESTINATION__") or key.startswith("GGM_"):
            sqlmesh_env[key] = value
    
    subprocess.run(
        _get_sqlmesh_command()
        + ["-p", "sqlmesh", "--gateway", gateway, "plan", "--auto-apply"],
        cwd=project_root,
        check=True,
        env=sqlmesh_env,
    )
    
    print(
        "[dev] Done! Run 'uv run sqlmesh -p sqlmesh --gateway %s ui' to explore."
        % gateway
    )


if __name__ == "__main__":
    main()
