"""Quick-start dev environment: Oracle -> raw -> stg -> silver demo.

This script automates the full pipeline setup for development:
1. Start Docker containers (Oracle source + target DB)
2. Wait for Oracle to be ready
3. Clean target database (drop raw/stg/silver schemas)
4. Load synthetic data to Oracle
5. Run dlt pipeline to extract to raw layer
6. Run SQLMesh to transform raw -> stg -> silver

Usage:
    uv run dev                    # Uses default Postgres target
    uv run dev --dest mssql       # Use MSSQL as target
    uv run dev --dest duckdb      # Use DuckDB (no Docker needed for target)
    uv run dev --no-clean         # Skip database cleanup
"""
from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

# Import constants from ingest folder
sys.path.insert(0, str(Path(__file__).parent.parent / "ingest"))
from constants import DESTINATION_TO_GATEWAY


def clean_postgres_database() -> None:
    """Drop all GGM schemas in PostgreSQL to ensure a clean slate."""
    schemas_to_drop = [
        "raw",
        "stg", 
        "silver",
        "sqlmesh",
        "sqlmesh__raw",
        "sqlmesh__stg",
        "sqlmesh__silver",
    ]
    drop_commands = "; ".join(
        f"DROP SCHEMA IF EXISTS {schema} CASCADE" for schema in schemas_to_drop
    )
    subprocess.run(
        [
            "docker", "exec", "ggm-postgres",
            "psql", "-U", "ggm", "-d", "ggm_dev",
            "-c", drop_commands,
        ],
        capture_output=True,
    )


def clean_mssql_database() -> None:
    """Drop all GGM schemas in MSSQL to ensure a clean slate."""
    # First, ensure the ggm_dev database exists (create if not)
    subprocess.run(
        [
            "docker", "exec", "ggm-mssql",
            "/opt/mssql-tools18/bin/sqlcmd",
            "-S", "localhost", "-U", "sa", "-P", "GGM_Dev123!",
            "-d", "master", "-C",
            "-Q", "IF DB_ID('ggm_dev') IS NULL CREATE DATABASE ggm_dev;",
        ],
        capture_output=True,
    )
    
    # MSSQL requires dropping all objects before dropping schema
    # Using a script that handles this properly
    cleanup_sql = """
    DECLARE @schema NVARCHAR(128)
    DECLARE @sql NVARCHAR(MAX)
    
    DECLARE schema_cursor CURSOR FOR 
    SELECT name FROM sys.schemas 
    WHERE name IN ('raw', 'stg', 'silver', 'sqlmesh', 'sqlmesh__raw', 'sqlmesh__stg', 'sqlmesh__silver')
    
    OPEN schema_cursor
    FETCH NEXT FROM schema_cursor INTO @schema
    
    WHILE @@FETCH_STATUS = 0
    BEGIN
        -- Drop all tables in schema
        SELECT @sql = COALESCE(@sql + '; ', '') + 'DROP TABLE [' + @schema + '].[' + name + ']'
        FROM sys.tables WHERE schema_id = SCHEMA_ID(@schema)
        
        IF @sql IS NOT NULL
            EXEC sp_executesql @sql
        
        SET @sql = NULL
        
        -- Drop all views in schema
        SELECT @sql = COALESCE(@sql + '; ', '') + 'DROP VIEW [' + @schema + '].[' + name + ']'
        FROM sys.views WHERE schema_id = SCHEMA_ID(@schema)
        
        IF @sql IS NOT NULL
            EXEC sp_executesql @sql
        
        SET @sql = NULL
        
        -- Drop schema
        SET @sql = 'DROP SCHEMA [' + @schema + ']'
        EXEC sp_executesql @sql
        
        FETCH NEXT FROM schema_cursor INTO @schema
    END
    
    CLOSE schema_cursor
    DEALLOCATE schema_cursor
    """
    subprocess.run(
        [
            "docker", "exec", "ggm-mssql",
            "/opt/mssql-tools18/bin/sqlcmd",
            "-S", "localhost", "-U", "sa", "-P", "GGM_Dev123!",
            "-d", "ggm_dev", "-C",
            "-Q", cleanup_sql,
        ],
        capture_output=True,
    )


def clean_mysql_database() -> None:
    """Drop all GGM schemas/databases in MySQL to ensure a clean slate."""
    # MySQL uses databases, not schemas within a database
    # We drop and recreate the ggm_dev database
    subprocess.run(
        [
            "docker", "exec", "ggm-mysql",
            "mysql", "-u", "root", "-pggm_dev",
            "-e", "DROP DATABASE IF EXISTS ggm_dev; CREATE DATABASE ggm_dev;",
        ],
        capture_output=True,
    )


def clean_duckdb_database(project_root: Path) -> None:
    """Delete DuckDB database file to ensure a clean slate."""
    db_file = project_root / "ggm_dev.db"
    if db_file.exists():
        db_file.unlink()
    # Also clean any .wal files
    for wal_file in project_root.glob("ggm_dev.db.*"):
        wal_file.unlink()


def clean_database(dest: str, project_root: Path = None) -> None:
    """Clean the target database based on destination type."""
    if dest == "duckdb":
        if project_root:
            print(f"[dev] Cleaning {dest} database...")
            clean_duckdb_database(project_root)
        return
    
    cleaners = {
        "postgres": clean_postgres_database,
        "mssql": clean_mssql_database,
        "mysql": clean_mysql_database,
    }
    cleaner = cleaners.get(dest)
    if cleaner:
        print(f"[dev] Cleaning {dest} database...")
        cleaner()


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
        choices=["postgres", "mssql", "mysql", "duckdb"],
        help="Target database type (duckdb requires no Docker for target)",
    )
    parser.add_argument(
        "--skip-docker",
        action="store_true",
        help="Skip starting Docker containers",
    )
    parser.add_argument(
        "--no-clean",
        action="store_true",
        help="Skip cleaning database (keep existing data/schemas)",
    )
    args = parser.parse_args()
    
    # DuckDB doesn't need a Docker container for the target
    needs_docker_target = args.dest != "duckdb"
    
    if not args.skip_docker:
        if needs_docker_target:
            print("[dev] Starting Docker (Oracle + %s)..." % args.dest)
            subprocess.run(
                ["docker", "compose", "-f", "docker/docker-compose.yml", 
                 "up", "-d", "oracle", args.dest],
                cwd=project_root,
                check=True,
            )
        else:
            print("[dev] Starting Docker (Oracle only, %s is file-based)..." % args.dest)
            subprocess.run(
                ["docker", "compose", "-f", "docker/docker-compose.yml", 
                 "up", "-d", "oracle"],
                cwd=project_root,
                check=True,
            )
        
        print("[dev] Waiting for Oracle to be ready (can take 2-3 min)...")
        if not wait_for_oracle():
            print("[dev] ERROR: Oracle did not become ready in time")
            sys.exit(1)
        print("[dev] Oracle is ready.")
    
    # Clean target database (drop raw/stg/silver schemas) unless --no-clean
    if not args.no_clean:
        clean_database(args.dest, project_root)
        # Also clean SQLMesh cache to avoid stale state
        cache_dir = project_root / "transform" / ".cache"
        if cache_dir.exists():
            shutil.rmtree(cache_dir)
            print("[dev] Cleaned SQLMesh cache.")
    
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
    env = {
        **os.environ,
        # Oracle source - tables are in 'appuser' schema (Oracle schema = user)
        "SOURCES__SQL_DATABASE__CREDENTIALS": "oracle+oracledb://appuser:apppass@localhost:1521/?service_name=ggm",
        "SOURCES__SQL_DATABASE__SCHEMA": "appuser",  # Tables are in the appuser schema
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
    env["DLT_PROJECT_DIR"] = str(project_root / "ingest")
    subprocess.run(
        [sys.executable, str(project_root / "ingest" / "pipeline.py"), "--dest", args.dest],
        cwd=project_root,
        check=True,
        env=env,
    )
    
    print("[dev] SQLMesh: raw -> stg -> silver...")
    gateway = DESTINATION_TO_GATEWAY.get(args.dest, "local")
    
    sqlmesh_env = {**os.environ}
    subprocess.run(
        _get_sqlmesh_command()
        + ["-p", "transform", "--gateway", gateway, "plan", "--auto-apply"],
        cwd=project_root,
        check=True,
        env=sqlmesh_env,
    )
    
    print(
        "[dev] Done! Run 'uv run sqlmesh -p transform --gateway %s ui' to explore."
        % gateway
    )


if __name__ == "__main__":
    main()
