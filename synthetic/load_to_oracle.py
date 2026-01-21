#!/usr/bin/env python3
"""Load synthetic CSV data to Oracle database for dlt extraction.

This script loads the generated synthetic CSVs into an Oracle database
so they can be extracted by the dlt pipeline.

Usage:
    python synthetic/load_to_oracle.py
    python synthetic/load_to_oracle.py --csv-dir data/synthetic --schema SOURCE
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, Numeric, text


def load_csvs_to_oracle(
    csv_dir: Path,
    host: str = "localhost",
    port: int = 1521,
    service_name: str = "ggm",
    user: str = "appuser",
    password: str = "apppass",
) -> None:
    """Load CSV files to Oracle database.

    Args:
        csv_dir: Directory containing CSV files
        host: Oracle host
        port: Oracle port
        service_name: Oracle service name
        user: Oracle user (schema owner)
        password: Oracle password
    """
    # Create connection string
    dsn = f"oracle+oracledb://{user}:{password}@{host}:{port}/?service_name={service_name}"
    engine = create_engine(dsn)

    # Create schema if needed (in Oracle, schema = user)
    # For simplicity, we load into the connected user's schema

    csvs = sorted(csv_dir.glob("*.csv"))
    if not csvs:
        raise FileNotFoundError(f"No CSV files found in {csv_dir}")

    for path in csvs:
        table = path.stem.lower()  # Lowercase for Oracle reflection compatibility
        df = pd.read_csv(path)
        # Lower-case columns (dlt will normalize anyway)
        df.columns = [c.lower() for c in df.columns]

        # Build dtype mapping: use NUMBER for float columns to avoid Oracle FLOAT issues
        dtype_map = {}
        for col in df.columns:
            if df[col].dtype == "float64":
                dtype_map[col] = Numeric(precision=18, scale=2)

        # Drop table if exists (use uppercase for Oracle identifiers)
        table_upper = table.upper()
        with engine.begin() as conn:
            try:
                conn.execute(text(f"DROP TABLE {table_upper}"))
            except Exception:
                pass  # Table doesn't exist

        # Load data - if_exists="append" since we dropped the table above
        # This avoids pandas reflection issues with Oracle
        df.to_sql(table, con=engine, if_exists="append", index=False, dtype=dtype_map)
        print(f"[synthetic] Loaded {table} ({len(df)} rows)")

    print(f"[synthetic] Loaded {len(csvs)} tables to Oracle")


def main() -> None:
    # Load .env file if it exists (unified config with dlt/SQLMesh)
    try:
        from dotenv import load_dotenv

        load_dotenv()
    except ImportError:
        pass  # python-dotenv not required if env vars are set directly

    parser = argparse.ArgumentParser(description="Load synthetic CSVs to Oracle")
    parser.add_argument(
        "--csv-dir",
        type=Path,
        default=Path("data/synthetic"),
        help="Directory containing CSV files",
    )
    # Use dlt native env var pattern for Oracle source credentials
    parser.add_argument(
        "--host",
        default=os.environ.get("SOURCES__SQL_DATABASE__CREDENTIALS__HOST", "localhost"),
    )
    parser.add_argument(
        "--port",
        type=int,
        default=int(os.environ.get("SOURCES__SQL_DATABASE__CREDENTIALS__PORT", "1521")),
    )
    parser.add_argument(
        "--service-name",
        default=os.environ.get("SOURCES__SQL_DATABASE__CREDENTIALS__DATABASE", "ggm"),
    )
    parser.add_argument(
        "--user",
        default=os.environ.get(
            "SOURCES__SQL_DATABASE__CREDENTIALS__USERNAME", "appuser"
        ),
    )
    parser.add_argument(
        "--password",
        default=os.environ.get(
            "SOURCES__SQL_DATABASE__CREDENTIALS__PASSWORD", "apppass"
        ),
    )
    args = parser.parse_args()

    # Generate synthetic data if not exists
    if not args.csv_dir.exists() or not list(args.csv_dir.glob("*.csv")):
        print("[synthetic] No CSVs found, generating synthetic data...")
        from synthetic.generate_synthetic_data import generate, GenConfig

        args.csv_dir.mkdir(parents=True, exist_ok=True)
        generate(args.csv_dir, GenConfig(rows=20, seed=42))

    load_csvs_to_oracle(
        csv_dir=args.csv_dir,
        host=args.host,
        port=args.port,
        service_name=args.service_name,
        user=args.user,
        password=args.password,
    )


if __name__ == "__main__":
    main()
