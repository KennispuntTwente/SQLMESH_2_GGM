"""Load synthetic CSVs directly to Postgres raw schema with dlt-like metadata.

This script simulates what dlt does when loading data - it adds the metadata columns
that dlt would normally add: _dlt_load_id and _dlt_load_time.
"""
from __future__ import annotations

import argparse
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text


def load_csvs_to_raw(
    csv_dir: Path,
    schema: str = "raw",
    host: str = "localhost",
    port: int = 5432,
    database: str = "ggm_dev",
    user: str = "ggm",
    password: str = "ggm_dev",
) -> None:
    """Load CSV files to raw schema with dlt-like metadata columns."""
    dsn = f"postgresql://{user}:{password}@{host}:{port}/{database}"
    engine = create_engine(dsn)
    
    # Create schema if not exists
    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
    
    csvs = sorted(csv_dir.glob("*.csv"))
    if not csvs:
        raise FileNotFoundError(f"No CSV files found in {csv_dir}")
    
    # Generate a load_id for this batch (simulates dlt's load_id)
    load_id = str(uuid.uuid4())[:8]
    load_time = datetime.now(timezone.utc).isoformat()
    
    print(f"[load] Load ID: {load_id}")
    print(f"[load] Load Time: {load_time}")
    
    for path in csvs:
        table = path.stem
        df = pd.read_csv(path)
        df.columns = [c.lower() for c in df.columns]
        
        # Add dlt metadata columns (simulates what dlt does)
        df["_dlt_load_id"] = load_id
        df["_dlt_load_time"] = load_time
        
        df.to_sql(table, con=engine, schema=schema, if_exists="replace", index=False)
        print(f"[load] Loaded {schema}.{table} ({len(df)} rows)")
    
    print(f"[load] Done - {len(csvs)} tables loaded to {schema} schema with dlt metadata")


def main() -> None:
    parser = argparse.ArgumentParser(description="Load synthetic CSVs to raw schema with dlt metadata")
    parser.add_argument("--csv-dir", type=Path, default=Path("data/synthetic"))
    parser.add_argument("--schema", default="raw")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=5432)
    parser.add_argument("--database", default="ggm_dev")
    parser.add_argument("--user", default="ggm")
    parser.add_argument("--password", default="ggm_dev")
    args = parser.parse_args()
    
    load_csvs_to_raw(
        csv_dir=args.csv_dir,
        schema=args.schema,
        host=args.host,
        port=args.port,
        database=args.database,
        user=args.user,
        password=args.password,
    )


if __name__ == "__main__":
    main()
