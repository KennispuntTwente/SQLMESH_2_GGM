"""dlt pipeline: source -> raw layer with historical tracking.

Uses dlt's native functionality - _dlt_load_id is added automatically
during the normalize phase. Uses pyarrow for performance.

Configuration:
- Set GGM_DESTINATION env var to change default target
- Or use --dest CLI argument to override
- Set GGM_DATASET env var to change raw dataset/schema (default: raw)
- Or use --dataset CLI argument to override

Oracle Configuration (optional):
- ORACLE_THICK_MODE=1 to enable Oracle Instant Client (thick mode)
- ORACLE_CLIENT_LIB_DIR=/path/to/instantclient for thick mode library path
- TNS_ADMIN=/path/to/tnsnames for TNS resolution
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

# Add parent directory to path to allow imports when running directly
sys.path.insert(0, str(Path(__file__).parent.parent))

# Import dlt library (using full package name to avoid confusion)
import dlt as dlt_lib
from dlt.sources.sql_database import sql_database

# Import local constants
from constants import (
    DEFAULT_DESTINATION,
    DEFAULT_DATASET,
    DLT_DESTINATIONS,
    normalize_dlt_destination,
)


def _init_oracle_thick_mode() -> None:
    """Initialize Oracle thick mode if ORACLE_THICK_MODE=1 is set.
    
    Thick mode is optional and only needed for:
    - Oracle Advanced Security (encryption, Kerberos)
    - External authentication
    - Some legacy features
    """
    if os.environ.get("ORACLE_THICK_MODE", "").lower() in ("1", "true", "yes"):
        try:
            import oracledb
            lib_dir = os.environ.get("ORACLE_CLIENT_LIB_DIR")
            oracledb.init_oracle_client(lib_dir=lib_dir)
            print(f"[oracle] Thick mode enabled (lib_dir={lib_dir})")
        except Exception as e:
            print(f"[oracle] Warning: Failed to init thick mode: {e}")


# Initialize Oracle thick mode if configured
_init_oracle_thick_mode()

# Tables to extract (lowercase for Oracle reflection compatibility)
SOURCE_TABLES = [
    "szclient",
    "wvbesl",
    "wvind_b",
    "wvdos",
    "szukhis",
    "szwerker",
    "szregel",
    "abc_refcod",
]


def get_default_destination() -> str:
    return os.environ.get("GGM_DESTINATION", DEFAULT_DESTINATION)

def get_default_dataset() -> str:
    return os.environ.get("GGM_DATASET", DEFAULT_DATASET)


def run_pipeline(
    destination: str | None = None,
    dataset_name: str | None = None,
) -> dlt_lib.Pipeline:
    """Run dlt pipeline: source -> raw layer."""
    destination = destination or get_default_destination()
    dataset_name = dataset_name or get_default_dataset()
    
    # Normalize destination name (e.g., 'mysql' -> 'sqlalchemy')
    dlt_destination = normalize_dlt_destination(destination)
    
    print(f"[dlt] Source -> Destination: {dlt_destination}.{dataset_name}")
    if dlt_destination != destination:
        print(f"[dlt] Note: '{destination}' normalized to dlt destination '{dlt_destination}'")
    
    pipeline = dlt_lib.pipeline(
        pipeline_name="ggm_raw",
        destination=dlt_destination,
        dataset_name=dataset_name,
    )
    
    source = sql_database(
        table_names=SOURCE_TABLES,
        backend="pyarrow",  # Fast, native
    )
    
    # dlt automatically adds _dlt_load_id during normalize phase
    load_info = pipeline.run(source, write_disposition="append")
    print(load_info)
    return pipeline


def main() -> None:
    parser = argparse.ArgumentParser(description="Run dlt pipeline: source -> raw")
    parser.add_argument(
        "--dest",
        default=None,
        choices=DLT_DESTINATIONS,
        help="Destination (default: $GGM_DESTINATION or postgres)",
    )
    parser.add_argument(
        "--dataset",
        default=None,
        help="Dataset/schema name (default: $GGM_DATASET or raw)",
    )
    args = parser.parse_args()
    run_pipeline(destination=args.dest, dataset_name=args.dataset)


if __name__ == "__main__":
    main()
