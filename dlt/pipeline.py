"""dlt pipeline: source -> raw layer with historical tracking.

Uses dlt's native functionality - _dlt_load_id is added automatically
during the normalize phase. Uses pyarrow for performance when available.

Configuration:
- All settings are read from environment variables via the config module
- See .env.example for the full list of configuration options

Usage:
    # From command line (standalone):
    python -m dlt.pipeline --dest postgres

    # Or via orchestrator:
    uv run pipeline --dest postgres
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

# Add parent directory to path to allow imports when running directly
sys.path.insert(0, str(Path(__file__).parent.parent))

# Load unified config (reads .env)
from config import (
    load_config,
    DESTINATION,
    DATASET,
    ROW_LIMIT,
    DLT_BACKEND,
    get_oracle_thick_mode,
    get_oracle_client_lib_dir,
)

load_config()

# Import dlt library (using full package name to avoid confusion)
import dlt as dlt_lib
from dlt.sources.sql_database import sql_database

# Import local constants
from constants import (
    DLT_DESTINATIONS,
    DLT_BACKENDS,
    normalize_dlt_destination,
)


def _init_oracle_thick_mode() -> None:
    """Initialize Oracle thick mode if configured.

    Thick mode is optional and only needed for:
    - Oracle Advanced Security (encryption, Kerberos)
    - External authentication
    - Some legacy features
    """
    if get_oracle_thick_mode():
        try:
            import oracledb

            lib_dir = get_oracle_client_lib_dir()
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


def _resolve_backend(backend: str | None) -> str:
    """Resolve dlt sql_database backend with a safe fallback.

    On some systems (especially Windows), PyArrow/other native dependencies can
    fail to load due to missing system runtimes or path-length limitations.
    Using the SQLAlchemy backend is slower but avoids PyArrow.
    """
    requested = (backend or DLT_BACKEND).strip().lower()
    if requested not in DLT_BACKENDS:
        raise ValueError(
            f"Invalid backend '{requested}'. Expected one of: {', '.join(DLT_BACKENDS)}"
        )

    if requested == "sqlalchemy":
        return "sqlalchemy"

    if requested == "auto":
        try:
            import pyarrow  # noqa: F401

            return "pyarrow"
        except Exception as e:
            msg = str(e).strip()
            print(f"[dlt] Warning: PyArrow backend unavailable ({type(e).__name__}: {msg})")
            print("[dlt] Falling back to SQLAlchemy backend (slower, but more compatible).")
            print("[dlt] Fix tips:")
            print(
                "      - Windows: install Microsoft Visual C++ 2015-2022 Redistributable (x64)"
            )
            print("      - Windows: enable long paths or move repo to a shorter path")
            print("      - Override: set GGM_DLT_BACKEND=pyarrow to fail fast")
            return "sqlalchemy"

    if requested == "pyarrow":
        try:
            import pyarrow  # noqa: F401

            return "pyarrow"
        except Exception as e:
            msg = str(e).strip()
            print(
                f"[dlt] ERROR: PyArrow backend requested but unavailable ({type(e).__name__}: {msg})"
            )
            print("[dlt] Fix tips:")
            print(
                "      - Windows: install Microsoft Visual C++ 2015-2022 Redistributable (x64)"
            )
            print("      - Windows: enable long paths or move repo to a shorter path")
            print("      - Workaround: set GGM_DLT_BACKEND=sqlalchemy")
            raise

    if requested == "pandas":
        try:
            import pandas  # noqa: F401

            return "pandas"
        except Exception as e:
            msg = str(e).strip()
            print(
                f"[dlt] ERROR: pandas backend requested but unavailable ({type(e).__name__}: {msg})"
            )
            print("[dlt] Fix tips:")
            print("      - Install pandas (and its dependencies) in your environment")
            print("      - Workaround: set GGM_DLT_BACKEND=sqlalchemy")
            raise

    if requested == "connectorx":
        try:
            import connectorx  # noqa: F401

            return "connectorx"
        except Exception as e:
            msg = str(e).strip()
            print(
                f"[dlt] ERROR: connectorx backend requested but unavailable ({type(e).__name__}: {msg})"
            )
            print("[dlt] Fix tips:")
            print("      - Install connectorx in your environment (e.g. `uv add connectorx`)")
            print("      - Workaround: set GGM_DLT_BACKEND=sqlalchemy")
            raise

    return "sqlalchemy"


def run_pipeline(
    destination: str | None = None,
    dataset_name: str | None = None,
    backend: str | None = None,
    row_limit: int | None = None,
) -> dlt_lib.Pipeline:
    """Run dlt pipeline: source -> raw layer.
    
    Args:
        destination: Target destination (default: from config)
        dataset_name: Schema name for raw layer (default: from config)
        backend: Extraction backend (default: from config)
        row_limit: Optional row limit per table (default: from config)
    """
    # Use config module values as defaults
    destination = destination or DESTINATION
    dataset_name = dataset_name or DATASET
    backend = _resolve_backend(backend)
    row_limit = row_limit if row_limit is not None else ROW_LIMIT

    # Normalize destination name (e.g., 'mysql' -> 'sqlalchemy')
    dlt_destination = normalize_dlt_destination(destination)

    print(f"[dlt] Source -> Destination: {dlt_destination}.{dataset_name}")
    if dlt_destination != destination:
        print(
            f"[dlt] Note: '{destination}' normalized to dlt destination '{dlt_destination}'"
        )

    pipeline = dlt_lib.pipeline(
        pipeline_name="ggm_raw",
        destination=dlt_destination,
        dataset_name=dataset_name,
    )

    # Check for optional row limit (useful for dev/testing)
    if row_limit:
        print(f"[dlt] Row limit enabled: {row_limit} rows per table")

    source = sql_database(
        table_names=SOURCE_TABLES,
        backend=backend,
    )

    # Apply row limit using dlt's native add_limit method
    # count_rows=True ensures we limit by actual rows, not chunks/pages
    if row_limit:
        source = source.add_limit(max_items=row_limit, count_rows=True)

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
        help=f"Destination (default: $GGM_DESTINATION or {DESTINATION})",
    )
    parser.add_argument(
        "--dataset",
        default=None,
        help=f"Dataset/schema name (default: $GGM_DATASET or {DATASET})",
    )
    parser.add_argument(
        "--backend",
        default=None,
        choices=DLT_BACKENDS,
        help=f"Extraction backend (default: $GGM_DLT_BACKEND or {DLT_BACKEND})",
    )
    parser.add_argument(
        "--row-limit",
        type=int,
        default=None,
        help="Limit rows per table for test runs",
    )
    args = parser.parse_args()
    run_pipeline(
        destination=args.dest,
        dataset_name=args.dataset,
        backend=args.backend,
        row_limit=args.row_limit,
    )


if __name__ == "__main__":
    main()
