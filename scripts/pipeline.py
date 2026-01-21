#!/usr/bin/env python
"""Run the full GGM pipeline: source -> dlt -> SQLMesh -> done.

A simple one-liner to run the complete pipeline with configurable options.
Supports environment variables and command line arguments.

This script runs everything in a single Python process (no subprocess spawning)
for maximum compatibility across environments.

Usage:
    uv run pipeline --dest postgres                    # Full pipeline to PostgreSQL
    uv run pipeline --dest snowflake --gateway snowflake  # Snowflake
    uv run pipeline --dest postgres --skip-dlt         # SQLMesh only
    uv run pipeline --dest postgres --skip-sqlmesh     # dlt only
    uv run pipeline --dest postgres --dry-run          # Preview what would run

Environment variables (or set in .env):
    GGM_GATEWAY      - SQLMesh gateway (default: matches --dest where possible)
    GGM_DATASET      - Dataset/schema name for raw layer (default: raw)
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

# Load .env file if it exists
try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass  # python-dotenv not required if env vars are set directly

# Add dlt folder to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "dlt"))

# Import constants
from constants import (
    DLT_DESTINATIONS,
    SQLMESH_GATEWAYS,
    DESTINATION_TO_GATEWAY,
    DEFAULT_GATEWAY,
    DEFAULT_DATASET,
)


def get_env(key: str, default: str | None) -> str | None:
    """Get environment variable with fallback."""
    return os.environ.get(key, default)


def _detect_gateway(destination: str) -> str:
    """Auto-detect SQLMesh gateway from dlt destination."""
    return DESTINATION_TO_GATEWAY.get(destination, DEFAULT_GATEWAY)


def run_dlt(destination: str, dataset: str, dry_run: bool, verbose: bool) -> bool:
    """Run dlt pipeline: source -> raw layer.

    Returns True on success, False on failure.
    """
    print(f"\n{'=' * 60}")
    print(f"  dlt: Extracting source -> {destination}.{dataset}")
    print(f"{'=' * 60}\n")

    if dry_run:
        print("[dry-run] Would run dlt pipeline")
        print(f"[dry-run]   destination: {destination}")
        print(f"[dry-run]   dataset: {dataset}")
        return True

    # Set DLT_PROJECT_DIR so dlt finds its .dlt/ config
    dlt_dir = Path(__file__).parent.parent / "dlt"
    os.environ["DLT_PROJECT_DIR"] = str(dlt_dir)

    try:
        # Import and run dlt pipeline directly (no subprocess)
        from pipeline import run_pipeline as dlt_run_pipeline

        if verbose:
            print(f"[dlt] Running pipeline: {destination}.{dataset}")

        dlt_run_pipeline(destination=destination, dataset_name=dataset)
        return True
    except Exception as e:
        print(f"\n[!] dlt pipeline failed: {e}")
        if verbose:
            import traceback

            traceback.print_exc()
        return False


def run_sqlmesh(gateway: str, auto_apply: bool, dry_run: bool, verbose: bool) -> bool:
    """Run SQLMesh: raw -> stg -> silver.

    Returns True on success, False on failure.
    """
    print(f"\n{'=' * 60}")
    print(f"  SQLMesh: Transforming raw -> stg -> silver (gateway: {gateway})")
    print(f"{'=' * 60}\n")

    if dry_run:
        print("[dry-run] Would run SQLMesh plan")
        print(f"[dry-run]   gateway: {gateway}")
        print(f"[dry-run]   auto_apply: {auto_apply}")
        return True

    try:
        # Import SQLMesh Context and run directly (no subprocess)
        from sqlmesh import Context

        sqlmesh_dir = Path(__file__).parent.parent / "sqlmesh"

        if verbose:
            print(f"[sqlmesh] Loading context from: {sqlmesh_dir}")
            print(f"[sqlmesh] Gateway: {gateway}")

        # Create SQLMesh context with specified gateway
        ctx = Context(paths=str(sqlmesh_dir), gateway=gateway)

        if verbose:
            print("[sqlmesh] Running plan...")

        # Run plan with auto_apply setting
        ctx.plan(auto_apply=auto_apply)

        return True
    except Exception as e:
        print(f"\n[!] SQLMesh failed: {e}")
        if verbose:
            import traceback

            traceback.print_exc()
        return False


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run the full GGM pipeline: source -> dlt -> SQLMesh",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    uv run pipeline --dest postgres           # Full pipeline to PostgreSQL
    uv run pipeline --dest snowflake          # Full pipeline to Snowflake
    uv run pipeline --dest mssql --skip-dlt   # SQLMesh only (MSSQL)
    uv run pipeline --dest postgres --dry-run # Preview commands
        """,
    )

    # Destination/target options
    parser.add_argument(
        "-d",
        "--dest",
        "--destination",
        required=True,
        choices=DLT_DESTINATIONS,
        metavar="DEST",
        help=f"dlt destination (required). Choices: {', '.join(DLT_DESTINATIONS)}",
    )
    parser.add_argument(
        "-g",
        "--gateway",
        default=None,
        choices=SQLMESH_GATEWAYS,
        metavar="GW",
        help=f"SQLMesh gateway (default: auto-detected from --dest). "
        f"Choices: {', '.join(SQLMESH_GATEWAYS)}",
    )
    parser.add_argument(
        "--dataset",
        default=None,
        metavar="NAME",
        help="Dataset/schema name for raw layer (env: GGM_DATASET, default: raw)",
    )

    # Skip options
    parser.add_argument(
        "--skip-dlt",
        action="store_true",
        help="Skip dlt extraction (only run SQLMesh)",
    )
    parser.add_argument(
        "--skip-sqlmesh",
        action="store_true",
        help="Skip SQLMesh transformations (only run dlt)",
    )

    # SQLMesh options
    parser.add_argument(
        "--no-auto-apply",
        action="store_true",
        help="Don't auto-apply SQLMesh plan (interactive mode)",
    )

    # General options
    parser.add_argument(
        "-n",
        "--dry-run",
        action="store_true",
        help="Show what would be executed without running",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Show detailed output",
    )

    args = parser.parse_args()

    # Resolve configuration
    destination = args.dest  # Required, always set
    # Auto-detect gateway from destination if not specified
    gateway = (
        args.gateway or get_env("GGM_GATEWAY", None) or _detect_gateway(destination)
    )
    dataset = args.dataset or get_env("GGM_DATASET", DEFAULT_DATASET)
    auto_apply = not args.no_auto_apply

    # Print configuration
    print("\n" + "=" * 60)
    print("  GGM Pipeline")
    print("=" * 60)
    print(f"  Destination : {destination}")
    print(f"  Gateway     : {gateway}")
    print(f"  Dataset     : {dataset}")
    print(f"  dlt         : {'skip' if args.skip_dlt else 'run'}")
    print(f"  SQLMesh     : {'skip' if args.skip_sqlmesh else 'run'}")
    print(f"  Auto-apply  : {auto_apply}")
    print("=" * 60)

    if args.skip_dlt and args.skip_sqlmesh:
        print("\n[!] Nothing to do (both --skip-dlt and --skip-sqlmesh specified)")
        return 0

    # Run dlt
    if not args.skip_dlt:
        success = run_dlt(destination, dataset, args.dry_run, args.verbose)
        if not success:
            print("\n[!] dlt failed")
            return 1

    # Run SQLMesh
    if not args.skip_sqlmesh:
        success = run_sqlmesh(gateway, auto_apply, args.dry_run, args.verbose)
        if not success:
            print("\n[!] SQLMesh failed")
            return 1

    # Success
    print("\n" + "=" * 60)
    print("  Pipeline complete!")
    print("=" * 60)
    print(f"\n  Explore with: uv run sqlmesh -p sqlmesh --gateway {gateway} ui")
    print(
        f'  Query data:   uv run sqlmesh -p sqlmesh --gateway {gateway} fetchdf "SELECT * FROM silver.client LIMIT 10"'
    )
    print()

    return 0


if __name__ == "__main__":
    sys.exit(main())
