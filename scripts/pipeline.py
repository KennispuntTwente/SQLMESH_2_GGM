#!/usr/bin/env python
"""Run the full GGM pipeline: source -> dlt -> SQLMesh -> done.

A simple one-liner to run the complete pipeline with configurable options.
All configuration is read from environment variables via the config module.

Usage:
    uv run pipeline --dest postgres                    # Full pipeline to PostgreSQL
    uv run pipeline --dest snowflake --gateway snowflake  # Snowflake
    uv run pipeline --dest postgres --skip-dlt         # SQLMesh only
    uv run pipeline --dest postgres --skip-sqlmesh     # dlt only
    uv run pipeline --dest postgres --dry-run          # Preview what would run

See .env.example for the full list of configuration options.
"""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

# Load unified config (reads .env)
from config import (
    load_config,
    DESTINATION,
    DATASET,
    GATEWAY,
    DLT_BACKEND,
    ROW_LIMIT,
)

load_config()

# Import constants
sys.path.insert(0, str(Path(__file__).parent.parent / "dlt"))
from constants import (
    DLT_DESTINATIONS,
    DLT_BACKENDS,
    SQLMESH_GATEWAYS,
    get_gateway_for_destination,
)


def _get_python_command() -> list[str]:
    """Get the best Python command for spawning subprocesses.

    Preference order:
    1) Current interpreter when already in a venv (most reliable, avoids wrappers)
    2) `uv run python` if uv is available (bootstrap from any shell)
    3) Current interpreter as last resort
    """
    in_venv = bool(os.environ.get("VIRTUAL_ENV")) or sys.prefix != sys.base_prefix
    if in_venv:
        return [sys.executable]

    uv = shutil.which("uv")
    if uv:
        return [uv, "run", "python"]

    return [sys.executable]


def _get_sqlmesh_command() -> list[str]:
    """Get the best SQLMesh command for spawning subprocesses.

    Uses sqlmesh CLI directly to avoid local 'sqlmesh/' directory shadowing the package
    when using 'python -m sqlmesh'.

    Preference order:
    1) `uv run sqlmesh` if uv is available (ensures correct venv)
    2) `sqlmesh` CLI directly if in venv
    """
    uv = shutil.which("uv")
    if uv:
        return [uv, "run", "sqlmesh"]

    # Fall back to direct sqlmesh command if in venv
    sqlmesh = shutil.which("sqlmesh")
    if sqlmesh:
        return [sqlmesh]

    # Last resort: try running it anyway
    return ["sqlmesh"]


def run_command(cmd: list[str], dry_run: bool = False, verbose: bool = False) -> int:
    """Run a command, optionally in dry-run mode."""
    cmd_str = " ".join(cmd)
    if dry_run:
        print(f"[dry-run] Would run: {cmd_str}")
        return 0

    if verbose:
        print(f"[run] {cmd_str}")

    result = subprocess.run(cmd)
    return result.returncode


def run_dlt(
    destination: str,
    dataset: str,
    backend: str | None,
    row_limit: int | None,
    dry_run: bool,
    verbose: bool,
) -> int:
    """Run dlt pipeline: source -> raw layer."""
    print(f"\n{'=' * 60}")
    print(f"  dlt: Extracting source -> {destination}.{dataset}")
    print(f"{'=' * 60}\n")

    dlt_dir = Path(__file__).parent.parent / "dlt"
    python_cmd = _get_python_command()
    cmd = [
        *python_cmd,
        str(dlt_dir / "pipeline.py"),
        "--dest",
        destination,
        "--dataset",
        dataset,
    ]
    if backend:
        cmd.extend(["--backend", backend])
    if row_limit:
        cmd.extend(["--row-limit", str(row_limit)])

    if dry_run:
        cmd_str = " ".join(cmd)
        print(f"[dry-run] Would run: {cmd_str}")
        print(f"[dry-run] With DLT_PROJECT_DIR={dlt_dir}")
        return 0

    if verbose:
        print(f"[run] {' '.join(cmd)}")

    # Set DLT_PROJECT_DIR so dlt finds its .dlt/ config
    env = {**os.environ, "DLT_PROJECT_DIR": str(dlt_dir)}
    result = subprocess.run(cmd, env=env)
    return result.returncode


def run_sqlmesh(
    gateway: str,
    auto_apply: bool,
    restate_models: list[str] | None = None,
    dry_run: bool = False,
    verbose: bool = False,
) -> int:
    """Run SQLMesh: raw -> stg -> silver.

    Args:
        gateway: SQLMesh gateway to use
        auto_apply: If True, auto-apply the plan without prompting
        restate_models: List of model patterns to restate. Defaults to ["raw.*"]
            to ensure stg/silver refresh on new data loads. Pass empty list to skip.
        dry_run: If True, only show what would be executed
        verbose: If True, show detailed output
    """
    # Default to restating raw.* to ensure stg/silver refresh on new data
    if restate_models is None:
        restate_models = ["raw.*"]

    print(f"\n{'=' * 60}")
    print(f"  SQLMesh: Transforming raw -> stg -> silver (gateway: {gateway})")
    if restate_models:
        print(f"  Restating: {', '.join(restate_models)}")
    print(f"{'=' * 60}\n")

    # Use sqlmesh CLI directly to avoid local 'sqlmesh/' directory shadowing the package
    cmd = _get_sqlmesh_command() + ["-p", "sqlmesh", "--gateway", gateway, "plan"]
    if auto_apply:
        cmd.append("--auto-apply")

    # Add restate-model flags for each pattern
    # This triggers cascading backfill for downstream models (stg, silver)
    if restate_models:
        for model in restate_models:
            cmd.extend(["--restate-model", model])

    return run_command(cmd, dry_run=dry_run, verbose=verbose)


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
        default=None,
        choices=DLT_DESTINATIONS,
        metavar="DEST",
        help=f"dlt destination (default: {DESTINATION}). Choices: {', '.join(DLT_DESTINATIONS)}",
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
        help=f"Dataset/schema name for raw layer (default: {DATASET})",
    )
    parser.add_argument(
        "--dlt-backend",
        default=None,
        choices=DLT_BACKENDS,
        metavar="BACKEND",
        help=f"dlt extraction backend (default: {DLT_BACKEND})",
    )
    parser.add_argument(
        "--row-limit",
        type=int,
        default=None,
        metavar="N",
        help="Limit rows per table for test runs",
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
    parser.add_argument(
        "--restate-raw",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Restate raw.* external models to refresh stg/silver (default: True)",
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

    # Resolve configuration from args > config module
    destination = args.dest or DESTINATION
    gateway = args.gateway or GATEWAY or get_gateway_for_destination(destination)
    dataset = args.dataset or DATASET
    dlt_backend = args.dlt_backend or DLT_BACKEND
    row_limit = args.row_limit if args.row_limit is not None else ROW_LIMIT
    auto_apply = not args.no_auto_apply
    # Restate raw.* by default (unless --no-restate-raw is passed)
    # This ensures stg/silver are refreshed when new data is loaded to raw
    restate_raw = args.restate_raw

    # Print configuration
    print("\n" + "=" * 60)
    print("  GGM Pipeline")
    print("=" * 60)
    print(f"  Destination : {destination}")
    print(f"  Gateway     : {gateway}")
    print(f"  Dataset     : {dataset}")
    print(f"  dlt backend : {dlt_backend}")
    print(f"  dlt         : {'skip' if args.skip_dlt else 'run'}")
    print(f"  SQLMesh     : {'skip' if args.skip_sqlmesh else 'run'}")
    print(f"  Auto-apply  : {auto_apply}")
    print(f"  Restate raw : {restate_raw}")
    if row_limit:
        print(f"  Row limit   : {row_limit}")
    print("=" * 60)

    if args.skip_dlt and args.skip_sqlmesh:
        print("\n[!] Nothing to do (both --skip-dlt and --skip-sqlmesh specified)")
        return 0

    # Run dlt
    if not args.skip_dlt:
        rc = run_dlt(destination, dataset, dlt_backend, row_limit, args.dry_run, args.verbose)
        if rc != 0:
            print(f"\n[!] dlt failed with exit code {rc}")
            return rc

    # Run SQLMesh
    if not args.skip_sqlmesh:
        # Determine which models to restate
        # Restating external models (raw.*) triggers cascading backfill of stg/silver
        # Pass empty list to explicitly disable restatement (None uses default of raw.*)
        restate_models = ["raw.*"] if restate_raw else []
        rc = run_sqlmesh(
            gateway, auto_apply, restate_models, args.dry_run, args.verbose
        )
        if rc != 0:
            print(f"\n[!] SQLMesh failed with exit code {rc}")
            return rc

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
