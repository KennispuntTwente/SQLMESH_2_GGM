#!/usr/bin/env python
"""Run the full GGM pipeline: source -> dlt -> SQLMesh -> done.

A simple one-liner to run the complete pipeline with configurable options.
Supports environment variables and command line arguments.

Usage:
    uv run pipeline --dest postgres                    # Full pipeline to PostgreSQL
    uv run pipeline --dest snowflake --gateway snowflake  # Snowflake
    uv run pipeline --dest postgres --skip-dlt         # SQLMesh only
    uv run pipeline --dest postgres --skip-sqlmesh     # dlt only
    uv run pipeline --dest postgres --dry-run          # Preview what would run

Environment variables (or set in .env):
    GGM_GATEWAY      - SQLMesh gateway override (default: auto-detected from --dest)
    GGM_DATASET      - Dataset/schema name for raw layer (default: raw)
"""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
from pathlib import Path

# Load .env file if it exists
try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass  # python-dotenv not required if env vars are set directly

# Import constants - add dlt folder to path
sys.path.insert(0, str(Path(__file__).parent.parent / "dlt"))
from constants import (
    DLT_DESTINATIONS,
    SQLMESH_GATEWAYS,
    DEFAULT_DATASET,
    get_gateway_for_destination,
)


def get_env(key: str, default: str | None) -> str | None:
    """Get environment variable with fallback."""
    return os.environ.get(key, default)


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


def _detect_gateway(destination: str) -> str:
    """Auto-detect SQLMesh gateway from dlt destination."""
    return get_gateway_for_destination(destination)


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


def run_sqlmesh(gateway: str, auto_apply: bool, dry_run: bool, verbose: bool) -> int:
    """Run SQLMesh: raw -> stg -> silver."""
    print(f"\n{'=' * 60}")
    print(f"  SQLMesh: Transforming raw -> stg -> silver (gateway: {gateway})")
    print(f"{'=' * 60}\n")

    python_cmd = _get_python_command()
    cmd = [*python_cmd, "-m", "sqlmesh", "-p", "sqlmesh", "--gateway", gateway, "plan"]
    if auto_apply:
        cmd.append("--auto-apply")

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
    parser.add_argument(
        "--dlt-backend",
        default=None,
        choices=["auto", "pyarrow", "sqlalchemy"],
        metavar="BACKEND",
        help="dlt extraction backend (env: GGM_DLT_BACKEND, default: auto)",
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
    dlt_backend = args.dlt_backend or get_env("GGM_DLT_BACKEND", "auto")
    auto_apply = not args.no_auto_apply

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
    print("=" * 60)

    if args.skip_dlt and args.skip_sqlmesh:
        print("\n[!] Nothing to do (both --skip-dlt and --skip-sqlmesh specified)")
        return 0

    # Run dlt
    if not args.skip_dlt:
        rc = run_dlt(destination, dataset, dlt_backend, args.dry_run, args.verbose)
        if rc != 0:
            print(f"\n[!] dlt failed with exit code {rc}")
            return rc

    # Run SQLMesh
    if not args.skip_sqlmesh:
        rc = run_sqlmesh(gateway, auto_apply, args.dry_run, args.verbose)
        if rc != 0:
            print(f"\n[!] SQLMesh failed with exit code {rc}")
            return rc

    # Success
    print("\n" + "=" * 60)
    print("  Pipeline complete!")
    print("=" * 60)
    print(
        f"\n  Explore with: uv run sqlmesh -p sqlmesh --gateway {gateway} ui"
    )
    print(
        f'  Query data:   uv run sqlmesh -p sqlmesh --gateway {gateway} fetchdf "SELECT * FROM silver.client LIMIT 10"'
    )
    print()

    return 0


if __name__ == "__main__":
    sys.exit(main())
