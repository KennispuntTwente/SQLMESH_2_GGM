"""Unified configuration module for dlt + SQLMesh.

This module provides a single source of truth for configuration across the
entire pipeline. Both dlt (extraction) and SQLMesh (transform) import from here.
Reads from environment variables which can be set directly or loaded from a
.env file; see .env.example for details.

Usage:
    from config import DESTINATION, DATASET, get_postgres_credentials
    from config import load_config  # Call once at startup to load .env
"""

from .config import (
    # Core config values
    DESTINATION,
    DATASET,
    GATEWAY,
    ROW_LIMIT,
    DLT_BACKEND,
    SOURCE_SCHEMA,
    # Credential getters - source
    get_oracle_credentials,
    get_oracle_thick_mode,
    get_oracle_client_lib_dir,
    # Credential getters - destinations
    get_postgres_credentials,
    get_mssql_credentials,
    get_mysql_credentials,
    get_duckdb_path,
    get_snowflake_credentials,
    get_bigquery_credentials,
    get_credentials_for_destination,
    # Helpers
    get_trust_server_certificate,
    load_config,
)

__all__ = [
    # Core config
    "DESTINATION",
    "DATASET",
    "GATEWAY",
    "ROW_LIMIT",
    "DLT_BACKEND",
    "SOURCE_SCHEMA",
    # Source credentials
    "get_oracle_credentials",
    "get_oracle_thick_mode",
    "get_oracle_client_lib_dir",
    # Destination credentials
    "get_postgres_credentials",
    "get_mssql_credentials",
    "get_mysql_credentials",
    "get_duckdb_path",
    "get_snowflake_credentials",
    "get_bigquery_credentials",
    "get_credentials_for_destination",
    # Helpers
    "get_trust_server_certificate",
    "load_config",
]
