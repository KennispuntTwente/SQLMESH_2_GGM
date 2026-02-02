"""Unified configuration for dlt + SQLMesh pipeline.

Single source of truth for all configuration. Reads from environment variables
which can be set directly or loaded from .env file; see .env.example for details.

Configuration precedence:
1. Environment variables (highest priority)
2. .env file in project root
3. Defaults defined here (lowest priority)

This module is imported by:
- ingest/pipeline.py (dlt extraction)
- scripts/pipeline.py (orchestration)
- scripts/dev.py (dev environment)
- transform/config.yaml (SQLMesh, via env_var())
"""

import os
from pathlib import Path
from typing import Any, Dict, Optional

# Lazy-loaded flag to avoid loading .env multiple times
_config_loaded = False


def load_config(env_file: Optional[Path] = None) -> None:
    """Load configuration from .env file.
    
    Call this once at application startup. Safe to call multiple times
    (subsequent calls are no-ops).
    
    Args:
        env_file: Path to .env file. Defaults to project root .env
    """
    global _config_loaded
    if _config_loaded:
        return
    
    try:
        from dotenv import load_dotenv
        
        if env_file is None:
            # Find project root (config/ -> project root)
            project_root = Path(__file__).parent.parent
            env_file = project_root / ".env"
        
        if env_file.exists():
            load_dotenv(env_file, override=False)
        
        _config_loaded = True
    except ImportError:
        # python-dotenv not required if env vars are set directly
        pass


# Auto-load config on import (safe - uses override=False)
load_config()


# =============================================================================
# Helper Functions
# =============================================================================

def _get_env(key: str, default: Optional[str] = None) -> Optional[str]:
    """Get environment variable with fallback."""
    return os.environ.get(key, default)


def _get_env_int(key: str, default: int) -> int:
    """Get integer from environment variable."""
    val = os.environ.get(key)
    if val is None or val == "":
        return default
    try:
        return int(val)
    except ValueError:
        return default


def _get_env_bool(key: str, default: bool = True) -> bool:
    """Get boolean from environment variable."""
    val = os.environ.get(key)
    if val is None or val == "":
        return default
    return val.lower() in ("true", "1", "yes", "on")


# =============================================================================
# Core Configuration
# =============================================================================

# Destination type: postgres, mssql, duckdb, mysql, snowflake, bigquery
DESTINATION = _get_env("GGM_DESTINATION", "postgres")

# Dataset/schema name for raw layer
DATASET = _get_env("GGM_DATASET", "raw")

# SQLMesh gateway (auto-detected from DESTINATION if not set)
GATEWAY = _get_env("GGM_GATEWAY") or DESTINATION

# Optional row limit for test runs
ROW_LIMIT = _get_env_int("GGM_ROW_LIMIT", 0) or None

# dlt extraction backend: auto, sqlalchemy, pyarrow, pandas, connectorx
DLT_BACKEND = _get_env("GGM_DLT_BACKEND", "auto")

# Source schema for Oracle
SOURCE_SCHEMA = _get_env("SOURCES__SQL_DATABASE__SCHEMA", "source")


# =============================================================================
# Trust Certificate (unified for all destinations)
# =============================================================================

def get_trust_server_certificate() -> bool:
    """Get unified trust certificate setting.
    
    Used by both dlt (ODBC connection string) and SQLMesh (pyodbc).
    
    Returns:
        True if server certificates should be trusted (default: True for dev)
    """
    return _get_env_bool("GGM_TRUST_SERVER_CERTIFICATE", True)


def _sync_dlt_trust_certificate() -> None:
    """Sync dlt's MSSQL trust certificate env var from unified setting.
    
    dlt reads DESTINATION__MSSQL__CREDENTIALS__QUERY__TRUSTSERVERCERTIFICATE
    directly for ODBC connection strings. This function automatically sets it
    based on GGM_TRUST_SERVER_CERTIFICATE so users only need to configure one var.
    """
    # Only set if not already explicitly configured
    dlt_var = "DESTINATION__MSSQL__CREDENTIALS__QUERY__TRUSTSERVERCERTIFICATE"
    if dlt_var not in os.environ:
        if get_trust_server_certificate():
            os.environ[dlt_var] = "yes"


# Auto-sync dlt trust certificate on import
_sync_dlt_trust_certificate()


# =============================================================================
# Oracle Configuration
# =============================================================================

def get_oracle_thick_mode() -> bool:
    """Check if Oracle thick mode is enabled."""
    return _get_env_bool("ORACLE_THICK_MODE", False)


def get_oracle_client_lib_dir() -> Optional[str]:
    """Get Oracle Instant Client library directory."""
    return _get_env("ORACLE_CLIENT_LIB_DIR")


def get_oracle_credentials() -> Dict[str, Any]:
    """Get Oracle source connection credentials from environment.
    
    Supports both individual fields and connection string.
    """
    # Check for connection string first
    conn_str = _get_env("SOURCES__SQL_DATABASE__CREDENTIALS")
    if conn_str:
        return {"connection_string": conn_str}
    
    # Fall back to individual fields
    return {
        "host": _get_env("SOURCES__SQL_DATABASE__CREDENTIALS__HOST", "localhost"),
        "port": _get_env_int("SOURCES__SQL_DATABASE__CREDENTIALS__PORT", 1521),
        "database": _get_env("SOURCES__SQL_DATABASE__CREDENTIALS__DATABASE", "FREEPDB1"),
        "username": _get_env("SOURCES__SQL_DATABASE__CREDENTIALS__USERNAME", "system"),
        "password": _get_env("SOURCES__SQL_DATABASE__CREDENTIALS__PASSWORD", ""),
        "drivername": _get_env("SOURCES__SQL_DATABASE__CREDENTIALS__DRIVERNAME", "oracle+oracledb"),
    }


# =============================================================================
# Destination Credentials
# =============================================================================

def get_postgres_credentials() -> Dict[str, Any]:
    """Get PostgreSQL connection credentials from environment."""
    return {
        "host": _get_env("DESTINATION__POSTGRES__CREDENTIALS__HOST", "localhost"),
        "port": _get_env_int("DESTINATION__POSTGRES__CREDENTIALS__PORT", 5432),
        "database": _get_env("DESTINATION__POSTGRES__CREDENTIALS__DATABASE", "ggm_dev"),
        "username": _get_env("DESTINATION__POSTGRES__CREDENTIALS__USERNAME", "ggm"),
        "password": _get_env("DESTINATION__POSTGRES__CREDENTIALS__PASSWORD", ""),
    }


def get_mssql_credentials() -> Dict[str, Any]:
    """Get MSSQL connection credentials from environment."""
    return {
        "host": _get_env("DESTINATION__MSSQL__CREDENTIALS__HOST", "localhost"),
        "port": _get_env_int("DESTINATION__MSSQL__CREDENTIALS__PORT", 1433),
        "database": _get_env("DESTINATION__MSSQL__CREDENTIALS__DATABASE", "ggm_dev"),
        "username": _get_env("DESTINATION__MSSQL__CREDENTIALS__USERNAME", "sa"),
        "password": _get_env("DESTINATION__MSSQL__CREDENTIALS__PASSWORD", ""),
        "driver": _get_env("DESTINATION__MSSQL__CREDENTIALS__DRIVER", "ODBC Driver 18 for SQL Server"),
        "trust_server_certificate": get_trust_server_certificate(),
    }


def get_mysql_credentials() -> Dict[str, Any]:
    """Get MySQL connection credentials from environment.
    
    Note: dlt uses sqlalchemy destination for MySQL.
    """
    return {
        "host": _get_env("DESTINATION__SQLALCHEMY__CREDENTIALS__HOST", "localhost"),
        "port": _get_env_int("DESTINATION__SQLALCHEMY__CREDENTIALS__PORT", 3306),
        "database": _get_env("DESTINATION__SQLALCHEMY__CREDENTIALS__DATABASE", "ggm_dev"),
        "username": _get_env("DESTINATION__SQLALCHEMY__CREDENTIALS__USERNAME", "root"),
        "password": _get_env("DESTINATION__SQLALCHEMY__CREDENTIALS__PASSWORD", ""),
        "drivername": _get_env("DESTINATION__SQLALCHEMY__CREDENTIALS__DRIVERNAME", "mysql+pymysql"),
    }


def get_duckdb_path() -> str:
    """Get DuckDB database file path from environment."""
    return _get_env("DESTINATION__DUCKDB__CREDENTIALS", "ggm_dev.db")


def get_snowflake_credentials() -> Dict[str, Any]:
    """Get Snowflake connection credentials from environment."""
    return {
        "account": _get_env("DESTINATION__SNOWFLAKE__CREDENTIALS__ACCOUNT", ""),
        "username": _get_env("DESTINATION__SNOWFLAKE__CREDENTIALS__USERNAME", ""),
        "password": _get_env("DESTINATION__SNOWFLAKE__CREDENTIALS__PASSWORD", ""),
        "database": _get_env("DESTINATION__SNOWFLAKE__CREDENTIALS__DATABASE", ""),
        "warehouse": _get_env("DESTINATION__SNOWFLAKE__CREDENTIALS__WAREHOUSE", ""),
    }


def get_bigquery_credentials() -> Dict[str, Any]:
    """Get BigQuery connection credentials from environment."""
    return {
        "project_id": _get_env("DESTINATION__BIGQUERY__CREDENTIALS__PROJECT_ID", ""),
        "location": _get_env("DESTINATION__BIGQUERY__CREDENTIALS__LOCATION", "EU"),
    }


def get_credentials_for_destination(dest: Optional[str] = None) -> Dict[str, Any]:
    """Get credentials for the specified or configured destination.
    
    Args:
        dest: Destination name. Uses DESTINATION if not specified.
        
    Returns:
        Dictionary with connection credentials
    """
    dest = (dest or DESTINATION).lower()
    
    if dest == "postgres":
        return get_postgres_credentials()
    elif dest == "mssql":
        return get_mssql_credentials()
    elif dest in ("mysql", "sqlalchemy"):
        return get_mysql_credentials()
    elif dest == "duckdb":
        return {"database": get_duckdb_path()}
    elif dest == "snowflake":
        return get_snowflake_credentials()
    elif dest == "bigquery":
        return get_bigquery_credentials()
    else:
        raise ValueError(f"Unknown destination: {dest}")
