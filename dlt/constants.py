"""Central configuration constants for the GGM pipeline.

All destination and gateway options are defined here to ensure consistency
across the codebase.
"""

from __future__ import annotations

# ============================================================================
# DLT Destinations
# ============================================================================
# Supported dlt destinations for data extraction.
# See: https://dlthub.com/docs/dlt-ecosystem/destinations/
#
# Note: For MySQL, dlt uses the 'sqlalchemy' destination type.
# The 'mysql' entry here is for user convenience - it maps to 'sqlalchemy' internally.
DLT_DESTINATIONS = [
    "postgres",
    "mssql",
    "duckdb",
    "mysql",  # User-friendly alias → maps to 'sqlalchemy' for dlt
    "sqlalchemy",  # Generic SQLAlchemy (used for MySQL)
    "snowflake",
    "bigquery",
    "redshift",
    "databricks",
    "athena",
]

# ============================================================================
# SQLMesh Gateways
# ============================================================================
# Available gateways for SQLMesh (must match config.yaml).
# See: https://sqlmesh.readthedocs.io/en/stable/guides/connections/
SQLMESH_GATEWAYS = [
    "local",  # Default PostgreSQL gateway
    "mssql",
    "mysql",
    "duckdb",
    "snowflake",
    "bigquery",
    "redshift",
    "databricks",
    "trino",
]

# ============================================================================
# Destination to Gateway Mapping
# ============================================================================
# Maps dlt destinations to SQLMesh gateways for auto-detection.
DESTINATION_TO_GATEWAY = {
    "postgres": "postgres",  # PostgreSQL gateway
    "mssql": "mssql",
    "mysql": "mysql",
    "sqlalchemy": "mysql",  # dlt's sqlalchemy destination → SQLMesh mysql gateway
    "duckdb": "duckdb",
    "snowflake": "snowflake",
    "bigquery": "bigquery",
    "redshift": "redshift",
    "databricks": "databricks",
}

# ============================================================================
# DLT Destination Normalization
# ============================================================================
# Maps user-friendly destination names to dlt's actual destination types.
# For example, 'mysql' is user-friendly but dlt uses 'sqlalchemy'.
DLT_DESTINATION_ALIASES = {
    "mysql": "sqlalchemy",  # dlt uses sqlalchemy for MySQL connections
}


def normalize_dlt_destination(destination: str) -> str:
    """Convert user-friendly destination name to dlt's actual destination type."""
    return DLT_DESTINATION_ALIASES.get(destination, destination)


# ============================================================================
# Defaults
# ============================================================================
DEFAULT_DESTINATION = "postgres"
DEFAULT_GATEWAY = "local"
DEFAULT_DATASET = "raw"
