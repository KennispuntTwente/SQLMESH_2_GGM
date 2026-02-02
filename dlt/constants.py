"""Central constants for the GGM pipeline.

Pure constants only — no configuration reading here.
All configuration is handled by the config/ module.
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
    "athena",
]

# ============================================================================
# SQLMesh Gateways
# ============================================================================
# Available gateways for SQLMesh (must match config.yaml).
# See: https://sqlmesh.readthedocs.io/en/stable/guides/connections/
SQLMESH_GATEWAYS = [
    "local",  # Default PostgreSQL gateway
    "postgres",
    "mssql",
    "mysql",
    "duckdb",
    "snowflake",
    "bigquery",
    "redshift",
    "trino",
]

# ============================================================================
# DLT Backends
# ============================================================================
# Available backends for dlt sql_database source.
DLT_BACKENDS = ["auto", "sqlalchemy", "pyarrow", "pandas", "connectorx"]

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


def get_gateway_for_destination(destination: str) -> str:
    """Auto-detect SQLMesh gateway from dlt destination."""
    return DESTINATION_TO_GATEWAY.get(destination, destination)
