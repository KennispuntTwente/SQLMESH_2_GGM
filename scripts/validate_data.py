"""Validate actual database data against GGM DDL definitions.

This script connects to the target database after pipeline + SQLMesh 
transformations have run, and validates the actual table structure 
against DDL specifications.

Features:
- Supports multiple database types: PostgreSQL, MSSQL, MySQL, DuckDB
- Validates column names and types against DDL
- Can be used in CI for quality control
- Reuses type normalization from validate_schema.py

Usage:
    # Using SQLMesh gateway configuration
    python scripts/validate_data.py --gateway local
    python scripts/validate_data.py --gateway mssql
    
    # With explicit DDL path
    python scripts/validate_data.py --gateway local --ddl-dir ggm/selectie/cssd
    
    # Validate specific schema
    python scripts/validate_data.py --gateway local --schema silver
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any, Protocol, runtime_checkable

# Import shared DDL parsing utilities
from scripts.ddl_parser import (
    normalize_type,
    parse_ddl_tables,
    parse_ddl_directory,
    find_default_ddl_path,
)


# =============================================================================
# Database abstraction layer
# =============================================================================


@runtime_checkable
class DatabaseConnection(Protocol):
    """Protocol for database connections that can fetch table metadata."""
    
    def get_tables(self, schema: str) -> list[str]:
        """Get list of table names in a schema."""
        ...
    
    def get_columns(self, schema: str, table: str) -> dict[str, str]:
        """Get column names and types for a table.
        
        Returns:
            Dictionary mapping uppercase column names to normalized types.
        """
        ...
    
    def close(self) -> None:
        """Close the database connection."""
        ...


class PostgresConnection:
    """PostgreSQL database connection for metadata extraction."""
    
    def __init__(self, connection_config: dict[str, Any]) -> None:
        """Initialize PostgreSQL connection.
        
        Args:
            connection_config: SQLMesh gateway connection configuration
        """
        import psycopg2
        
        self.conn = psycopg2.connect(
            host=connection_config.get("host", "localhost"),
            port=connection_config.get("port", 5432),
            database=connection_config.get("database", "ggm_dev"),
            user=connection_config.get("user", "ggm"),
            password=connection_config.get("password", ""),
        )
    
    def get_tables(self, schema: str) -> list[str]:
        """Get list of table names in a schema."""
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = %s 
                AND table_type = 'BASE TABLE'
                ORDER BY table_name
                """,
                (schema,),
            )
            return [row[0].upper() for row in cur.fetchall()]
    
    def get_columns(self, schema: str, table: str) -> dict[str, str]:
        """Get column names and types for a table."""
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT column_name, data_type, character_maximum_length,
                       numeric_precision, numeric_scale
                FROM information_schema.columns
                WHERE table_schema = %s AND UPPER(table_name) = %s
                ORDER BY ordinal_position
                """,
                (schema, table.upper()),
            )
            columns = {}
            for row in cur.fetchall():
                col_name = row[0].upper()
                data_type = row[1].upper()
                # Normalize the PostgreSQL type
                col_type = _normalize_postgres_type(data_type)
                columns[col_name] = col_type
            return columns
    
    def close(self) -> None:
        """Close the database connection."""
        self.conn.close()


class MSSQLConnection:
    """Microsoft SQL Server database connection for metadata extraction."""
    
    def __init__(self, connection_config: dict[str, Any]) -> None:
        """Initialize MSSQL connection.
        
        Args:
            connection_config: SQLMesh gateway connection configuration
        """
        import os
        
        import pyodbc
        
        host = connection_config.get("host", "localhost")
        port = connection_config.get("port", 1433)
        database = connection_config.get("database", "ggm_dev")
        user = connection_config.get("user", "sa")
        password = connection_config.get("password", "")
        
        # Get ODBC driver from env var, with fallback to auto-detection
        odbc_driver = os.environ.get("MSSQL_ODBC_DRIVER", "")
        trust_cert = os.environ.get("MSSQL_TRUST_CERT", "yes").lower()
        trust_cert_value = "yes" if trust_cert in ("true", "yes", "1") else "no"
        
        if odbc_driver:
            # Use specified driver
            conn_str = (
                f"DRIVER={{{odbc_driver}}};"
                f"SERVER={host},{port};"
                f"DATABASE={database};"
                f"UID={user};"
                f"PWD={password};"
                f"TrustServerCertificate={trust_cert_value}"
            )
            self.conn = pyodbc.connect(conn_str)
        else:
            # Auto-detect: try ODBC Driver 18 first (newer), then fall back to 17
            for driver_version in [18, 17]:
                driver = f"ODBC Driver {driver_version} for SQL Server"
                conn_str = (
                    f"DRIVER={{{driver}}};"
                    f"SERVER={host},{port};"
                    f"DATABASE={database};"
                    f"UID={user};"
                    f"PWD={password};"
                    f"TrustServerCertificate={trust_cert_value}"
                )
                try:
                    self.conn = pyodbc.connect(conn_str)
                    break
                except pyodbc.Error:
                    if driver_version == 17:
                        raise  # Re-raise if both drivers failed
    
    def get_tables(self, schema: str) -> list[str]:
        """Get list of table names in a schema."""
        cursor = self.conn.cursor()
        cursor.execute(
            """
            SELECT TABLE_NAME 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = ? 
            AND TABLE_TYPE = 'BASE TABLE'
            ORDER BY TABLE_NAME
            """,
            (schema,),
        )
        tables = [row[0].upper() for row in cursor.fetchall()]
        cursor.close()
        return tables
    
    def get_columns(self, schema: str, table: str) -> dict[str, str]:
        """Get column names and types for a table."""
        cursor = self.conn.cursor()
        cursor.execute(
            """
            SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH,
                   NUMERIC_PRECISION, NUMERIC_SCALE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = ? AND UPPER(TABLE_NAME) = ?
            ORDER BY ORDINAL_POSITION
            """,
            (schema, table.upper()),
        )
        columns = {}
        for row in cursor.fetchall():
            col_name = row[0].upper()
            data_type = row[1].upper()
            # Normalize the MSSQL type
            col_type = _normalize_mssql_type(data_type)
            columns[col_name] = col_type
        cursor.close()
        return columns
    
    def close(self) -> None:
        """Close the database connection."""
        self.conn.close()


class MySQLConnection:
    """MySQL database connection for metadata extraction."""
    
    def __init__(self, connection_config: dict[str, Any]) -> None:
        """Initialize MySQL connection.
        
        Args:
            connection_config: SQLMesh gateway connection configuration
        """
        import pymysql
        
        self.conn = pymysql.connect(
            host=connection_config.get("host", "localhost"),
            port=connection_config.get("port", 3306),
            database=connection_config.get("database", "ggm_dev"),
            user=connection_config.get("user", "root"),
            password=connection_config.get("password", ""),
        )
    
    def get_tables(self, schema: str) -> list[str]:
        """Get list of table names in a schema (database in MySQL terms)."""
        with self.conn.cursor() as cur:
            # In MySQL, schema and database are the same concept
            cur.execute(
                """
                SELECT TABLE_NAME 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_SCHEMA = %s 
                AND TABLE_TYPE = 'BASE TABLE'
                ORDER BY TABLE_NAME
                """,
                (schema,),
            )
            return [row[0].upper() for row in cur.fetchall()]
    
    def get_columns(self, schema: str, table: str) -> dict[str, str]:
        """Get column names and types for a table."""
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH,
                       NUMERIC_PRECISION, NUMERIC_SCALE
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = %s AND UPPER(TABLE_NAME) = %s
                ORDER BY ORDINAL_POSITION
                """,
                (schema, table.upper()),
            )
            columns = {}
            for row in cur.fetchall():
                col_name = row[0].upper()
                data_type = row[1].upper()
                col_type = _normalize_mysql_type(data_type)
                columns[col_name] = col_type
            return columns
    
    def close(self) -> None:
        """Close the database connection."""
        self.conn.close()


class DuckDBConnection:
    """DuckDB database connection for metadata extraction."""
    
    def __init__(self, connection_config: dict[str, Any]) -> None:
        """Initialize DuckDB connection.
        
        Args:
            connection_config: SQLMesh gateway connection configuration
        """
        import duckdb
        
        database = connection_config.get("database", "ggm_dev.db")
        self.conn = duckdb.connect(database)
    
    def get_tables(self, schema: str) -> list[str]:
        """Get list of table names in a schema."""
        result = self.conn.execute(
            """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = ? 
            AND table_type = 'BASE TABLE'
            ORDER BY table_name
            """,
            (schema,),
        )
        return [row[0].upper() for row in result.fetchall()]
    
    def get_columns(self, schema: str, table: str) -> dict[str, str]:
        """Get column names and types for a table."""
        result = self.conn.execute(
            """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = ? AND UPPER(table_name) = ?
            ORDER BY ordinal_position
            """,
            (schema, table.upper()),
        )
        columns = {}
        for row in result.fetchall():
            col_name = row[0].upper()
            data_type = row[1].upper()
            col_type = _normalize_duckdb_type(data_type)
            columns[col_name] = col_type
        return columns
    
    def close(self) -> None:
        """Close the database connection."""
        self.conn.close()


# =============================================================================
# Type normalization helpers for each database
# =============================================================================


def _normalize_postgres_type(data_type: str) -> str:
    """Normalize PostgreSQL data types to canonical forms.
    
    Args:
        data_type: PostgreSQL data type from information_schema
        
    Returns:
        Normalized type string
    """
    type_map = {
        # String types
        "CHARACTER VARYING": "VARCHAR",
        "CHARACTER": "VARCHAR",
        "TEXT": "VARCHAR",
        "BPCHAR": "VARCHAR",
        # Integer types
        "INTEGER": "INTEGER",
        "INT": "INTEGER",
        "SMALLINT": "INTEGER",
        "BIGINT": "INTEGER",
        "SERIAL": "INTEGER",
        "BIGSERIAL": "INTEGER",
        "INT2": "INTEGER",
        "INT4": "INTEGER",
        "INT8": "INTEGER",
        # Decimal types
        "NUMERIC": "DECIMAL",
        "DECIMAL": "DECIMAL",
        "REAL": "DECIMAL",
        "DOUBLE PRECISION": "DECIMAL",
        "FLOAT4": "DECIMAL",
        "FLOAT8": "DECIMAL",
        "MONEY": "DECIMAL",
        # Date types
        "DATE": "DATE",
        "TIMESTAMP WITHOUT TIME ZONE": "DATE",
        "TIMESTAMP WITH TIME ZONE": "DATE",
        "TIME WITHOUT TIME ZONE": "DATE",
        "TIME WITH TIME ZONE": "DATE",
        "TIMESTAMPTZ": "DATE",
        "TIMETZ": "DATE",
        # Boolean
        "BOOLEAN": "BOOLEAN",
        "BOOL": "BOOLEAN",
        # UUID
        "UUID": "UUID",
        # JSON
        "JSON": "JSON",
        "JSONB": "JSON",
    }
    return type_map.get(data_type, normalize_type(data_type))


def _normalize_mssql_type(data_type: str) -> str:
    """Normalize MSSQL data types to canonical forms.
    
    Args:
        data_type: MSSQL data type from INFORMATION_SCHEMA
        
    Returns:
        Normalized type string
    """
    type_map = {
        # String types
        "VARCHAR": "VARCHAR",
        "NVARCHAR": "VARCHAR",
        "CHAR": "VARCHAR",
        "NCHAR": "VARCHAR",
        "TEXT": "VARCHAR",
        "NTEXT": "VARCHAR",
        # Integer types
        "INT": "INTEGER",
        "INTEGER": "INTEGER",
        "SMALLINT": "INTEGER",
        "BIGINT": "INTEGER",
        "TINYINT": "INTEGER",
        # Decimal types
        "DECIMAL": "DECIMAL",
        "NUMERIC": "DECIMAL",
        "FLOAT": "DECIMAL",
        "REAL": "DECIMAL",
        "MONEY": "DECIMAL",
        "SMALLMONEY": "DECIMAL",
        # Date types
        "DATE": "DATE",
        "DATETIME": "DATE",
        "DATETIME2": "DATE",
        "SMALLDATETIME": "DATE",
        "DATETIMEOFFSET": "DATE",
        "TIME": "DATE",
        # Boolean
        "BIT": "BOOLEAN",
        # Binary
        "VARBINARY": "BINARY",
        "BINARY": "BINARY",
        "IMAGE": "BINARY",
        # Unique identifier
        "UNIQUEIDENTIFIER": "UUID",
    }
    return type_map.get(data_type, normalize_type(data_type))


def _normalize_mysql_type(data_type: str) -> str:
    """Normalize MySQL data types to canonical forms.
    
    Args:
        data_type: MySQL data type from information_schema
        
    Returns:
        Normalized type string
    """
    type_map = {
        # String types
        "VARCHAR": "VARCHAR",
        "CHAR": "VARCHAR",
        "TEXT": "VARCHAR",
        "TINYTEXT": "VARCHAR",
        "MEDIUMTEXT": "VARCHAR",
        "LONGTEXT": "VARCHAR",
        # Integer types
        "INT": "INTEGER",
        "INTEGER": "INTEGER",
        "SMALLINT": "INTEGER",
        "BIGINT": "INTEGER",
        "TINYINT": "INTEGER",
        "MEDIUMINT": "INTEGER",
        # Decimal types
        "DECIMAL": "DECIMAL",
        "NUMERIC": "DECIMAL",
        "FLOAT": "DECIMAL",
        "DOUBLE": "DECIMAL",
        # Date types
        "DATE": "DATE",
        "DATETIME": "DATE",
        "TIMESTAMP": "DATE",
        "TIME": "DATE",
        "YEAR": "DATE",
        # Boolean (MySQL uses TINYINT(1) for boolean, but may report TINYINT)
        # Already handled in integer types
        # Binary
        "BLOB": "BINARY",
        "TINYBLOB": "BINARY",
        "MEDIUMBLOB": "BINARY",
        "LONGBLOB": "BINARY",
        "VARBINARY": "BINARY",
        "BINARY": "BINARY",
        # JSON
        "JSON": "JSON",
    }
    return type_map.get(data_type, normalize_type(data_type))


def _normalize_duckdb_type(data_type: str) -> str:
    """Normalize DuckDB data types to canonical forms.
    
    Args:
        data_type: DuckDB data type from information_schema
        
    Returns:
        Normalized type string
    """
    type_map = {
        # String types
        "VARCHAR": "VARCHAR",
        "TEXT": "VARCHAR",
        "STRING": "VARCHAR",
        # Integer types
        "INTEGER": "INTEGER",
        "INT": "INTEGER",
        "SMALLINT": "INTEGER",
        "BIGINT": "INTEGER",
        "TINYINT": "INTEGER",
        "HUGEINT": "INTEGER",
        "UTINYINT": "INTEGER",
        "USMALLINT": "INTEGER",
        "UINTEGER": "INTEGER",
        "UBIGINT": "INTEGER",
        # Decimal types
        "DECIMAL": "DECIMAL",
        "NUMERIC": "DECIMAL",
        "FLOAT": "DECIMAL",
        "DOUBLE": "DECIMAL",
        "REAL": "DECIMAL",
        # Date types
        "DATE": "DATE",
        "TIMESTAMP": "DATE",
        "TIMESTAMP WITH TIME ZONE": "DATE",
        "TIME": "DATE",
        "INTERVAL": "DATE",
        # Boolean
        "BOOLEAN": "BOOLEAN",
        "BOOL": "BOOLEAN",
        # UUID
        "UUID": "UUID",
        # Binary
        "BLOB": "BINARY",
        # JSON
        "JSON": "JSON",
    }
    return type_map.get(data_type, normalize_type(data_type))


# =============================================================================
# Connection factory
# =============================================================================


def create_connection(db_type: str, connection_config: dict[str, Any]) -> DatabaseConnection:
    """Create a database connection based on type.
    
    Args:
        db_type: Database type (postgres, mssql, mysql, duckdb)
        connection_config: Gateway connection configuration
        
    Returns:
        DatabaseConnection instance
        
    Raises:
        ValueError: If database type is not supported
    """
    db_type_lower = db_type.lower()
    
    if db_type_lower in ("postgres", "postgresql"):
        return PostgresConnection(connection_config)
    elif db_type_lower == "mssql":
        return MSSQLConnection(connection_config)
    elif db_type_lower == "mysql":
        return MySQLConnection(connection_config)
    elif db_type_lower == "duckdb":
        return DuckDBConnection(connection_config)
    else:
        raise ValueError(f"Unsupported database type: {db_type}")


def get_gateway_config(gateway_name: str) -> tuple[str, dict[str, Any]]:
    """Get database type and connection config from SQLMesh gateway.
    
    Args:
        gateway_name: Name of the gateway in config.yaml
        
    Returns:
        Tuple of (db_type, connection_config)
        
    Raises:
        ValueError: If gateway is not found or invalid
    """
    import os
    import re
    from pathlib import Path
    
    import yaml
    
    # Find config.yaml in sqlmesh/ directory
    project_root = Path(__file__).parent.parent
    config_path = project_root / "sqlmesh" / "config.yaml"
    
    if not config_path.exists():
        raise ValueError(f"SQLMesh config not found: {config_path}")
    
    content = config_path.read_text(encoding="utf-8")
    
    # Handle Jinja2-style env_var substitutions
    def replace_env_var(match: re.Match) -> str:
        groups = match.groups()
        var_name = groups[0]
        default_value = groups[1] if len(groups) > 1 else ""
        return os.environ.get(var_name, default_value.strip("'\"") if default_value else "")
    
    # Pattern: {{ env_var('VAR_NAME', 'default') }} or {{ env_var('VAR_NAME') }}
    pattern = r"\{\{\s*env_var\s*\(\s*'([^']+)'(?:\s*,\s*([^)]+))?\s*\)\s*\}\}"
    content = re.sub(pattern, replace_env_var, content)
    
    config = yaml.safe_load(content)
    
    gateways = config.get("gateways", {})
    if gateway_name not in gateways:
        raise ValueError(f"Gateway '{gateway_name}' not found in config.yaml")
    
    gateway = gateways[gateway_name]
    connection = gateway.get("connection", {})
    db_type = connection.get("type", "")
    
    if not db_type:
        raise ValueError(f"No connection type specified for gateway '{gateway_name}'")
    
    return db_type, connection


# =============================================================================
# Main validation logic
# =============================================================================


def get_database_tables(
    db_connection: DatabaseConnection,
    schema: str,
) -> dict[str, dict[str, str]]:
    """Get all table definitions from the database.
    
    Args:
        db_connection: Database connection instance
        schema: Schema name to query
        
    Returns:
        Dictionary mapping uppercase table names to column definitions
    """
    tables: dict[str, dict[str, str]] = {}
    
    table_names = db_connection.get_tables(schema)
    for table_name in table_names:
        columns = db_connection.get_columns(schema, table_name)
        if columns:
            tables[table_name] = columns
    
    return tables


def validate_data(
    gateway: str | None = None,
    db_type: str | None = None,
    connection_config: dict[str, Any] | None = None,
    ddl_path: Path | None = None,
    ddl_dir: Path | None = None,
    schema: str = "silver",
) -> bool:
    """Validate database tables against GGM DDL.
    
    This function:
    1. Connects to the target database
    2. Gets actual table definitions from the database
    3. Parses DDL files for expected definitions
    4. Validates that database tables match DDL specifications
    
    Args:
        gateway: SQLMesh gateway name (e.g., "local", "mssql")
        db_type: Explicit database type (alternative to gateway)
        connection_config: Explicit connection config (alternative to gateway)
        ddl_path: Path to a specific DDL file
        ddl_dir: Path to directory containing DDL files
        schema: Database schema to validate (default: "silver")
        
    Returns:
        True if validation passes, False if there are mismatches
    """
    project_root = Path(__file__).parent.parent
    
    # Get database connection
    if gateway:
        try:
            resolved_db_type, resolved_config = get_gateway_config(gateway)
        except Exception as e:
            print(f"[validate_data] ERROR: Could not load gateway config: {e}")
            return False
    elif db_type and connection_config:
        resolved_db_type = db_type
        resolved_config = connection_config
    else:
        print("[validate_data] ERROR: Must specify either --gateway or both --db-type and connection config")
        return False
    
    # Connect to database
    try:
        print(f"[validate_data] Connecting to {resolved_db_type} database...")
        db_conn = create_connection(resolved_db_type, resolved_config)
    except Exception as e:
        print(f"[validate_data] ERROR: Could not connect to database: {e}")
        return False
    
    try:
        # Get tables from database
        print(f"[validate_data] Fetching tables from schema '{schema}'...")
        db_tables = get_database_tables(db_conn, schema)
        
        if not db_tables:
            print(f"[validate_data] WARNING: No tables found in schema '{schema}'")
            return True  # Not an error, just nothing to validate
        
        print(f"[validate_data] Found {len(db_tables)} tables in database:")
        for table_name in sorted(db_tables.keys()):
            print(f"  - {table_name} ({len(db_tables[table_name])} columns)")
        
        # Get DDL definitions
        if ddl_path:
            if not ddl_path.exists():
                print(f"[validate_data] ERROR: DDL file not found: {ddl_path}")
                return False
            ggm_tables = parse_ddl_tables(ddl_path)
            ddl_source = str(ddl_path)
        elif ddl_dir:
            if not ddl_dir.exists():
                print(f"[validate_data] ERROR: DDL directory not found: {ddl_dir}")
                return False
            ggm_tables = parse_ddl_directory(ddl_dir)
            ddl_source = str(ddl_dir)
        else:
            # Auto-discover DDL location
            default_ddl = find_default_ddl_path(project_root)
            if default_ddl is None:
                print("[validate_data] ERROR: No DDL files found. Use --ddl or --ddl-dir to specify location.")
                return False
            ggm_tables = parse_ddl_directory(default_ddl)
            ddl_source = str(default_ddl)
        
        if not ggm_tables:
            print(f"[validate_data] ERROR: No tables found in DDL: {ddl_source}")
            return False
        
        print(f"[validate_data] Found {len(ggm_tables)} GGM tables in DDL: {ddl_source}")
        
        # Track validation results
        has_errors = False
        validated_tables: set[str] = set()
        
        # Validate: Each DDL table should have a corresponding database table
        for ggm_table, ddl_cols in ggm_tables.items():
            if ggm_table not in db_tables:
                # This is fine - not all DDL tables need to be in the database
                continue
            
            validated_tables.add(ggm_table)
            db_cols = db_tables[ggm_table]
            
            # Check column names
            ddl_names = set(ddl_cols.keys())
            db_names = set(db_cols.keys())
            
            missing = ddl_names - db_names
            extra = db_names - ddl_names
            
            # Check column types for matching columns
            type_mismatches = []
            for col in ddl_names & db_names:
                ddl_type = ddl_cols[col]
                db_type_val = db_cols[col]
                if ddl_type != db_type_val and db_type_val != "UNKNOWN":
                    type_mismatches.append((col, ddl_type, db_type_val))
            
            if missing or extra or type_mismatches:
                print(f"[validate_data] ERROR: {ggm_table} has mismatches")
                if missing:
                    print(f"  Missing columns (in DDL but not in DB): {sorted(missing)}")
                if extra:
                    print(f"  Extra columns (in DB but not in DDL): {sorted(extra)}")
                for col, expected, actual in type_mismatches:
                    print(f"  Type mismatch: {col} (DDL: {expected}, DB: {actual})")
                has_errors = True
            else:
                print(f"[validate_data] OK: {ggm_table} ({len(db_cols)} columns, types verified)")
        
        # Warn about database tables without DDL definition (non-fatal)
        extra_tables = set(db_tables.keys()) - set(ggm_tables.keys())
        if extra_tables:
            print("[validate_data] INFO: Database tables without DDL definition (not validated):")
            for table_name in sorted(extra_tables):
                print(f"  - {table_name}")
        
        # Summary
        print()
        if has_errors:
            print("[validate_data] FAILED: Column or type mismatches found")
            print("::error::Database schema validation failed - see above for details")
        elif validated_tables:
            print(f"[validate_data] PASSED: {len(validated_tables)} tables validated successfully")
        else:
            print("[validate_data] INFO: No matching tables found to validate")
        
        return not has_errors
        
    finally:
        db_conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Validate database tables against GGM DDL definitions",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Validate using SQLMesh gateway config
    python scripts/validate_data.py --gateway local
    python scripts/validate_data.py --gateway mssql
    
    # Specify DDL source
    python scripts/validate_data.py --gateway local --ddl-dir ggm/selectie/cssd
    
    # Validate specific schema
    python scripts/validate_data.py --gateway local --schema silver
        """
    )
    parser.add_argument(
        "--gateway",
        type=str,
        help="SQLMesh gateway name from config.yaml (e.g., local, mssql)"
    )
    parser.add_argument(
        "--ddl",
        type=Path,
        help="Path to a specific DDL file to validate against"
    )
    parser.add_argument(
        "--ddl-dir",
        type=Path,
        help="Path to directory containing DDL files"
    )
    parser.add_argument(
        "--schema",
        default="silver",
        help="Database schema to validate (default: silver)"
    )
    args = parser.parse_args()
    
    if args.ddl and args.ddl_dir:
        print("[validate_data] ERROR: Cannot specify both --ddl and --ddl-dir")
        sys.exit(1)
    
    if not args.gateway:
        print("[validate_data] ERROR: --gateway is required")
        sys.exit(1)
    
    success = validate_data(
        gateway=args.gateway,
        ddl_path=args.ddl,
        ddl_dir=args.ddl_dir,
        schema=args.schema,
    )
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
