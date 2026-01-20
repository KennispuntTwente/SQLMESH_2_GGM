"""Scripts package for GGM pipeline validation and utilities.

This package provides validation tools for the GGM data pipeline:

- ddl_parser: Central DDL parsing utilities (dataclasses, type normalization)
- validate_schema: Validates SQLMesh model definitions against GGM DDL
- validate_data: Validates actual database tables against GGM DDL after pipeline runs
- ddl_to_sqlmesh: Converts DDL to SQLMesh model files
"""
# Import from central ddl_parser module
from scripts.ddl_parser import (
    # Dataclasses
    ForeignKeyReference,
    ColumnDefinition,
    TableDefinition,
    TableSchema,
    ModelSchema,
    # Type normalization
    normalize_type,
    # Parsing functions
    parse_ddl_tables,
    parse_ddl_directory,
    parse_ddl_to_table_definitions,
    parse_ddl_directory_to_table_definitions,
    parse_ddl_to_table_schemas,
    parse_ddl_directory_to_table_schemas,
    find_default_ddl_path,
)
from scripts.validate_schema import (
    get_model_columns_with_types,
    get_model_columns_from_sql,
    get_model_schemas,
    validate,
)
from scripts.validate_data import (
    validate_data,
    create_connection,
    get_gateway_config,
    get_database_tables,
    PostgresConnection,
    MSSQLConnection,
    MySQLConnection,
    DuckDBConnection,
)

__all__ = [
    # ddl_parser exports
    "ForeignKeyReference",
    "ColumnDefinition",
    "TableDefinition",
    "TableSchema",
    "ModelSchema",
    "normalize_type",
    "parse_ddl_tables",
    "parse_ddl_directory",
    "parse_ddl_to_table_definitions",
    "parse_ddl_directory_to_table_definitions",
    "parse_ddl_to_table_schemas",
    "parse_ddl_directory_to_table_schemas",
    "find_default_ddl_path",
    # validate_schema exports
    "get_model_columns_with_types",
    "get_model_columns_from_sql",
    "get_model_schemas",
    "validate",
    # validate_data exports
    "validate_data",
    "create_connection",
    "get_gateway_config",
    "get_database_tables",
    "PostgresConnection",
    "MSSQLConnection",
    "MySQLConnection",
    "DuckDBConnection",
]
