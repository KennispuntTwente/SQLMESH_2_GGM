"""Central DDL parsing utilities for GGM schema validation and conversion.

This module provides shared dataclasses, parsing functions, and type normalization
used across multiple scripts:
- validate_schema.py: Validates SQLMesh models against DDL
- validate_data.py: Validates database data against DDL
- ddl_to_sqlmesh.py: Converts DDL to SQLMesh models

Features:
- Type normalization for cross-dialect comparison
- DDL parsing for CREATE TABLE, PRIMARY KEY, FOREIGN KEY, COMMENT ON
- Dataclasses for representing table schemas
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import sqlglot
from sqlglot import exp
from sqlglot.expressions import DataType


# =============================================================================
# Dataclasses for DDL representation
# =============================================================================


@dataclass
class ForeignKeyReference:
    """Represents a foreign key reference from DDL."""
    
    column: str  # Local column name
    referenced_table: str  # Referenced table name
    referenced_column: str  # Referenced column name
    
    def __post_init__(self):
        self.column = self.column.lower()
        self.referenced_table = self.referenced_table.lower()
        self.referenced_column = self.referenced_column.lower()


@dataclass
class ColumnDefinition:
    """Represents a column definition from DDL."""
    
    name: str
    data_type: str  # Normalized type (e.g., VARCHAR, INTEGER)
    raw_type: str  # Original DDL type (e.g., VARCHAR(255))
    is_primary_key: bool = False
    is_nullable: bool = True
    description: Optional[str] = None  # Column comment/description
    
    def __post_init__(self):
        # Normalize name to lowercase for SQLMesh model (GGM uses uppercase in DDL)
        self.name = self.name.lower()


@dataclass
class TableDefinition:
    """Represents a table definition from DDL for model generation."""
    
    name: str
    columns: list[ColumnDefinition] = field(default_factory=list)
    source_file: Optional[str] = None
    description: Optional[str] = None  # Table comment/description
    references: list[ForeignKeyReference] = field(default_factory=list)
    
    @property
    def primary_key_columns(self) -> list[ColumnDefinition]:
        """Get primary key columns."""
        return [col for col in self.columns if col.is_primary_key]
    
    @property
    def grains(self) -> list[str]:
        """Get grain columns (primary key column names)."""
        return [col.name for col in self.primary_key_columns]
    
    @property
    def reference_columns(self) -> list[str]:
        """Get columns that reference other tables (for SQLMesh references)."""
        return [ref.column for ref in self.references]
    
    @property
    def model_name(self) -> str:
        """Get SQLMesh model name (lowercase table name)."""
        return self.name.lower()
    
    @property
    def column_descriptions(self) -> dict[str, str]:
        """Get column descriptions as a dictionary."""
        return {
            col.name: col.description
            for col in self.columns
            if col.description
        }
    
    def get_description_from_source(self) -> Optional[str]:
        """Generate description from source file name if no explicit description."""
        if self.description:
            return self.description
        if self.source_file:
            # Extract meaningful name from file path
            # e.g., "Sociaal_Domein_Beschikking_en_Voorziening__Domain_Objects_postgres.sql"
            # -> "Sociaal Domein Beschikking en Voorziening - Domain Objects"
            file_name = Path(self.source_file).stem
            # Remove dialect suffix
            for suffix in ["_postgres", "_mysql", "_mssql", "_oracle"]:
                file_name = file_name.replace(suffix, "")
            # Convert underscores to spaces, double underscore to dash
            file_name = file_name.replace("__", " - ")
            file_name = file_name.replace("_", " ")
            return f"GGM-tabel van {file_name}"
        return None


@dataclass 
class TableSchema:
    """Complete table schema from DDL including all validatable properties."""
    
    name: str
    columns: dict[str, str]  # {column_name: normalized_type}
    primary_keys: list[str] = field(default_factory=list)  # For grains
    foreign_keys: list[ForeignKeyReference] = field(default_factory=list)  # For references
    description: Optional[str] = None
    column_descriptions: dict[str, str] = field(default_factory=dict)
    
    @property
    def grains(self) -> list[str]:
        """Get grain columns (primary key column names, lowercase)."""
        return [pk.lower() for pk in self.primary_keys]
    
    @property
    def reference_columns(self) -> list[str]:
        """Get reference columns (foreign key column names, lowercase)."""
        return [fk.column for fk in self.foreign_keys]


@dataclass
class ModelSchema:
    """Complete model schema from SQLMesh including all validatable properties."""
    
    name: str
    columns: dict[str, str]  # {column_name: normalized_type}
    grains: list[str] = field(default_factory=list)
    references: list[str] = field(default_factory=list)
    description: Optional[str] = None
    column_descriptions: dict[str, str] = field(default_factory=dict)


# =============================================================================
# Type normalization
# =============================================================================


def normalize_type(dtype: DataType | exp.DataType | str | None) -> str:
    """Normalize a data type for comparison.
    
    Maps various SQL types to canonical forms for comparison.
    This enables cross-dialect validation (e.g., Oracle NUMBER == DECIMAL).
    
    Args:
        dtype: A sqlglot DataType, string, or None
        
    Returns:
        Normalized type string (e.g., "VARCHAR", "INTEGER", "DECIMAL")
    """
    if dtype is None:
        return "UNKNOWN"
    
    type_str = str(dtype).upper() if not isinstance(dtype, str) else dtype.upper()
    
    # Extract base type (ignore precision/length)
    # VARCHAR(255) -> VARCHAR, DECIMAL(10,2) -> DECIMAL
    base = type_str.split("(")[0].strip()
    
    # Handle multi-word types like "DOUBLE PRECISION"
    if base.startswith("DOUBLE"):
        base = "DOUBLE"
    
    # Normalize common type aliases to canonical forms
    type_map = {
        # Integer types
        "INT": "INTEGER",
        "BIGINT": "INTEGER",
        "SMALLINT": "INTEGER",
        "TINYINT": "INTEGER",
        "MEDIUMINT": "INTEGER",
        "SERIAL": "INTEGER",
        "BIGSERIAL": "INTEGER",
        # String types
        "TEXT": "VARCHAR",
        "CHAR": "VARCHAR",
        "NVARCHAR": "VARCHAR",
        "NCHAR": "VARCHAR",
        "VARCHAR2": "VARCHAR",
        "CLOB": "VARCHAR",
        "NCLOB": "VARCHAR",
        "LONGTEXT": "VARCHAR",
        "MEDIUMTEXT": "VARCHAR",
        "TINYTEXT": "VARCHAR",
        # Numeric types
        "NUMERIC": "DECIMAL",
        "DOUBLE": "DECIMAL",
        "FLOAT": "DECIMAL",
        "REAL": "DECIMAL",
        "MONEY": "DECIMAL",
        "SMALLMONEY": "DECIMAL",
        "NUMBER": "DECIMAL",
        # Date/time types
        "TIMESTAMP": "DATE",
        "DATETIME": "DATE",
        "DATETIME2": "DATE",
        "TIME": "DATE",
        "TIMESTAMPTZ": "DATE",
    }
    
    return type_map.get(base, base)


# =============================================================================
# DDL parsing helper functions
# =============================================================================


def parse_comment_statements(content: str) -> tuple[dict[str, str], dict[str, dict[str, str]]]:
    """Parse COMMENT ON statements from DDL content.
    
    Args:
        content: DDL file content
        
    Returns:
        Tuple of (table_comments, column_comments) where:
        - table_comments: {TABLE_NAME: description}
        - column_comments: {TABLE_NAME: {COLUMN_NAME: description}}
    """
    table_comments: dict[str, str] = {}
    column_comments: dict[str, dict[str, str]] = {}
    
    # Match: COMMENT ON TABLE [schema.]table_name IS 'description';
    table_comment_pattern = re.compile(
        r"COMMENT\s+ON\s+TABLE\s+(?:[\w]+\.)?(\w+)\s+IS\s+'([^']+)'",
        re.IGNORECASE
    )
    
    # Match: COMMENT ON COLUMN [schema.]table_name.column_name IS 'description';
    column_comment_pattern = re.compile(
        r"COMMENT\s+ON\s+COLUMN\s+(?:[\w]+\.)?(\w+)\.(\w+)\s+IS\s+'([^']+)'",
        re.IGNORECASE
    )
    
    for match in table_comment_pattern.finditer(content):
        table_name = match.group(1).upper()
        description = match.group(2)
        table_comments[table_name] = description
    
    for match in column_comment_pattern.finditer(content):
        table_name = match.group(1).upper()
        column_name = match.group(2).upper()
        description = match.group(3)
        
        if table_name not in column_comments:
            column_comments[table_name] = {}
        column_comments[table_name][column_name] = description
    
    return table_comments, column_comments


def parse_foreign_keys(content: str) -> dict[str, list[ForeignKeyReference]]:
    """Parse ALTER TABLE FOREIGN KEY statements from DDL content.
    
    Handles both active and commented-out FK constraints (for GGM DDL).
    
    Args:
        content: DDL file content
        
    Returns:
        Dictionary mapping table names to lists of foreign key references
    """
    foreign_keys: dict[str, list[ForeignKeyReference]] = {}
    
    # Match: ALTER TABLE table_name ADD CONSTRAINT ... FOREIGN KEY (column) REFERENCES ref_table (ref_column)
    # Also matches commented-out version: -- ALTER TABLE ...
    fk_pattern = re.compile(
        r"(?:--\s*)?ALTER\s+TABLE\s+(\w+)\s+ADD\s+CONSTRAINT\s+\w+\s+"
        r"FOREIGN\s+KEY\s*\((\w+)\)\s+REFERENCES\s+(\w+)\s*\((\w+)\)",
        re.IGNORECASE
    )
    
    for match in fk_pattern.finditer(content):
        table_name = match.group(1).upper()
        column = match.group(2)
        ref_table = match.group(3)
        ref_column = match.group(4)
        
        if table_name not in foreign_keys:
            foreign_keys[table_name] = []
        
        foreign_keys[table_name].append(ForeignKeyReference(
            column=column,
            referenced_table=ref_table,
            referenced_column=ref_column,
        ))
    
    return foreign_keys


def parse_primary_keys(content: str, statement_pks: dict[str, list[str]]) -> dict[str, list[str]]:
    """Parse ALTER TABLE PRIMARY KEY statements from DDL content.
    
    Merges with inline primary keys detected during CREATE TABLE parsing.
    
    Args:
        content: DDL file content
        statement_pks: Primary keys detected inline from CREATE TABLE
        
    Returns:
        Dictionary mapping table names to primary key column lists
    """
    primary_keys = dict(statement_pks)  # Start with inline PKs
    
    # Match: ALTER TABLE table_name ADD PRIMARY KEY (col1, col2, ...);
    pk_pattern = re.compile(
        r"ALTER\s+TABLE\s+(\w+)\s+ADD\s+(?:CONSTRAINT\s+\w+\s+)?PRIMARY\s+KEY\s*\(([^)]+)\)",
        re.IGNORECASE
    )
    
    for match in pk_pattern.finditer(content):
        table_name = match.group(1).upper()
        columns = [col.strip().upper() for col in match.group(2).split(",")]
        
        if table_name not in primary_keys:
            primary_keys[table_name] = columns
        else:
            # Merge (unlikely to have both inline and ALTER TABLE PK)
            for col in columns:
                if col not in primary_keys[table_name]:
                    primary_keys[table_name].append(col)
    
    return primary_keys


def detect_inline_primary_key(col_expr: exp.ColumnDef) -> bool:
    """Detect if a column has an inline PRIMARY KEY constraint.
    
    Args:
        col_expr: sqlglot ColumnDef expression
        
    Returns:
        True if column has PRIMARY KEY constraint
    """
    constraints = col_expr.args.get("constraints", [])
    for constraint in constraints:
        if isinstance(constraint, exp.PrimaryKeyColumnConstraint):
            return True
        # Check for nested constraint expressions
        if hasattr(constraint, "kind") and "primary" in str(constraint.kind).lower():
            return True
        if isinstance(constraint, exp.ColumnConstraint):
            if hasattr(constraint, "this"):
                if isinstance(constraint.this, exp.PrimaryKeyColumnConstraint):
                    return True
    return False


# =============================================================================
# DDL parsing main functions
# =============================================================================


def parse_ddl_tables(ddl_path: Path) -> dict[str, dict[str, str]]:
    """Parse CREATE TABLE statements and extract table -> {column: type} mapping.
    
    Args:
        ddl_path: Path to a SQL file containing CREATE TABLE statements
        
    Returns:
        Dictionary mapping uppercase table names to column definitions,
        where column definitions are {column_name: normalized_type}
        
    Raises:
        FileNotFoundError: If ddl_path does not exist
    """
    if not ddl_path.exists():
        raise FileNotFoundError(f"DDL file not found: {ddl_path}")
    
    tables: dict[str, dict[str, str]] = {}
    content = ddl_path.read_text(encoding="utf-8")
    
    for statement in sqlglot.parse(content, read="postgres"):
        if statement is None:
            continue
        if isinstance(statement, exp.Create) and statement.kind == "TABLE":
            schema = statement.this
            if not isinstance(schema, exp.Schema):
                continue
            
            table_expr = schema.this
            # Handle schema-qualified names (e.g., public.table_name)
            if hasattr(table_expr, "name"):
                table_name = table_expr.name.upper()
            else:
                table_name = str(table_expr).upper().split(".")[-1]
            
            columns: dict[str, str] = {}
            for col_expr in schema.expressions:
                if isinstance(col_expr, exp.ColumnDef):
                    col_name = col_expr.name.upper()
                    col_type = normalize_type(col_expr.args.get("kind"))
                    columns[col_name] = col_type
            
            if columns:
                tables[table_name] = columns
    
    return tables


def parse_ddl_directory(ddl_dir: Path) -> dict[str, dict[str, str]]:
    """Parse all SQL files in a directory and return combined table definitions.
    
    Args:
        ddl_dir: Path to directory containing SQL files
        
    Returns:
        Combined dictionary of all table definitions from all SQL files
    """
    all_tables: dict[str, dict[str, str]] = {}
    
    for sql_file in ddl_dir.glob("**/*.sql"):
        try:
            tables = parse_ddl_tables(sql_file)
            all_tables.update(tables)
        except Exception as e:
            print(f"[ddl_parser] WARN: Could not parse {sql_file}: {e}")
    
    return all_tables


def parse_ddl_to_table_definitions(ddl_path: Path) -> list[TableDefinition]:
    """Parse DDL file and extract detailed table definitions.
    
    This is a more comprehensive parser that extracts:
    - Primary key information for grains
    - Raw types for model generation
    - Table/column descriptions from COMMENT ON statements
    - Foreign key references from ALTER TABLE statements
    
    Args:
        ddl_path: Path to a SQL file containing CREATE TABLE statements
        
    Returns:
        List of TableDefinition objects with column details
        
    Raises:
        FileNotFoundError: If ddl_path does not exist
    """
    if not ddl_path.exists():
        raise FileNotFoundError(f"DDL file not found: {ddl_path}")
    
    tables: list[TableDefinition] = []
    content = ddl_path.read_text(encoding="utf-8")
    
    # Pre-parse comments and foreign keys using regex
    table_comments, column_comments = parse_comment_statements(content)
    foreign_keys = parse_foreign_keys(content)
    
    for statement in sqlglot.parse(content, read="postgres"):
        if statement is None:
            continue
        if isinstance(statement, exp.Create) and statement.kind == "TABLE":
            schema = statement.this
            if not isinstance(schema, exp.Schema):
                continue
            
            table_expr = schema.this
            if hasattr(table_expr, "name"):
                table_name = table_expr.name.upper()
            else:
                table_name = str(table_expr).upper().split(".")[-1]
            
            table_def = TableDefinition(
                name=table_name,
                source_file=str(ddl_path),
                description=table_comments.get(table_name),
                references=foreign_keys.get(table_name, []),
            )
            
            col_comments = column_comments.get(table_name, {})
            
            for col_expr in schema.expressions:
                if isinstance(col_expr, exp.ColumnDef):
                    col_name = col_expr.name
                    col_name_upper = col_name.upper()
                    kind = col_expr.args.get("kind")
                    raw_type = str(kind) if kind else "VARCHAR(255)"
                    normalized_type = normalize_type(kind)
                    
                    is_pk = detect_inline_primary_key(col_expr)
                    
                    table_def.columns.append(ColumnDefinition(
                        name=col_name,
                        data_type=normalized_type,
                        raw_type=raw_type,
                        is_primary_key=is_pk,
                        description=col_comments.get(col_name_upper),
                    ))
            
            if table_def.columns:
                tables.append(table_def)
    
    return tables


def parse_ddl_to_table_schemas(ddl_path: Path) -> dict[str, TableSchema]:
    """Parse DDL file and extract complete table schemas with all properties.
    
    Args:
        ddl_path: Path to a SQL file containing CREATE TABLE statements
        
    Returns:
        Dictionary mapping uppercase table names to TableSchema objects
        
    Raises:
        FileNotFoundError: If ddl_path does not exist
    """
    if not ddl_path.exists():
        raise FileNotFoundError(f"DDL file not found: {ddl_path}")
    
    schemas: dict[str, TableSchema] = {}
    content = ddl_path.read_text(encoding="utf-8")
    
    # Pre-parse comments and foreign keys using regex
    table_comments, column_comments = parse_comment_statements(content)
    foreign_keys = parse_foreign_keys(content)
    
    # Track inline primary keys
    inline_pks: dict[str, list[str]] = {}
    
    for statement in sqlglot.parse(content, read="postgres"):
        if statement is None:
            continue
        if isinstance(statement, exp.Create) and statement.kind == "TABLE":
            schema_node = statement.this
            if not isinstance(schema_node, exp.Schema):
                continue
            
            table_expr = schema_node.this
            if hasattr(table_expr, "name"):
                table_name = table_expr.name.upper()
            else:
                table_name = str(table_expr).upper().split(".")[-1]
            
            columns: dict[str, str] = {}
            pk_columns: list[str] = []
            
            for col_expr in schema_node.expressions:
                if isinstance(col_expr, exp.ColumnDef):
                    col_name = col_expr.name.upper()
                    col_type = normalize_type(col_expr.args.get("kind"))
                    columns[col_name] = col_type
                    
                    if detect_inline_primary_key(col_expr):
                        pk_columns.append(col_name)
            
            if columns:
                if pk_columns:
                    inline_pks[table_name] = pk_columns
                
                schemas[table_name] = TableSchema(
                    name=table_name,
                    columns=columns,
                    primary_keys=pk_columns,
                    foreign_keys=foreign_keys.get(table_name, []),
                    description=table_comments.get(table_name),
                    column_descriptions=column_comments.get(table_name, {}),
                )
    
    # Merge ALTER TABLE primary keys
    all_pks = parse_primary_keys(content, inline_pks)
    for table_name, pk_cols in all_pks.items():
        if table_name in schemas:
            schemas[table_name].primary_keys = pk_cols
    
    return schemas


def parse_ddl_directory_to_table_definitions(ddl_dir: Path) -> list[TableDefinition]:
    """Parse all SQL files in a directory and return table definitions.
    
    Args:
        ddl_dir: Path to directory containing SQL files
        
    Returns:
        List of TableDefinition objects from all SQL files
    """
    all_tables: list[TableDefinition] = []
    
    for sql_file in ddl_dir.glob("**/*.sql"):
        try:
            tables = parse_ddl_to_table_definitions(sql_file)
            all_tables.extend(tables)
        except Exception as e:
            print(f"[ddl_parser] WARN: Could not parse {sql_file}: {e}")
    
    return all_tables


def parse_ddl_directory_to_table_schemas(ddl_dir: Path) -> dict[str, TableSchema]:
    """Parse all SQL files in a directory and return complete table schemas.
    
    Args:
        ddl_dir: Path to directory containing SQL files
        
    Returns:
        Dictionary mapping table names to TableSchema objects
    """
    all_schemas: dict[str, TableSchema] = {}
    
    for sql_file in ddl_dir.glob("**/*.sql"):
        try:
            schemas = parse_ddl_to_table_schemas(sql_file)
            all_schemas.update(schemas)
        except Exception as e:
            print(f"[ddl_parser] WARN: Could not parse {sql_file}: {e}")
    
    return all_schemas


def find_default_ddl_path(project_root: Path) -> Optional[Path]:
    """Find the default GGM DDL file or directory.
    
    Searches for common locations in the project.
    
    Args:
        project_root: Root directory of the project
        
    Returns:
        Path to DDL file/directory if found, None otherwise
    """
    # Look for common DDL locations
    candidates = [
        project_root / "ggm" / "selectie" / "cssd",
        project_root / "ggm" / "selectie",
        project_root / "ggm" / "ddl",
        project_root / "ddl",
    ]
    
    for candidate in candidates:
        if candidate.is_dir() and list(candidate.glob("*.sql")):
            return candidate
    
    return None
