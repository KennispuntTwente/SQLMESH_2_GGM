"""Convert GGM PostgreSQL DDL to SQLMesh silver models.

This script parses CREATE TABLE statements from DDL files and generates
SQLMesh model files with proper column definitions and NULL placeholders.

Features:
- Parses PostgreSQL DDL (CREATE TABLE statements)
- Extracts table/column descriptions from COMMENT ON statements
- Detects PRIMARY KEY for grains configuration
- Parses FOREIGN KEY for references configuration
- Generates SQLMesh model files with FULL kind
- Creates placeholder SELECT with proper CAST types
- Supports single file or directory input
- Dry-run mode to preview generated models

Usage:
    python scripts/ddl_to_sqlmesh.py --ddl path/to/ddl.sql --output-dir sqlmesh/models/silver
    python scripts/ddl_to_sqlmesh.py --ddl-dir path/to/ddl/folder --output-dir sqlmesh/models/silver
    python scripts/ddl_to_sqlmesh.py --ddl path/to/ddl.sql --dry-run  # Preview output
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Optional

# Import shared DDL parsing utilities
from scripts.ddl_parser import (
    # Dataclasses
    ForeignKeyReference,
    ColumnDefinition,
    TableDefinition,
    # Type normalization
    normalize_type,
    # Parsing functions (private, exposed for backwards compat)
    parse_comment_statements as _parse_comment_statements,
    parse_foreign_keys as _parse_foreign_keys,
    # Main parsing functions
    parse_ddl_to_table_definitions as parse_ddl_to_tables,
    parse_ddl_directory_to_table_definitions as parse_ddl_directory_to_tables,
    find_default_ddl_path,
)

def format_sql_type(raw_type: str) -> str:
    """Format DDL type for SQLMesh CAST expression.
    
    Preserves precision/length from the raw DDL type.
    
    Args:
        raw_type: Original DDL type (e.g., VARCHAR(255), DECIMAL(10,2))
        
    Returns:
        Type suitable for CAST expression
    """
    # Clean up the type string
    type_str = raw_type.strip().upper()
    
    # Map some types to SQLMesh-compatible versions
    if type_str.startswith("VARCHAR2"):
        type_str = type_str.replace("VARCHAR2", "VARCHAR")
    
    return type_str


def _escape_sql_string(s: str) -> str:
    """Escape a string for use in SQL single quotes."""
    return s.replace("'", "''")


def generate_sqlmesh_model(
    table: TableDefinition,
    schema: str = "silver",
    source_table: Optional[str] = None,
    include_comments: bool = True,
    include_description: bool = True,
    include_grains: bool = True,
    include_references: bool = True,
    include_column_descriptions: bool = True,
) -> str:
    """Generate SQLMesh model SQL from a table definition.
    
    Creates a placeholder model that selects NULL values with proper CAST types.
    The user should then customize the model to map actual source columns.
    
    Args:
        table: TableDefinition with columns to include
        schema: Target schema name (default: "silver")
        source_table: Optional source table reference for FROM clause
        include_comments: Whether to include helpful SQL comments
        include_description: Whether to include table description in MODEL block
        include_grains: Whether to include grains from primary keys
        include_references: Whether to include references from foreign keys
        include_column_descriptions: Whether to include column_descriptions
        
    Returns:
        SQLMesh model SQL as a string
    """
    lines = []
    
    # Gather optional properties
    description = table.get_description_from_source() if include_description else None
    grains = table.grains if include_grains else []
    references = table.reference_columns if include_references else []
    col_descriptions = table.column_descriptions if include_column_descriptions else {}
    
    # MODEL block
    lines.append("MODEL (")
    lines.append(f"    name {schema}.{table.model_name},")
    lines.append("    kind FULL,")
    
    # Add description
    if description:
        escaped_desc = _escape_sql_string(description)
        lines.append(f"    description '{escaped_desc}',")
    
    # Add grains (primary key columns)
    if grains:
        if len(grains) == 1:
            lines.append(f"    grains {grains[0]},")
        else:
            grains_str = ", ".join(grains)
            lines.append(f"    grains ({grains_str}),")
    
    # Add references (foreign key columns)
    if references:
        if len(references) == 1:
            lines.append(f"    references {references[0]},")
        else:
            refs_str = ", ".join(references)
            lines.append(f"    references ({refs_str}),")
    
    # Add column descriptions
    if col_descriptions:
        lines.append("    column_descriptions (")
        col_desc_items = list(col_descriptions.items())
        for i, (col_name, desc) in enumerate(col_desc_items):
            escaped_desc = _escape_sql_string(desc)
            comma = "," if i < len(col_desc_items) - 1 else ""
            lines.append(f"        {col_name} = '{escaped_desc}'{comma}")
        lines.append("    ),")
    
    # Remove trailing comma from last property line
    if lines[-1].endswith(","):
        lines[-1] = lines[-1][:-1]
    
    lines.append(");")
    lines.append("")
    
    # Optional SQL comments
    if include_comments:
        lines.append(f"-- Transformatie bron -> {schema} {table.name} (GGM-schema)")
        lines.append("-- TODO: Vervang NULL-placeholders door daadwerkelijke bronkolomkoppelingen")
        if table.source_file:
            lines.append(f"-- Bron DDL: {Path(table.source_file).name}")
        lines.append("")
    
    # SELECT statement
    lines.append("SELECT")
    
    # Generate column expressions
    column_lines = []
    for i, col in enumerate(table.columns):
        sql_type = format_sql_type(col.raw_type)
        
        # Format: CAST(NULL AS TYPE) AS column_name
        expr = f"    CAST(NULL AS {sql_type}) AS {col.name}"
        
        # Add comma except for last column
        if i < len(table.columns) - 1:
            expr += ","
        
        # Add primary key indicator in comment
        if col.is_primary_key:
            expr += "  -- PRIMARY KEY"
        
        column_lines.append(expr)
    
    lines.extend(column_lines)
    
    # FROM clause placeholder
    if source_table:
        lines.append(f"FROM {source_table}")
    elif include_comments:
        lines.append("-- FROM stg.brontabel  -- TODO: Specificeer brontabel")
    else:
        lines.append("FROM stg.source_table")
    
    return "\n".join(lines) + "\n"


def generate_model_with_source_mapping(
    table: TableDefinition,
    schema: str = "silver",
    source_table: str = "stg.source_table",
    column_mappings: Optional[dict[str, str]] = None,
    include_description: bool = True,
    include_grains: bool = True,
    include_references: bool = True,
    include_column_descriptions: bool = True,
) -> str:
    """Generate SQLMesh model with source column mappings.
    
    More sophisticated generator that can map source columns to target columns.
    
    Args:
        table: TableDefinition with columns to include
        schema: Target schema name (default: "silver")
        source_table: Source table reference
        column_mappings: Dict mapping target column names to source expressions
        include_description: Whether to include table description
        include_grains: Whether to include grains from primary keys
        include_references: Whether to include references from foreign keys
        include_column_descriptions: Whether to include column_descriptions
        
    Returns:
        SQLMesh model SQL as a string
    """
    if column_mappings is None:
        column_mappings = {}
    
    # Gather optional properties
    description = table.get_description_from_source() if include_description else None
    grains = table.grains if include_grains else []
    references = table.reference_columns if include_references else []
    col_descriptions = table.column_descriptions if include_column_descriptions else {}
    
    lines = []
    
    # MODEL block
    lines.append("MODEL (")
    lines.append(f"    name {schema}.{table.model_name},")
    lines.append("    kind FULL,")
    
    # Add description
    if description:
        escaped_desc = _escape_sql_string(description)
        lines.append(f"    description '{escaped_desc}',")
    
    # Add grains (primary key columns)
    if grains:
        if len(grains) == 1:
            lines.append(f"    grains {grains[0]},")
        else:
            grains_str = ", ".join(grains)
            lines.append(f"    grains ({grains_str}),")
    
    # Add references (foreign key columns)
    if references:
        if len(references) == 1:
            lines.append(f"    references {references[0]},")
        else:
            refs_str = ", ".join(references)
            lines.append(f"    references ({refs_str}),")
    
    # Add column descriptions
    if col_descriptions:
        lines.append("    column_descriptions (")
        col_desc_items = list(col_descriptions.items())
        for i, (col_name, desc) in enumerate(col_desc_items):
            escaped_desc = _escape_sql_string(desc)
            comma = "," if i < len(col_desc_items) - 1 else ""
            lines.append(f"        {col_name} = '{escaped_desc}'{comma}")
        lines.append("    ),")
    
    # Remove trailing comma from last property line
    if lines[-1].endswith(","):
        lines[-1] = lines[-1][:-1]
    
    lines.append(");")
    lines.append("")
    
    lines.append(f"-- Transform {source_table} -> {schema}.{table.model_name} (GGM schema)")
    lines.append("-- Explicit casts to match GGM DDL types")
    lines.append("")
    
    # SELECT statement
    lines.append("SELECT")
    
    # Generate column expressions
    for i, col in enumerate(table.columns):
        sql_type = format_sql_type(col.raw_type)
        col_name = col.name.lower()
        
        # Check if we have a mapping for this column
        if col_name in column_mappings:
            source_expr = column_mappings[col_name]
            expr = f"    CAST({source_expr} AS {sql_type}) AS {col_name}"
        else:
            # Default to NULL placeholder
            expr = f"    CAST(NULL AS {sql_type}) AS {col_name}"
        
        # Add comma except for last column
        if i < len(table.columns) - 1:
            expr += ","
        
        # Add primary key indicator in comment
        if col.is_primary_key:
            expr += "  -- PRIMARY KEY"
        
        lines.append(expr)
    
    # FROM clause
    lines.append(f"FROM {source_table}")
    
    return "\n".join(lines) + "\n"


def write_model_file(
    table: TableDefinition,
    output_dir: Path,
    schema: str = "silver",
    overwrite: bool = False,
) -> Path:
    """Write SQLMesh model to file.
    
    Args:
        table: TableDefinition to generate model for
        output_dir: Directory to write model file
        schema: Target schema name
        overwrite: Whether to overwrite existing files
        
    Returns:
        Path to written file
        
    Raises:
        FileExistsError: If file exists and overwrite=False
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    
    file_name = f"{table.model_name}.sql"
    file_path = output_dir / file_name
    
    if file_path.exists() and not overwrite:
        raise FileExistsError(f"Model file already exists: {file_path}")
    
    model_sql = generate_sqlmesh_model(table, schema=schema)
    file_path.write_text(model_sql, encoding="utf-8")
    
    return file_path


def convert_ddl_to_models(
    ddl_path: Optional[Path] = None,
    ddl_dir: Optional[Path] = None,
    output_dir: Optional[Path] = None,
    schema: str = "silver",
    dry_run: bool = False,
    overwrite: bool = False,
    tables_filter: Optional[list[str]] = None,
) -> dict[str, str]:
    """Convert DDL files to SQLMesh models.
    
    Main entry point for DDL conversion.
    
    Args:
        ddl_path: Path to a specific DDL file (mutually exclusive with ddl_dir)
        ddl_dir: Path to directory containing DDL files
        output_dir: Directory to write model files (required unless dry_run)
        schema: Target schema name (default: "silver")
        dry_run: If True, return generated SQL without writing files
        overwrite: Whether to overwrite existing files
        tables_filter: Optional list of table names to include (case-insensitive)
        
    Returns:
        Dictionary mapping table names to generated SQL
        
    Raises:
        ValueError: If neither ddl_path nor ddl_dir is provided
        ValueError: If output_dir not provided and not dry_run
    """
    if not ddl_path and not ddl_dir:
        raise ValueError("Must provide either ddl_path or ddl_dir")
    
    if not dry_run and not output_dir:
        raise ValueError("Must provide output_dir when not in dry_run mode")
    
    # Parse DDL
    if ddl_path:
        tables = parse_ddl_to_tables(ddl_path)
    else:
        tables = parse_ddl_directory_to_tables(ddl_dir)  # type: ignore
    
    if not tables:
        print("[ddl_to_sqlmesh] WARN: No tables found in DDL")
        return {}
    
    # Filter tables if requested
    if tables_filter:
        filter_set = {t.upper() for t in tables_filter}
        tables = [t for t in tables if t.name.upper() in filter_set]
    
    results: dict[str, str] = {}
    
    for table in tables:
        model_sql = generate_sqlmesh_model(table, schema=schema)
        results[table.name] = model_sql
        
        if not dry_run:
            try:
                file_path = write_model_file(
                    table, output_dir, schema=schema, overwrite=overwrite  # type: ignore
                )
                print(f"[ddl_to_sqlmesh] Created: {file_path}")
            except FileExistsError as e:
                print(f"[ddl_to_sqlmesh] SKIP: {e} (use --overwrite to replace)")
        else:
            print(f"[ddl_to_sqlmesh] Would create: {table.model_name}.sql")
            print("-" * 60)
            print(model_sql)
            print()
    
    return results


def main() -> None:
    """Command-line interface for DDL to SQLMesh conversion."""
    parser = argparse.ArgumentParser(
        description="Convert GGM PostgreSQL DDL to SQLMesh silver models",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Convert a single DDL file
    python scripts/ddl_to_sqlmesh.py --ddl path/to/schema.sql --output-dir sqlmesh/models/silver
    
    # Convert all DDL files in a directory
    python scripts/ddl_to_sqlmesh.py --ddl-dir path/to/ddl/ --output-dir sqlmesh/models/silver
    
    # Preview generated models without writing
    python scripts/ddl_to_sqlmesh.py --ddl path/to/schema.sql --dry-run
    
    # Convert specific tables only
    python scripts/ddl_to_sqlmesh.py --ddl path/to/schema.sql --output-dir sqlmesh/models/silver --tables BESCHIKKING CLIENT
    
    # Auto-discover DDL files and generate models
    python scripts/ddl_to_sqlmesh.py --output-dir sqlmesh/models/silver
        """
    )
    parser.add_argument(
        "--ddl",
        type=Path,
        help="Path to a specific DDL file"
    )
    parser.add_argument(
        "--ddl-dir",
        type=Path,
        help="Path to directory containing DDL files"
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        help="Directory to write SQLMesh model files"
    )
    parser.add_argument(
        "--schema",
        default="silver",
        help="Target schema name (default: silver)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview generated models without writing files"
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing model files"
    )
    parser.add_argument(
        "--tables",
        nargs="+",
        help="Specific table names to convert (case-insensitive)"
    )
    args = parser.parse_args()
    
    # Determine DDL source
    ddl_path = args.ddl
    ddl_dir = args.ddl_dir
    
    if not ddl_path and not ddl_dir:
        # Auto-discover DDL location
        project_root = Path(__file__).parent.parent
        default_ddl = find_default_ddl_path(project_root)
        if default_ddl:
            ddl_dir = default_ddl
            print(f"[ddl_to_sqlmesh] Auto-discovered DDL: {ddl_dir}")
        else:
            print("[ddl_to_sqlmesh] ERROR: No DDL files found. Use --ddl or --ddl-dir")
            sys.exit(1)
    
    if ddl_path and ddl_dir:
        print("[ddl_to_sqlmesh] ERROR: Cannot specify both --ddl and --ddl-dir")
        sys.exit(1)
    
    if not args.dry_run and not args.output_dir:
        print("[ddl_to_sqlmesh] ERROR: Must specify --output-dir or use --dry-run")
        sys.exit(1)
    
    try:
        results = convert_ddl_to_models(
            ddl_path=ddl_path,
            ddl_dir=ddl_dir,
            output_dir=args.output_dir,
            schema=args.schema,
            dry_run=args.dry_run,
            overwrite=args.overwrite,
            tables_filter=args.tables,
        )
        
        print()
        print(f"[ddl_to_sqlmesh] Processed {len(results)} table(s)")
        
    except Exception as e:
        print(f"[ddl_to_sqlmesh] ERROR: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
