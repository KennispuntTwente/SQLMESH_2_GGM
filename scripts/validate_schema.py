"""Validate SQLMesh silver models against GGM DDL definitions.

This script compares the column definitions (names and types) in the SQLMesh 
silver models against DDL files to ensure they match.

Uses sqlglot for robust SQL parsing and SQLMesh Context for model introspection.

Features:
- Dynamically discovers tables from DDL files (no hardcoding)
- Validates column names and types by default
- Optionally validates grains (primary keys), references (foreign keys), descriptions
- Warns if silver models exist without DDL definition (non-fatal)
- Fails if column or type mismatches are found

Usage:
    python scripts/validate_schema.py
    python scripts/validate_schema.py --ddl path/to/ddl.sql
    python scripts/validate_schema.py --ddl-dir path/to/ddl/folder
    python scripts/validate_schema.py --validate-grains --validate-references
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Optional

import sqlglot
from sqlglot import exp

# Import shared DDL parsing utilities
from scripts.ddl_parser import (
    # Dataclasses
    ForeignKeyReference,
    TableSchema,
    ModelSchema,
    # Type normalization
    normalize_type,
    # Parsing functions (private, exposed for backwards compat)
    parse_comment_statements as _parse_comment_statements,
    parse_foreign_keys as _parse_foreign_keys,
    parse_primary_keys as _parse_primary_keys,
    # Main parsing functions
    parse_ddl_tables,
    parse_ddl_directory,
    parse_ddl_to_table_schemas as parse_ddl_schemas,
    parse_ddl_directory_to_table_schemas as parse_ddl_directory_schemas,
    find_default_ddl_path,
)


def get_model_columns_with_types(silver_schema: str = "silver") -> dict[str, dict[str, str]]:
    """Get column names and types from SQLMesh silver models via Context.
    
    This uses SQLMesh's built-in model introspection to get accurate
    column types after model compilation.
    
    Args:
        silver_schema: The schema name to filter models (default: "silver")
        
    Returns:
        Dictionary mapping uppercase model names to column definitions,
        where column definitions are {column_name: normalized_type}
    """
    try:
        from sqlmesh import Context
        # Use the sqlmesh/ directory relative to project root
        project_root = Path(__file__).parent.parent
        sqlmesh_path = project_root / "sqlmesh"
        ctx = Context(paths=str(sqlmesh_path))
    except Exception as e:
        print(f"[validate] ERROR: Could not create SQLMesh context: {e}")
        return {}
    
    models: dict[str, dict[str, str]] = {}
    
    for model_name, model in ctx.models.items():
        # Extract table name from fully qualified name
        # "ggm_dev"."silver"."beschikking" -> BESCHIKKING
        parts = model_name.replace('"', '').split('.')
        if len(parts) >= 2 and parts[-2] == silver_schema:
            table_name = parts[-1].upper()
            
            columns: dict[str, str] = {}
            if model.columns_to_types:
                for col, dtype in model.columns_to_types.items():
                    col_name = col.upper()
                    # dtype is a sqlglot DataType
                    col_type = normalize_type(dtype.this.name if hasattr(dtype, 'this') else str(dtype))
                    columns[col_name] = col_type
            
            if columns:
                models[table_name] = columns
    
    return models


def get_model_schemas(silver_schema: str = "silver") -> dict[str, ModelSchema]:
    """Get complete model schemas from SQLMesh models via Context.
    
    Extracts all validatable properties: columns, grains, references, descriptions.
    
    Args:
        silver_schema: The schema name to filter models (default: "silver")
        
    Returns:
        Dictionary mapping uppercase model names to ModelSchema objects
    """
    try:
        from sqlmesh import Context
        project_root = Path(__file__).parent.parent
        sqlmesh_path = project_root / "sqlmesh"
        ctx = Context(paths=str(sqlmesh_path))
    except Exception as e:
        print(f"[validate] ERROR: Could not create SQLMesh context: {e}")
        return {}
    
    schemas: dict[str, ModelSchema] = {}
    
    for model_name, model in ctx.models.items():
        parts = model_name.replace('"', '').split('.')
        if len(parts) >= 2 and parts[-2] == silver_schema:
            table_name = parts[-1].upper()
            
            # Extract columns and types
            columns: dict[str, str] = {}
            if model.columns_to_types:
                for col, dtype in model.columns_to_types.items():
                    col_name = col.upper()
                    col_type = normalize_type(dtype.this.name if hasattr(dtype, 'this') else str(dtype))
                    columns[col_name] = col_type
            
            # Extract grains (primary key columns)
            grains: list[str] = []
            if hasattr(model, 'grains') and model.grains:
                for grain in model.grains:
                    # Grain can be a column expression or tuple of columns
                    if hasattr(grain, 'name'):
                        grains.append(grain.name.upper())
                    elif isinstance(grain, (list, tuple)):
                        for g in grain:
                            if hasattr(g, 'name'):
                                grains.append(g.name.upper())
                    else:
                        grains.append(str(grain).upper())
            
            # Extract references (foreign key columns)
            references: list[str] = []
            if hasattr(model, 'references') and model.references:
                for ref in model.references:
                    if hasattr(ref, 'name'):
                        references.append(ref.name.upper())
                    elif isinstance(ref, (list, tuple)):
                        for r in ref:
                            if hasattr(r, 'name'):
                                references.append(r.name.upper())
                    else:
                        references.append(str(ref).upper())
            
            # Extract description
            description = None
            if hasattr(model, 'description') and model.description:
                description = model.description
            
            # Extract column descriptions
            column_descriptions: dict[str, str] = {}
            if hasattr(model, 'column_descriptions') and model.column_descriptions:
                for col, desc in model.column_descriptions.items():
                    column_descriptions[col.upper()] = desc
            
            if columns:
                schemas[table_name] = ModelSchema(
                    name=table_name,
                    columns=columns,
                    grains=grains,
                    references=references,
                    description=description,
                    column_descriptions=column_descriptions,
                )
    
    return schemas


def get_model_columns_from_sql(model_path: Path) -> list[str]:
    """Extract column names from SQLMesh model file using sqlglot.
    
    This is a fallback when SQLMesh Context is not available.
    Only extracts column names, not types.
    
    Args:
        model_path: Path to a SQLMesh model .sql file
        
    Returns:
        List of uppercase column names
    """
    content = model_path.read_text(encoding="utf-8")
    
    # Remove MODEL block (SQLMesh-specific, not valid SQL)
    model_end = content.find(");")
    if model_end != -1 and "MODEL" in content[:model_end]:
        sql_content = content[model_end + 2:].strip()
    else:
        sql_content = content
    
    columns = []
    try:
        for statement in sqlglot.parse(sql_content, read="postgres"):
            if statement is None:
                continue
            if isinstance(statement, exp.Select):
                for col_expr in statement.expressions:
                    if isinstance(col_expr, exp.Alias):
                        columns.append(col_expr.alias.upper())
                    elif hasattr(col_expr, "name"):
                        columns.append(col_expr.name.upper())
    except Exception as e:
        print(f"[validate] WARN: Could not parse {model_path.name}: {e}")
    
    return columns
def validate(
    ddl_path: Optional[Path] = None,
    ddl_dir: Optional[Path] = None,
    silver_schema: str = "silver",
    validate_grains: bool = False,
    validate_references: bool = False,
    validate_descriptions: bool = False,
    validate_column_descriptions: bool = False,
) -> bool:
    """Validate SQLMesh models against GGM DDL.
    
    This function:
    1. Parses DDL files to discover expected table definitions
    2. Gets actual model definitions from SQLMesh Context
    3. Validates that all DDL tables exist as silver models with matching columns/types
    4. Optionally validates grains, references, and descriptions
    5. Warns (but doesn't fail) if silver models exist without DDL definition
    
    Args:
        ddl_path: Path to a specific DDL file (mutually exclusive with ddl_dir)
        ddl_dir: Path to directory containing DDL files
        silver_schema: Schema name for silver models (default: "silver")
        validate_grains: If True, validate primary keys match model grains
        validate_references: If True, validate foreign keys match model references
        validate_descriptions: If True, validate table descriptions
        validate_column_descriptions: If True, validate column descriptions
        
    Returns:
        True if validation passes, False if there are mismatches
    """
    project_root = Path(__file__).parent.parent
    
    # Determine whether we need enhanced parsing
    needs_enhanced = validate_grains or validate_references or validate_descriptions or validate_column_descriptions
    
    # Determine DDL source
    if ddl_path:
        if not ddl_path.exists():
            print(f"[validate] ERROR: DDL file not found: {ddl_path}")
            return False
        if needs_enhanced:
            ggm_schemas = parse_ddl_schemas(ddl_path)
        else:
            ggm_tables = parse_ddl_tables(ddl_path)
        ddl_source = str(ddl_path)
    elif ddl_dir:
        if not ddl_dir.exists():
            print(f"[validate] ERROR: DDL directory not found: {ddl_dir}")
            return False
        if needs_enhanced:
            ggm_schemas = parse_ddl_directory_schemas(ddl_dir)
        else:
            ggm_tables = parse_ddl_directory(ddl_dir)
        ddl_source = str(ddl_dir)
    else:
        # Auto-discover DDL location
        default_ddl = find_default_ddl_path(project_root)
        if default_ddl is None:
            print("[validate] ERROR: No DDL files found. Use --ddl or --ddl-dir to specify location.")
            return False
        if needs_enhanced:
            ggm_schemas = parse_ddl_directory_schemas(default_ddl)
        else:
            ggm_tables = parse_ddl_directory(default_ddl)
        ddl_source = str(default_ddl)
    
    # Normalize to enhanced format if not already
    if needs_enhanced:
        ggm_tables = {name: schema.columns for name, schema in ggm_schemas.items()}
    else:
        ggm_schemas = {}  # Not needed
    
    if not ggm_tables:
        print(f"[validate] ERROR: No tables found in DDL: {ddl_source}")
        return False
    
    print(f"[validate] Found {len(ggm_tables)} GGM tables in DDL: {ddl_source}")
    for table_name in sorted(ggm_tables.keys()):
        print(f"  - {table_name} ({len(ggm_tables[table_name])} columns)")
    
    # Get model definitions from SQLMesh Context
    if needs_enhanced:
        model_schemas = get_model_schemas(silver_schema)
        model_tables = {name: schema.columns for name, schema in model_schemas.items()}
    else:
        model_tables = get_model_columns_with_types(silver_schema)
        model_schemas = {}
    
    if not model_tables:
        print("[validate] ERROR: Could not get models from SQLMesh context")
        return False
    
    # Build validation info string
    validation_types = ["columns", "types"]
    if validate_grains:
        validation_types.append("grains")
    if validate_references:
        validation_types.append("references")
    if validate_descriptions:
        validation_types.append("descriptions")
    if validate_column_descriptions:
        validation_types.append("column_descriptions")
    
    print(f"[validate] Found {len(model_tables)} silver models")
    print(f"[validate] Validating: {', '.join(validation_types)}")
    
    # Track validation results
    has_errors = False
    validated_models: set[str] = set()
    
    # Validate: Each DDL table should have a corresponding silver model
    for ggm_table, ddl_cols in ggm_tables.items():
        if ggm_table not in model_tables:
            print(f"[validate] ERROR: DDL table {ggm_table} has no corresponding silver model")
            has_errors = True
            continue
        
        validated_models.add(ggm_table)
        model_cols = model_tables[ggm_table]
        table_errors: list[str] = []
        
        # Check column names
        ddl_names = set(ddl_cols.keys())
        model_names = set(model_cols.keys())
        
        missing = ddl_names - model_names
        extra = model_names - ddl_names
        
        if missing:
            table_errors.append(f"  Missing columns (in DDL but not in model): {sorted(missing)}")
        if extra:
            table_errors.append(f"  Extra columns (in model but not in DDL): {sorted(extra)}")
        
        # Check column types for matching columns
        for col in ddl_names & model_names:
            ddl_type = ddl_cols[col]
            model_type = model_cols[col]
            if ddl_type != model_type and model_type != "UNKNOWN":
                table_errors.append(f"  Type mismatch: {col} (DDL: {ddl_type}, model: {model_type})")
        
        # Enhanced validations
        if needs_enhanced and ggm_table in ggm_schemas and ggm_table in model_schemas:
            ddl_schema = ggm_schemas[ggm_table]
            model_schema = model_schemas[ggm_table]
            
            # Validate grains (primary keys)
            if validate_grains:
                ddl_grains = set(g.upper() for g in ddl_schema.grains)
                model_grains = set(g.upper() for g in model_schema.grains)
                
                if ddl_grains and ddl_grains != model_grains:
                    missing_grains = ddl_grains - model_grains
                    extra_grains = model_grains - ddl_grains
                    if missing_grains:
                        table_errors.append(f"  Missing grains (in DDL but not in model): {sorted(missing_grains)}")
                    if extra_grains:
                        table_errors.append(f"  Extra grains (in model but not in DDL): {sorted(extra_grains)}")
            
            # Validate references (foreign keys)
            if validate_references:
                ddl_refs = set(r.upper() for r in ddl_schema.reference_columns)
                model_refs = set(r.upper() for r in model_schema.references)
                
                if ddl_refs and ddl_refs != model_refs:
                    missing_refs = ddl_refs - model_refs
                    extra_refs = model_refs - ddl_refs
                    if missing_refs:
                        table_errors.append(f"  Missing references (in DDL but not in model): {sorted(missing_refs)}")
                    if extra_refs:
                        table_errors.append(f"  Extra references (in model but not in DDL): {sorted(extra_refs)}")
            
            # Validate table description
            if validate_descriptions:
                if ddl_schema.description and not model_schema.description:
                    table_errors.append(f"  Missing description (DDL has: '{ddl_schema.description[:50]}...')")
            
            # Validate column descriptions
            if validate_column_descriptions:
                for col, ddl_desc in ddl_schema.column_descriptions.items():
                    model_desc = model_schema.column_descriptions.get(col.upper())
                    if ddl_desc and not model_desc:
                        table_errors.append(f"  Missing column description for {col}")
        
        if table_errors:
            print(f"[validate] ERROR: {ggm_table} has mismatches")
            for error in table_errors:
                print(error)
            has_errors = True
        else:
            extra_info = []
            if validate_grains and ggm_table in ggm_schemas:
                grains_count = len(ggm_schemas[ggm_table].grains)
                if grains_count:
                    extra_info.append(f"{grains_count} grains")
            if validate_references and ggm_table in ggm_schemas:
                refs_count = len(ggm_schemas[ggm_table].reference_columns)
                if refs_count:
                    extra_info.append(f"{refs_count} references")
            
            info_str = f", {', '.join(extra_info)}" if extra_info else ""
            print(f"[validate] OK: {ggm_table} ({len(model_cols)} columns{info_str})")
    
    # Warn about silver models without DDL definition (non-fatal)
    extra_models = set(model_tables.keys()) - set(ggm_tables.keys())
    if extra_models:
        print(f"[validate] WARN: Silver models without DDL definition (not validated):")
        for model_name in sorted(extra_models):
            print(f"  - {model_name}")
    
    # Summary
    print()
    if has_errors:
        print("[validate] FAILED: Schema mismatches found")
        print("::error::GGM schema validation failed - see above for details")
    else:
        print(f"[validate] PASSED: All {len(validated_models)} DDL tables validated successfully")
    
    return not has_errors


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Validate SQLMesh silver models against GGM DDL definitions",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Auto-discover DDL files (validate columns and types only)
    python scripts/validate_schema.py
    
    # Validate grains (primary keys) and references (foreign keys)
    python scripts/validate_schema.py --validate-grains --validate-references
    
    # Validate all properties
    python scripts/validate_schema.py --validate-all
    
    # Specify a directory with DDL files
    python scripts/validate_schema.py --ddl-dir path/to/ddl/
        """
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
        "--silver-schema",
        default="silver",
        help="Schema name for silver models (default: silver)"
    )
    parser.add_argument(
        "--validate-grains",
        action="store_true",
        help="Validate that model grains match DDL primary keys"
    )
    parser.add_argument(
        "--validate-references",
        action="store_true",
        help="Validate that model references match DDL foreign keys"
    )
    parser.add_argument(
        "--validate-descriptions",
        action="store_true",
        help="Validate that model descriptions match DDL comments"
    )
    parser.add_argument(
        "--validate-column-descriptions",
        action="store_true",
        help="Validate that column descriptions match DDL column comments"
    )
    parser.add_argument(
        "--validate-all",
        action="store_true",
        help="Enable all validations (grains, references, descriptions)"
    )
    args = parser.parse_args()
    
    if args.ddl and args.ddl_dir:
        print("[validate] ERROR: Cannot specify both --ddl and --ddl-dir")
        sys.exit(1)
    
    # Handle --validate-all flag
    validate_grains = args.validate_grains or args.validate_all
    validate_references = args.validate_references or args.validate_all
    validate_descriptions = args.validate_descriptions or args.validate_all
    validate_column_descriptions = args.validate_column_descriptions or args.validate_all
    
    success = validate(
        ddl_path=args.ddl,
        ddl_dir=args.ddl_dir,
        silver_schema=args.silver_schema,
        validate_grains=validate_grains,
        validate_references=validate_references,
        validate_descriptions=validate_descriptions,
        validate_column_descriptions=validate_column_descriptions,
    )
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
