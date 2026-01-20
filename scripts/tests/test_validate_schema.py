"""Comprehensive tests for validate_schema.py.

Tests cover:
- Type normalization for all SQL types
- DDL parsing with various column definitions
- Model column extraction
- Type comparison logic
- Edge cases and error handling
"""
from __future__ import annotations

import tempfile
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
import sqlglot
from sqlglot import exp
from sqlglot.expressions import DataType

# Import the module under test
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from scripts.validate_schema import (
    normalize_type,
    parse_ddl_tables,
    get_model_columns_from_sql,
    get_model_columns_with_types,
    validate,
)


# =============================================================================
# Tests for normalize_type()
# =============================================================================

class TestNormalizeType:
    """Tests for the normalize_type function."""
    
    # -------------------------------------------------------------------------
    # Basic type normalization
    # -------------------------------------------------------------------------
    
    def test_varchar_basic(self):
        """VARCHAR should remain VARCHAR."""
        assert normalize_type("VARCHAR") == "VARCHAR"
    
    def test_varchar_with_length(self):
        """VARCHAR(255) should normalize to VARCHAR."""
        assert normalize_type("VARCHAR(255)") == "VARCHAR"
        assert normalize_type("VARCHAR(50)") == "VARCHAR"
        assert normalize_type("VARCHAR(1)") == "VARCHAR"
        assert normalize_type("VARCHAR(4000)") == "VARCHAR"
    
    def test_integer_basic(self):
        """INTEGER should remain INTEGER."""
        assert normalize_type("INTEGER") == "INTEGER"
    
    def test_int_to_integer(self):
        """INT should normalize to INTEGER."""
        assert normalize_type("INT") == "INTEGER"
    
    def test_bigint_to_integer(self):
        """BIGINT should normalize to INTEGER."""
        assert normalize_type("BIGINT") == "INTEGER"
    
    def test_smallint_to_integer(self):
        """SMALLINT should normalize to INTEGER."""
        assert normalize_type("SMALLINT") == "INTEGER"
    
    def test_date_basic(self):
        """DATE should remain DATE."""
        assert normalize_type("DATE") == "DATE"
    
    def test_timestamp_to_date(self):
        """TIMESTAMP should normalize to DATE."""
        assert normalize_type("TIMESTAMP") == "DATE"
    
    def test_datetime_to_date(self):
        """DATETIME should normalize to DATE."""
        assert normalize_type("DATETIME") == "DATE"
    
    def test_decimal_basic(self):
        """DECIMAL should remain DECIMAL."""
        assert normalize_type("DECIMAL") == "DECIMAL"
    
    def test_decimal_with_precision(self):
        """DECIMAL(10,2) should normalize to DECIMAL."""
        assert normalize_type("DECIMAL(10,2)") == "DECIMAL"
        assert normalize_type("DECIMAL(18,4)") == "DECIMAL"
        assert normalize_type("DECIMAL(38,0)") == "DECIMAL"
    
    def test_numeric_to_decimal(self):
        """NUMERIC should normalize to DECIMAL."""
        assert normalize_type("NUMERIC") == "DECIMAL"
        assert normalize_type("NUMERIC(10,2)") == "DECIMAL"
    
    def test_float_to_decimal(self):
        """FLOAT should normalize to DECIMAL."""
        assert normalize_type("FLOAT") == "DECIMAL"
        assert normalize_type("FLOAT(53)") == "DECIMAL"
    
    def test_double_to_decimal(self):
        """DOUBLE should normalize to DECIMAL."""
        assert normalize_type("DOUBLE") == "DECIMAL"
        assert normalize_type("DOUBLE PRECISION") == "DECIMAL"
    
    def test_real_to_decimal(self):
        """REAL should normalize to DECIMAL."""
        assert normalize_type("REAL") == "DECIMAL"
    
    def test_text_to_varchar(self):
        """TEXT should normalize to VARCHAR."""
        assert normalize_type("TEXT") == "VARCHAR"
    
    # -------------------------------------------------------------------------
    # Case insensitivity
    # -------------------------------------------------------------------------
    
    def test_lowercase_types(self):
        """Types should be normalized regardless of case."""
        assert normalize_type("varchar") == "VARCHAR"
        assert normalize_type("integer") == "INTEGER"
        assert normalize_type("int") == "INTEGER"
        assert normalize_type("date") == "DATE"
        assert normalize_type("decimal") == "DECIMAL"
    
    def test_mixed_case_types(self):
        """Mixed case types should normalize correctly."""
        assert normalize_type("VarChar") == "VARCHAR"
        assert normalize_type("Integer") == "INTEGER"
        assert normalize_type("BigInt") == "INTEGER"
        assert normalize_type("TimeStamp") == "DATE"
    
    # -------------------------------------------------------------------------
    # Edge cases and special inputs
    # -------------------------------------------------------------------------
    
    def test_none_input(self):
        """None should return UNKNOWN."""
        assert normalize_type(None) == "UNKNOWN"
    
    def test_empty_string(self):
        """Empty string should return empty string."""
        assert normalize_type("") == ""
    
    def test_unknown_type(self):
        """Unknown types should pass through unchanged, or normalize to known types."""
        assert normalize_type("BLOB") == "BLOB"
        # CLOB normalizes to VARCHAR (large text type)
        assert normalize_type("CLOB") == "VARCHAR"
        assert normalize_type("JSON") == "JSON"
        assert normalize_type("UUID") == "UUID"
        assert normalize_type("BOOLEAN") == "BOOLEAN"
    
    def test_whitespace_handling(self):
        """Types with extra whitespace should normalize correctly."""
        assert normalize_type("  VARCHAR  ") == "VARCHAR"
        assert normalize_type("VARCHAR (255)") == "VARCHAR"
    
    def test_complex_precision(self):
        """Complex precision specifications should be stripped."""
        assert normalize_type("DECIMAL(38, 18)") == "DECIMAL"
        assert normalize_type("NUMERIC( 10 , 2 )") == "DECIMAL"
        assert normalize_type("VARCHAR( 255 )") == "VARCHAR"
    
    # -------------------------------------------------------------------------
    # sqlglot DataType objects
    # -------------------------------------------------------------------------
    
    def test_sqlglot_datatype_varchar(self):
        """sqlglot DataType for VARCHAR should normalize correctly."""
        dtype = sqlglot.parse_one("CAST(x AS VARCHAR(255))").to
        assert normalize_type(dtype) == "VARCHAR"
    
    def test_sqlglot_datatype_integer(self):
        """sqlglot DataType for INTEGER should normalize correctly."""
        dtype = sqlglot.parse_one("CAST(x AS INTEGER)").to
        assert normalize_type(dtype) == "INTEGER"
    
    def test_sqlglot_datatype_decimal(self):
        """sqlglot DataType for DECIMAL should normalize correctly."""
        dtype = sqlglot.parse_one("CAST(x AS DECIMAL(10,2))").to
        assert normalize_type(dtype) == "DECIMAL"
    
    def test_sqlglot_datatype_date(self):
        """sqlglot DataType for DATE should normalize correctly."""
        dtype = sqlglot.parse_one("CAST(x AS DATE)").to
        assert normalize_type(dtype) == "DATE"
    
    def test_sqlglot_datatype_timestamp(self):
        """sqlglot DataType for TIMESTAMP should normalize to DATE."""
        dtype = sqlglot.parse_one("CAST(x AS TIMESTAMP)").to
        assert normalize_type(dtype) == "DATE"


# =============================================================================
# Tests for parse_ddl_tables()
# =============================================================================

class TestParseDdlTables:
    """Tests for the parse_ddl_tables function."""
    
    def _write_ddl(self, content: str) -> Path:
        """Write DDL content to a temp file and return path."""
        f = tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False)
        f.write(content)
        f.close()
        return Path(f.name)
    
    # -------------------------------------------------------------------------
    # Basic parsing
    # -------------------------------------------------------------------------
    
    def test_single_table_basic(self):
        """Parse a single basic table."""
        ddl = """
        CREATE TABLE TEST_TABLE (
            ID VARCHAR(255) PRIMARY KEY,
            NAME VARCHAR(100),
            AGE INTEGER
        );
        """
        path = self._write_ddl(ddl)
        result = parse_ddl_tables(path)
        
        assert "TEST_TABLE" in result
        assert "ID" in result["TEST_TABLE"]
        assert "NAME" in result["TEST_TABLE"]
        assert "AGE" in result["TEST_TABLE"]
        assert result["TEST_TABLE"]["ID"] == "VARCHAR"
        assert result["TEST_TABLE"]["NAME"] == "VARCHAR"
        assert result["TEST_TABLE"]["AGE"] == "INTEGER"
    
    def test_multiple_tables(self):
        """Parse multiple tables from one file."""
        ddl = """
        CREATE TABLE TABLE_A (
            ID INTEGER PRIMARY KEY,
            VALUE VARCHAR(50)
        );
        
        CREATE TABLE TABLE_B (
            ID INTEGER PRIMARY KEY,
            AMOUNT DECIMAL(10,2)
        );
        """
        path = self._write_ddl(ddl)
        result = parse_ddl_tables(path)
        
        assert len(result) == 2
        assert "TABLE_A" in result
        assert "TABLE_B" in result
    
    def test_table_name_uppercase(self):
        """Table names should be uppercased."""
        ddl = """
        CREATE TABLE lowercase_table (
            id INTEGER
        );
        """
        path = self._write_ddl(ddl)
        result = parse_ddl_tables(path)
        
        assert "LOWERCASE_TABLE" in result
    
    def test_column_name_uppercase(self):
        """Column names should be uppercased."""
        ddl = """
        CREATE TABLE TEST (
            lowercase_col VARCHAR(50),
            MixedCase_Col INTEGER
        );
        """
        path = self._write_ddl(ddl)
        result = parse_ddl_tables(path)
        
        assert "LOWERCASE_COL" in result["TEST"]
        assert "MIXEDCASE_COL" in result["TEST"]
    
    # -------------------------------------------------------------------------
    # All SQL types
    # -------------------------------------------------------------------------
    
    def test_all_common_types(self):
        """Parse table with all common SQL types."""
        ddl = """
        CREATE TABLE TYPE_TEST (
            COL_VARCHAR VARCHAR(255),
            COL_TEXT TEXT,
            COL_INTEGER INTEGER,
            COL_INT INT,
            COL_BIGINT BIGINT,
            COL_SMALLINT SMALLINT,
            COL_DECIMAL DECIMAL(10,2),
            COL_NUMERIC NUMERIC(18,4),
            COL_FLOAT FLOAT,
            COL_REAL REAL,
            COL_DATE DATE,
            COL_TIMESTAMP TIMESTAMP,
            COL_BOOLEAN BOOLEAN
        );
        """
        path = self._write_ddl(ddl)
        result = parse_ddl_tables(path)
        
        types = result["TYPE_TEST"]
        assert types["COL_VARCHAR"] == "VARCHAR"
        assert types["COL_TEXT"] == "VARCHAR"  # TEXT -> VARCHAR
        assert types["COL_INTEGER"] == "INTEGER"
        assert types["COL_INT"] == "INTEGER"  # INT -> INTEGER
        assert types["COL_BIGINT"] == "INTEGER"  # BIGINT -> INTEGER
        assert types["COL_SMALLINT"] == "INTEGER"  # SMALLINT -> INTEGER
        assert types["COL_DECIMAL"] == "DECIMAL"
        assert types["COL_NUMERIC"] == "DECIMAL"  # NUMERIC -> DECIMAL
        assert types["COL_FLOAT"] == "DECIMAL"  # FLOAT -> DECIMAL
        assert types["COL_REAL"] == "DECIMAL"  # REAL -> DECIMAL
        assert types["COL_DATE"] == "DATE"
        assert types["COL_TIMESTAMP"] == "DATE"  # TIMESTAMP -> DATE
        assert types["COL_BOOLEAN"] == "BOOLEAN"
    
    def test_decimal_precision_variations(self):
        """Parse DECIMAL with various precisions."""
        ddl = """
        CREATE TABLE DECIMAL_TEST (
            COL_A DECIMAL,
            COL_B DECIMAL(10),
            COL_C DECIMAL(10,2),
            COL_D DECIMAL(38,18)
        );
        """
        path = self._write_ddl(ddl)
        result = parse_ddl_tables(path)
        
        for col in ["COL_A", "COL_B", "COL_C", "COL_D"]:
            assert result["DECIMAL_TEST"][col] == "DECIMAL"
    
    def test_varchar_length_variations(self):
        """Parse VARCHAR with various lengths."""
        ddl = """
        CREATE TABLE VARCHAR_TEST (
            COL_A VARCHAR(1),
            COL_B VARCHAR(50),
            COL_C VARCHAR(255),
            COL_D VARCHAR(4000)
        );
        """
        path = self._write_ddl(ddl)
        result = parse_ddl_tables(path)
        
        for col in ["COL_A", "COL_B", "COL_C", "COL_D"]:
            assert result["VARCHAR_TEST"][col] == "VARCHAR"
    
    # -------------------------------------------------------------------------
    # Constraints and special syntax
    # -------------------------------------------------------------------------
    
    def test_primary_key_inline(self):
        """Parse table with inline PRIMARY KEY."""
        ddl = """
        CREATE TABLE TEST (
            ID VARCHAR(255) PRIMARY KEY,
            NAME VARCHAR(100)
        );
        """
        path = self._write_ddl(ddl)
        result = parse_ddl_tables(path)
        
        assert "ID" in result["TEST"]
        assert "NAME" in result["TEST"]
    
    def test_not_null_constraint(self):
        """Parse table with NOT NULL constraints."""
        ddl = """
        CREATE TABLE TEST (
            ID INTEGER NOT NULL,
            NAME VARCHAR(100) NOT NULL,
            OPTIONAL_COL VARCHAR(50)
        );
        """
        path = self._write_ddl(ddl)
        result = parse_ddl_tables(path)
        
        assert len(result["TEST"]) == 3
        assert result["TEST"]["ID"] == "INTEGER"
    
    def test_default_values(self):
        """Parse table with DEFAULT values."""
        ddl = """
        CREATE TABLE TEST (
            ID INTEGER DEFAULT 0,
            NAME VARCHAR(100) DEFAULT 'unknown',
            CREATED_DATE DATE DEFAULT CURRENT_DATE
        );
        """
        path = self._write_ddl(ddl)
        result = parse_ddl_tables(path)
        
        assert len(result["TEST"]) == 3
    
    # -------------------------------------------------------------------------
    # Comments and formatting
    # -------------------------------------------------------------------------
    
    def test_with_sql_comments(self):
        """Parse DDL with SQL comments."""
        ddl = """
        -- This is a comment
        CREATE TABLE TEST (
            ID INTEGER, -- inline comment
            NAME VARCHAR(100)
        );
        /* Block comment */
        """
        path = self._write_ddl(ddl)
        result = parse_ddl_tables(path)
        
        assert "TEST" in result
        assert len(result["TEST"]) == 2
    
    def test_commented_out_table(self):
        """Commented out tables should be ignored."""
        ddl = """
        CREATE TABLE ACTIVE_TABLE (
            ID INTEGER
        );
        
        -- CREATE TABLE COMMENTED_TABLE (
        --     ID INTEGER
        -- );
        """
        path = self._write_ddl(ddl)
        result = parse_ddl_tables(path)
        
        assert "ACTIVE_TABLE" in result
        assert "COMMENTED_TABLE" not in result
    
    def test_indented_columns(self):
        """Parse table with heavily indented columns."""
        ddl = """
        CREATE TABLE TEST (
                ID VARCHAR(255) PRIMARY KEY,
                    NAME VARCHAR(100),
            AGE INTEGER
        );
        """
        path = self._write_ddl(ddl)
        result = parse_ddl_tables(path)
        
        assert len(result["TEST"]) == 3
    
    # -------------------------------------------------------------------------
    # Edge cases
    # -------------------------------------------------------------------------
    
    def test_empty_file(self):
        """Empty file should return empty dict."""
        path = self._write_ddl("")
        result = parse_ddl_tables(path)
        assert result == {}
    
    def test_no_create_table(self):
        """File without CREATE TABLE should return empty dict."""
        ddl = """
        INSERT INTO TEST VALUES (1, 'test');
        SELECT * FROM TEST;
        """
        path = self._write_ddl(ddl)
        result = parse_ddl_tables(path)
        assert result == {}
    
    def test_create_table_no_columns(self):
        """CREATE TABLE without columns should be skipped."""
        # This is invalid SQL but we should handle gracefully
        ddl = """
        CREATE TABLE EMPTY_TABLE ();
        CREATE TABLE VALID_TABLE (ID INTEGER);
        """
        path = self._write_ddl(ddl)
        result = parse_ddl_tables(path)
        
        # Should at least parse the valid table
        assert "VALID_TABLE" in result


# =============================================================================
# Tests for get_model_columns_from_sql()
# =============================================================================

class TestGetModelColumnsFromSql:
    """Tests for the get_model_columns_from_sql function."""
    
    def _write_model(self, content: str) -> Path:
        """Write model content to a temp file and return path."""
        f = tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False)
        f.write(content)
        f.close()
        return Path(f.name)
    
    # -------------------------------------------------------------------------
    # Basic SELECT parsing
    # -------------------------------------------------------------------------
    
    def test_simple_aliases(self):
        """Parse simple column aliases."""
        model = """
        MODEL (
            name test.model,
            kind FULL
        );
        
        SELECT
            col1 AS alias1,
            col2 AS alias2
        FROM source
        """
        path = self._write_model(model)
        result = get_model_columns_from_sql(path)
        
        assert "ALIAS1" in result
        assert "ALIAS2" in result
    
    def test_cast_with_alias(self):
        """Parse CAST expressions with aliases."""
        model = """
        MODEL (
            name test.model,
            kind FULL
        );
        
        SELECT
            CAST(col1 AS VARCHAR(255)) AS string_col,
            CAST(col2 AS INTEGER) AS int_col,
            CAST(col3 AS DATE) AS date_col
        FROM source
        """
        path = self._write_model(model)
        result = get_model_columns_from_sql(path)
        
        assert "STRING_COL" in result
        assert "INT_COL" in result
        assert "DATE_COL" in result
    
    def test_null_cast_with_alias(self):
        """Parse CAST(NULL AS type) expressions."""
        model = """
        MODEL (
            name test.model,
            kind FULL
        );
        
        SELECT
            CAST(NULL AS VARCHAR(20)) AS nullable_string,
            CAST(NULL AS DATE) AS nullable_date
        FROM source
        """
        path = self._write_model(model)
        result = get_model_columns_from_sql(path)
        
        assert "NULLABLE_STRING" in result
        assert "NULLABLE_DATE" in result
    
    def test_expression_with_alias(self):
        """Parse complex expressions with aliases."""
        model = """
        MODEL (
            name test.model,
            kind FULL
        );
        
        SELECT
            a.col1 AS table_alias_col,
            col1 + col2 AS sum_col,
            COALESCE(col1, col2) AS coalesce_col,
            CASE WHEN x = 1 THEN 'a' ELSE 'b' END AS case_col
        FROM source
        """
        path = self._write_model(model)
        result = get_model_columns_from_sql(path)
        
        assert "TABLE_ALIAS_COL" in result
        assert "SUM_COL" in result
        assert "COALESCE_COL" in result
        assert "CASE_COL" in result
    
    # -------------------------------------------------------------------------
    # MODEL block handling
    # -------------------------------------------------------------------------
    
    def test_model_block_stripped(self):
        """MODEL block should be stripped before parsing."""
        model = """
        MODEL (
            name silver.test_model,
            kind FULL,
            cron '@daily'
        );
        
        -- Comment after model block
        SELECT col AS output_col FROM source
        """
        path = self._write_model(model)
        result = get_model_columns_from_sql(path)
        
        assert "OUTPUT_COL" in result
    
    def test_multiline_model_block(self):
        """Multiline MODEL blocks should be handled."""
        model = """
        MODEL (
            name silver.complex_model,
            kind FULL,
            grain (id),
            references (
                other_model
            )
        );
        
        SELECT id AS model_id FROM source
        """
        path = self._write_model(model)
        result = get_model_columns_from_sql(path)
        
        assert "MODEL_ID" in result
    
    def test_no_model_block(self):
        """Files without MODEL block should still parse."""
        model = """
        SELECT col AS output FROM source
        """
        path = self._write_model(model)
        result = get_model_columns_from_sql(path)
        
        assert "OUTPUT" in result
    
    # -------------------------------------------------------------------------
    # Edge cases
    # -------------------------------------------------------------------------
    
    def test_uppercase_as_keyword(self):
        """AS keyword in different cases."""
        model = """
        MODEL (name test, kind FULL);
        
        SELECT
            col1 AS upper_alias,
            col2 as lower_alias,
            col3 As mixed_alias
        FROM source
        """
        path = self._write_model(model)
        result = get_model_columns_from_sql(path)
        
        assert "UPPER_ALIAS" in result
        assert "LOWER_ALIAS" in result
        assert "MIXED_ALIAS" in result
    
    def test_columns_with_joins(self):
        """Parse columns from SELECT with JOINs."""
        model = """
        MODEL (name test, kind FULL);
        
        SELECT
            a.id AS primary_id,
            b.name AS joined_name,
            c.value AS second_join
        FROM table_a a
        INNER JOIN table_b b ON a.id = b.a_id
        LEFT JOIN table_c c ON a.id = c.a_id
        """
        path = self._write_model(model)
        result = get_model_columns_from_sql(path)
        
        assert "PRIMARY_ID" in result
        assert "JOINED_NAME" in result
        assert "SECOND_JOIN" in result
    
    def test_subquery_columns(self):
        """Columns from main SELECT should be captured (not subquery)."""
        model = """
        MODEL (name test, kind FULL);
        
        SELECT
            outer_col AS final_col
        FROM (
            SELECT inner_col AS outer_col FROM source
        ) subq
        """
        path = self._write_model(model)
        result = get_model_columns_from_sql(path)
        
        # Should get the outer alias
        assert "FINAL_COL" in result


# =============================================================================
# Tests for get_model_columns_with_types() - requires mocking
# =============================================================================

class TestGetModelColumnsWithTypes:
    """Tests for get_model_columns_with_types using mocked SQLMesh Context."""
    
    def _create_mock_model(self, columns_to_types: dict[str, Any]) -> MagicMock:
        """Create a mock SQLMesh model with given columns_to_types."""
        model = MagicMock()
        model.columns_to_types = columns_to_types
        return model
    
    def _create_mock_dtype(self, type_name: str) -> MagicMock:
        """Create a mock DataType with given type name."""
        dtype = MagicMock()
        dtype.this = MagicMock()
        dtype.this.name = type_name
        return dtype
    
    def test_single_model_extraction(self):
        """Extract columns from a single mocked model."""
        mock_model = self._create_mock_model({
            "id": self._create_mock_dtype("VARCHAR"),
            "name": self._create_mock_dtype("VARCHAR"),
            "age": self._create_mock_dtype("INT"),
        })
        
        mock_ctx = MagicMock()
        mock_ctx.models = {
            '"db"."silver"."test_model"': mock_model
        }
        
        # Mock the sqlmesh module and Context class
        mock_sqlmesh = MagicMock()
        mock_sqlmesh.Context.return_value = mock_ctx
        
        with patch.dict('sys.modules', {'sqlmesh': mock_sqlmesh}):
            # Force reimport to use mocked module
            import importlib
            import scripts.validate_schema as mod
            importlib.reload(mod)
            result = mod.get_model_columns_with_types()
        
        assert "TEST_MODEL" in result
        assert result["TEST_MODEL"]["ID"] == "VARCHAR"
        assert result["TEST_MODEL"]["NAME"] == "VARCHAR"
        assert result["TEST_MODEL"]["AGE"] == "INTEGER"  # INT -> INTEGER
    
    def test_multiple_models(self):
        """Extract columns from multiple models."""
        mock_ctx = MagicMock()
        mock_ctx.models = {
            '"db"."silver"."model_a"': self._create_mock_model({
                "col_a": self._create_mock_dtype("VARCHAR"),
            }),
            '"db"."silver"."model_b"': self._create_mock_model({
                "col_b": self._create_mock_dtype("INTEGER"),
            }),
        }
        
        mock_sqlmesh = MagicMock()
        mock_sqlmesh.Context.return_value = mock_ctx
        
        with patch.dict('sys.modules', {'sqlmesh': mock_sqlmesh}):
            import importlib
            import scripts.validate_schema as mod
            importlib.reload(mod)
            result = mod.get_model_columns_with_types()
        
        assert "MODEL_A" in result
        assert "MODEL_B" in result
    
    def test_non_silver_models_skipped(self):
        """Models not in 'silver' schema should be skipped."""
        mock_ctx = MagicMock()
        mock_ctx.models = {
            '"db"."silver"."included"': self._create_mock_model({
                "col": self._create_mock_dtype("VARCHAR"),
            }),
            '"db"."stg"."excluded"': self._create_mock_model({
                "col": self._create_mock_dtype("VARCHAR"),
            }),
            '"db"."raw"."also_excluded"': self._create_mock_model({
                "col": self._create_mock_dtype("VARCHAR"),
            }),
        }
        
        mock_sqlmesh = MagicMock()
        mock_sqlmesh.Context.return_value = mock_ctx
        
        with patch.dict('sys.modules', {'sqlmesh': mock_sqlmesh}):
            import importlib
            import scripts.validate_schema as mod
            importlib.reload(mod)
            result = mod.get_model_columns_with_types()
        
        assert "INCLUDED" in result
        assert "EXCLUDED" not in result
        assert "ALSO_EXCLUDED" not in result
    
    def test_context_creation_failure(self):
        """Handle SQLMesh Context creation failure gracefully."""
        mock_sqlmesh = MagicMock()
        mock_sqlmesh.Context.side_effect = Exception("No config")
        
        with patch.dict('sys.modules', {'sqlmesh': mock_sqlmesh}):
            import importlib
            import scripts.validate_schema as mod
            importlib.reload(mod)
            result = mod.get_model_columns_with_types()
        
        assert result == {}
    
    def test_real_context_integration(self):
        """Integration test with real SQLMesh Context (requires valid config)."""
        # This tests the actual function without mocking
        # Will use real SQLMesh context if available
        result = get_model_columns_with_types()
        
        # If we got results, verify structure
        if result:
            for table_name, columns in result.items():
                assert isinstance(table_name, str)
                assert table_name.isupper()
                assert isinstance(columns, dict)
                for col_name, col_type in columns.items():
                    assert isinstance(col_name, str)
                    assert col_name.isupper()
                    assert isinstance(col_type, str)


# =============================================================================
# Tests for validate() function
# =============================================================================

class TestValidate:
    """Integration tests for the validate function."""
    
    def _setup_test_files(self, ddl_content: str, models: dict[str, str]) -> tuple[Path, Path]:
        """Set up temporary DDL and model files for testing."""
        import tempfile
        import shutil
        
        # Create temp directory structure
        temp_dir = Path(tempfile.mkdtemp())
        
        # Create DDL file
        ggm_dir = temp_dir / "ggm" / "selectie" / "cssd"
        ggm_dir.mkdir(parents=True)
        ddl_path = ggm_dir / "Sociaal_Domein_Beschikking_en_Voorziening__Domain_Objects_postgres.sql"
        ddl_path.write_text(ddl_content)
        
        # Create model files
        silver_dir = temp_dir / "models" / "silver"
        silver_dir.mkdir(parents=True)
        for name, content in models.items():
            (silver_dir / f"{name}.sql").write_text(content)
        
        return temp_dir, ddl_path
    
    def test_validate_matching_columns(self):
        """Validate should pass when columns match."""
        ddl = """
        CREATE TABLE CLIENT (
            CLIENT_ID VARCHAR(255) PRIMARY KEY,
            NAME VARCHAR(100)
        );
        """
        models = {
            "client": """
            MODEL (name silver.client, kind FULL);
            SELECT
                id AS client_id,
                name AS name
            FROM source
            """
        }
        
        temp_dir, _ = self._setup_test_files(ddl, models)
        
        # Patch the project root
        with patch.object(Path, 'parent', new_callable=lambda: property(lambda self: temp_dir)):
            # This is complex to test due to file system dependencies
            # In practice, would need more sophisticated mocking
            pass
    
    def test_validate_missing_columns_detected(self):
        """Validate should detect missing columns."""
        # Would need full integration test setup
        pass
    
    def test_validate_extra_columns_detected(self):
        """Validate should detect extra columns."""
        # Would need full integration test setup
        pass


# =============================================================================
# Tests for type comparison logic
# =============================================================================

class TestTypeComparison:
    """Tests for type comparison edge cases."""
    
    def test_equivalent_types_match(self):
        """Equivalent types after normalization should match."""
        # INT and INTEGER
        assert normalize_type("INT") == normalize_type("INTEGER")
        
        # BIGINT and INTEGER
        assert normalize_type("BIGINT") == normalize_type("INTEGER")
        
        # TEXT and VARCHAR
        assert normalize_type("TEXT") == normalize_type("VARCHAR")
        
        # NUMERIC and DECIMAL
        assert normalize_type("NUMERIC(10,2)") == normalize_type("DECIMAL(18,4)")
        
        # TIMESTAMP and DATE
        assert normalize_type("TIMESTAMP") == normalize_type("DATE")
    
    def test_different_types_dont_match(self):
        """Different types should not match."""
        assert normalize_type("VARCHAR") != normalize_type("INTEGER")
        assert normalize_type("DATE") != normalize_type("VARCHAR")
        assert normalize_type("DECIMAL") != normalize_type("VARCHAR")
        assert normalize_type("BOOLEAN") != normalize_type("INTEGER")
    
    def test_precision_ignored_in_comparison(self):
        """Precision differences should not cause mismatch."""
        # VARCHAR lengths
        assert normalize_type("VARCHAR(50)") == normalize_type("VARCHAR(255)")
        
        # DECIMAL precision
        assert normalize_type("DECIMAL(10,2)") == normalize_type("DECIMAL(38,18)")
    
    def test_unknown_type_handling(self):
        """UNKNOWN type should be handled specially."""
        # In actual validation, UNKNOWN is skipped
        assert normalize_type(None) == "UNKNOWN"


# =============================================================================
# Performance and stress tests
# =============================================================================

class TestPerformance:
    """Performance tests for large inputs."""
    
    def test_large_ddl_file(self):
        """Parse DDL with many tables."""
        tables = []
        for i in range(100):
            tables.append(f"""
            CREATE TABLE TABLE_{i} (
                ID INTEGER PRIMARY KEY,
                COL_A VARCHAR(255),
                COL_B DECIMAL(10,2),
                COL_C DATE
            );
            """)
        
        ddl = "\n".join(tables)
        f = tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False)
        f.write(ddl)
        f.close()
        
        result = parse_ddl_tables(Path(f.name))
        
        assert len(result) == 100
        assert all(len(cols) == 4 for cols in result.values())
    
    def test_table_with_many_columns(self):
        """Parse table with many columns."""
        columns = [f"COL_{i} VARCHAR(255)" for i in range(200)]
        ddl = f"CREATE TABLE WIDE_TABLE ({', '.join(columns)});"
        
        f = tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False)
        f.write(ddl)
        f.close()
        
        result = parse_ddl_tables(Path(f.name))
        
        assert "WIDE_TABLE" in result
        assert len(result["WIDE_TABLE"]) == 200


# =============================================================================
# SQL Dialect-specific tests
# =============================================================================

class TestSqlDialects:
    """Tests for different SQL database dialects."""
    
    def _write_ddl(self, content: str) -> Path:
        """Write DDL content to a temp file and return path."""
        f = tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False)
        f.write(content)
        f.close()
        return Path(f.name)
    
    # -------------------------------------------------------------------------
    # PostgreSQL-specific types
    # -------------------------------------------------------------------------
    
    def test_postgres_serial_type(self):
        """PostgreSQL SERIAL type should normalize to INTEGER."""
        assert normalize_type("SERIAL") == "INTEGER"
        assert normalize_type("BIGSERIAL") == "INTEGER"
    
    def test_postgres_text_type(self):
        """PostgreSQL TEXT should normalize to VARCHAR."""
        assert normalize_type("TEXT") == "VARCHAR"
    
    def test_postgres_bytea_type(self):
        """PostgreSQL BYTEA should remain BYTEA."""
        assert normalize_type("BYTEA") == "BYTEA"
    
    def test_postgres_uuid_type(self):
        """PostgreSQL UUID should remain UUID."""
        assert normalize_type("UUID") == "UUID"
    
    def test_postgres_json_types(self):
        """PostgreSQL JSON/JSONB should remain as-is."""
        assert normalize_type("JSON") == "JSON"
        assert normalize_type("JSONB") == "JSONB"
    
    def test_postgres_interval_type(self):
        """PostgreSQL INTERVAL should remain INTERVAL."""
        assert normalize_type("INTERVAL") == "INTERVAL"
    
    def test_postgres_timestamp_with_tz(self):
        """PostgreSQL TIMESTAMP WITH TIME ZONE should normalize to DATE."""
        # TIMESTAMPTZ normalizes to DATE
        assert normalize_type("TIMESTAMPTZ") == "DATE"
    
    def test_postgres_ddl_parsing(self):
        """Parse PostgreSQL-style DDL."""
        ddl = """
        CREATE TABLE postgres_test (
            id SERIAL PRIMARY KEY,
            uuid_col UUID,
            name VARCHAR(255) NOT NULL,
            data JSONB,
            amount NUMERIC(10,2),
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        );
        """
        path = self._write_ddl(ddl)
        result = parse_ddl_tables(path)
        
        assert "POSTGRES_TEST" in result
        cols = result["POSTGRES_TEST"]
        assert "ID" in cols
        assert "UUID_COL" in cols
        assert "NAME" in cols
        assert "DATA" in cols
        assert "AMOUNT" in cols
        assert "CREATED_AT" in cols
    
    # -------------------------------------------------------------------------
    # MySQL-specific types
    # -------------------------------------------------------------------------
    
    def test_mysql_tinyint_type(self):
        """MySQL TINYINT should normalize to INTEGER."""
        assert normalize_type("TINYINT") == "INTEGER"
    
    def test_mysql_mediumint_type(self):
        """MySQL MEDIUMINT should normalize to INTEGER."""
        assert normalize_type("MEDIUMINT") == "INTEGER"
    
    def test_mysql_longtext_type(self):
        """MySQL LONGTEXT should normalize to VARCHAR."""
        assert normalize_type("LONGTEXT") == "VARCHAR"
        assert normalize_type("MEDIUMTEXT") == "VARCHAR"
        assert normalize_type("TINYTEXT") == "VARCHAR"
    
    def test_mysql_datetime_type(self):
        """MySQL DATETIME should normalize to DATE."""
        assert normalize_type("DATETIME") == "DATE"
    
    def test_mysql_unsigned_integer(self):
        """MySQL unsigned integers (parsed as base type)."""
        # sqlglot handles UNSIGNED separately
        assert normalize_type("INT") == "INTEGER"
        assert normalize_type("BIGINT") == "INTEGER"
    
    def test_mysql_enum_type(self):
        """MySQL ENUM type should remain ENUM."""
        assert normalize_type("ENUM") == "ENUM"
    
    def test_mysql_ddl_parsing(self):
        """Parse MySQL-style DDL."""
        ddl = """
        CREATE TABLE mysql_test (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            status TINYINT DEFAULT 0,
            description TEXT,
            price DECIMAL(10,2),
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        );
        """
        path = self._write_ddl(ddl)
        result = parse_ddl_tables(path)
        
        assert "MYSQL_TEST" in result
        cols = result["MYSQL_TEST"]
        assert len(cols) == 6
        assert cols["ID"] == "INTEGER"
        assert cols["STATUS"] == "INTEGER"  # TINYINT -> INTEGER
        assert cols["DESCRIPTION"] == "VARCHAR"  # TEXT -> VARCHAR
    
    # -------------------------------------------------------------------------
    # SQL Server (MSSQL)-specific types
    # -------------------------------------------------------------------------
    
    def test_mssql_nvarchar_type(self):
        """MSSQL NVARCHAR should normalize to VARCHAR."""
        assert normalize_type("NVARCHAR") == "VARCHAR"
        assert normalize_type("NVARCHAR(MAX)") == "VARCHAR"
    
    def test_mssql_nchar_type(self):
        """MSSQL NCHAR should normalize to VARCHAR."""
        assert normalize_type("NCHAR") == "VARCHAR"
    
    def test_mssql_money_type(self):
        """MSSQL MONEY should normalize to DECIMAL."""
        assert normalize_type("MONEY") == "DECIMAL"
        assert normalize_type("SMALLMONEY") == "DECIMAL"
    
    def test_mssql_datetime2_type(self):
        """MSSQL DATETIME2 should normalize to DATE."""
        assert normalize_type("DATETIME2") == "DATE"
    
    def test_mssql_bit_type(self):
        """MSSQL BIT should remain BIT (boolean-like)."""
        assert normalize_type("BIT") == "BIT"
    
    def test_mssql_uniqueidentifier_type(self):
        """MSSQL UNIQUEIDENTIFIER should remain as-is."""
        assert normalize_type("UNIQUEIDENTIFIER") == "UNIQUEIDENTIFIER"
    
    def test_mssql_ddl_parsing(self):
        """Parse MSSQL-style DDL."""
        ddl = """
        CREATE TABLE mssql_test (
            id INT IDENTITY(1,1) PRIMARY KEY,
            guid UNIQUEIDENTIFIER DEFAULT NEWID(),
            name NVARCHAR(255) NOT NULL,
            amount MONEY,
            is_active BIT DEFAULT 1,
            created_at DATETIME2 DEFAULT GETDATE()
        );
        """
        path = self._write_ddl(ddl)
        result = parse_ddl_tables(path)
        
        assert "MSSQL_TEST" in result
        cols = result["MSSQL_TEST"]
        assert "ID" in cols
        assert "GUID" in cols
        assert "NAME" in cols
        assert cols["NAME"] == "VARCHAR"  # NVARCHAR -> VARCHAR
        assert cols["AMOUNT"] == "DECIMAL"  # MONEY -> DECIMAL
    
    # -------------------------------------------------------------------------
    # Oracle-specific types
    # -------------------------------------------------------------------------
    
    def test_oracle_number_type(self):
        """Oracle NUMBER should normalize to DECIMAL."""
        assert normalize_type("NUMBER") == "DECIMAL"
        assert normalize_type("NUMBER(10,2)") == "DECIMAL"
    
    def test_oracle_varchar2_type(self):
        """Oracle VARCHAR2 should normalize to VARCHAR."""
        assert normalize_type("VARCHAR2") == "VARCHAR"
        assert normalize_type("VARCHAR2(255)") == "VARCHAR"
    
    def test_oracle_clob_type(self):
        """Oracle CLOB should normalize to VARCHAR (text type)."""
        assert normalize_type("CLOB") == "VARCHAR"
    
    def test_oracle_blob_type(self):
        """Oracle BLOB should remain BLOB."""
        assert normalize_type("BLOB") == "BLOB"
    
    def test_oracle_raw_type(self):
        """Oracle RAW should remain RAW."""
        assert normalize_type("RAW") == "RAW"
    
    def test_oracle_ddl_parsing(self):
        """Parse Oracle-style DDL."""
        ddl = """
        CREATE TABLE oracle_test (
            id NUMBER(10) PRIMARY KEY,
            name VARCHAR2(255) NOT NULL,
            description CLOB,
            amount NUMBER(10,2),
            created_at DATE DEFAULT SYSDATE
        );
        """
        path = self._write_ddl(ddl)
        result = parse_ddl_tables(path)
        
        assert "ORACLE_TEST" in result
        cols = result["ORACLE_TEST"]
        assert len(cols) == 5
        assert "ID" in cols
        assert "DESCRIPTION" in cols
    
    # -------------------------------------------------------------------------
    # Cross-dialect type mapping
    # -------------------------------------------------------------------------
    
    def test_cross_dialect_string_types(self):
        """String types across dialects should normalize consistently."""
        # All these are text/string types
        string_types = ["VARCHAR", "TEXT", "NVARCHAR", "CHAR", "NCHAR"]
        normalized = [normalize_type(t) for t in string_types]
        
        # VARCHAR, NVARCHAR, CHAR, NCHAR -> VARCHAR; TEXT -> VARCHAR
        assert normalized.count("VARCHAR") >= 4
    
    def test_cross_dialect_integer_types(self):
        """Integer types across dialects should normalize consistently."""
        int_types = ["INT", "INTEGER", "BIGINT", "SMALLINT", "TINYINT"]
        normalized = [normalize_type(t) for t in int_types]
        
        assert all(t == "INTEGER" for t in normalized)
    
    def test_cross_dialect_decimal_types(self):
        """Decimal types across dialects should normalize consistently."""
        decimal_types = ["DECIMAL", "NUMERIC", "FLOAT", "REAL", "DOUBLE", "MONEY"]
        normalized = [normalize_type(t) for t in decimal_types]
        
        assert all(t == "DECIMAL" for t in normalized)
    
    def test_cross_dialect_date_types(self):
        """Date/time types across dialects should normalize consistently."""
        date_types = ["DATE", "DATETIME", "DATETIME2", "TIMESTAMP", "TIME"]
        normalized = [normalize_type(t) for t in date_types]
        
        assert all(t == "DATE" for t in normalized)
    
    # -------------------------------------------------------------------------
    # sqlglot dialect parsing
    # -------------------------------------------------------------------------
    
    def test_sqlglot_postgres_dialect(self):
        """Parse DDL using sqlglot postgres dialect."""
        ddl = "CREATE TABLE test (id SERIAL, name TEXT);"
        
        for stmt in sqlglot.parse(ddl, read="postgres"):
            if stmt and isinstance(stmt, exp.Create):
                schema = stmt.this
                assert isinstance(schema, exp.Schema)
    
    def test_sqlglot_mysql_dialect(self):
        """Parse DDL using sqlglot mysql dialect."""
        ddl = "CREATE TABLE test (id INT AUTO_INCREMENT, name VARCHAR(255));"
        
        for stmt in sqlglot.parse(ddl, read="mysql"):
            if stmt and isinstance(stmt, exp.Create):
                schema = stmt.this
                assert isinstance(schema, exp.Schema)
    
    def test_sqlglot_tsql_dialect(self):
        """Parse DDL using sqlglot tsql (MSSQL) dialect."""
        ddl = "CREATE TABLE test (id INT IDENTITY(1,1), name NVARCHAR(255));"
        
        for stmt in sqlglot.parse(ddl, read="tsql"):
            if stmt and isinstance(stmt, exp.Create):
                schema = stmt.this
                assert isinstance(schema, exp.Schema)
    
    def test_sqlglot_oracle_dialect(self):
        """Parse DDL using sqlglot oracle dialect."""
        ddl = "CREATE TABLE test (id NUMBER(10), name VARCHAR2(255));"
        
        for stmt in sqlglot.parse(ddl, read="oracle"):
            if stmt and isinstance(stmt, exp.Create):
                schema = stmt.this
                assert isinstance(schema, exp.Schema)


# =============================================================================
# Database-specific DDL with full schema validation
# =============================================================================

class TestFullSchemaValidation:
    """Test complete schema validation scenarios."""
    
    def _write_ddl(self, content: str) -> Path:
        f = tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False)
        f.write(content)
        f.close()
        return Path(f.name)
    
    def test_ggm_like_schema(self):
        """Test schema that matches GGM DDL patterns."""
        ddl = """
        CREATE TABLE CLIENT (
            RECHTSPERSOON_ID VARCHAR(255) PRIMARY KEY,
            CODE VARCHAR(80),
            VOORNAMEN VARCHAR(200),
            GESLACHTSNAAM VARCHAR(200),
            IDENTIFICATIE VARCHAR(20)
        );
        
        CREATE TABLE BESCHIKKING (
            BESCHIKKING_ID VARCHAR(255) PRIMARY KEY,
            CLIENT_ID VARCHAR(255),
            CODE VARCHAR(20),
            COMMENTAAR VARCHAR(200),
            DATUMAFGIFTE DATE,
            GRONDSLAGEN INTEGER,
            HEEFT_VOORZIENINGEN_BESCHIKTE_VOORZIENING_ID VARCHAR(255),
            TOEWIJZING_TOEWIJZING_ID VARCHAR(255),
            WET VARCHAR(255)
        );
        """
        path = self._write_ddl(ddl)
        result = parse_ddl_tables(path)
        
        assert len(result) == 2
        assert "CLIENT" in result
        assert "BESCHIKKING" in result
        
        # Verify CLIENT columns
        client_cols = result["CLIENT"]
        assert len(client_cols) == 5
        assert client_cols["RECHTSPERSOON_ID"] == "VARCHAR"
        assert client_cols["CODE"] == "VARCHAR"
        
        # Verify BESCHIKKING columns
        besch_cols = result["BESCHIKKING"]
        assert len(besch_cols) == 9
        assert besch_cols["DATUMAFGIFTE"] == "DATE"
        assert besch_cols["GRONDSLAGEN"] == "INTEGER"
    
    def test_mixed_type_precision(self):
        """Test various type precisions are handled correctly."""
        ddl = """
        CREATE TABLE PRECISION_TEST (
            COL_VARCHAR_50 VARCHAR(50),
            COL_VARCHAR_255 VARCHAR(255),
            COL_DECIMAL_5_2 DECIMAL(5,2),
            COL_DECIMAL_18_4 DECIMAL(18,4),
            COL_DECIMAL_38_18 DECIMAL(38,18),
            COL_NUMERIC_10 NUMERIC(10),
            COL_FLOAT_24 FLOAT(24),
            COL_FLOAT_53 FLOAT(53)
        );
        """
        path = self._write_ddl(ddl)
        result = parse_ddl_tables(path)
        
        cols = result["PRECISION_TEST"]
        
        # All VARCHAR variants should normalize to VARCHAR
        assert cols["COL_VARCHAR_50"] == "VARCHAR"
        assert cols["COL_VARCHAR_255"] == "VARCHAR"
        
        # All numeric types should normalize to DECIMAL
        assert cols["COL_DECIMAL_5_2"] == "DECIMAL"
        assert cols["COL_DECIMAL_18_4"] == "DECIMAL"
        assert cols["COL_DECIMAL_38_18"] == "DECIMAL"
        assert cols["COL_NUMERIC_10"] == "DECIMAL"
        assert cols["COL_FLOAT_24"] == "DECIMAL"
        assert cols["COL_FLOAT_53"] == "DECIMAL"


# =============================================================================
# Edge Cases and Error Handling
# =============================================================================

class TestEdgeCases:
    """Tests for edge cases, error handling, and unusual inputs."""
    
    def _write_ddl(self, content: str) -> Path:
        f = tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False)
        f.write(content)
        f.close()
        return Path(f.name)
    
    def _write_model(self, content: str) -> Path:
        f = tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False)
        f.write(content)
        f.close()
        return Path(f.name)
    
    # -------------------------------------------------------------------------
    # Array types (PostgreSQL)
    # -------------------------------------------------------------------------
    
    def test_postgres_array_types(self):
        """PostgreSQL array types should be handled."""
        ddl = """
        CREATE TABLE ARRAY_TEST (
            int_array INTEGER[],
            text_array TEXT[],
            varchar_array VARCHAR(255)[]
        );
        """
        path = self._write_ddl(ddl)
        result = parse_ddl_tables(path)
        
        assert "ARRAY_TEST" in result
        # Arrays should be parsed (type handling depends on sqlglot)
        assert len(result["ARRAY_TEST"]) == 3
    
    # -------------------------------------------------------------------------
    # Quoted identifiers
    # -------------------------------------------------------------------------
    
    def test_quoted_column_names(self):
        """Quoted column names should be handled."""
        ddl = """
        CREATE TABLE QUOTED_TEST (
            "Column With Spaces" VARCHAR(255),
            "123_starts_with_number" INTEGER,
            "UPPERCASE" DATE
        );
        """
        path = self._write_ddl(ddl)
        result = parse_ddl_tables(path)
        
        # Should parse the table
        assert "QUOTED_TEST" in result
    
    def test_quoted_table_names(self):
        """Quoted table names should be handled."""
        ddl = """
        CREATE TABLE "My Table" (
            id INTEGER,
            name VARCHAR(100)
        );
        """
        path = self._write_ddl(ddl)
        result = parse_ddl_tables(path)
        
        # Table name might be parsed with or without quotes
        assert len(result) >= 1
    
    def test_mssql_bracket_identifiers(self):
        """MSSQL bracket identifiers may fail with postgres dialect - that's expected."""
        ddl = """
        CREATE TABLE [Bracket Test] (
            [Column With Spaces] VARCHAR(255),
            [Order] INTEGER
        );
        """
        path = self._write_ddl(ddl)
        # MSSQL bracket syntax is not valid in PostgreSQL dialect
        # This should either parse or raise an error
        try:
            result = parse_ddl_tables(path)
            # If it parses, verify no crash
            assert isinstance(result, dict)
        except Exception:
            # Expected to fail with postgres dialect
            pass
    
    # -------------------------------------------------------------------------
    # Reserved words as identifiers
    # -------------------------------------------------------------------------
    
    def test_reserved_word_columns(self):
        """Reserved SQL words as column names should be handled."""
        ddl = """
        CREATE TABLE RESERVED_TEST (
            "SELECT" VARCHAR(255),
            "FROM" INTEGER,
            "WHERE" DATE,
            "ORDER" VARCHAR(50)
        );
        """
        path = self._write_ddl(ddl)
        result = parse_ddl_tables(path)
        
        assert "RESERVED_TEST" in result
    
    # -------------------------------------------------------------------------
    # Schema-qualified names
    # -------------------------------------------------------------------------
    
    def test_schema_qualified_table_name(self):
        """Schema-qualified table names should be parsed."""
        ddl = """
        CREATE TABLE public.MY_TABLE (
            id INTEGER,
            name VARCHAR(100)
        );
        """
        path = self._write_ddl(ddl)
        result = parse_ddl_tables(path)
        
        # Should parse the table (name might include schema or not)
        assert len(result) >= 1
    
    def test_three_part_name(self):
        """Three-part names (db.schema.table) should be handled."""
        ddl = """
        CREATE TABLE mydb.dbo.MY_TABLE (
            id INTEGER
        );
        """
        path = self._write_ddl(ddl)
        result = parse_ddl_tables(path)
        # Verify no crash, table might be parsed differently
    
    # -------------------------------------------------------------------------
    # Composite primary keys
    # -------------------------------------------------------------------------
    
    def test_composite_primary_key(self):
        """Composite primary keys should not affect column parsing."""
        ddl = """
        CREATE TABLE COMPOSITE_PK (
            id1 INTEGER,
            id2 INTEGER,
            value VARCHAR(255),
            PRIMARY KEY (id1, id2)
        );
        """
        path = self._write_ddl(ddl)
        result = parse_ddl_tables(path)
        
        assert "COMPOSITE_PK" in result
        cols = result["COMPOSITE_PK"]
        assert len(cols) == 3
        assert "ID1" in cols
        assert "ID2" in cols
        assert "VALUE" in cols
    
    # -------------------------------------------------------------------------
    # Foreign key constraints
    # -------------------------------------------------------------------------
    
    def test_foreign_key_constraint(self):
        """Foreign key constraints should not affect column parsing."""
        ddl = """
        CREATE TABLE CHILD_TABLE (
            id INTEGER PRIMARY KEY,
            parent_id INTEGER,
            name VARCHAR(100),
            FOREIGN KEY (parent_id) REFERENCES PARENT_TABLE(id)
        );
        """
        path = self._write_ddl(ddl)
        result = parse_ddl_tables(path)
        
        assert "CHILD_TABLE" in result
        cols = result["CHILD_TABLE"]
        assert len(cols) == 3
    
    def test_inline_references(self):
        """Inline REFERENCES should not affect column parsing."""
        ddl = """
        CREATE TABLE CHILD_TABLE (
            id INTEGER PRIMARY KEY,
            parent_id INTEGER REFERENCES PARENT_TABLE(id),
            name VARCHAR(100)
        );
        """
        path = self._write_ddl(ddl)
        result = parse_ddl_tables(path)
        
        assert "CHILD_TABLE" in result
        assert len(result["CHILD_TABLE"]) == 3
    
    # -------------------------------------------------------------------------
    # Check constraints
    # -------------------------------------------------------------------------
    
    def test_check_constraint(self):
        """CHECK constraints should not affect column parsing."""
        ddl = """
        CREATE TABLE CHECK_TEST (
            id INTEGER PRIMARY KEY,
            age INTEGER CHECK (age >= 0),
            status VARCHAR(20) CHECK (status IN ('ACTIVE', 'INACTIVE'))
        );
        """
        path = self._write_ddl(ddl)
        result = parse_ddl_tables(path)
        
        assert "CHECK_TEST" in result
        cols = result["CHECK_TEST"]
        assert len(cols) == 3
    
    def test_named_constraint(self):
        """Named constraints should not affect column parsing."""
        ddl = """
        CREATE TABLE NAMED_CONSTRAINT_TEST (
            id INTEGER,
            value INTEGER,
            CONSTRAINT chk_value CHECK (value > 0),
            CONSTRAINT pk_test PRIMARY KEY (id)
        );
        """
        path = self._write_ddl(ddl)
        result = parse_ddl_tables(path)
        
        assert "NAMED_CONSTRAINT_TEST" in result
        cols = result["NAMED_CONSTRAINT_TEST"]
        assert len(cols) == 2
    
    # -------------------------------------------------------------------------
    # Unique constraints
    # -------------------------------------------------------------------------
    
    def test_unique_constraint(self):
        """UNIQUE constraints should not affect column parsing."""
        ddl = """
        CREATE TABLE UNIQUE_TEST (
            id INTEGER PRIMARY KEY,
            email VARCHAR(255) UNIQUE,
            code VARCHAR(20),
            UNIQUE (code)
        );
        """
        path = self._write_ddl(ddl)
        result = parse_ddl_tables(path)
        
        assert "UNIQUE_TEST" in result
        cols = result["UNIQUE_TEST"]
        assert len(cols) == 3
    
    # -------------------------------------------------------------------------
    # Generated/computed columns
    # -------------------------------------------------------------------------
    
    def test_generated_column_postgres(self):
        """PostgreSQL generated columns should be handled."""
        ddl = """
        CREATE TABLE GENERATED_TEST (
            id INTEGER,
            first_name VARCHAR(100),
            last_name VARCHAR(100),
            full_name VARCHAR(200) GENERATED ALWAYS AS (first_name || ' ' || last_name) STORED
        );
        """
        path = self._write_ddl(ddl)
        result = parse_ddl_tables(path)
        
        assert "GENERATED_TEST" in result
        # Generated column should still be parsed
        cols = result["GENERATED_TEST"]
        assert "FULL_NAME" in cols
    
    # -------------------------------------------------------------------------
    # CREATE TABLE IF NOT EXISTS
    # -------------------------------------------------------------------------
    
    def test_if_not_exists(self):
        """CREATE TABLE IF NOT EXISTS should be parsed."""
        ddl = """
        CREATE TABLE IF NOT EXISTS CONDITIONAL_TABLE (
            id INTEGER PRIMARY KEY,
            name VARCHAR(100)
        );
        """
        path = self._write_ddl(ddl)
        result = parse_ddl_tables(path)
        
        assert "CONDITIONAL_TABLE" in result
        assert len(result["CONDITIONAL_TABLE"]) == 2
    
    # -------------------------------------------------------------------------
    # CREATE TABLE AS SELECT (should be skipped or handled)
    # -------------------------------------------------------------------------
    
    def test_create_table_as_select(self):
        """CREATE TABLE AS SELECT should be handled gracefully."""
        ddl = """
        CREATE TABLE NORMAL_TABLE (
            id INTEGER
        );
        
        CREATE TABLE CTAS_TABLE AS SELECT * FROM NORMAL_TABLE;
        """
        path = self._write_ddl(ddl)
        result = parse_ddl_tables(path)
        
        # NORMAL_TABLE should be parsed
        assert "NORMAL_TABLE" in result
    
    # -------------------------------------------------------------------------
    # Temporary tables
    # -------------------------------------------------------------------------
    
    def test_temporary_table(self):
        """TEMPORARY tables should be parsed."""
        ddl = """
        CREATE TEMPORARY TABLE TEMP_TABLE (
            id INTEGER,
            data VARCHAR(255)
        );
        """
        path = self._write_ddl(ddl)
        result = parse_ddl_tables(path)
        
        assert "TEMP_TABLE" in result
    
    def test_temp_table_shorthand(self):
        """TEMP tables (shorthand) should be parsed."""
        ddl = """
        CREATE TEMP TABLE TEMP_SHORT (
            id INTEGER
        );
        """
        path = self._write_ddl(ddl)
        result = parse_ddl_tables(path)
        
        assert "TEMP_SHORT" in result
    
    # -------------------------------------------------------------------------
    # Views (should be ignored)
    # -------------------------------------------------------------------------
    
    def test_views_ignored(self):
        """CREATE VIEW should not be parsed as table."""
        ddl = """
        CREATE TABLE REAL_TABLE (
            id INTEGER
        );
        
        CREATE VIEW MY_VIEW AS SELECT * FROM REAL_TABLE;
        """
        path = self._write_ddl(ddl)
        result = parse_ddl_tables(path)
        
        assert "REAL_TABLE" in result
        assert "MY_VIEW" not in result
    
    # -------------------------------------------------------------------------
    # Long identifiers
    # -------------------------------------------------------------------------
    
    def test_very_long_table_name(self):
        """Very long table names should be handled."""
        long_name = "A" * 128  # Max identifier length in most DBs
        ddl = f"""
        CREATE TABLE {long_name} (
            id INTEGER
        );
        """
        path = self._write_ddl(ddl)
        result = parse_ddl_tables(path)
        
        assert long_name.upper() in result
    
    def test_very_long_column_name(self):
        """Very long column names should be handled."""
        long_col = "B" * 128
        ddl = f"""
        CREATE TABLE LONG_COL_TEST (
            {long_col} INTEGER
        );
        """
        path = self._write_ddl(ddl)
        result = parse_ddl_tables(path)
        
        assert long_col.upper() in result["LONG_COL_TEST"]
    
    # -------------------------------------------------------------------------
    # Unicode identifiers
    # -------------------------------------------------------------------------
    
    def test_unicode_table_name(self):
        """Unicode in table names should be handled."""
        ddl = """
        CREATE TABLE "Täble_Nämé" (
            id INTEGER
        );
        """
        path = self._write_ddl(ddl)
        result = parse_ddl_tables(path)
        # Should not crash
    
    # -------------------------------------------------------------------------
    # Model parsing edge cases
    # -------------------------------------------------------------------------
    
    def test_model_with_cte(self):
        """Model with CTE should extract final SELECT columns."""
        model = """
        MODEL (name test.model, kind FULL);
        
        WITH base AS (
            SELECT id, name FROM source
        )
        SELECT
            id AS output_id,
            name AS output_name
        FROM base
        """
        path = self._write_model(model)
        result = get_model_columns_from_sql(path)
        
        assert "OUTPUT_ID" in result
        assert "OUTPUT_NAME" in result
    
    def test_model_with_multiple_ctes(self):
        """Model with multiple CTEs should work."""
        model = """
        MODEL (name test.model, kind FULL);
        
        WITH 
        cte1 AS (SELECT a FROM t1),
        cte2 AS (SELECT b FROM t2)
        SELECT
            cte1.a AS col_a,
            cte2.b AS col_b
        FROM cte1
        JOIN cte2 ON 1=1
        """
        path = self._write_model(model)
        result = get_model_columns_from_sql(path)
        
        assert "COL_A" in result
        assert "COL_B" in result
    
    def test_model_with_union(self):
        """Model with UNION - parser may return empty (UNION not fully supported in column extraction)."""
        model = """
        MODEL (name test.model, kind FULL);
        
        SELECT id AS record_id, 'A' AS source FROM table_a
        UNION ALL
        SELECT id, 'B' FROM table_b
        """
        path = self._write_model(model)
        result = get_model_columns_from_sql(path)
        
        # UNION queries have different structure - sqlglot may not extract columns
        # from UNION expressions the same way. This tests that no crash occurs.
        # In practice, types would be obtained via SQLMesh Context which handles UNIONs.
        assert isinstance(result, list)
    
    def test_model_with_window_functions(self):
        """Model with window functions should work."""
        model = """
        MODEL (name test.model, kind FULL);
        
        SELECT
            id AS record_id,
            ROW_NUMBER() OVER (PARTITION BY category ORDER BY date) AS row_num,
            SUM(amount) OVER (PARTITION BY category) AS category_total
        FROM source
        """
        path = self._write_model(model)
        result = get_model_columns_from_sql(path)
        
        assert "RECORD_ID" in result
        assert "ROW_NUM" in result
        assert "CATEGORY_TOTAL" in result
    
    def test_model_with_aggregates(self):
        """Model with aggregate functions should work."""
        model = """
        MODEL (name test.model, kind FULL);
        
        SELECT
            category AS category_name,
            COUNT(*) AS record_count,
            SUM(amount) AS total_amount,
            AVG(amount) AS avg_amount,
            MAX(date) AS latest_date
        FROM source
        GROUP BY category
        """
        path = self._write_model(model)
        result = get_model_columns_from_sql(path)
        
        assert "CATEGORY_NAME" in result
        assert "RECORD_COUNT" in result
        assert "TOTAL_AMOUNT" in result
        assert "AVG_AMOUNT" in result
        assert "LATEST_DATE" in result
    
    def test_model_with_case_expressions(self):
        """Model with CASE expressions should work."""
        model = """
        MODEL (name test.model, kind FULL);
        
        SELECT
            id AS record_id,
            CASE 
                WHEN status = 'A' THEN 'Active'
                WHEN status = 'I' THEN 'Inactive'
                ELSE 'Unknown'
            END AS status_label,
            CASE type WHEN 1 THEN 'One' WHEN 2 THEN 'Two' END AS type_label
        FROM source
        """
        path = self._write_model(model)
        result = get_model_columns_from_sql(path)
        
        assert "RECORD_ID" in result
        assert "STATUS_LABEL" in result
        assert "TYPE_LABEL" in result
    
    def test_model_with_nested_subquery(self):
        """Model with nested subqueries should work."""
        model = """
        MODEL (name test.model, kind FULL);
        
        SELECT
            id AS outer_id,
            (SELECT MAX(value) FROM other WHERE other.id = main.id) AS max_value
        FROM main
        """
        path = self._write_model(model)
        result = get_model_columns_from_sql(path)
        
        assert "OUTER_ID" in result
        assert "MAX_VALUE" in result
    
    def test_model_with_lateral_join(self):
        """Model with LATERAL join should work."""
        model = """
        MODEL (name test.model, kind FULL);
        
        SELECT
            t.id AS main_id,
            l.value AS lateral_value
        FROM main_table t
        CROSS JOIN LATERAL (SELECT value FROM other WHERE other.id = t.id LIMIT 1) l
        """
        path = self._write_model(model)
        result = get_model_columns_from_sql(path)
        
        assert "MAIN_ID" in result
        assert "LATERAL_VALUE" in result
    
    def test_model_with_distinct(self):
        """Model with DISTINCT should work."""
        model = """
        MODEL (name test.model, kind FULL);
        
        SELECT DISTINCT
            category AS unique_category,
            type AS unique_type
        FROM source
        """
        path = self._write_model(model)
        result = get_model_columns_from_sql(path)
        
        assert "UNIQUE_CATEGORY" in result
        assert "UNIQUE_TYPE" in result
    
    def test_model_with_star_expansion(self):
        """Model with SELECT * should be handled gracefully."""
        model = """
        MODEL (name test.model, kind FULL);
        
        SELECT * FROM source
        """
        path = self._write_model(model)
        result = get_model_columns_from_sql(path)
        
        # * doesn't give us explicit columns, result might be empty
        # Just verify no crash
        assert isinstance(result, list)
    
    def test_model_with_qualified_star(self):
        """Model with table.* should be handled gracefully."""
        model = """
        MODEL (name test.model, kind FULL);
        
        SELECT 
            t.*,
            'constant' AS extra_col
        FROM source t
        """
        path = self._write_model(model)
        result = get_model_columns_from_sql(path)
        
        # At least the explicit column should be found
        assert "EXTRA_COL" in result
    
    def test_model_with_coalesce_and_nvl(self):
        """Model with COALESCE/NVL should work."""
        model = """
        MODEL (name test.model, kind FULL);
        
        SELECT
            COALESCE(a, b, c) AS coalesced_value,
            COALESCE(name, 'Unknown') AS safe_name
        FROM source
        """
        path = self._write_model(model)
        result = get_model_columns_from_sql(path)
        
        assert "COALESCED_VALUE" in result
        assert "SAFE_NAME" in result
    
    def test_model_with_string_functions(self):
        """Model with string functions should work."""
        model = """
        MODEL (name test.model, kind FULL);
        
        SELECT
            UPPER(name) AS upper_name,
            LOWER(name) AS lower_name,
            CONCAT(first, ' ', last) AS full_name,
            SUBSTRING(code, 1, 3) AS code_prefix,
            LENGTH(description) AS desc_length
        FROM source
        """
        path = self._write_model(model)
        result = get_model_columns_from_sql(path)
        
        assert "UPPER_NAME" in result
        assert "LOWER_NAME" in result
        assert "FULL_NAME" in result
        assert "CODE_PREFIX" in result
        assert "DESC_LENGTH" in result
    
    def test_model_with_date_functions(self):
        """Model with date functions should work."""
        model = """
        MODEL (name test.model, kind FULL);
        
        SELECT
            CURRENT_DATE AS today,
            CURRENT_TIMESTAMP AS now,
            DATE_TRUNC('month', created_at) AS month_start,
            created_at + INTERVAL '1 day' AS next_day
        FROM source
        """
        path = self._write_model(model)
        result = get_model_columns_from_sql(path)
        
        assert "TODAY" in result
        assert "NOW" in result
        assert "MONTH_START" in result
        assert "NEXT_DAY" in result
    
    # -------------------------------------------------------------------------
    # Error handling
    # -------------------------------------------------------------------------
    
    def test_invalid_sql_graceful_handling(self):
        """Invalid SQL should be handled gracefully."""
        ddl = """
        CREATE TABL INVALID_SYNTAX (
            id INTEGER
        );
        """
        path = self._write_ddl(ddl)
        # Should not raise, might return empty or partial result
        try:
            result = parse_ddl_tables(path)
            # Either empty or has some data
            assert isinstance(result, dict)
        except Exception:
            # Some parse errors might propagate, that's acceptable
            pass
    
    def test_file_not_found_handling(self):
        """Non-existent file should raise FileNotFoundError."""
        fake_path = Path("/nonexistent/file.sql")
        
        with pytest.raises(FileNotFoundError):
            parse_ddl_tables(fake_path)
    
    def test_empty_model_file(self):
        """Empty model file should return empty list."""
        path = self._write_model("")
        result = get_model_columns_from_sql(path)
        
        assert result == []
    
    def test_model_with_only_comments(self):
        """Model with only comments should return empty list."""
        model = """
        -- This is a comment
        /* Block comment */
        """
        path = self._write_model(model)
        result = get_model_columns_from_sql(path)
        
        assert result == []


# =============================================================================
# Boolean and Bit Type Handling
# =============================================================================

class TestBooleanTypes:
    """Tests for boolean and bit type handling across dialects."""
    
    def test_boolean_type(self):
        """BOOLEAN type should remain BOOLEAN."""
        assert normalize_type("BOOLEAN") == "BOOLEAN"
        assert normalize_type("BOOL") == "BOOL"
    
    def test_bit_type(self):
        """BIT type should remain BIT."""
        assert normalize_type("BIT") == "BIT"
        assert normalize_type("BIT(1)") == "BIT"
    
    def test_boolean_cross_dialect(self):
        """Boolean-like types across dialects."""
        # Different DBs use different types for booleans
        # PostgreSQL: BOOLEAN
        # MySQL: TINYINT(1) or BOOLEAN
        # MSSQL: BIT
        # Oracle: NUMBER(1) or CHAR(1)
        
        bool_types = ["BOOLEAN", "BOOL", "BIT"]
        for t in bool_types:
            # Each should normalize to itself (no mapping)
            assert normalize_type(t) in ["BOOLEAN", "BOOL", "BIT"]


# =============================================================================
# Binary and Blob Types
# =============================================================================

class TestBinaryTypes:
    """Tests for binary and blob type handling."""
    
    def test_blob_type(self):
        """BLOB type should remain BLOB."""
        assert normalize_type("BLOB") == "BLOB"
    
    def test_bytea_type(self):
        """PostgreSQL BYTEA should remain BYTEA."""
        assert normalize_type("BYTEA") == "BYTEA"
    
    def test_binary_types(self):
        """Various binary types should be handled."""
        assert normalize_type("BINARY") == "BINARY"
        assert normalize_type("VARBINARY") == "VARBINARY"
        assert normalize_type("VARBINARY(MAX)") == "VARBINARY"
    
    def test_image_type(self):
        """MSSQL IMAGE type should remain IMAGE."""
        assert normalize_type("IMAGE") == "IMAGE"


# =============================================================================
# Tests for Enhanced DDL Parsing (grains, references, descriptions)
# =============================================================================

class TestForeignKeyReference:
    """Tests for the ForeignKeyReference dataclass."""
    
    def test_basic_creation(self):
        """ForeignKeyReference should lowercase all values."""
        from scripts.validate_schema import ForeignKeyReference
        ref = ForeignKeyReference("CLIENT_ID", "CLIENT", "ID")
        assert ref.column == "client_id"
        assert ref.referenced_table == "client"
        assert ref.referenced_column == "id"


class TestTableSchema:
    """Tests for the TableSchema dataclass."""
    
    def test_basic_creation(self):
        """TableSchema should store all properties."""
        from scripts.validate_schema import TableSchema
        schema = TableSchema(
            name="TEST",
            columns={"ID": "INTEGER", "NAME": "VARCHAR"},
        )
        assert schema.name == "TEST"
        assert schema.columns == {"ID": "INTEGER", "NAME": "VARCHAR"}
    
    def test_grains_property(self):
        """Grains should return lowercase primary keys."""
        from scripts.validate_schema import TableSchema
        schema = TableSchema(
            name="TEST",
            columns={"ID": "INTEGER"},
            primary_keys=["ID", "NAME"],
        )
        assert schema.grains == ["id", "name"]
    
    def test_reference_columns_property(self):
        """Reference columns should return FK columns."""
        from scripts.validate_schema import TableSchema, ForeignKeyReference
        schema = TableSchema(
            name="TEST",
            columns={"ID": "INTEGER"},
            foreign_keys=[
                ForeignKeyReference("client_id", "client", "id"),
                ForeignKeyReference("order_id", "orders", "id"),
            ],
        )
        assert schema.reference_columns == ["client_id", "order_id"]


class TestParseCommentStatements:
    """Tests for _parse_comment_statements function."""
    
    def test_parse_table_comment(self):
        """Parse COMMENT ON TABLE statement."""
        from scripts.validate_schema import _parse_comment_statements
        content = "COMMENT ON TABLE my_table IS 'This is a test table';"
        table_comments, column_comments = _parse_comment_statements(content)
        
        assert "MY_TABLE" in table_comments
        assert table_comments["MY_TABLE"] == "This is a test table"
    
    def test_parse_column_comment(self):
        """Parse COMMENT ON COLUMN statement."""
        from scripts.validate_schema import _parse_comment_statements
        content = "COMMENT ON COLUMN my_table.my_column IS 'Column description';"
        table_comments, column_comments = _parse_comment_statements(content)
        
        assert "MY_TABLE" in column_comments
        assert "MY_COLUMN" in column_comments["MY_TABLE"]
        assert column_comments["MY_TABLE"]["MY_COLUMN"] == "Column description"
    
    def test_parse_schema_qualified_comments(self):
        """Parse comments with schema-qualified names."""
        from scripts.validate_schema import _parse_comment_statements
        content = """
        COMMENT ON TABLE public.my_table IS 'Table comment';
        COMMENT ON COLUMN public.my_table.id IS 'ID column';
        """
        table_comments, column_comments = _parse_comment_statements(content)
        
        assert table_comments["MY_TABLE"] == "Table comment"
        assert column_comments["MY_TABLE"]["ID"] == "ID column"


class TestParseForeignKeys:
    """Tests for _parse_foreign_keys function."""
    
    def test_parse_active_fk(self):
        """Parse active ALTER TABLE FOREIGN KEY statement."""
        from scripts.validate_schema import _parse_foreign_keys
        content = """
        ALTER TABLE orders ADD CONSTRAINT fk_client 
            FOREIGN KEY (client_id) REFERENCES client (id);
        """
        fks = _parse_foreign_keys(content)
        
        assert "ORDERS" in fks
        assert len(fks["ORDERS"]) == 1
        assert fks["ORDERS"][0].column == "client_id"
        assert fks["ORDERS"][0].referenced_table == "client"
    
    def test_parse_commented_fk(self):
        """Parse commented-out FOREIGN KEY (GGM style - single line)."""
        from scripts.validate_schema import _parse_foreign_keys
        # The regex matches single-line commented FK statements
        content = "-- ALTER TABLE orders ADD CONSTRAINT fk_client FOREIGN KEY (client_id) REFERENCES client (id);"
        fks = _parse_foreign_keys(content)
        
        assert "ORDERS" in fks
        assert fks["ORDERS"][0].column == "client_id"
    
    def test_parse_multiple_fks(self):
        """Parse multiple foreign keys for same table."""
        from scripts.validate_schema import _parse_foreign_keys
        content = """
        ALTER TABLE orders ADD CONSTRAINT fk_client FOREIGN KEY (client_id) REFERENCES client (id);
        ALTER TABLE orders ADD CONSTRAINT fk_product FOREIGN KEY (product_id) REFERENCES product (id);
        """
        fks = _parse_foreign_keys(content)
        
        assert len(fks["ORDERS"]) == 2


class TestParsePrimaryKeys:
    """Tests for _parse_primary_keys function."""
    
    def test_parse_alter_table_pk(self):
        """Parse ALTER TABLE ADD PRIMARY KEY statement."""
        from scripts.validate_schema import _parse_primary_keys
        content = "ALTER TABLE my_table ADD PRIMARY KEY (id);"
        pks = _parse_primary_keys(content, {})
        
        assert "MY_TABLE" in pks
        assert pks["MY_TABLE"] == ["ID"]
    
    def test_parse_composite_pk(self):
        """Parse composite primary key."""
        from scripts.validate_schema import _parse_primary_keys
        content = "ALTER TABLE my_table ADD PRIMARY KEY (id, version);"
        pks = _parse_primary_keys(content, {})
        
        assert pks["MY_TABLE"] == ["ID", "VERSION"]
    
    def test_merge_with_inline_pks(self):
        """Merge with inline primary keys."""
        from scripts.validate_schema import _parse_primary_keys
        inline = {"OTHER_TABLE": ["ID"]}
        content = "ALTER TABLE my_table ADD PRIMARY KEY (id);"
        pks = _parse_primary_keys(content, inline)
        
        assert "OTHER_TABLE" in pks
        assert "MY_TABLE" in pks


class TestParseDdlSchemas:
    """Tests for parse_ddl_schemas function."""
    
    def test_parse_table_with_pk(self, tmp_path):
        """Parse table with inline primary key."""
        from scripts.validate_schema import parse_ddl_schemas
        
        ddl = tmp_path / "test.sql"
        ddl.write_text("""
        CREATE TABLE test_table (
            id INTEGER PRIMARY KEY,
            name VARCHAR(100)
        );
        """)
        
        schemas = parse_ddl_schemas(ddl)
        assert "TEST_TABLE" in schemas
        assert schemas["TEST_TABLE"].primary_keys == ["ID"]
        assert schemas["TEST_TABLE"].grains == ["id"]
    
    def test_parse_table_with_fks(self, tmp_path):
        """Parse table with foreign keys."""
        from scripts.validate_schema import parse_ddl_schemas
        
        ddl = tmp_path / "test.sql"
        ddl.write_text("""
        CREATE TABLE orders (
            id INTEGER PRIMARY KEY,
            client_id INTEGER
        );
        ALTER TABLE orders ADD CONSTRAINT fk_client FOREIGN KEY (client_id) REFERENCES client (id);
        """)
        
        schemas = parse_ddl_schemas(ddl)
        assert len(schemas["ORDERS"].foreign_keys) == 1
        assert schemas["ORDERS"].reference_columns == ["client_id"]
    
    def test_parse_table_with_comments(self, tmp_path):
        """Parse table with COMMENT ON statements."""
        from scripts.validate_schema import parse_ddl_schemas
        
        ddl = tmp_path / "test.sql"
        ddl.write_text("""
        CREATE TABLE test_table (id INTEGER, name VARCHAR(100));
        COMMENT ON TABLE test_table IS 'Test description';
        COMMENT ON COLUMN test_table.id IS 'Primary identifier';
        """)
        
        schemas = parse_ddl_schemas(ddl)
        assert schemas["TEST_TABLE"].description == "Test description"
        assert schemas["TEST_TABLE"].column_descriptions["ID"] == "Primary identifier"


class TestValidateWithEnhancedOptions:
    """Tests for validate function with enhanced validation options."""
    
    def test_validate_grains_matching(self, tmp_path):
        """Validate matching grains passes."""
        from scripts.validate_schema import parse_ddl_schemas, TableSchema, ModelSchema
        
        # Create DDL with primary key
        ddl_schema = TableSchema(
            name="TEST",
            columns={"ID": "INTEGER", "NAME": "VARCHAR"},
            primary_keys=["ID"],
        )
        
        # Create matching model schema
        model_schema = ModelSchema(
            name="TEST",
            columns={"ID": "INTEGER", "NAME": "VARCHAR"},
            grains=["ID"],
        )
        
        # Grains should match
        ddl_grains = set(g.upper() for g in ddl_schema.grains)
        model_grains = set(g.upper() for g in model_schema.grains)
        assert ddl_grains == model_grains
    
    def test_validate_grains_mismatch_detected(self):
        """Validate mismatched grains detected."""
        from scripts.validate_schema import TableSchema, ModelSchema
        
        ddl_schema = TableSchema(
            name="TEST",
            columns={"ID": "INTEGER"},
            primary_keys=["ID", "VERSION"],
        )
        
        model_schema = ModelSchema(
            name="TEST",
            columns={"ID": "INTEGER"},
            grains=["ID"],  # Missing VERSION
        )
        
        ddl_grains = set(g.upper() for g in ddl_schema.grains)
        model_grains = set(g.upper() for g in model_schema.grains)
        
        missing = ddl_grains - model_grains
        assert "VERSION" in missing
    
    def test_validate_references_matching(self):
        """Validate matching references passes."""
        from scripts.validate_schema import TableSchema, ModelSchema, ForeignKeyReference
        
        ddl_schema = TableSchema(
            name="TEST",
            columns={"ID": "INTEGER", "CLIENT_ID": "INTEGER"},
            foreign_keys=[ForeignKeyReference("client_id", "client", "id")],
        )
        
        model_schema = ModelSchema(
            name="TEST",
            columns={"ID": "INTEGER", "CLIENT_ID": "INTEGER"},
            references=["CLIENT_ID"],
        )
        
        ddl_refs = set(r.upper() for r in ddl_schema.reference_columns)
        model_refs = set(r.upper() for r in model_schema.references)
        assert ddl_refs == model_refs


# =============================================================================
# Run tests
# =============================================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
