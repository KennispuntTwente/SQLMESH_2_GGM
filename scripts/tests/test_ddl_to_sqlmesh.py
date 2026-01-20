"""Comprehensive tests for ddl_to_sqlmesh.py.

Tests cover:
- DDL parsing with various column definitions
- Primary key detection for grains
- Table and column description parsing
- Foreign key parsing for references
- SQLMesh model generation with all properties
- File writing and overwrite protection
- Edge cases and error handling
- Integration with actual GGM DDL files
"""
from __future__ import annotations

import tempfile
from pathlib import Path
from typing import Generator

import pytest
import sqlglot

# Import module under test
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from scripts.ddl_to_sqlmesh import (
    ColumnDefinition,
    TableDefinition,
    ForeignKeyReference,
    parse_ddl_to_tables,
    parse_ddl_directory_to_tables,
    format_sql_type,
    generate_sqlmesh_model,
    generate_model_with_source_mapping,
    write_model_file,
    convert_ddl_to_models,
    find_default_ddl_path,
    _parse_comment_statements,
    _parse_foreign_keys,
    _escape_sql_string,
)


# =============================================================================
# Fixtures
# =============================================================================

@pytest.fixture
def temp_dir() -> Generator[Path, None, None]:
    """Create a temporary directory for test files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def sample_ddl_content() -> str:
    """Sample DDL content for testing."""
    return """
CREATE TABLE BESCHIKKING (
    BESCHIKKING_ID VARCHAR(255) PRIMARY KEY,
    CLIENT_ID VARCHAR(255),
    CODE VARCHAR(20),
    COMMENTAAR VARCHAR(200),
    DATUMAFGIFTE DATE,
    GRONDSLAGEN INTEGER,
    WET VARCHAR(255)
);

CREATE TABLE CLIENT (
    RECHTSPERSOON_ID VARCHAR(255) PRIMARY KEY,
    CODE VARCHAR(80),
    JURIDISCHESTATUS VARCHAR(80)
);
"""


@pytest.fixture
def sample_ddl_file(temp_dir: Path, sample_ddl_content: str) -> Path:
    """Create a temporary DDL file."""
    ddl_file = temp_dir / "test_schema.sql"
    ddl_file.write_text(sample_ddl_content)
    return ddl_file


# =============================================================================
# Tests for ColumnDefinition dataclass
# =============================================================================

class TestColumnDefinition:
    """Tests for ColumnDefinition dataclass."""
    
    def test_basic_creation(self):
        """Basic column definition creation."""
        col = ColumnDefinition(
            name="BESCHIKKING_ID",
            data_type="VARCHAR",
            raw_type="VARCHAR(255)",
        )
        # Name should be lowercased
        assert col.name == "beschikking_id"
        assert col.data_type == "VARCHAR"
        assert col.raw_type == "VARCHAR(255)"
        assert col.is_primary_key is False
        assert col.is_nullable is True
    
    def test_primary_key_column(self):
        """Primary key column definition."""
        col = ColumnDefinition(
            name="ID",
            data_type="INTEGER",
            raw_type="INTEGER",
            is_primary_key=True,
        )
        assert col.is_primary_key is True
    
    def test_name_lowercasing(self):
        """Column names should be lowercased."""
        col = ColumnDefinition(
            name="DATUMAFGIFTE",
            data_type="DATE",
            raw_type="DATE",
        )
        assert col.name == "datumafgifte"
    
    def test_mixed_case_name(self):
        """Mixed case names should be lowercased."""
        col = ColumnDefinition(
            name="DatumAfgifte",
            data_type="DATE",
            raw_type="DATE",
        )
        assert col.name == "datumafgifte"


# =============================================================================
# Tests for TableDefinition dataclass
# =============================================================================

class TestTableDefinition:
    """Tests for TableDefinition dataclass."""
    
    def test_basic_creation(self):
        """Basic table definition creation."""
        table = TableDefinition(name="BESCHIKKING")
        assert table.name == "BESCHIKKING"
        assert table.columns == []
        assert table.source_file is None
    
    def test_model_name_lowercase(self):
        """Model name should be lowercase."""
        table = TableDefinition(name="BESCHIKKING")
        assert table.model_name == "beschikking"
    
    def test_primary_key_columns(self):
        """Primary key columns property."""
        table = TableDefinition(
            name="TEST_TABLE",
            columns=[
                ColumnDefinition("ID", "VARCHAR", "VARCHAR(255)", is_primary_key=True),
                ColumnDefinition("NAME", "VARCHAR", "VARCHAR(100)", is_primary_key=False),
                ColumnDefinition("CODE", "INTEGER", "INTEGER", is_primary_key=True),
            ]
        )
        pk_cols = table.primary_key_columns
        assert len(pk_cols) == 2
        assert pk_cols[0].name == "id"
        assert pk_cols[1].name == "code"
    
    def test_no_primary_key(self):
        """Table without primary key."""
        table = TableDefinition(
            name="TEST_TABLE",
            columns=[
                ColumnDefinition("NAME", "VARCHAR", "VARCHAR(100)"),
            ]
        )
        assert table.primary_key_columns == []
    
    def test_grains_property(self):
        """Grains should return primary key column names."""
        table = TableDefinition(
            name="TEST_TABLE",
            columns=[
                ColumnDefinition("ID", "VARCHAR", "VARCHAR(255)", is_primary_key=True),
                ColumnDefinition("NAME", "VARCHAR", "VARCHAR(100)"),
            ]
        )
        assert table.grains == ["id"]
    
    def test_grains_composite_key(self):
        """Grains with composite primary key."""
        table = TableDefinition(
            name="TEST_TABLE",
            columns=[
                ColumnDefinition("ID1", "VARCHAR", "VARCHAR(255)", is_primary_key=True),
                ColumnDefinition("ID2", "VARCHAR", "VARCHAR(255)", is_primary_key=True),
                ColumnDefinition("NAME", "VARCHAR", "VARCHAR(100)"),
            ]
        )
        assert table.grains == ["id1", "id2"]
    
    def test_reference_columns(self):
        """Reference columns from foreign keys."""
        table = TableDefinition(
            name="TEST_TABLE",
            columns=[ColumnDefinition("ID", "VARCHAR", "VARCHAR(255)")],
            references=[
                ForeignKeyReference("client_id", "client", "id"),
                ForeignKeyReference("order_id", "orders", "id"),
            ]
        )
        assert table.reference_columns == ["client_id", "order_id"]
    
    def test_column_descriptions_property(self):
        """Column descriptions dictionary."""
        table = TableDefinition(
            name="TEST",
            columns=[
                ColumnDefinition("ID", "VARCHAR", "VARCHAR(255)", description="Primary identifier"),
                ColumnDefinition("NAME", "VARCHAR", "VARCHAR(100)", description=None),
                ColumnDefinition("CODE", "VARCHAR", "VARCHAR(20)", description="Unique code"),
            ]
        )
        assert table.column_descriptions == {
            "id": "Primary identifier",
            "code": "Unique code",
        }
    
    def test_description_from_source_with_explicit(self):
        """get_description_from_source returns explicit description if set."""
        table = TableDefinition(
            name="TEST",
            description="Explicit description",
            source_file="test_postgres.sql",
        )
        assert table.get_description_from_source() == "Explicit description"
    
    def test_description_from_source_file(self):
        """get_description_from_source generates from file name."""
        table = TableDefinition(
            name="TEST",
            source_file="Sociaal_Domein_Beschikking__Test_postgres.sql",
        )
        desc = table.get_description_from_source()
        assert desc is not None
        assert "Sociaal Domein Beschikking" in desc
        assert "_postgres" not in desc
    
    def test_description_from_source_no_source(self):
        """get_description_from_source returns None if no source."""
        table = TableDefinition(name="TEST")
        assert table.get_description_from_source() is None


# =============================================================================
# Tests for ForeignKeyReference dataclass
# =============================================================================

class TestForeignKeyReference:
    """Tests for ForeignKeyReference dataclass."""
    
    def test_basic_creation(self):
        """Basic FK reference creation."""
        fk = ForeignKeyReference(
            column="CLIENT_ID",
            referenced_table="CLIENT",
            referenced_column="ID"
        )
        # Names should be lowercased
        assert fk.column == "client_id"
        assert fk.referenced_table == "client"
        assert fk.referenced_column == "id"
    
    def test_mixed_case(self):
        """Mixed case names are lowercased."""
        fk = ForeignKeyReference(
            column="ClientId",
            referenced_table="ClientTable",
            referenced_column="Id"
        )
        assert fk.column == "clientid"
        assert fk.referenced_table == "clienttable"


# =============================================================================
# Tests for _parse_comment_statements()
# =============================================================================

class TestParseCommentStatements:
    """Tests for _parse_comment_statements function."""
    
    def test_parse_table_comment(self):
        """Parse COMMENT ON TABLE statement."""
        content = """
CREATE TABLE TEST (ID INTEGER);
COMMENT ON TABLE TEST IS 'This is a test table';
"""
        table_comments, col_comments = _parse_comment_statements(content)
        assert table_comments == {"TEST": "This is a test table"}
        assert col_comments == {}
    
    def test_parse_column_comment(self):
        """Parse COMMENT ON COLUMN statement."""
        content = """
CREATE TABLE TEST (ID INTEGER, NAME VARCHAR(100));
COMMENT ON COLUMN TEST.ID IS 'Primary identifier';
COMMENT ON COLUMN TEST.NAME IS 'Full name';
"""
        table_comments, col_comments = _parse_comment_statements(content)
        assert table_comments == {}
        assert col_comments == {
            "TEST": {
                "ID": "Primary identifier",
                "NAME": "Full name",
            }
        }
    
    def test_parse_mixed_comments(self):
        """Parse both table and column comments."""
        content = """
CREATE TABLE TEST (ID INTEGER);
COMMENT ON TABLE TEST IS 'Test table';
COMMENT ON COLUMN TEST.ID IS 'The ID';
"""
        table_comments, col_comments = _parse_comment_statements(content)
        assert table_comments == {"TEST": "Test table"}
        assert col_comments == {"TEST": {"ID": "The ID"}}
    
    def test_parse_schema_qualified_comments(self):
        """Parse comments with schema-qualified names."""
        content = """
COMMENT ON TABLE public.TEST IS 'Public test';
COMMENT ON COLUMN public.TEST.ID IS 'The ID';
"""
        table_comments, col_comments = _parse_comment_statements(content)
        assert table_comments == {"TEST": "Public test"}
        assert col_comments == {"TEST": {"ID": "The ID"}}
    
    def test_no_comments(self):
        """No comments returns empty dicts."""
        content = "CREATE TABLE TEST (ID INTEGER);"
        table_comments, col_comments = _parse_comment_statements(content)
        assert table_comments == {}
        assert col_comments == {}


# =============================================================================
# Tests for _parse_foreign_keys()
# =============================================================================

class TestParseForeignKeys:
    """Tests for _parse_foreign_keys function."""
    
    def test_parse_active_fk(self):
        """Parse active ALTER TABLE FOREIGN KEY statement."""
        content = """
ALTER TABLE BESCHIKKING ADD CONSTRAINT FK_TEST FOREIGN KEY (CLIENT_ID) REFERENCES CLIENT (ID);
"""
        fks = _parse_foreign_keys(content)
        assert "BESCHIKKING" in fks
        assert len(fks["BESCHIKKING"]) == 1
        assert fks["BESCHIKKING"][0].column == "client_id"
        assert fks["BESCHIKKING"][0].referenced_table == "client"
        assert fks["BESCHIKKING"][0].referenced_column == "id"
    
    def test_parse_commented_fk(self):
        """Parse commented-out FK statement (GGM pattern)."""
        content = """
-- ALTER TABLE BESCHIKKING ADD CONSTRAINT FK_TEST FOREIGN KEY (CLIENT_ID) REFERENCES CLIENT (ID);
"""
        fks = _parse_foreign_keys(content)
        assert "BESCHIKKING" in fks
        assert len(fks["BESCHIKKING"]) == 1
        assert fks["BESCHIKKING"][0].column == "client_id"
    
    def test_parse_multiple_fks(self):
        """Parse multiple foreign keys for same table."""
        content = """
ALTER TABLE ORDERS ADD CONSTRAINT FK1 FOREIGN KEY (CLIENT_ID) REFERENCES CLIENT (ID);
ALTER TABLE ORDERS ADD CONSTRAINT FK2 FOREIGN KEY (PRODUCT_ID) REFERENCES PRODUCT (ID);
"""
        fks = _parse_foreign_keys(content)
        assert "ORDERS" in fks
        assert len(fks["ORDERS"]) == 2
        columns = [fk.column for fk in fks["ORDERS"]]
        assert "client_id" in columns
        assert "product_id" in columns
    
    def test_no_foreign_keys(self):
        """No foreign keys returns empty dict."""
        content = "CREATE TABLE TEST (ID INTEGER);"
        fks = _parse_foreign_keys(content)
        assert fks == {}


# =============================================================================
# Tests for _escape_sql_string()
# =============================================================================

class TestEscapeSqlString:
    """Tests for _escape_sql_string function."""
    
    def test_no_escaping_needed(self):
        """String without quotes needs no escaping."""
        assert _escape_sql_string("Hello World") == "Hello World"
    
    def test_single_quote_escaped(self):
        """Single quotes are doubled."""
        assert _escape_sql_string("It's a test") == "It''s a test"
    
    def test_multiple_quotes(self):
        """Multiple quotes are all escaped."""
        assert _escape_sql_string("'quoted'") == "''quoted''"


# =============================================================================
# Tests for parse_ddl_to_tables()
# =============================================================================

class TestParseDdlToTables:
    """Tests for parse_ddl_to_tables function."""
    
    def test_parse_single_table(self, temp_dir: Path):
        """Parse a DDL file with a single table."""
        ddl_content = """
CREATE TABLE BESCHIKKING (
    BESCHIKKING_ID VARCHAR(255) PRIMARY KEY,
    CLIENT_ID VARCHAR(255),
    DATUMAFGIFTE DATE
);
"""
        ddl_file = temp_dir / "single.sql"
        ddl_file.write_text(ddl_content)
        
        tables = parse_ddl_to_tables(ddl_file)
        
        assert len(tables) == 1
        assert tables[0].name == "BESCHIKKING"
        assert len(tables[0].columns) == 3
    
    def test_parse_multiple_tables(self, sample_ddl_file: Path):
        """Parse a DDL file with multiple tables."""
        tables = parse_ddl_to_tables(sample_ddl_file)
        
        assert len(tables) == 2
        table_names = {t.name for t in tables}
        assert "BESCHIKKING" in table_names
        assert "CLIENT" in table_names
    
    def test_parse_primary_key_detection(self, temp_dir: Path):
        """Primary key columns should be detected."""
        ddl_content = """
CREATE TABLE TEST_TABLE (
    ID VARCHAR(255) PRIMARY KEY,
    NAME VARCHAR(100)
);
"""
        ddl_file = temp_dir / "pk.sql"
        ddl_file.write_text(ddl_content)
        
        tables = parse_ddl_to_tables(ddl_file)
        
        assert len(tables) == 1
        id_col = next(c for c in tables[0].columns if c.name == "id")
        name_col = next(c for c in tables[0].columns if c.name == "name")
        
        assert id_col.is_primary_key is True
        assert name_col.is_primary_key is False
    
    def test_parse_various_types(self, temp_dir: Path):
        """Parse various SQL types."""
        ddl_content = """
CREATE TABLE TYPE_TEST (
    VARCHAR_COL VARCHAR(255),
    INT_COL INTEGER,
    DATE_COL DATE,
    DECIMAL_COL DECIMAL(10,2),
    TEXT_COL TEXT
);
"""
        ddl_file = temp_dir / "types.sql"
        ddl_file.write_text(ddl_content)
        
        tables = parse_ddl_to_tables(ddl_file)
        
        assert len(tables) == 1
        cols = {c.name: c for c in tables[0].columns}
        
        assert cols["varchar_col"].data_type == "VARCHAR"
        assert cols["int_col"].data_type == "INTEGER"
        assert cols["date_col"].data_type == "DATE"
        assert cols["decimal_col"].data_type == "DECIMAL"
        assert cols["text_col"].data_type == "VARCHAR"  # TEXT normalizes to VARCHAR
    
    def test_parse_file_not_found(self, temp_dir: Path):
        """FileNotFoundError when DDL file doesn't exist."""
        with pytest.raises(FileNotFoundError):
            parse_ddl_to_tables(temp_dir / "nonexistent.sql")
    
    def test_parse_empty_file(self, temp_dir: Path):
        """Empty DDL file returns empty list."""
        ddl_file = temp_dir / "empty.sql"
        ddl_file.write_text("")
        
        tables = parse_ddl_to_tables(ddl_file)
        assert tables == []
    
    def test_parse_comments_only(self, temp_dir: Path):
        """DDL file with only comments returns empty list."""
        ddl_content = """
-- This is a comment
-- Another comment
/* Block comment */
"""
        ddl_file = temp_dir / "comments.sql"
        ddl_file.write_text(ddl_content)
        
        tables = parse_ddl_to_tables(ddl_file)
        assert tables == []
    
    def test_parse_commented_table(self, temp_dir: Path):
        """Commented-out tables should be ignored."""
        ddl_content = """
-- CREATE TABLE IGNORED (
--     ID VARCHAR(255)
-- );

CREATE TABLE INCLUDED (
    ID VARCHAR(255) PRIMARY KEY
);
"""
        ddl_file = temp_dir / "commented.sql"
        ddl_file.write_text(ddl_content)
        
        tables = parse_ddl_to_tables(ddl_file)
        
        assert len(tables) == 1
        assert tables[0].name == "INCLUDED"
    
    def test_parse_schema_qualified_name(self, temp_dir: Path):
        """Schema-qualified table names should extract just the table name."""
        ddl_content = """
CREATE TABLE public.BESCHIKKING (
    ID VARCHAR(255) PRIMARY KEY
);
"""
        ddl_file = temp_dir / "schema_qualified.sql"
        ddl_file.write_text(ddl_content)
        
        tables = parse_ddl_to_tables(ddl_file)
        
        assert len(tables) == 1
        assert tables[0].name == "BESCHIKKING"
    
    def test_parse_preserves_raw_type(self, temp_dir: Path):
        """Raw type with precision should be preserved."""
        ddl_content = """
CREATE TABLE MONEY_TABLE (
    AMOUNT DECIMAL(18,4),
    NAME VARCHAR(500)
);
"""
        ddl_file = temp_dir / "raw_type.sql"
        ddl_file.write_text(ddl_content)
        
        tables = parse_ddl_to_tables(ddl_file)
        cols = {c.name: c for c in tables[0].columns}
        
        assert "18" in cols["amount"].raw_type or "DECIMAL" in cols["amount"].raw_type
        assert cols["name"].data_type == "VARCHAR"


# =============================================================================
# Tests for parse_ddl_directory_to_tables()
# =============================================================================

class TestParseDdlDirectoryToTables:
    """Tests for parse_ddl_directory_to_tables function."""
    
    def test_parse_directory(self, temp_dir: Path):
        """Parse all SQL files in a directory."""
        # Create multiple DDL files
        (temp_dir / "table1.sql").write_text("CREATE TABLE T1 (ID INTEGER PRIMARY KEY);")
        (temp_dir / "table2.sql").write_text("CREATE TABLE T2 (ID INTEGER PRIMARY KEY);")
        
        tables = parse_ddl_directory_to_tables(temp_dir)
        
        assert len(tables) == 2
        names = {t.name for t in tables}
        assert "T1" in names
        assert "T2" in names
    
    def test_parse_subdirectories(self, temp_dir: Path):
        """Parse SQL files in subdirectories."""
        subdir = temp_dir / "subdir"
        subdir.mkdir()
        (subdir / "table3.sql").write_text("CREATE TABLE T3 (ID INTEGER PRIMARY KEY);")
        
        tables = parse_ddl_directory_to_tables(temp_dir)
        
        assert len(tables) == 1
        assert tables[0].name == "T3"
    
    def test_empty_directory(self, temp_dir: Path):
        """Empty directory returns empty list."""
        empty_dir = temp_dir / "empty"
        empty_dir.mkdir()
        
        tables = parse_ddl_directory_to_tables(empty_dir)
        assert tables == []
    
    def test_skip_invalid_files(self, temp_dir: Path, capsys):
        """Invalid SQL files should be skipped with warning."""
        # Valid file
        (temp_dir / "valid.sql").write_text("CREATE TABLE VALID (ID INTEGER);")
        # Invalid file (not parseable)
        (temp_dir / "invalid.sql").write_text("THIS IS NOT VALID SQL :::: &&&&")
        
        tables = parse_ddl_directory_to_tables(temp_dir)
        
        # Should have parsed the valid file
        assert len(tables) >= 1
        assert any(t.name == "VALID" for t in tables)


# =============================================================================
# Tests for format_sql_type()
# =============================================================================

class TestFormatSqlType:
    """Tests for format_sql_type function."""
    
    def test_varchar_with_length(self):
        """VARCHAR with length should be preserved."""
        assert "VARCHAR" in format_sql_type("VARCHAR(255)")
        assert "255" in format_sql_type("VARCHAR(255)")
    
    def test_decimal_with_precision(self):
        """DECIMAL with precision should be preserved."""
        result = format_sql_type("DECIMAL(10,2)")
        assert "DECIMAL" in result
    
    def test_uppercase_conversion(self):
        """Types should be uppercased."""
        assert format_sql_type("varchar") == "VARCHAR"
        assert format_sql_type("integer") == "INTEGER"
    
    def test_varchar2_to_varchar(self):
        """VARCHAR2 should be converted to VARCHAR."""
        assert "VARCHAR" in format_sql_type("VARCHAR2(255)")
        assert "VARCHAR2" not in format_sql_type("VARCHAR2(255)")


# =============================================================================
# Tests for generate_sqlmesh_model()
# =============================================================================

class TestGenerateSqlmeshModel:
    """Tests for generate_sqlmesh_model function."""
    
    def test_basic_model_generation(self):
        """Generate a basic model from table definition."""
        table = TableDefinition(
            name="BESCHIKKING",
            columns=[
                ColumnDefinition("id", "VARCHAR", "VARCHAR(255)", is_primary_key=True),
                ColumnDefinition("name", "VARCHAR", "VARCHAR(100)"),
            ]
        )
        
        model_sql = generate_sqlmesh_model(table)
        
        # Check MODEL block
        assert "MODEL (" in model_sql
        assert "name silver.beschikking" in model_sql
        assert "kind FULL" in model_sql
        
        # Check SELECT
        assert "SELECT" in model_sql
        assert "CAST(NULL AS VARCHAR(255)) AS id" in model_sql
        assert "CAST(NULL AS VARCHAR(100)) AS name" in model_sql
    
    def test_model_with_custom_schema(self):
        """Generate model with custom schema name."""
        table = TableDefinition(
            name="TEST",
            columns=[ColumnDefinition("id", "INTEGER", "INTEGER")]
        )
        
        model_sql = generate_sqlmesh_model(table, schema="bronze")
        
        assert "name bronze.test" in model_sql
    
    def test_model_without_comments(self):
        """Generate model without comments."""
        table = TableDefinition(
            name="TEST",
            columns=[ColumnDefinition("id", "INTEGER", "INTEGER")]
        )
        
        model_sql = generate_sqlmesh_model(table, include_comments=False)
        
        assert "-- Transform" not in model_sql
        assert "-- TODO" not in model_sql
    
    def test_primary_key_indicator(self):
        """Primary key columns should have indicator comment."""
        table = TableDefinition(
            name="TEST",
            columns=[
                ColumnDefinition("id", "VARCHAR", "VARCHAR(255)", is_primary_key=True),
                ColumnDefinition("name", "VARCHAR", "VARCHAR(100)"),
            ]
        )
        
        model_sql = generate_sqlmesh_model(table)
        
        assert "-- PRIMARY KEY" in model_sql
        # Primary key comment should be on the id line
        for line in model_sql.split("\n"):
            if "AS id" in line:
                assert "PRIMARY KEY" in line
    
    def test_model_with_source_table(self):
        """Generate model with source table reference."""
        table = TableDefinition(
            name="TEST",
            columns=[ColumnDefinition("id", "INTEGER", "INTEGER")]
        )
        
        model_sql = generate_sqlmesh_model(table, source_table="stg.source")
        
        assert "FROM stg.source" in model_sql
    
    def test_model_column_order(self):
        """Columns should be in definition order."""
        table = TableDefinition(
            name="TEST",
            columns=[
                ColumnDefinition("alpha", "VARCHAR", "VARCHAR(10)"),
                ColumnDefinition("beta", "INTEGER", "INTEGER"),
                ColumnDefinition("gamma", "DATE", "DATE"),
            ]
        )
        
        model_sql = generate_sqlmesh_model(table)
        
        # Find positions of columns in output
        alpha_pos = model_sql.find("AS alpha")
        beta_pos = model_sql.find("AS beta")
        gamma_pos = model_sql.find("AS gamma")
        
        assert alpha_pos < beta_pos < gamma_pos
    
    def test_model_ends_with_newline(self):
        """Generated model should end with newline."""
        table = TableDefinition(
            name="TEST",
            columns=[ColumnDefinition("id", "INTEGER", "INTEGER")]
        )
        
        model_sql = generate_sqlmesh_model(table)
        
        assert model_sql.endswith("\n")
    
    def test_model_with_description(self):
        """Generate model with description property."""
        table = TableDefinition(
            name="TEST",
            columns=[ColumnDefinition("id", "INTEGER", "INTEGER")],
            description="This is a test table",
        )
        
        model_sql = generate_sqlmesh_model(table, include_description=True)
        
        assert "description 'This is a test table'" in model_sql
    
    def test_model_description_from_source_file(self):
        """Generate model with description from source file name."""
        table = TableDefinition(
            name="TEST",
            columns=[ColumnDefinition("id", "INTEGER", "INTEGER")],
            source_file="Test_Domain_Objects_postgres.sql",
        )
        
        model_sql = generate_sqlmesh_model(table, include_description=True)
        
        assert "description 'GGM-tabel van Test Domain Objects'" in model_sql
    
    def test_model_with_grains(self):
        """Generate model with grains from primary key."""
        table = TableDefinition(
            name="TEST",
            columns=[
                ColumnDefinition("id", "INTEGER", "INTEGER", is_primary_key=True),
                ColumnDefinition("name", "VARCHAR", "VARCHAR(100)"),
            ]
        )
        
        model_sql = generate_sqlmesh_model(table, include_grains=True)
        
        assert "grains id" in model_sql
    
    def test_model_with_composite_grains(self):
        """Generate model with composite grains."""
        table = TableDefinition(
            name="TEST",
            columns=[
                ColumnDefinition("id1", "INTEGER", "INTEGER", is_primary_key=True),
                ColumnDefinition("id2", "INTEGER", "INTEGER", is_primary_key=True),
            ]
        )
        
        model_sql = generate_sqlmesh_model(table, include_grains=True)
        
        assert "grains (id1, id2)" in model_sql
    
    def test_model_with_references(self):
        """Generate model with references from foreign keys."""
        table = TableDefinition(
            name="TEST",
            columns=[ColumnDefinition("id", "INTEGER", "INTEGER")],
            references=[
                ForeignKeyReference("client_id", "client", "id"),
            ]
        )
        
        model_sql = generate_sqlmesh_model(table, include_references=True)
        
        assert "references client_id" in model_sql
    
    def test_model_with_multiple_references(self):
        """Generate model with multiple references."""
        table = TableDefinition(
            name="TEST",
            columns=[ColumnDefinition("id", "INTEGER", "INTEGER")],
            references=[
                ForeignKeyReference("client_id", "client", "id"),
                ForeignKeyReference("order_id", "orders", "id"),
            ]
        )
        
        model_sql = generate_sqlmesh_model(table, include_references=True)
        
        assert "references (client_id, order_id)" in model_sql
    
    def test_model_with_column_descriptions(self):
        """Generate model with column descriptions."""
        table = TableDefinition(
            name="TEST",
            columns=[
                ColumnDefinition("id", "INTEGER", "INTEGER", description="Primary identifier"),
                ColumnDefinition("name", "VARCHAR", "VARCHAR(100)", description="Full name"),
            ]
        )
        
        model_sql = generate_sqlmesh_model(table, include_column_descriptions=True)
        
        assert "column_descriptions (" in model_sql
        assert "id = 'Primary identifier'" in model_sql
        assert "name = 'Full name'" in model_sql
    
    def test_model_description_with_quotes(self):
        """Description with quotes should be escaped."""
        table = TableDefinition(
            name="TEST",
            columns=[ColumnDefinition("id", "INTEGER", "INTEGER")],
            description="It's a test table",
        )
        
        model_sql = generate_sqlmesh_model(table, include_description=True)
        
        assert "description 'It''s a test table'" in model_sql
    
    def test_model_without_optional_properties(self):
        """Model without optional properties enabled."""
        table = TableDefinition(
            name="TEST",
            columns=[ColumnDefinition("id", "INTEGER", "INTEGER", is_primary_key=True)],
            description="Test",
            references=[ForeignKeyReference("ref", "other", "id")],
        )
        
        model_sql = generate_sqlmesh_model(
            table,
            include_description=False,
            include_grains=False,
            include_references=False,
            include_column_descriptions=False,
        )
        
        assert "description" not in model_sql
        assert "grains" not in model_sql
        assert "references" not in model_sql
        assert "column_descriptions" not in model_sql


# =============================================================================
# Tests for generate_model_with_source_mapping()
# =============================================================================

class TestGenerateModelWithSourceMapping:
    """Tests for generate_model_with_source_mapping function."""
    
    def test_with_column_mappings(self):
        """Generate model with column mappings."""
        table = TableDefinition(
            name="CLIENT",
            columns=[
                ColumnDefinition("rechtspersoon_id", "VARCHAR", "VARCHAR(255)", is_primary_key=True),
                ColumnDefinition("code", "VARCHAR", "VARCHAR(80)"),
            ]
        )
        
        mappings = {
            "rechtspersoon_id": "clientnr",
            "code": "client_code",
        }
        
        model_sql = generate_model_with_source_mapping(
            table,
            source_table="stg.szclient",
            column_mappings=mappings,
        )
        
        assert "CAST(clientnr AS VARCHAR(255)) AS rechtspersoon_id" in model_sql
        assert "CAST(client_code AS VARCHAR(80)) AS code" in model_sql
        assert "FROM stg.szclient" in model_sql
    
    def test_partial_mappings(self):
        """Unmapped columns should use NULL."""
        table = TableDefinition(
            name="TEST",
            columns=[
                ColumnDefinition("id", "VARCHAR", "VARCHAR(255)"),
                ColumnDefinition("unmapped", "VARCHAR", "VARCHAR(100)"),
            ]
        )
        
        mappings = {"id": "source_id"}
        
        model_sql = generate_model_with_source_mapping(
            table,
            source_table="stg.source",
            column_mappings=mappings,
        )
        
        assert "CAST(source_id AS VARCHAR(255)) AS id" in model_sql
        assert "CAST(NULL AS VARCHAR(100)) AS unmapped" in model_sql
    
    def test_no_mappings(self):
        """Without mappings, all columns use NULL."""
        table = TableDefinition(
            name="TEST",
            columns=[ColumnDefinition("id", "INTEGER", "INTEGER")]
        )
        
        model_sql = generate_model_with_source_mapping(table, source_table="stg.source")
        
        assert "CAST(NULL AS INTEGER) AS id" in model_sql


# =============================================================================
# Tests for write_model_file()
# =============================================================================

class TestWriteModelFile:
    """Tests for write_model_file function."""
    
    def test_write_new_file(self, temp_dir: Path):
        """Write a new model file."""
        table = TableDefinition(
            name="TEST",
            columns=[ColumnDefinition("id", "INTEGER", "INTEGER")]
        )
        
        output_dir = temp_dir / "output"
        file_path = write_model_file(table, output_dir)
        
        assert file_path.exists()
        assert file_path.name == "test.sql"
        content = file_path.read_text()
        assert "name silver.test" in content
    
    def test_creates_output_directory(self, temp_dir: Path):
        """Output directory should be created if it doesn't exist."""
        table = TableDefinition(
            name="TEST",
            columns=[ColumnDefinition("id", "INTEGER", "INTEGER")]
        )
        
        output_dir = temp_dir / "nested" / "output" / "dir"
        file_path = write_model_file(table, output_dir)
        
        assert output_dir.exists()
        assert file_path.exists()
    
    def test_file_exists_error(self, temp_dir: Path):
        """FileExistsError when file exists and overwrite=False."""
        table = TableDefinition(
            name="TEST",
            columns=[ColumnDefinition("id", "INTEGER", "INTEGER")]
        )
        
        # Create existing file
        output_dir = temp_dir / "output"
        output_dir.mkdir()
        (output_dir / "test.sql").write_text("existing content")
        
        with pytest.raises(FileExistsError):
            write_model_file(table, output_dir, overwrite=False)
    
    def test_overwrite_existing(self, temp_dir: Path):
        """Overwrite existing file when overwrite=True."""
        table = TableDefinition(
            name="TEST",
            columns=[ColumnDefinition("id", "INTEGER", "INTEGER")]
        )
        
        output_dir = temp_dir / "output"
        output_dir.mkdir()
        existing_file = output_dir / "test.sql"
        existing_file.write_text("old content")
        
        file_path = write_model_file(table, output_dir, overwrite=True)
        
        content = file_path.read_text()
        assert "old content" not in content
        assert "name silver.test" in content
    
    def test_custom_schema(self, temp_dir: Path):
        """Write file with custom schema."""
        table = TableDefinition(
            name="TEST",
            columns=[ColumnDefinition("id", "INTEGER", "INTEGER")]
        )
        
        file_path = write_model_file(table, temp_dir, schema="bronze")
        
        content = file_path.read_text()
        assert "name bronze.test" in content


# =============================================================================
# Tests for convert_ddl_to_models()
# =============================================================================

class TestConvertDdlToModels:
    """Tests for convert_ddl_to_models function."""
    
    def test_dry_run_mode(self, sample_ddl_file: Path):
        """Dry run should return SQL without writing files."""
        results = convert_ddl_to_models(
            ddl_path=sample_ddl_file,
            dry_run=True,
        )
        
        assert "BESCHIKKING" in results
        assert "CLIENT" in results
        assert "name silver.beschikking" in results["BESCHIKKING"]
    
    def test_write_mode(self, sample_ddl_file: Path, temp_dir: Path):
        """Write mode should create model files."""
        output_dir = temp_dir / "models"
        
        results = convert_ddl_to_models(
            ddl_path=sample_ddl_file,
            output_dir=output_dir,
        )
        
        assert (output_dir / "beschikking.sql").exists()
        assert (output_dir / "client.sql").exists()
    
    def test_tables_filter(self, sample_ddl_file: Path):
        """Filter specific tables to convert."""
        results = convert_ddl_to_models(
            ddl_path=sample_ddl_file,
            tables_filter=["BESCHIKKING"],
            dry_run=True,
        )
        
        assert "BESCHIKKING" in results
        assert "CLIENT" not in results
    
    def test_tables_filter_case_insensitive(self, sample_ddl_file: Path):
        """Table filter should be case-insensitive."""
        results = convert_ddl_to_models(
            ddl_path=sample_ddl_file,
            tables_filter=["beschikking", "CLIENT"],
            dry_run=True,
        )
        
        assert "BESCHIKKING" in results
        assert "CLIENT" in results
    
    def test_missing_ddl_source_error(self):
        """ValueError when no DDL source provided."""
        with pytest.raises(ValueError, match="Must provide either"):
            convert_ddl_to_models(dry_run=True)
    
    def test_missing_output_dir_error(self, sample_ddl_file: Path):
        """ValueError when output_dir not provided in write mode."""
        with pytest.raises(ValueError, match="Must provide output_dir"):
            convert_ddl_to_models(ddl_path=sample_ddl_file)
    
    def test_directory_input(self, temp_dir: Path):
        """Process DDL directory."""
        ddl_dir = temp_dir / "ddl"
        ddl_dir.mkdir()
        (ddl_dir / "t1.sql").write_text("CREATE TABLE T1 (ID INTEGER);")
        (ddl_dir / "t2.sql").write_text("CREATE TABLE T2 (ID INTEGER);")
        
        results = convert_ddl_to_models(
            ddl_dir=ddl_dir,
            dry_run=True,
        )
        
        assert "T1" in results
        assert "T2" in results
    
    def test_custom_schema(self, sample_ddl_file: Path):
        """Generate models with custom schema."""
        results = convert_ddl_to_models(
            ddl_path=sample_ddl_file,
            schema="bronze",
            dry_run=True,
        )
        
        assert "name bronze.beschikking" in results["BESCHIKKING"]


# =============================================================================
# Tests for find_default_ddl_path()
# =============================================================================

class TestFindDefaultDdlPath:
    """Tests for find_default_ddl_path function."""
    
    def test_finds_ggm_selectie_cssd(self, temp_dir: Path):
        """Find ggm/selectie/cssd directory."""
        cssd_dir = temp_dir / "ggm" / "selectie" / "cssd"
        cssd_dir.mkdir(parents=True)
        (cssd_dir / "schema.sql").write_text("CREATE TABLE T (ID INT);")
        
        result = find_default_ddl_path(temp_dir)
        
        assert result == cssd_dir
    
    def test_finds_ggm_selectie(self, temp_dir: Path):
        """Find ggm/selectie directory when cssd doesn't exist."""
        selectie_dir = temp_dir / "ggm" / "selectie"
        selectie_dir.mkdir(parents=True)
        (selectie_dir / "schema.sql").write_text("CREATE TABLE T (ID INT);")
        
        result = find_default_ddl_path(temp_dir)
        
        assert result == selectie_dir
    
    def test_no_ddl_found(self, temp_dir: Path):
        """Return None when no DDL directories found."""
        result = find_default_ddl_path(temp_dir)
        assert result is None
    
    def test_empty_directory_not_found(self, temp_dir: Path):
        """Empty directories (no .sql files) should not be found."""
        cssd_dir = temp_dir / "ggm" / "selectie" / "cssd"
        cssd_dir.mkdir(parents=True)
        # No .sql files
        
        result = find_default_ddl_path(temp_dir)
        assert result is None


# =============================================================================
# Integration tests with real GGM DDL patterns
# =============================================================================

class TestGgmDdlPatterns:
    """Integration tests with real GGM DDL patterns."""
    
    def test_ggm_beschikking_pattern(self, temp_dir: Path):
        """Parse GGM BESCHIKKING table pattern."""
        ddl_content = """
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
        ddl_file = temp_dir / "beschikking.sql"
        ddl_file.write_text(ddl_content)
        
        tables = parse_ddl_to_tables(ddl_file)
        
        assert len(tables) == 1
        table = tables[0]
        assert table.name == "BESCHIKKING"
        assert len(table.columns) == 9
        
        # Check primary key
        pk_cols = table.primary_key_columns
        assert len(pk_cols) == 1
        assert pk_cols[0].name == "beschikking_id"
        
        # Generate model
        model_sql = generate_sqlmesh_model(table)
        assert "name silver.beschikking" in model_sql
        assert "CAST(NULL AS VARCHAR(255)) AS beschikking_id" in model_sql
        assert "PRIMARY KEY" in model_sql
    
    def test_ggm_enum_table_pattern(self, temp_dir: Path):
        """Parse GGM enum table with INSERT statements."""
        ddl_content = """
CREATE TABLE WET_ENUM (
    WET_ENUM_ID VARCHAR(255) PRIMARY KEY,
    VALUE VARCHAR(255)
);

INSERT INTO WET_ENUM (WET_ENUM_ID, VALUE) VALUES
    (1, 'ANDERE_WET'),
    (2, 'JEUGDWET'),
    (3, 'WMO');
"""
        ddl_file = temp_dir / "wet_enum.sql"
        ddl_file.write_text(ddl_content)
        
        tables = parse_ddl_to_tables(ddl_file)
        
        # Should only get CREATE TABLE, not INSERT
        assert len(tables) == 1
        assert tables[0].name == "WET_ENUM"
        assert len(tables[0].columns) == 2
    
    def test_ggm_decimal_type(self, temp_dir: Path):
        """Parse GGM DECIMAL types with precision."""
        ddl_content = """
CREATE TABLE DECLARATIEREGEL (
    DECLARATIEREGEL_ID VARCHAR(255) PRIMARY KEY,
    BEDRAG DECIMAL(10,2)
);
"""
        ddl_file = temp_dir / "declaratieregel.sql"
        ddl_file.write_text(ddl_content)
        
        tables = parse_ddl_to_tables(ddl_file)
        cols = {c.name: c for c in tables[0].columns}
        
        assert cols["bedrag"].data_type == "DECIMAL"
    
    def test_generated_model_is_valid_sql(self, temp_dir: Path):
        """Generated model should be parseable SQL (ignoring MODEL block)."""
        table = TableDefinition(
            name="TEST",
            columns=[
                ColumnDefinition("id", "VARCHAR", "VARCHAR(255)", is_primary_key=True),
                ColumnDefinition("name", "VARCHAR", "VARCHAR(100)"),
                ColumnDefinition("amount", "DECIMAL", "DECIMAL(10,2)"),
                ColumnDefinition("created", "DATE", "DATE"),
            ]
        )
        
        model_sql = generate_sqlmesh_model(table)
        
        # Extract SQL after MODEL block
        model_end = model_sql.find(");")
        sql_content = model_sql[model_end + 2:].strip()
        
        # Remove comments and parse
        sql_lines = [line for line in sql_content.split("\n") 
                     if line.strip() and not line.strip().startswith("--")]
        sql_to_parse = "\n".join(sql_lines)
        
        # Add a dummy FROM clause for parsing
        if "FROM" not in sql_to_parse:
            sql_to_parse = sql_to_parse.rstrip()
            if sql_to_parse.endswith(","):
                sql_to_parse = sql_to_parse[:-1]
            sql_to_parse += "\nFROM dual"
        
        # This should not raise an exception
        parsed = sqlglot.parse(sql_to_parse, read="postgres")
        assert len(parsed) >= 1


# =============================================================================
# Edge case tests
# =============================================================================

class TestEdgeCases:
    """Edge case and error handling tests."""
    
    def test_table_with_no_columns(self, temp_dir: Path):
        """Table definition without columns should be skipped."""
        # This is invalid SQL but let's test the parser handles it
        ddl_content = "CREATE TABLE EMPTY ();"
        ddl_file = temp_dir / "empty_table.sql"
        ddl_file.write_text(ddl_content)
        
        # Should not crash, may return empty or skip
        tables = parse_ddl_to_tables(ddl_file)
        # Either empty or table with no columns should be handled
        if tables:
            assert tables[0].columns == []
    
    def test_special_characters_in_names(self, temp_dir: Path):
        """Handle special characters in column names."""
        ddl_content = """
CREATE TABLE TEST (
    "ID" VARCHAR(255) PRIMARY KEY,
    "Column With Spaces" VARCHAR(100)
);
"""
        ddl_file = temp_dir / "special.sql"
        ddl_file.write_text(ddl_content)
        
        # Should not crash
        tables = parse_ddl_to_tables(ddl_file)
        # May or may not parse depending on sqlglot handling
    
    def test_unicode_content(self, temp_dir: Path):
        """Handle unicode characters in DDL."""
        ddl_content = """
CREATE TABLE TËST (
    ID VARCHAR(255) PRIMARY KEY,
    NÄME VARCHAR(100)
);
"""
        ddl_file = temp_dir / "unicode.sql"
        ddl_file.write_text(ddl_content, encoding="utf-8")
        
        # Should not crash
        tables = parse_ddl_to_tables(ddl_file)
    
    def test_very_long_type_definitions(self, temp_dir: Path):
        """Handle very long type definitions."""
        ddl_content = """
CREATE TABLE TEST (
    ID VARCHAR(65535) PRIMARY KEY,
    BIG_DECIMAL DECIMAL(38,18)
);
"""
        ddl_file = temp_dir / "long_types.sql"
        ddl_file.write_text(ddl_content)
        
        tables = parse_ddl_to_tables(ddl_file)
        assert len(tables) == 1
    
    def test_multiple_files_same_table_name(self, temp_dir: Path):
        """Handle multiple files with same table name."""
        (temp_dir / "file1.sql").write_text("CREATE TABLE SAME (ID INTEGER);")
        (temp_dir / "file2.sql").write_text("CREATE TABLE SAME (ID VARCHAR(255));")
        
        tables = parse_ddl_directory_to_tables(temp_dir)
        
        # Should get both (even if duplicates)
        same_tables = [t for t in tables if t.name == "SAME"]
        assert len(same_tables) == 2
