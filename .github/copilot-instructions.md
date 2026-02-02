# GitHub Copilot Instructions for SQLMESH_2_GGM

GGM data pipeline: **dlt** (extraction) + **SQLMesh** (transformation) for Dutch municipalities.
Pipeline: `Oracle CSSD → raw.* → stg.* → silver.* (GGM)`

## Architecture

```
Oracle CSSD → dlt (append) → raw.* (historical) → stg.* (latest) → silver.* (GGM)
```

| Layer | Tool | Purpose |
|-------|------|---------|
| **raw** | dlt | Historical loads with auto-generated `_dlt_load_id`, `_dlt_load_time`. Never add these manually. |
| **stg** | SQLMesh | Latest snapshot filtered by `MAX(_dlt_load_id)` |
| **silver** | SQLMesh | GGM-compliant schema (Dutch column names from `ggm/selectie/cssd/*.sql` DDL) |

## Commands

```bash
uv run dev                                 # Full dev setup: Docker + Oracle + dlt + SQLMesh
uv run dev --dest mssql                    # Dev with MSSQL target
uv run dev --dest duckdb                   # Dev with DuckDB (no Docker needed for target)
uv run pipeline --dest postgres            # Production pipeline
uv run pipeline --dest postgres --dry-run  # Preview commands
uv run pipeline --dest postgres --no-restate-raw  # Skip restatement (model changes only)
uv run sqlmesh -p transform plan --auto-apply  # Apply SQLMesh transformations
uv run python scripts/validate_schema.py   # Validate silver vs DDL (runs in CI)
uv run pytest                              # Run all tests
```

### External Model Restatement

By default, `uv run pipeline` includes `--restate-model raw.*` to ensure stg/silver models are refreshed when new data is loaded. This is required because raw tables are external models managed by dlt.

- **Default behavior**: All raw.* models are restated, triggering cascading backfill of stg.* and silver.*
- **Skip restatement**: Use `--no-restate-raw` to only apply model changes without refreshing data

## SQL Model Patterns

### Staging (`transform/models/stg/*.sql`)
```sql
MODEL (name stg.tablename, kind FULL);

WITH latest_load AS (SELECT MAX(_dlt_load_id) AS load_id FROM raw.tablename)
SELECT col1, col2, _dlt_load_id
FROM raw.tablename
WHERE _dlt_load_id = (SELECT load_id FROM latest_load)
```

### Silver (`transform/models/silver/*.sql`)
```sql
MODEL (name silver.tablename, kind FULL,
    description 'GGM table description',
    grains primary_key_column,
    references (fk_column_id)
);

SELECT
    CAST(source_col AS INT) AS dutch_ggm_column,  -- Match DDL column name exactly
    CAST(NULL AS VARCHAR(80)) AS optional_col     -- Nullable columns with correct type
FROM stg.source_table
```

## Critical Rules

1. **SQL Dialect**: SQLGlot-compatible only. Use `COALESCE` not `NVL`, `CONCAT` not `||`.
2. **Column Names**: Silver columns must match `ggm/selectie/cssd/*.sql` DDL exactly (case-insensitive).
3. **Audits**: DDL constraints become non-blocking audits in `transform/audits/` (not enforced in silver).
4. **Dependencies**: Use `uv` exclusively. Never `pip install`.
5. **SQLMesh Path**: Always use `-p transform` when running sqlmesh CLI directly.

## Adding New Tables

1. Add table name (lowercase) to `SOURCE_TABLES` list in `ingest/pipeline.py`
2. Add external model definition to `transform/external_models.yaml`
3. Create `transform/models/stg/tablename.sql` with `_dlt_load_id` filter pattern
4. Create `transform/models/silver/tablename.sql` mapping to GGM columns
5. Create `transform/audits/tablename.sql` for PK/constraint validations
6. Run `uv run python scripts/validate_schema.py` to verify

## DDL-to-Model Generation

Generate SQLMesh models from GGM DDL files:
```bash
# Dry-run to preview
uv run python scripts/ddl_to_sqlmesh.py --ddl-dir ggm/selectie/cssd --dry-run

# Generate models
uv run python scripts/ddl_to_sqlmesh.py --ddl-dir ggm/selectie/cssd --output-dir transform/models/silver
```

## External Models

Raw tables are managed by dlt and defined as external models in `transform/external_models.yaml`:

```yaml
- name: raw.tablename
  description: Description of the source table
  columns:
    column_name: data_type
    _dlt_load_id: text       # Auto-added by dlt
    _dlt_load_time: timestamp # Auto-added by dlt
```

**Commands:**
```bash
uv run sqlmesh -p transform create_external_models  # Auto-generate from database metadata
```

## File Reference

| Path | Purpose |
|------|---------|
| `ingest/pipeline.py` | dlt pipeline (`SOURCE_TABLES` list) |
| `ingest/constants.py` | Destination/gateway mappings |
| `transform/external_models.yaml` | External model definitions for `raw.*` tables |
| `transform/models/stg/*.sql` | Staging models (latest load filter) |
| `transform/models/silver/*.sql` | GGM transformations |
| `transform/audits/*.sql` | Non-blocking constraint checks |
| `transform/config.yaml` | SQLMesh gateways (postgres/mssql/mysql/duckdb) |
| `ggm/selectie/cssd/*.sql` | **Authoritative DDL** (validation source) |
| `scripts/ddl_parser.py` | Shared DDL parsing utilities |
| `scripts/validate_schema.py` | CI validation against DDL |
| `scripts/ddl_to_sqlmesh.py` | DDL to SQLMesh model converter |

## Gateway vs Destination Mapping

dlt destinations map to SQLMesh gateways via `ingest/constants.py`:

| dlt `--dest` | SQLMesh gateway | Notes |
|--------------|-----------------|-------|
| `postgres` | `postgres` | Default dev setup |
| `mssql` | `mssql` | SQL Server (requires ODBC driver) |
| `mysql` | `mysql` | MySQL (dlt uses `sqlalchemy` internally) |
| `duckdb` | `duckdb` | No server needed, local file |
| `snowflake` | `snowflake` | Cloud (configure in `config.yaml`) |

To add a new destination: update `DLT_DESTINATIONS`, `SQLMESH_GATEWAYS`, and `DESTINATION_TO_GATEWAY` in `ingest/constants.py`, then add gateway config to `transform/config.yaml`.

## Environment Configuration

All credentials use dlt's native env var pattern. Set in `.env` file (see `.env.example`):

```bash
# Oracle source (Easy Connect format - recommended)
SOURCES__SQL_DATABASE__CREDENTIALS=oracle+oracledb://user:pass@hostname:1521/?service_name=myservice

# PostgreSQL destination
DESTINATION__POSTGRES__CREDENTIALS__HOST=localhost
DESTINATION__POSTGRES__CREDENTIALS__DATABASE=ggm_dev
DESTINATION__POSTGRES__CREDENTIALS__USERNAME=ggm
DESTINATION__POSTGRES__CREDENTIALS__PASSWORD=xxx

# MSSQL destination
DESTINATION__MSSQL__CREDENTIALS__HOST=localhost
DESTINATION__MSSQL__CREDENTIALS__DATABASE=ggm_dev
DESTINATION__MSSQL__CREDENTIALS__USERNAME=sa
DESTINATION__MSSQL__CREDENTIALS__PASSWORD=xxx
```

## Docker Development

`docker/docker-compose.yml` provides Oracle source + target DBs:

```bash
uv run dev --dest postgres            # Starts Docker, waits for Oracle, loads synthetic data
uv run dev --skip-docker              # Skip Docker if already running
```

Oracle startup takes 2-3 minutes. The `dev` script automatically waits and loads `data/synthetic/*.csv` to Oracle before running the pipeline.

## Testing

```bash
uv run pytest                         # All tests
uv run pytest -m "not integration"    # Unit tests only (no Docker)
uv run pytest -m integration          # Integration tests (require Docker)
```

Test locations: `tests/` (pipeline), `scripts/tests/` (validation), `synthetic/tests/` (data generation)

Markers defined in `pyproject.toml`: `@pytest.mark.integration`, `@pytest.mark.slow`
