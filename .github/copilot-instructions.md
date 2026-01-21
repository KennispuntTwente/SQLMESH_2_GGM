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
uv run pipeline --dest postgres            # Production pipeline
uv run pipeline --dest postgres --dry-run  # Preview commands
uv run sqlmesh plan --auto-apply           # Apply SQLMesh transformations
uv run python scripts/validate_schema.py   # Validate silver vs DDL (runs in CI)
uv run pytest                              # Run all tests
```

## SQL Model Patterns

### Staging (`models/stg/stg_*.sql`)
```sql
MODEL (name stg.tablename, kind FULL);

WITH latest_load AS (SELECT MAX(_dlt_load_id) AS load_id FROM raw.tablename)
SELECT col1, col2, _dlt_load_id
FROM raw.tablename
WHERE _dlt_load_id = (SELECT load_id FROM latest_load)
```

### Silver (`models/silver/*.sql`)
```sql
MODEL (name silver.tablename, kind FULL);

SELECT
    source_col AS dutch_ggm_column,           -- Match DDL exactly
    CAST(NULL AS VARCHAR(80)) AS optional_col -- Nullable columns with correct type
FROM stg.source_table
```

## Critical Rules

1. **SQL Dialect**: SQLGlot-compatible only. Use `COALESCE` not `NVL`, `CONCAT` not `||`.
2. **Column Names**: Silver columns must match `ggm/selectie/cssd/*.sql` DDL exactly (case-insensitive).
3. **Audits**: Constraints become non-blocking audits in `audits/` (not enforced in silver).
4. **Dependencies**: Use `uv` exclusively. Never `pip install`.

## Adding New Tables

1. Add to `SOURCE_TABLES` list in `pipelines/source_to_raw.py`
2. Add external model definition to `external_models.yaml` (or run `sqlmesh create_external_models`)
3. Create `models/stg/stg_tablename.sql` with `_dlt_load_id` filter pattern
4. Create `models/silver/tablename.sql` mapping to GGM columns
5. Create `audits/tablename.sql` for PK/constraint validations
6. Run `uv run python scripts/validate_schema.py` to verify

## External Models

Raw tables are managed by dlt and defined as external models in `external_models.yaml`:

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
uv run sqlmesh create_external_models  # Auto-generate from database metadata
```

**Manual additions:** Place in `external_models/` directory to avoid overwriting by `create_external_models`.

## File Reference

| Path | Purpose |
|------|---------|
| `pipelines/source_to_raw.py` | dlt pipeline (`SOURCE_TABLES` list) |
| `pipelines/constants.py` | Destination/gateway mappings |
| `external_models.yaml` | External model definitions for `raw.*` tables |
| `external_models/` | Manual external model additions (not overwritten) |
| `models/stg/stg_*.sql` | Staging models (latest load filter) |
| `models/silver/*.sql` | GGM transformations |
| `audits/*.sql` | Non-blocking constraint checks |
| `ggm/selectie/cssd/*.sql` | **Authoritative DDL** (validation source) |
| `config.yaml` | SQLMesh gateways (local/mssql/mysql/duckdb) |
| `scripts/validate_schema.py` | CI validation against DDL |

## Gateway vs Destination Mapping

dlt destinations map to SQLMesh gateways via `pipelines/constants.py`:

| dlt `--dest` | SQLMesh gateway | Notes |
|--------------|-----------------|-------|
| `postgres` | `local` | Default dev setup |
| `mssql` | `mssql` | SQL Server |
| `mysql` | `mysql` | MySQL |
| `duckdb` | `duckdb` | No server needed, local file |
| `snowflake` | `snowflake` | Cloud (configure in `config.yaml`) |

To add a new destination: update `DLT_DESTINATIONS`, `SQLMESH_GATEWAYS`, and `DESTINATION_TO_GATEWAY` in `pipelines/constants.py`, then add gateway config to `config.yaml`.

## Environment Configuration

All credentials use dlt's native env var pattern. Set in `.env` file (see `.env.example`):

```bash
# Pipeline settings
GGM_DESTINATION=postgres              # dlt destination type
GGM_GATEWAY=local                     # SQLMesh gateway

# Oracle source (Option 1: Host/Port/Service - Easy Connect)
SOURCES__SQL_DATABASE__CREDENTIALS__HOST=localhost
SOURCES__SQL_DATABASE__CREDENTIALS__PORT=1521
SOURCES__SQL_DATABASE__CREDENTIALS__DATABASE=FREEPDB1
SOURCES__SQL_DATABASE__CREDENTIALS__USERNAME=system
SOURCES__SQL_DATABASE__CREDENTIALS__PASSWORD=xxx

# Oracle source (Option 2: TNS Alias - requires tnsnames.ora)
# SOURCES__SQL_DATABASE__CREDENTIALS__DATABASE=MYDB_ALIAS
# TNS_ADMIN=/opt/oracle/network/admin

# PostgreSQL destination
DESTINATION__POSTGRES__CREDENTIALS__HOST=localhost
DESTINATION__POSTGRES__CREDENTIALS__DATABASE=ggm_dev
DESTINATION__POSTGRES__CREDENTIALS__USERNAME=ggm
DESTINATION__POSTGRES__CREDENTIALS__PASSWORD=xxx

# MSSQL destination (requires Microsoft ODBC Driver for SQL Server)
# Both dlt and SQLMesh use the same ODBC driver
DESTINATION__MSSQL__CREDENTIALS__HOST=localhost
DESTINATION__MSSQL__CREDENTIALS__DATABASE=ggm_dev
DESTINATION__MSSQL__CREDENTIALS__USERNAME=sa
DESTINATION__MSSQL__CREDENTIALS__PASSWORD=xxx
# DESTINATION__MSSQL__CREDENTIALS__DRIVER=ODBC Driver 18 for SQL Server
# TrustServerCertificate (different format per tool):
# DESTINATION__MSSQL__CREDENTIALS__QUERY__TRUSTSERVERCERTIFICATE=yes  # dlt
# GGM_MSSQL_TRUST_SERVER_CERTIFICATE=true                             # SQLMesh

# Oracle thick mode (optional - required for some features)
# ORACLE_THICK_MODE=1
# ORACLE_CLIENT_LIB_DIR=/path/to/lib
```

## Docker Development

`docker/docker-compose.yml` provides Oracle source + target DBs:

```bash
# Start containers manually
docker compose -f docker/docker-compose.yml up -d oracle postgres

# Or use the dev script (handles wait + data load)
uv run dev --dest postgres            # Starts Docker, waits for Oracle, loads synthetic data
uv run dev --skip-docker              # Skip Docker if already running
```

Oracle startup takes 2-3 minutes. The `dev` script automatically waits and loads `data/synthetic/*.csv` to Oracle before running the pipeline.

## Testing

```bash
uv run pytest                         # All tests
uv run pytest -m "not integration"    # Unit tests only (no Docker)
uv run pytest -m integration          # Integration tests (require Docker)
uv run pytest -m "not slow"           # Skip slow tests
```

Test locations:
- `tests/` - Pipeline integration tests
- `scripts/tests/` - Validation script unit tests
- `synthetic/tests/` - Synthetic data tests

Markers defined in `pyproject.toml`: `@pytest.mark.integration`, `@pytest.mark.slow`
