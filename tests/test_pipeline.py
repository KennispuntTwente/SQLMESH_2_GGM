"""Integration tests for the GGM pipeline.

These tests verify that:
1. The `uv run pipeline` script works correctly
2. The Docker image can run the pipeline
3. Both produce valid output

Run with: uv run pytest tests/test_pipeline.py -v
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest


def _get_sqlmesh_command() -> list[str]:
    """Get the best SQLMesh command for spawning subprocesses.

    Uses sqlmesh CLI directly to avoid local 'sqlmesh/' directory shadowing the package.
    """
    uv = shutil.which("uv")
    if uv:
        return [uv, "run", "sqlmesh"]

    sqlmesh = shutil.which("sqlmesh")
    if sqlmesh:
        return [sqlmesh]

    return ["sqlmesh"]


# Markers for different test categories
pytestmark = [
    pytest.mark.integration,
]


class TestPipelineScript:
    """Tests for the `uv run pipeline` script."""

    def test_pipeline_help(self, project_root: str) -> None:
        """Pipeline script shows help without errors."""
        result = subprocess.run(
            [sys.executable, "scripts/pipeline.py", "--help"],
            cwd=project_root,
            capture_output=True,
            text=True,
            timeout=30,
        )
        assert result.returncode == 0
        assert "GGM pipeline" in result.stdout.lower() or "--dest" in result.stdout

    def test_pipeline_dry_run(self, project_root: str) -> None:
        """Pipeline script dry-run works without errors."""
        result = subprocess.run(
            [sys.executable, "scripts/pipeline.py", "--dest", "postgres", "--dry-run"],
            cwd=project_root,
            capture_output=True,
            text=True,
            timeout=30,
        )
        assert result.returncode == 0
        assert (
            "dry-run" in result.stdout.lower() or "would run" in result.stdout.lower()
        )

    def test_pipeline_dry_run_includes_restate_model(self, project_root: str) -> None:
        """Pipeline dry-run includes --restate-model raw.* by default."""
        result = subprocess.run(
            [sys.executable, "scripts/pipeline.py", "--dest", "postgres", "--dry-run"],
            cwd=project_root,
            capture_output=True,
            text=True,
            timeout=30,
        )
        assert result.returncode == 0
        # Verify restate-model flag is included for raw.* models
        assert "--restate-model raw.*" in result.stdout
        assert "Restate raw : True" in result.stdout

    def test_pipeline_no_restate_raw_flag(self, project_root: str) -> None:
        """Pipeline --no-restate-raw disables restatement of raw models."""
        result = subprocess.run(
            [
                sys.executable,
                "scripts/pipeline.py",
                "--dest",
                "postgres",
                "--dry-run",
                "--no-restate-raw",
            ],
            cwd=project_root,
            capture_output=True,
            text=True,
            timeout=30,
        )
        assert result.returncode == 0
        # Verify restate-model flag is NOT included
        assert "--restate-model" not in result.stdout
        assert "Restate raw : False" in result.stdout

    def test_pipeline_invalid_dest(self, project_root: str) -> None:
        """Pipeline script rejects invalid destination."""
        result = subprocess.run(
            [sys.executable, "scripts/pipeline.py", "--dest", "invalid_db"],
            cwd=project_root,
            capture_output=True,
            text=True,
            timeout=30,
        )
        assert result.returncode != 0
        assert "invalid" in result.stderr.lower() or "choice" in result.stderr.lower()

    def test_pipeline_skip_both(self, project_root: str) -> None:
        """Pipeline script handles --skip-dlt and --skip-sqlmesh correctly."""
        result = subprocess.run(
            [
                sys.executable,
                "scripts/pipeline.py",
                "--dest",
                "postgres",
                "--skip-dlt",
                "--skip-sqlmesh",
            ],
            cwd=project_root,
            capture_output=True,
            text=True,
            timeout=30,
        )
        # Should succeed but do nothing
        assert result.returncode == 0
        assert "nothing to do" in result.stdout.lower()


class TestPipelineImports:
    """Tests that verify pipeline modules can be imported correctly."""

    def test_import_pipeline(self) -> None:
        """Pipeline module imports without errors."""
        from scripts import pipeline

        assert hasattr(pipeline, "main")
        assert hasattr(pipeline, "run_dlt")
        assert hasattr(pipeline, "run_sqlmesh")

    def test_import_source_to_raw(self) -> None:
        """Source to raw module imports without errors."""
        import sys
        from pathlib import Path

        # Add dlt folder to path for imports
        dlt_path = str(Path(__file__).parent.parent / "dlt")
        if dlt_path not in sys.path:
            sys.path.insert(0, dlt_path)
        import pipeline as dlt_pipeline

        assert hasattr(dlt_pipeline, "run_pipeline")
        assert hasattr(dlt_pipeline, "SOURCE_TABLES")

    def test_import_constants(self) -> None:
        """Constants module has required values."""
        import sys
        from pathlib import Path

        # Add dlt folder to path for imports
        dlt_path = str(Path(__file__).parent.parent / "dlt")
        if dlt_path not in sys.path:
            sys.path.insert(0, dlt_path)
        from constants import (
            DLT_DESTINATIONS,
            SQLMESH_GATEWAYS,
            DESTINATION_TO_GATEWAY,
            normalize_dlt_destination,
        )

        assert "postgres" in DLT_DESTINATIONS
        assert "local" in SQLMESH_GATEWAYS
        assert "postgres" in DESTINATION_TO_GATEWAY

        # Test MySQL -> sqlalchemy normalization
        assert normalize_dlt_destination("mysql") == "sqlalchemy"
        assert normalize_dlt_destination("postgres") == "postgres"  # unchanged


class TestDockerImage:
    """Tests for the Docker image build and execution."""

    @pytest.fixture
    def dockerfile_exists(self, project_root: str) -> bool:
        """Check if Dockerfile exists."""
        return Path(project_root, "Dockerfile").exists()

    def test_dockerfile_exists(self, project_root: str) -> None:
        """Dockerfile exists in project root."""
        dockerfile = Path(project_root, "Dockerfile")
        assert dockerfile.exists(), "Dockerfile not found in project root"

    def test_dockerignore_exists(self, project_root: str) -> None:
        """.dockerignore exists in project root."""
        dockerignore = Path(project_root, ".dockerignore")
        assert dockerignore.exists(), ".dockerignore not found in project root"

    @pytest.mark.slow
    def test_docker_build(
        self,
        docker_available: bool,
        project_root: str,
    ) -> None:
        """Docker image builds successfully."""
        if not docker_available:
            pytest.skip("Docker is not available")

        result = subprocess.run(
            ["docker", "build", "-t", "ggm-pipeline:test", "."],
            cwd=project_root,
            capture_output=True,
            text=True,
            timeout=600,  # 10 min timeout for build
        )
        assert result.returncode == 0, f"Docker build failed: {result.stderr}"

    @pytest.mark.slow
    def test_docker_help(
        self,
        docker_available: bool,
        project_root: str,
    ) -> None:
        """Docker image runs and shows help."""
        if not docker_available:
            pytest.skip("Docker is not available")

        # First ensure image is built
        build_result = subprocess.run(
            ["docker", "build", "-t", "ggm-pipeline:test", "."],
            cwd=project_root,
            capture_output=True,
            timeout=600,
        )
        if build_result.returncode != 0:
            pytest.skip("Docker image could not be built")

        # Run with --help
        result = subprocess.run(
            ["docker", "run", "--rm", "ggm-pipeline:test", "--help"],
            capture_output=True,
            text=True,
            timeout=60,
        )
        assert result.returncode == 0
        assert "--dest" in result.stdout

    @pytest.mark.slow
    def test_docker_dry_run(
        self,
        docker_available: bool,
        project_root: str,
    ) -> None:
        """Docker image can run in dry-run mode."""
        if not docker_available:
            pytest.skip("Docker is not available")

        # First ensure image is built
        build_result = subprocess.run(
            ["docker", "build", "-t", "ggm-pipeline:test", "."],
            cwd=project_root,
            capture_output=True,
            timeout=600,
        )
        if build_result.returncode != 0:
            pytest.skip("Docker image could not be built")

        # Run with dry-run
        result = subprocess.run(
            [
                "docker",
                "run",
                "--rm",
                "ggm-pipeline:test",
                "--dest",
                "postgres",
                "--dry-run",
            ],
            capture_output=True,
            text=True,
            timeout=60,
        )
        assert result.returncode == 0
        assert (
            "dry-run" in result.stdout.lower() or "would run" in result.stdout.lower()
        )


class TestPipelineWithDocker:
    """Integration tests that run the pipeline against Docker services."""

    @pytest.mark.slow
    @pytest.mark.integration
    def test_sqlmesh_plan_dryrun(
        self,
        docker_services: dict[str, bool],
        project_root: str,
    ) -> None:
        """SQLMesh plan works in dry-run mode against PostgreSQL."""
        # Use 'info' command to validate SQLMesh can parse models and connect
        # This doesn't require raw tables to exist in the database
        # Use sqlmesh CLI directly to avoid local 'sqlmesh/' directory shadowing the package
        result = subprocess.run(
            ["sqlmesh", "-p", "sqlmesh", "--gateway", "local", "info"],
            cwd=project_root,
            capture_output=True,
            text=True,
            timeout=120,
            env={
                **os.environ,
                "DESTINATION__POSTGRES__CREDENTIALS__PASSWORD": "ggm_dev",
            },
        )
        assert result.returncode == 0, result.stderr
        # Verify models are recognized
        assert (
            "stg" in result.stdout
            or "silver" in result.stdout
            or "Models" in result.stdout
        )


class TestDockerCompose:
    """Tests for docker-compose configuration."""

    def test_compose_file_exists(self, compose_file: str) -> None:
        """docker-compose.yml exists."""
        assert Path(compose_file).exists()

    def test_compose_valid_yaml(self, compose_file: str) -> None:
        """docker-compose.yml is valid YAML."""
        import yaml

        with open(compose_file) as f:
            config = yaml.safe_load(f)

        assert "services" in config
        assert "postgres" in config["services"]

    def test_compose_has_database_services(self, compose_file: str) -> None:
        """docker-compose.yml has required database services."""
        import yaml

        with open(compose_file) as f:
            config = yaml.safe_load(f)

        # Check for database services (pipeline runs locally via uv, not in Docker)
        required_services = ["postgres", "oracle"]
        for service in required_services:
            assert service in config["services"], (
                f"{service} service not found in docker-compose.yml"
            )

    @pytest.mark.slow
    def test_compose_config_valid(
        self,
        docker_available: bool,
        compose_file: str,
        project_root: str,
    ) -> None:
        """docker-compose.yml configuration is valid."""
        if not docker_available:
            pytest.skip("Docker is not available")

        result = subprocess.run(
            ["docker", "compose", "-f", compose_file, "config"],
            cwd=project_root,
            capture_output=True,
            text=True,
            timeout=30,
        )
        assert result.returncode == 0, f"docker-compose config failed: {result.stderr}"


@pytest.mark.slow
class TestPipelineRestateIntegration:
    """Integration tests verifying stg/silver refresh on repeated data loads.

    Uses DuckDB for fast, no-docker testing. These tests verify that:
    1. Running the pipeline twice with different load_ids works
    2. stg/silver models correctly refresh to show the latest data
    """

    @pytest.fixture
    def duckdb_path(self, tmp_path: Path) -> Path:
        """Create a temporary DuckDB database path.

        Use a stable filename so SQLMesh's DuckDB catalog is predictable.

        SQLMesh qualifies DuckDB objects as `<catalog>.<schema>.<table>`, where
        `<catalog>` is derived from the DuckDB filename stem. Avoid reserved
        names like `main` which DuckDB may not allow as an attached catalog.
        """
        return tmp_path / "test.duckdb"

    @pytest.fixture
    def sqlmesh_state_dir(self, tmp_path: Path) -> Path:
        """Create a temporary SQLMesh state directory for test isolation."""
        state_dir = tmp_path / ".sqlmesh"
        state_dir.mkdir(exist_ok=True)
        return state_dir

    @pytest.fixture
    def load_synthetic_data(self, duckdb_path: Path, project_root: str):
        """Factory fixture to load synthetic data with unique load_id."""
        import uuid
        from datetime import datetime, timezone

        import duckdb
        import pandas as pd

        def _load(load_suffix: str = "") -> str:
            """Load synthetic CSVs to DuckDB raw schema with dlt-like metadata.

            Returns the load_id used.
            """
            csv_dir = Path(project_root) / "data" / "synthetic"
            load_id = f"test_{load_suffix}_{uuid.uuid4().hex[:8]}"
            load_time = datetime.now(timezone.utc).isoformat()

            # SQLMesh qualifies DuckDB objects with a catalog equal to the
            # DuckDB filename stem (e.g. "test"."raw"."szclient"). Load data
            # into that catalog to match SQLMesh's expectations.
            catalog = duckdb_path.stem
            conn = duckdb.connect(database=":memory:")
            conn.execute(f"ATTACH '{duckdb_path}' AS \"{catalog}\"")

            conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{catalog}"."raw"')
            conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{catalog}"."stg"')
            conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{catalog}"."silver"')

            csvs = sorted(csv_dir.glob("*.csv"))
            for csv_path in csvs:
                table = csv_path.stem
                df = pd.read_csv(csv_path)
                df.columns = [c.lower() for c in df.columns]
                df["_dlt_load_id"] = load_id
                df["_dlt_load_time"] = load_time

                conn.execute(
                    f'CREATE TABLE IF NOT EXISTS "{catalog}"."raw"."{table}" '
                    "AS SELECT * FROM df WHERE 1=0"
                )
                conn.execute(f'INSERT INTO "{catalog}"."raw"."{table}" SELECT * FROM df')

            conn.close()
            return load_id

        return _load

    @pytest.mark.integration
    def test_stg_refreshes_on_restate(
        self,
        duckdb_path: Path,
        sqlmesh_state_dir: Path,
        load_synthetic_data,
        project_root: str,
    ) -> None:
        """Verify stg layer refreshes when running pipeline twice with new data.

        This test:
        1. Loads data with load_id_1
        2. Runs SQLMesh to create stg/silver
        3. Loads more data with load_id_2
        4. Runs SQLMesh with restate (default behavior)
        5. Verifies stg now has load_id_2 data (not load_id_1)
        """
        import duckdb

        # Step 1: Load first batch of data
        load_id_1 = load_synthetic_data("first")

        # Step 2: Run SQLMesh to initialize models
        # Use SQLMESH_HOME to isolate state for this test
        env = {
            **os.environ,
            "DESTINATION__DUCKDB__CREDENTIALS": str(duckdb_path),
            "SQLMESH_HOME": str(sqlmesh_state_dir),
        }
        sqlmesh_cmd = _get_sqlmesh_command()
        result = subprocess.run(
            [
                *sqlmesh_cmd,
                "-p",
                "sqlmesh",
                "--gateway",
                "duckdb",
                "plan",
                "--auto-apply",
            ],
            cwd=project_root,
            capture_output=True,
            text=True,
            timeout=120,
            env=env,
        )
        assert result.returncode == 0, f"First SQLMesh run failed: {result.stdout}"

        # Verify stg has load_id_1
        catalog = duckdb_path.stem
        conn = duckdb.connect(database=":memory:")
        conn.execute(f"ATTACH '{duckdb_path}' AS \"{catalog}\"")
        stg_load_ids_1 = conn.execute(
            f'SELECT DISTINCT _dlt_load_id FROM "{catalog}"."stg"."szclient"'
        ).fetchall()
        conn.close()
        assert len(stg_load_ids_1) == 1
        assert stg_load_ids_1[0][0] == load_id_1

        # Step 3: Load second batch of data
        load_id_2 = load_synthetic_data("second")

        # Step 4: Run SQLMesh again with restate (should refresh stg/silver)
        result = subprocess.run(
            [
                *sqlmesh_cmd,
                "-p",
                "sqlmesh",
                "--gateway",
                "duckdb",
                "plan",
                "--auto-apply",
                "--restate-model",
                "raw.*",
            ],
            cwd=project_root,
            capture_output=True,
            text=True,
            timeout=120,
            env=env,
        )
        assert result.returncode == 0, f"Second SQLMesh run failed: {result.stderr}"

        # Step 5: Verify stg now has load_id_2 (the latest)
        conn = duckdb.connect(database=":memory:")
        conn.execute(f"ATTACH '{duckdb_path}' AS \"{catalog}\"")
        stg_load_ids_2 = conn.execute(
            f'SELECT DISTINCT _dlt_load_id FROM "{catalog}"."stg"."szclient"'
        ).fetchall()
        conn.close()

        assert len(stg_load_ids_2) == 1, (
            f"Expected 1 load_id in stg, got {len(stg_load_ids_2)}"
        )
        assert stg_load_ids_2[0][0] == load_id_2, (
            f"Expected stg to have load_id_2 ({load_id_2}), "
            f"but got {stg_load_ids_2[0][0]}"
        )

    @pytest.mark.integration
    def test_stg_does_not_refresh_without_restate(
        self,
        duckdb_path: Path,
        sqlmesh_state_dir: Path,
        load_synthetic_data,
        project_root: str,
    ) -> None:
        """Verify stg layer does NOT refresh when running without restate.

        This test confirms that --no-restate-raw behavior works:
        stg should keep showing load_id_1 even after load_id_2 is added.
        """
        import duckdb

        # Step 1: Load first batch of data
        load_id_1 = load_synthetic_data("first")

        # Step 2: Run SQLMesh to initialize models
        # Use SQLMESH_HOME to isolate state for this test
        env = {
            **os.environ,
            "DESTINATION__DUCKDB__CREDENTIALS": str(duckdb_path),
            "SQLMESH_HOME": str(sqlmesh_state_dir),
        }
        sqlmesh_cmd = _get_sqlmesh_command()
        result = subprocess.run(
            [
                *sqlmesh_cmd,
                "-p",
                "sqlmesh",
                "--gateway",
                "duckdb",
                "plan",
                "--auto-apply",
            ],
            cwd=project_root,
            capture_output=True,
            text=True,
            timeout=120,
            env=env,
        )
        assert result.returncode == 0, f"First SQLMesh run failed: {result.stdout}"

        # Step 3: Load second batch of data
        load_id_2 = load_synthetic_data("second")

        # Step 4: Run SQLMesh WITHOUT restate (should NOT refresh stg/silver)
        result = subprocess.run(
            [
                *sqlmesh_cmd,
                "-p",
                "sqlmesh",
                "--gateway",
                "duckdb",
                "plan",
                "--auto-apply",
            ],
            cwd=project_root,
            capture_output=True,
            text=True,
            timeout=120,
            env=env,
        )
        assert result.returncode == 0, f"Second SQLMesh run failed: {result.stdout}"

        # Step 5: Verify stg still has load_id_1 (NOT load_id_2)
        # because we didn't restate external models
        catalog = duckdb_path.stem
        conn = duckdb.connect(database=":memory:")
        conn.execute(f"ATTACH '{duckdb_path}' AS \"{catalog}\"")
        stg_load_ids = conn.execute(
            f'SELECT DISTINCT _dlt_load_id FROM "{catalog}"."stg"."szclient"'
        ).fetchall()
        conn.close()

        assert len(stg_load_ids) == 1, (
            f"Expected 1 load_id in stg, got {len(stg_load_ids)}"
        )
        # Without restate, stg should still show the OLD load_id_1
        assert stg_load_ids[0][0] == load_id_1, (
            f"Expected stg to keep load_id_1 ({load_id_1}) without restate, "
            f"but got {stg_load_ids[0][0]}"
        )
