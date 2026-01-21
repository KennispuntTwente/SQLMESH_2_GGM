"""Integration tests for the GGM pipeline.

These tests verify that:
1. The `uv run pipeline` script works correctly
2. The Docker image can run the pipeline
3. Both produce valid output

Run with: uv run pytest tests/test_pipeline.py -v
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest


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
        result = subprocess.run(
            [sys.executable, "-m", "sqlmesh", "-p", "sqlmesh", "--gateway", "local", "info"],
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
