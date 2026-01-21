"""Pytest fixtures for pipeline integration tests."""
from __future__ import annotations

import json
import os
import subprocess
import time
from typing import Generator

import pytest


def is_docker_available() -> bool:
    """Check if Docker is available and running."""
    try:
        result = subprocess.run(
            ["docker", "info"],
            capture_output=True,
            timeout=10,
        )
        return result.returncode == 0
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return False


def is_service_running(service_name: str, compose_file: str) -> bool:
    """Check if a Docker Compose service is running."""
    try:
        result = subprocess.run(
            ["docker", "compose", "-f", compose_file, "ps", "-q", service_name],
            capture_output=True,
            text=True,
            timeout=10,
        )
        return bool(result.stdout.strip())
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return False


def wait_for_postgres(host: str = "localhost", port: int = 5432, timeout: int = 60) -> bool:
    """Wait for PostgreSQL to be ready."""
    import socket
    
    start = time.time()
    while time.time() - start < timeout:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(1)
            result = sock.connect_ex((host, port))
            sock.close()
            if result == 0:
                return True
        except socket.error:
            pass
        time.sleep(1)
    return False


def wait_for_container_healthy(container_id: str, timeout: int = 420) -> bool:
    """Wait for a Docker container to report a healthy status."""
    start = time.time()
    while time.time() - start < timeout:
        result = subprocess.run(
            ["docker", "inspect", "-f", "{{.State.Health.Status}}", container_id],
            capture_output=True,
            text=True,
            timeout=10,
        )
        status = result.stdout.strip()
        if status == "healthy":
            return True
        if status == "unhealthy":
            return False
        time.sleep(5)
    return False


def get_container_network(container_id: str) -> str:
    """Return the first Docker network name the container is attached to."""
    result = subprocess.run(
        ["docker", "inspect", container_id],
        capture_output=True,
        text=True,
        timeout=10,
        check=True,
    )
    data = json.loads(result.stdout)[0]
    networks: dict[str, object] = (
        data.get("NetworkSettings", {}).get("Networks", {})  # type: ignore[assignment]
    )
    if not networks:
        raise RuntimeError(f"No networks found for container: {container_id}")
    return next(iter(networks.keys()))


@pytest.fixture(scope="session")
def docker_available() -> bool:
    """Session-scoped fixture that checks Docker availability."""
    return is_docker_available()


@pytest.fixture(scope="session")
def project_root() -> str:
    """Return the project root directory."""
    # Go up from tests/ to project root
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


@pytest.fixture(scope="session")
def compose_file(project_root: str) -> str:
    """Return path to docker-compose.yml."""
    return os.path.join(project_root, "docker", "docker-compose.yml")


@pytest.fixture(scope="session")
def docker_services(
    docker_available: bool,
    compose_file: str,
    project_root: str,
) -> Generator[dict[str, bool], None, None]:
    """
    Start Docker services for integration tests.
    
    This fixture starts PostgreSQL (fast to start) for pipeline tests.
    Oracle is optional and will be skipped if not available.
    """
    if not docker_available:
        pytest.skip("Docker is not available")
    
    # Start only PostgreSQL for faster tests
    subprocess.run(
        ["docker", "compose", "-f", compose_file, "up", "-d", "postgres"],
        cwd=project_root,
        check=True,
    )
    
    # Wait for PostgreSQL
    if not wait_for_postgres():
        pytest.fail("PostgreSQL did not become ready in time")
    
    yield {
        "postgres": True,
        "oracle": is_service_running("oracle", compose_file),
    }
    
    # Cleanup is optional - leave containers running for faster re-runs
    # Uncomment to stop services after tests:
    # subprocess.run(
    #     ["docker", "compose", "-f", compose_file, "down"],
    #     cwd=project_root,
    # )


@pytest.fixture(scope="session")
def oracle_service(
    docker_available: bool,
    compose_file: str,
    project_root: str,
) -> dict[str, str]:
    """Ensure the Oracle service is running and healthy; return container info."""
    if not docker_available:
        pytest.skip("Docker is not available")

    subprocess.run(
        ["docker", "compose", "-f", compose_file, "up", "-d", "oracle"],
        cwd=project_root,
        check=True,
    )

    container_id = subprocess.run(
        ["docker", "compose", "-f", compose_file, "ps", "-q", "oracle"],
        cwd=project_root,
        capture_output=True,
        text=True,
        timeout=10,
        check=True,
    ).stdout.strip()
    if not container_id:
        pytest.fail("Oracle container did not start (no container id found)")

    if not wait_for_container_healthy(container_id):
        pytest.fail("Oracle did not become healthy in time")

    return {
        "container_id": container_id,
        "network": get_container_network(container_id),
    }
