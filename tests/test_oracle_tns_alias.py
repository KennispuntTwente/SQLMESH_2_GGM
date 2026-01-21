"""Integration test: Oracle TNS alias connectivity (thick mode).

This test is intentionally Docker-based so it can validate:
- Oracle Instant Client is present (thick mode)
- `TNS_ADMIN` + `tnsnames.ora` resolution works
- SQLAlchemy URL form `oracle+oracledb://user:pass@<TNS_ALIAS>` connects successfully
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

pytestmark = [
    pytest.mark.integration,
    pytest.mark.slow,
]


def test_oracle_connects_via_tns_alias_in_thick_mode(
    docker_available: bool,
    project_root: str,
    oracle_service: dict[str, str],
    tmp_path: Path,
) -> None:
    if not docker_available:
        pytest.skip("Docker is not available")

    # Build the project image (includes Oracle Instant Client).
    build_result = subprocess.run(
        ["docker", "build", "-t", "ggm-pipeline:test", "."],
        cwd=project_root,
        capture_output=True,
        text=True,
        timeout=900,
    )
    if build_result.returncode != 0:
        pytest.skip("Docker image could not be built")

    # Use an alias that is not a valid hostname to ensure tnsnames.ora is used.
    tns_alias = "THIS_IS_NOT_A_HOST"

    (tmp_path / "tnsnames.ora").write_text(
        f"""{tns_alias} =
  (DESCRIPTION =
    (ADDRESS = (PROTOCOL = TCP)(HOST = oracle)(PORT = 1521))
    (CONNECT_DATA =
      (SERVER = DEDICATED)
      (SERVICE_NAME = ggm)
    )
  )
""",
        encoding="utf-8",
    )

    (tmp_path / "check_oracle_tns.py").write_text(
        """
import os

import oracledb
from sqlalchemy import create_engine, text

lib_dir = os.environ.get("ORACLE_CLIENT_LIB_DIR")
oracledb.init_oracle_client(lib_dir=lib_dir)
assert not oracledb.is_thin_mode(), "Expected thick mode (Instant Client) but driver is in thin mode"

url = os.environ["SOURCES__SQL_DATABASE__CREDENTIALS"]
engine = create_engine(url)
with engine.connect() as conn:
    value = conn.execute(text("SELECT 1 FROM DUAL")).scalar_one()
assert value == 1

print("OK")
""".lstrip(),
        encoding="utf-8",
    )

    docker_run = subprocess.run(
        [
            "docker",
            "run",
            "--rm",
            "--network",
            oracle_service["network"],
            "--mount",
            f"type=bind,source={tmp_path},target=/tns,readonly",
            "-e",
            "TNS_ADMIN=/tns",
            "-e",
            "ORACLE_THICK_MODE=true",
            "-e",
            "ORACLE_CLIENT_LIB_DIR=/opt/oracle/instantclient_23_5",
            "-e",
            f"SOURCES__SQL_DATABASE__CREDENTIALS=oracle+oracledb://appuser:apppass@{tns_alias}",
            "--entrypoint",
            "uv",
            "ggm-pipeline:test",
            "run",
            "python",
            "/tns/check_oracle_tns.py",
        ],
        capture_output=True,
        text=True,
        timeout=120,
    )

    assert docker_run.returncode == 0, (
        "TNS alias connection failed.\n"
        f"stdout:\n{docker_run.stdout}\n\n"
        f"stderr:\n{docker_run.stderr}\n"
    )
    assert "OK" in docker_run.stdout
