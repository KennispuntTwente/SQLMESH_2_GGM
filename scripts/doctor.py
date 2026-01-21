"""Environment diagnostics for common native-library (DLL/.so) issues.

This project relies on a handful of Python packages with native extensions
(e.g. pyarrow, duckdb, pyodbc). In some environments those extensions can fail
to load due to missing system runtimes/drivers, architecture mismatches, or
Windows path-length limitations.

Run:
  uv run doctor
"""

from __future__ import annotations

import argparse
import ctypes
import importlib
import json
import os
import platform
import struct
import subprocess
import sys
import textwrap
import traceback
from dataclasses import asdict, dataclass
from pathlib import Path
from urllib.parse import parse_qs, unquote_plus, urlparse

# Load .env if present (keeps existing env vars intact).
try:
    from dotenv import load_dotenv

    load_dotenv()
except Exception:
    pass


@dataclass(frozen=True)
class CheckResult:
    name: str
    ok: bool
    detail: str | None = None
    hint: str | None = None


def _is_windows() -> bool:
    return os.name == "nt"


def _shorten(text: str, limit: int = 220) -> str:
    text = text.strip().replace("\r\n", "\n")
    return text if len(text) <= limit else (text[: limit - 3] + "...")


def _run(cmd: list[str], timeout_s: int = 10) -> str | None:
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout_s,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None

    output = (result.stdout or "").strip()
    return output or None


def _uv_version() -> str | None:
    uv = os.environ.get("UV")
    if uv:
        out = _run([uv, "--version"])
        if out:
            return out
    out = _run(["uv", "--version"])
    return out


def _python_info() -> dict[str, str]:
    bits = struct.calcsize("P") * 8
    return {
        "version": sys.version.replace("\n", " "),
        "executable": sys.executable,
        "bits": str(bits),
        "prefix": sys.prefix,
        "base_prefix": getattr(sys, "base_prefix", ""),
        "platform": platform.platform(),
        "machine": platform.machine(),
    }


def _check_python_bitness() -> CheckResult:
    bits = struct.calcsize("P") * 8
    ok = bits >= 64
    hint = None
    if not ok:
        hint = (
            "32-bit Python detected. Install a 64-bit Python build and re-create "
            "the virtual environment to avoid native-extension load failures."
        )
    return CheckResult(name="python:bits", ok=ok, detail=f"{bits}-bit", hint=hint)


def _windows_long_paths_enabled() -> bool | None:
    if not _is_windows():
        return None
    try:
        import winreg

        with winreg.OpenKey(
            winreg.HKEY_LOCAL_MACHINE,
            r"SYSTEM\CurrentControlSet\Control\FileSystem",
        ) as key:
            value, _ = winreg.QueryValueEx(key, "LongPathsEnabled")
            return bool(value)
    except Exception:
        return None


def _check_windows_runtime_dlls() -> list[CheckResult]:
    if not _is_windows():
        return []

    dlls = [
        "vcruntime140.dll",
        "vcruntime140_1.dll",
        "msvcp140.dll",
        "ucrtbase.dll",
    ]
    results: list[CheckResult] = []
    for dll_name in dlls:
        try:
            ctypes.WinDLL(dll_name)
            results.append(CheckResult(name=f"runtime:{dll_name}", ok=True))
        except OSError as e:
            results.append(
                CheckResult(
                    name=f"runtime:{dll_name}",
                    ok=False,
                    detail=_shorten(str(e)),
                    hint=(
                        "Install Microsoft Visual C++ 2015-2022 Redistributable (x64). "
                        "This commonly fixes native-extension import errors on Windows."
                    ),
                )
            )
    return results


def _truthy_env(name: str) -> bool:
    return os.environ.get(name, "").strip().lower() in ("1", "true", "yes", "on")


def _is_mssql_configured() -> bool:
    if os.environ.get("GGM_DESTINATION", "").strip().lower() == "mssql":
        return True
    if os.environ.get("DESTINATION__MSSQL__CREDENTIALS"):
        return True
    prefix = "DESTINATION__MSSQL__CREDENTIALS__"
    return any(key.startswith(prefix) for key in os.environ)


def _get_mssql_driver_from_env() -> str | None:
    driver = os.environ.get("DESTINATION__MSSQL__CREDENTIALS__DRIVER", "").strip()
    if driver:
        return driver

    conn_str = os.environ.get("DESTINATION__MSSQL__CREDENTIALS", "").strip()
    if not conn_str:
        return None
    try:
        parsed = urlparse(conn_str)
        query = parse_qs(parsed.query)
        value = query.get("driver", [None])[0]
        if value:
            return unquote_plus(value)
    except Exception:
        return None
    return None


def _check_mssql_odbc_driver() -> CheckResult | None:
    if not _is_mssql_configured():
        return None

    expected = _get_mssql_driver_from_env() or "ODBC Driver 18 for SQL Server"
    try:
        import pyodbc
    except Exception as e:
        return CheckResult(
            name="mssql:odbc_driver",
            ok=False,
            detail=_shorten(f"pyodbc import failed: {type(e).__name__}: {e}"),
            hint=(
                "Install pyodbc and the Microsoft ODBC Driver 18 (or 17) for SQL Server. "
                "Then set DESTINATION__MSSQL__CREDENTIALS__DRIVER to the installed driver name."
            ),
        )

    try:
        drivers = pyodbc.drivers()
    except Exception as e:
        return CheckResult(
            name="mssql:odbc_driver",
            ok=False,
            detail=_shorten(f"drivers() failed: {type(e).__name__}: {e}"),
            hint="Repair the ODBC installation or re-install the SQL Server driver.",
        )

    if not drivers:
        return CheckResult(
            name="mssql:odbc_driver",
            ok=False,
            detail="no ODBC drivers found",
            hint=(
                "Install the Microsoft ODBC Driver 18 (or 17) for SQL Server. "
                "Ensure the driver is visible in the ODBC Data Sources control panel."
            ),
        )

    expected_lower = expected.lower()
    driver_map = {d.lower(): d for d in drivers}
    if expected_lower not in driver_map:
        return CheckResult(
            name="mssql:odbc_driver",
            ok=False,
            detail=f"expected='{expected}' available={drivers}",
            hint=(
                "Install the expected driver or update DESTINATION__MSSQL__CREDENTIALS__DRIVER "
                "to match an installed driver name."
            ),
        )

    return CheckResult(
        name="mssql:odbc_driver",
        ok=True,
        detail=f"driver='{driver_map[expected_lower]}' available",
    )


def _check_oracle_thick_mode_paths() -> list[CheckResult]:
    if not _truthy_env("ORACLE_THICK_MODE"):
        return []

    results: list[CheckResult] = []
    lib_dir = os.environ.get("ORACLE_CLIENT_LIB_DIR", "").strip()
    if not lib_dir:
        results.append(
            CheckResult(
                name="oracle:client_lib_dir",
                ok=False,
                detail="ORACLE_CLIENT_LIB_DIR not set",
                hint=(
                    "Set ORACLE_CLIENT_LIB_DIR to the Oracle Instant Client directory "
                    "or disable ORACLE_THICK_MODE to use thin mode."
                ),
            )
        )
    else:
        path = Path(lib_dir)
        if not path.exists():
            results.append(
                CheckResult(
                    name="oracle:client_lib_dir",
                    ok=False,
                    detail=f"path not found: {lib_dir}",
                    hint=(
                        "Ensure ORACLE_CLIENT_LIB_DIR points to a valid Instant Client "
                        "directory that contains oci.dll (Windows) or libclntsh.so (Linux)."
                    ),
                )
            )
        else:
            results.append(
                CheckResult(
                    name="oracle:client_lib_dir",
                    ok=True,
                    detail=f"path={lib_dir}",
                )
            )

    tns_admin = os.environ.get("TNS_ADMIN", "").strip()
    if tns_admin:
        tns_path = Path(tns_admin)
        results.append(
            CheckResult(
                name="oracle:tns_admin",
                ok=tns_path.exists(),
                detail=f"path={tns_admin}",
                hint=None
                if tns_path.exists()
                else "TNS_ADMIN points to a missing directory; update it or unset it.",
            )
        )
    return results


def _hint_for_import_error(module_name: str, exc: BaseException) -> str | None:
    message = str(exc)
    lower = message.lower()

    if _is_windows():
        if "winerror 193" in lower or "not a valid win32 application" in lower:
            return (
                "Architecture mismatch (32-bit vs 64-bit). Ensure you are using a 64-bit "
                "Python and matching wheels for all native packages."
            )
        if "winerror 206" in lower or "filename or extension is too long" in lower:
            return (
                "Likely Windows path-length issue. Enable long paths (LongPathsEnabled=1) "
                "or move the repo to a shorter path (e.g. C:\\src\\...)."
            )
        if "dll load failed" in lower or "winerror 126" in lower or "winerror 127" in lower:
            if module_name in {"pyarrow", "duckdb", "pyodbc"}:
                return (
                    "Native DLL failed to load. Common fixes: install Microsoft Visual C++ "
                    "2015-2022 Redistributable (x64) and verify you have a 64-bit Python."
                )
            return (
                "Native DLL failed to load. Common fixes: install Microsoft Visual C++ "
                "2015-2022 Redistributable (x64) and ensure PATH does not point to "
                "conflicting DLLs (e.g. older OpenSSL/Oracle clients)."
            )
    else:
        if "cannot open shared object file" in lower or "undefined symbol" in lower:
            if module_name == "pyodbc":
                return (
                    "pyodbc needs system ODBC libraries. Install unixODBC (e.g. "
                    "`apt-get install unixodbc` / `unixodbc-dev`) and the vendor driver."
                )
            if module_name == "psycopg2":
                return "Install libpq (e.g. `apt-get install libpq5`) or use psycopg (v3)."
            return (
                "A required system library (.so) is missing. Install the OS-level "
                "dependencies for this package (see Dockerfile for a known-good list)."
            )

    if module_name == "oracledb" and "dpi-1047" in lower:
        return (
            "Oracle thick mode can't find Instant Client libraries. Either disable "
            "thick mode (remove ORACLE_THICK_MODE) or install Oracle Instant Client "
            "and set ORACLE_CLIENT_LIB_DIR (and TNS_ADMIN if using TNS aliases)."
        )

    if module_name == "pyarrow":
        return "If you can't load PyArrow, set `GGM_DLT_BACKEND=sqlalchemy` to run without it."

    return None


def _check_import(module_name: str, verbose: bool = False) -> CheckResult:
    try:
        module = importlib.import_module(module_name)
    except Exception as e:
        detail = _shorten(f"{type(e).__name__}: {e}")
        hint = _hint_for_import_error(module_name, e)
        if verbose:
            detail = _shorten(traceback.format_exc(), limit=1200)
        return CheckResult(name=f"import:{module_name}", ok=False, detail=detail, hint=hint)

    version = getattr(module, "__version__", None)
    detail = f"version={version}" if version else None

    if module_name == "pyodbc":
        try:
            drivers = module.drivers()
            detail = f"{detail or ''} drivers={drivers}".strip()
        except Exception as e:
            detail = f"{detail or ''} drivers_error={type(e).__name__}: {e}".strip()

    if module_name == "oracledb":
        try:
            thin = module.is_thin_mode()
            detail = f"{detail or ''} thin={thin}".strip()
        except Exception:
            pass

        if os.environ.get("ORACLE_THICK_MODE", "").lower() in ("1", "true", "yes"):
            try:
                lib_dir = os.environ.get("ORACLE_CLIENT_LIB_DIR")
                module.init_oracle_client(lib_dir=lib_dir)
                detail = f"{detail or ''} thick_init=ok".strip()
            except Exception as e:
                hint = _hint_for_import_error(module_name, e)
                detail = f"{detail or ''} thick_init=fail ({type(e).__name__}: {e})".strip()
                return CheckResult(
                    name=f"import:{module_name}",
                    ok=False,
                    detail=_shorten(detail, limit=500),
                    hint=hint,
                )

    return CheckResult(name=f"import:{module_name}", ok=True, detail=_shorten(detail or "ok", limit=500))


def _native_path_scan(site_packages: Path) -> CheckResult:
    if not site_packages.exists():
        return CheckResult(name="paths:native_scan", ok=True, detail="site-packages not found")

    exts = {".pyd", ".dll", ".so", ".dylib"}
    longest: list[tuple[int, str]] = []
    try:
        for path in site_packages.rglob("*"):
            if not path.is_file():
                continue
            if path.suffix.lower() not in exts:
                continue
            p = str(path)
            longest.append((len(p), p))
        longest.sort(reverse=True)
    except Exception as e:
        return CheckResult(
            name="paths:native_scan",
            ok=False,
            detail=_shorten(f"{type(e).__name__}: {e}"),
        )

    if not longest:
        return CheckResult(name="paths:native_scan", ok=True, detail="no native files found")

    max_len, max_path = longest[0]
    top = ", ".join(f"{l}:{Path(p).name}" for l, p in longest[:5])
    detail = f"max_len={max_len} max_file={Path(max_path).name} top5=({top})"

    if _is_windows() and max_len >= 250:
        return CheckResult(
            name="paths:native_scan",
            ok=False,
            detail=_shorten(detail, limit=500),
            hint=(
                "Native extension paths are very long. On Windows this can break DLL loading. "
                "Enable long paths or move the repo/venv to a shorter path."
            ),
        )

    return CheckResult(name="paths:native_scan", ok=True, detail=_shorten(detail, limit=500))


def collect_checks(scan_paths: bool, verbose: bool) -> tuple[dict[str, str], list[CheckResult]]:
    info: dict[str, str] = {}
    py = _python_info()
    info.update({f"python.{k}": v for k, v in py.items()})

    uv_ver = _uv_version()
    if uv_ver:
        info["uv.version"] = uv_ver

    info["env.VIRTUAL_ENV"] = os.environ.get("VIRTUAL_ENV", "")
    info["env.UV_CACHE_DIR"] = os.environ.get("UV_CACHE_DIR", "")
    info["env.UV_PROJECT_ENVIRONMENT"] = os.environ.get("UV_PROJECT_ENVIRONMENT", "")

    # Project-specific toggles commonly involved in native-library errors.
    info["env.GGM_DESTINATION"] = os.environ.get("GGM_DESTINATION", "")
    info["env.GGM_GATEWAY"] = os.environ.get("GGM_GATEWAY", "")
    info["env.GGM_DLT_BACKEND"] = os.environ.get("GGM_DLT_BACKEND", "")
    info["env.ORACLE_THICK_MODE"] = os.environ.get("ORACLE_THICK_MODE", "")
    info["env.ORACLE_CLIENT_LIB_DIR"] = os.environ.get("ORACLE_CLIENT_LIB_DIR", "")
    info["env.TNS_ADMIN"] = os.environ.get("TNS_ADMIN", "")

    checks: list[CheckResult] = []
    checks.append(_check_python_bitness())

    long_paths = _windows_long_paths_enabled()
    if long_paths is not None:
        checks.append(
            CheckResult(
                name="windows:LongPathsEnabled",
                ok=bool(long_paths),
                detail=str(long_paths),
                hint=None
                if long_paths
                else (
                    "Enable Windows long paths (LongPathsEnabled=1) or use a shorter "
                    "repo location to avoid native import failures."
                ),
            )
        )

    checks.extend(_check_windows_runtime_dlls())
    checks.extend(_check_oracle_thick_mode_paths())

    # Imports that commonly trigger native loads.
    for module_name in [
        "dlt",
        "sqlmesh",
        "pyarrow",
        "duckdb",
        "pyodbc",
        "oracledb",
        "psycopg2",
        "pymysql",
    ]:
        checks.append(_check_import(module_name, verbose=verbose))

    mssql_driver_check = _check_mssql_odbc_driver()
    if mssql_driver_check is not None:
        checks.append(mssql_driver_check)

    if scan_paths:
        # Find site-packages for the current interpreter.
        site_packages: Path | None = None
        try:
            from site import getsitepackages

            candidates = [Path(p) for p in getsitepackages()]
            site_packages = next((p for p in candidates if p.name == "site-packages"), None)
        except Exception:
            pass

        if site_packages is None:
            # Fallback heuristic for venv layouts.
            if _is_windows():
                site_packages = Path(sys.prefix) / "Lib" / "site-packages"
            else:
                site_packages = Path(sys.prefix) / "lib"

        checks.append(_native_path_scan(site_packages))

    return info, checks


def _print_human(info: dict[str, str], checks: list[CheckResult]) -> int:
    print("GGM Environment Doctor")
    print("=" * 72)
    for key in [
        "python.version",
        "python.bits",
        "python.platform",
        "python.executable",
        "python.prefix",
        "uv.version",
        "env.VIRTUAL_ENV",
        "env.UV_CACHE_DIR",
        "env.UV_PROJECT_ENVIRONMENT",
    ]:
        value = info.get(key)
        if value:
            print(f"{key}: {value}")

    for key in [
        "env.GGM_DESTINATION",
        "env.GGM_GATEWAY",
        "env.GGM_DLT_BACKEND",
        "env.ORACLE_THICK_MODE",
        "env.ORACLE_CLIENT_LIB_DIR",
        "env.TNS_ADMIN",
    ]:
        value = info.get(key, "")
        if value:
            print(f"{key}: {value}")

    print("\nChecks")
    print("-" * 72)
    failed = 0
    for check in checks:
        status = "OK " if check.ok else "FAIL"
        line = f"[{status}] {check.name}"
        if check.detail:
            line += f"  ({check.detail})"
        print(line)
        if not check.ok and check.hint:
            failed += 1
            wrapped = textwrap.fill(check.hint, width=72, subsequent_indent="       ")
            print(f"       hint: {wrapped}")
        elif not check.ok:
            failed += 1

    if failed:
        print("\nSummary")
        print("-" * 72)
        print(
            "One or more checks failed. Fix the hints above, then re-run:\n"
            "  uv run doctor\n"
            "\nIf you're blocked by PyArrow DLL errors, a functional workaround is:\n"
            "  set GGM_DLT_BACKEND=sqlalchemy\n"
        )
    return 0 if failed == 0 else 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Diagnose DLL/.so loading issues")
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output JSON (useful for CI logs).",
    )
    parser.add_argument(
        "--scan-paths",
        action="store_true",
        help="Scan site-packages for long native-library paths (slower).",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Include full exception traces for failed imports.",
    )
    args = parser.parse_args(argv)

    info, checks = collect_checks(scan_paths=args.scan_paths, verbose=args.verbose)

    if args.json:
        payload = {
            "info": info,
            "checks": [asdict(c) for c in checks],
        }
        print(json.dumps(payload, indent=2, sort_keys=True))
        return 0 if all(c.ok for c in checks) else 1

    return _print_human(info, checks)


if __name__ == "__main__":
    raise SystemExit(main())
