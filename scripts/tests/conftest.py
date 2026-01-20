"""Pytest configuration and fixtures for tests."""

import pytest


def pytest_addoption(parser):
    """Add command-line option for integration tests."""
    parser.addoption(
        "--run-integration",
        action="store_true",
        default=False,
        help="Run integration tests that require database connections",
    )
