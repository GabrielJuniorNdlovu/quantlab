"""Small deterministic local fixtures."""

import os
from pathlib import Path

import pytest


@pytest.fixture
def fixture_dir() -> Path:
    return Path(__file__).resolve().parents[1] / "data" / "fixtures"


@pytest.fixture(scope="session", autouse=True)
def initialize_isolated_storage_database() -> None:
    """An explicitly selected test database must not depend on test execution order."""
    dsn = os.environ.get("QUANTLAB_TEST_DATABASE_URL")
    if dsn:
        from quantlab.storage.repository import migrate

        migrate(dsn)
