"""Small deterministic local fixtures."""

from pathlib import Path

import pytest


@pytest.fixture
def fixture_dir() -> Path:
    return Path(__file__).resolve().parents[1] / "data" / "fixtures"
