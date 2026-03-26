"""Shared test fixtures for the ARC Capstone pipeline."""

from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent


@pytest.fixture
def project_root() -> Path:
    """Return the repository root directory."""
    return PROJECT_ROOT
