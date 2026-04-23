"""Shared pytest fixtures for shelvez tests."""

from __future__ import annotations

from pathlib import Path

import pytest


@pytest.fixture
def db_path(tmp_path: Path) -> str:
    """Return a unique, non-existing SQLite file path inside tmp_path."""
    return str(tmp_path / "shelvez.db")


@pytest.fixture
def cache_path(tmp_path: Path) -> str:
    """Return a unique, non-existing sqlcache file path inside tmp_path."""
    return str(tmp_path / "cache.db")
