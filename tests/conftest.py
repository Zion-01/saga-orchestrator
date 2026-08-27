"""Shared fixtures for the journal/replay test suite."""

from __future__ import annotations

from pathlib import Path

import pytest


@pytest.fixture
def journal_path(tmp_path: Path) -> Path:
    """A fresh, not-yet-existing journal file path inside an isolated tmp dir."""
    return tmp_path / "wf.journal"
