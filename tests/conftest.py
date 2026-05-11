"""Pytest configuration and shared fixtures."""

from __future__ import annotations

import os
from pathlib import Path

import pytest


@pytest.fixture
def isolated_data_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Point FTTH_DATA_DIR at a tmp directory and clear API keys.

    Sets keys to empty strings (rather than delenv) because pydantic-settings
    falls back to the .env file when an OS env var is absent — and the dev's
    real .env may contain real keys.
    """
    monkeypatch.setenv("FTTH_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("CENSUS_API_KEY", "")
    monkeypatch.setenv("GOOGLE_PLACES_KEY", "")

    from ftth_compete import config

    config.get_settings.cache_clear()
    yield tmp_path
    config.get_settings.cache_clear()
