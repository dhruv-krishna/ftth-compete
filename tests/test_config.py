"""Tests for ftth_compete.config."""

from __future__ import annotations

from pathlib import Path

from ftth_compete import config


def test_settings_defaults_when_env_unset(isolated_data_dir: Path) -> None:
    s = config.get_settings()
    assert s.data_dir == isolated_data_dir
    assert s.raw_dir == isolated_data_dir / "raw"
    assert s.processed_dir == isolated_data_dir / "processed"
    assert s.cache_db_path == isolated_data_dir / "cache.db"
    assert s.census_api_key == ""
    assert s.google_places_key == ""


def test_ensure_dirs_creates_subdirs(isolated_data_dir: Path) -> None:
    s = config.get_settings()
    assert not s.raw_dir.exists()
    assert not s.processed_dir.exists()
    s.ensure_dirs()
    assert s.raw_dir.is_dir()
    assert s.processed_dir.is_dir()


def test_get_settings_is_cached() -> None:
    a = config.get_settings()
    b = config.get_settings()
    assert a is b
