"""Configuration via pydantic-settings; env-loaded.

Reads `.env` at repo root. All paths are derived from `FTTH_DATA_DIR` so that
bulk data and the SQLite cache can live outside OneDrive sync without
touching the source tree.
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


def _repo_root() -> Path:
    """Resolve the repo root from this file's location."""
    return Path(__file__).resolve().parent.parent.parent


def _default_data_dir() -> Path:
    """Default data directory: `<repo>/data` if FTTH_DATA_DIR is unset."""
    return _repo_root() / "data"


class Settings(BaseSettings):
    """Project-wide settings."""

    model_config = SettingsConfigDict(
        env_file=_repo_root() / ".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    census_api_key: str = Field(default="", alias="CENSUS_API_KEY")
    google_places_key: str = Field(default="", alias="GOOGLE_PLACES_KEY")
    fcc_username: str = Field(default="", alias="FCC_USERNAME")
    fcc_api_token: str = Field(default="", alias="FCC_API_TOKEN")
    data_dir: Path = Field(default_factory=_default_data_dir, alias="FTTH_DATA_DIR")

    @property
    def raw_dir(self) -> Path:
        return self.data_dir / "raw"

    @property
    def processed_dir(self) -> Path:
        return self.data_dir / "processed"

    @property
    def cache_db_path(self) -> Path:
        return self.data_dir / "cache.db"

    def ensure_dirs(self) -> None:
        """Create raw/ and processed/ if missing."""
        for d in (self.raw_dir, self.processed_dir):
            d.mkdir(parents=True, exist_ok=True)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Cached singleton accessor."""
    return Settings()
