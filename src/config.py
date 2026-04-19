"""
Configuration management for the Corporate Credit Rating Data Pipeline.

Uses pydantic-settings for environment-based configuration with validation.
"""

from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    # Database
    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_db: str = "credit_rating_dw"
    postgres_user: str = "pipeline_user"
    postgres_password: str = "pipeline_secret_2024"

    # API
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    api_debug: bool = False

    # Pipeline
    data_dir: str = str(Path(__file__).parent.parent / "data")
    log_level: str = "INFO"

    @property
    def database_url(self) -> str:
        """Construct PostgreSQL connection URL."""
        return (
            f"postgresql://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )

    @property
    def async_database_url(self) -> str:
        """Construct async PostgreSQL connection URL."""
        return (
            f"postgresql+asyncpg://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )

    @property
    def data_path(self) -> Path:
        """Return Path object for data directory."""
        return Path(self.data_dir)

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False


@lru_cache()
def get_settings() -> Settings:
    """Get cached application settings singleton."""
    return Settings()
