"""Runtime configuration, read from the environment exactly once.

Every service (two ingestion jobs, the dashboard, the training scripts) builds one ``Settings``
at startup and passes it down. Nothing else in the codebase touches ``os.environ`` — except
``predict.py``, which owns its own MODEL_PATH / threshold knobs because it is also used standalone
(training, tests) where no database configuration exists.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import quote

from psycopg.conninfo import make_conninfo


class ConfigError(RuntimeError):
    """A required environment variable is missing."""


def _require(name: str) -> str:
    value = os.environ.get(name)
    if value is None or value == "":
        raise ConfigError(f"environment variable {name} is required (see .env.example)")
    return value


@dataclass(frozen=True)
class Settings:
    db_host: str
    db_port: int
    db_name: str
    db_user: str
    db_password: str
    db_sslmode: str            # "require" for RDS; "disable" only for a local non-TLS Postgres
    s3_bucket: str
    aws_region: str
    log_level: str
    migrations_dir: Path

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            db_host=_require("DB_HOST"),
            db_port=int(os.getenv("DB_PORT", "5432")),
            db_name=os.getenv("DB_NAME", "airbreda"),
            db_user=_require("DB_USER"),
            db_password=_require("DB_PASSWORD"),
            db_sslmode=os.getenv("DB_SSLMODE", "require"),
            s3_bucket=os.getenv("S3_BUCKET", ""),
            aws_region=os.getenv("AWS_REGION", "eu-north-1"),
            log_level=os.getenv("LOG_LEVEL", "INFO"),
            # Relative to the working directory: the project root locally, /app in the containers.
            migrations_dir=Path(os.getenv("MIGRATIONS_DIR", "db/migrations")),
        )

    @property
    def dsn(self) -> str:
        """libpq connection string for psycopg (properly quoted for any password)."""
        return make_conninfo(
            host=self.db_host, port=self.db_port, dbname=self.db_name, user=self.db_user,
            password=self.db_password, connect_timeout=10, sslmode=self.db_sslmode,
        )

    @property
    def database_url(self) -> str:
        """URL form of the same connection, as yoyo-migrations expects it."""
        return (
            f"postgresql+psycopg://{quote(self.db_user, safe='')}:{quote(self.db_password, safe='')}"
            f"@{self.db_host}:{self.db_port}/{self.db_name}?sslmode={self.db_sslmode}"
        )
