"""Shared test setup. Database-backed tests need DB_* in the environment (locally: .env.local);
when it is present the migrations are applied once per session so the tests run against the
real schema, exactly as deployed."""
import os

import pytest


@pytest.fixture(scope="session", autouse=True)
def migrated_database():
    if "DB_HOST" not in os.environ:
        return
    from migrate import apply_migrations
    from settings import Settings

    apply_migrations(Settings.from_env())
