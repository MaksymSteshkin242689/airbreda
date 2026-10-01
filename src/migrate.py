"""migrate.py — apply the versioned SQL migrations in db/migrations with yoyo-migrations.

    python src/migrate.py            # apply pending migrations
    python src/migrate.py --status   # list applied / pending, change nothing

Run as a deploy step (``infra/deploy.sh`` does this inside the airbreda-air image before the
cron jobs start), never implicitly from the services: a service that silently alters the schema
on startup is how two containers end up racing each other on ALTER TABLE.
"""
from __future__ import annotations

import argparse
import logging
import sys

from yoyo import get_backend, read_migrations

from observability import get_logger, log_event
from settings import Settings

log = get_logger("migrate")


def apply_migrations(settings: Settings) -> list[str]:
    """Apply every pending migration in order inside yoyo's lock. Returns the ids applied."""
    backend = get_backend(settings.database_url)
    migrations = read_migrations(str(settings.migrations_dir))
    with backend.lock():
        pending = backend.to_apply(migrations)
        backend.apply_migrations(pending)
    ids = [m.id for m in pending]
    log_event(log, logging.INFO, event="migrations_applied", count=len(ids), ids=ids)
    return ids


def status(settings: Settings) -> tuple[list[str], list[str]]:
    backend = get_backend(settings.database_url)
    migrations = read_migrations(str(settings.migrations_dir))
    pending = [m.id for m in backend.to_apply(migrations)]
    applied = [m.id for m in migrations if m.id not in pending]
    return applied, pending


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--status", action="store_true", help="show applied/pending and exit")
    args = parser.parse_args(argv)
    settings = Settings.from_env()
    if args.status:
        applied, pending = status(settings)
        print("applied:", *applied, sep="\n  ")
        print("pending:", *pending, sep="\n  ")
        return 0
    apply_migrations(settings)
    return 0


if __name__ == "__main__":
    sys.exit(main())
