"""Packaged additive Alembic upgrades with a database-wide migration lock."""

from pathlib import Path

import psycopg


def migrate(dsn: str) -> None:
    """Run the packaged, additive Alembic head under a database migration lock."""
    from alembic import command
    from alembic.config import Config
    from sqlalchemy import create_engine
    from sqlalchemy.pool import NullPool

    with psycopg.connect(dsn, autocommit=True, connect_timeout=5) as lock:
        lock.execute("SELECT pg_advisory_lock(716850302)")
        engine = create_engine(
            "postgresql+psycopg://", creator=lambda: psycopg.connect(dsn), poolclass=NullPool
        )
        try:
            with engine.begin() as connection:
                config = Config()
                config.set_main_option("script_location", str(Path(__file__).parent / "migrations"))
                config.attributes["connection"] = connection
                command.upgrade(config, "head")
        finally:
            engine.dispose()
