"""Argparse operations for local TimescaleDB ingestion and completed-dataset replay."""

import argparse
import json
import logging
import os
import sys
from collections.abc import Sequence
from dataclasses import asdict, is_dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from uuid import UUID

from quantlab.parsing import parse_timestamp


def _value(value: object) -> object:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, (UUID, Decimal)):
        return str(value)
    if is_dataclass(value) and not isinstance(value, type):
        return asdict(value)
    raise TypeError(f"unsupported JSON value {type(value).__name__}")


def _print(value: object) -> None:
    print(json.dumps(value, default=_value, sort_keys=True, allow_nan=False))


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="QuantLab durable research storage")
    commands = parser.add_subparsers(dest="command", required=True)
    db = commands.add_parser("db").add_subparsers(dest="operation", required=True)
    db.add_parser("init", help="apply additive Alembic migrations")
    ingest = commands.add_parser("ingest").add_subparsers(dest="operation", required=True)
    importer = ingest.add_parser("import", help="preserve and import a canonical CSV")
    importer.add_argument("dataset_path", type=Path)
    importer.add_argument("--provider", required=True)
    importer.add_argument("--provider-symbol", required=True)
    importer.add_argument("--pip-size", required=True, type=int)
    importer.add_argument("--source-id", default="local-csv")
    importer.add_argument("--batch-size", type=int, default=100)
    status = ingest.add_parser("status")
    status.add_argument("dataset_id", type=UUID, nargs="?")
    status.add_argument("--limit", type=int, default=100)
    datasets = ingest.add_parser("datasets")
    datasets.add_argument("--limit", type=int, default=100)
    replay = commands.add_parser(
        "replay", help="stream one completed dataset in chronological order"
    )
    replay.add_argument("dataset_id", type=UUID)
    replay.add_argument("--start", type=parse_timestamp)
    replay.add_argument("--end", type=parse_timestamp)
    replay.add_argument("--limit", type=int, default=100)
    return parser


def _execute(args: argparse.Namespace, dsn: str) -> int:
    import psycopg
    from sqlalchemy.exc import SQLAlchemyError

    from quantlab.storage.contracts import BatchPolicy, StorageError
    from quantlab.storage.ingestion import IngestionCoordinator
    from quantlab.storage.repository import Repository
    from quantlab.storage.source_archive import SourceArchive

    try:
        if args.command == "db":
            from quantlab.storage.repository import migrate

            migrate(dsn)
            _print({"status": "schema_ready"})
            return 0
        with Repository(dsn) as repo:
            if args.command == "ingest":
                if args.operation == "import":
                    archive = SourceArchive(
                        Path(os.environ.get("QUANTLAB_ARCHIVE_DIR", "raw-archive"))
                    )
                    coordinator = IngestionCoordinator(
                        repo, archive, BatchPolicy(max_records=args.batch_size)
                    )
                    result = coordinator.import_csv(
                        args.dataset_path,
                        provider=args.provider,
                        provider_symbol=args.provider_symbol,
                        pip_size=args.pip_size,
                        source_id=args.source_id,
                    )
                    _print(result)
                    return 0 if result.status == "COMPLETE" else 1
                if args.operation == "status" and args.dataset_id is not None:
                    _print(repo.manifest(args.dataset_id))
                else:
                    manifests = repo.manifests(limit=args.limit)
                    _print(
                        [
                            m
                            for m in manifests
                            if args.operation == "status" or m.status == "COMPLETE"
                        ]
                    )
                return 0
            factory = repo.tick_factory(
                args.dataset_id, start_utc=args.start, end_utc=args.end, limit=args.limit
            )
            for tick in factory():
                _print({"schema_version": tick.schema_version, **asdict(tick)})
            return 0
    except (psycopg.Error, SQLAlchemyError) as exc:
        # Database exceptions can contain connection configuration; emit a bounded diagnostic.
        raise StorageError(
            f"database operation failed ({type(exc).__name__}); "
            "check database availability, migrations, and server logs"
        ) from None


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    try:
        dsn = os.environ.get("QUANTLAB_DATABASE_URL")
        if not dsn:
            raise ValueError("set QUANTLAB_DATABASE_URL to your PostgreSQL connection URL")
        return _execute(args, dsn)
    except ImportError:
        print(
            'QuantLab storage: install the storage extra: pip install -e ".[storage]"',
            file=sys.stderr,
        )
        return 2
    except (OSError, ValueError) as exc:
        print(f"QuantLab storage: {exc}", file=sys.stderr)
        return 2
