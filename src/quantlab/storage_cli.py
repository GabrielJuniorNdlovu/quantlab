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
    db.add_parser("status", help="read database availability and storage usage")
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
    status.add_argument(
        "--attempts", action="store_true", help="show committed acquisition progress"
    )
    datasets = ingest.add_parser("datasets")
    datasets.add_argument("--limit", type=int, default=100)
    replay = commands.add_parser(
        "replay", help="stream one completed dataset in chronological order"
    )
    replay.add_argument("dataset_id", type=UUID)
    replay.add_argument("--start", type=parse_timestamp)
    replay.add_argument("--end", type=parse_timestamp)
    replay.add_argument("--limit", type=int, default=100)
    data = commands.add_parser("data").add_subparsers(dest="operation", required=True)
    inspect = data.add_parser("inspect")
    inspect.add_argument("dataset_path", type=Path)
    inspect.add_argument("--config", type=Path, required=True)
    for operation in ("import", "stream-simulate"):
        command = data.add_parser(operation)
        command.add_argument("dataset_path", type=Path)
        command.add_argument("--config", type=Path, required=True)
        command.add_argument("--source-id", default="local-historical")
        command.add_argument("--batch-size", type=int, default=100)
        command.add_argument("--max-wait-ms", type=int, default=1000)
        command.add_argument("--queue-size", type=int, default=10000)
        command.add_argument("--max-attempts", type=int, default=3)
        command.add_argument("--no-monitoring", action="store_true")
        command.add_argument("--no-resume", action="store_true")
        command.add_argument("--no-recovery", action="store_true")
        if operation == "stream-simulate":
            command.add_argument("--rate", type=float, default=0)
            command.add_argument("--interrupt-after", type=int)
            command.add_argument("--disconnect-after", type=int)
            command.add_argument("--redeliver-every", type=int)
            command.add_argument("--drop-after", type=int)
    listing = data.add_parser("list", aliases=["datasets"])
    listing.add_argument("--limit", type=int, default=100)
    health = data.add_parser("status")
    health.add_argument("--limit", type=int, default=100)
    quality = data.add_parser("quality")
    quality.add_argument("dataset_id", type=UUID)
    quality.add_argument("--gap-threshold-seconds", type=float)
    export = data.add_parser("export")
    export.add_argument("dataset_id", type=UUID)
    export.add_argument("--format", choices=["parquet"], required=True)
    export.add_argument("--output", type=Path, required=True)
    export.add_argument("--instrument", default="BOOM_1000")
    export.add_argument("--train-end", type=parse_timestamp, required=True)
    export.add_argument("--validation-end", type=parse_timestamp, required=True)
    export.add_argument("--start", type=parse_timestamp)
    export.add_argument("--end", type=parse_timestamp)
    export.add_argument("--gap-threshold-seconds", type=float)
    validation = data.add_parser("validate")
    validation.add_argument("dataset_id", type=UUID)
    historical_replay = data.add_parser("replay")
    historical_replay.add_argument("dataset_id", type=UUID)
    historical_replay.add_argument("--start", type=parse_timestamp)
    historical_replay.add_argument("--end", type=parse_timestamp)
    historical_replay.add_argument("--limit", type=int, default=100)
    for replay_parser in (replay, historical_replay):
        replay_parser.add_argument("--show-chart", action="store_true")
        replay_parser.add_argument(
            "--headless", action="store_true", help="verify chart geometry without opening a window"
        )
        replay_parser.add_argument("--audit-dir", type=Path, default=Path("replay-runs"))
    return parser


def _execute(args: argparse.Namespace, dsn: str) -> int:
    import psycopg
    from sqlalchemy.exc import SQLAlchemyError

    from quantlab.storage.contracts import BatchPolicy, StorageError
    from quantlab.storage.ingestion import IngestionCoordinator
    from quantlab.storage.repository import Repository
    from quantlab.storage.source_archive import SourceArchive

    try:
        if args.command == "db" and args.operation == "init":
            from quantlab.storage.repository import migrate

            migrate(dsn)
            _print({"status": "schema_ready"})
            return 0
        with Repository(dsn) as repo:
            if args.command == "db":
                _print({"database": "connected", "storage": repo.storage_usage()})
                return 0
            if args.command == "data":
                archive = SourceArchive(Path(os.environ.get("QUANTLAB_ARCHIVE_DIR", "raw-archive")))
                if args.operation in ("import", "stream-simulate", "status", "quality", "export"):
                    from quantlab.collection_cli import execute_collection

                    output, code = execute_collection(args, repo, archive)
                    _print(output)
                    return code
                if args.operation in ("list", "datasets"):
                    _print(repo.manifests(limit=args.limit))
                    return 0
                if args.operation == "validate":
                    _print(repo.validate_dataset(args.dataset_id, archive))
                    return 0
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
                if args.operation == "status" and args.attempts and args.dataset_id is None:
                    raise ValueError("--attempts requires a dataset ID")
                if args.operation == "status" and args.dataset_id is not None:
                    _print(
                        repo.acquisitions(args.dataset_id, limit=args.limit)
                        if args.attempts
                        else repo.manifest(args.dataset_id)
                    )
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
            if args.headless and not args.show_chart:
                raise ValueError("--headless requires --show-chart")
            if args.show_chart:
                from quantlab.replay_app import run_gui, run_headless
                from quantlab.replay_session import ReplaySession

                manifest = repo.manifest(args.dataset_id)
                session = ReplaySession(
                    factory,
                    (),
                    manifest.source_sha256,
                    args.audit_dir,
                    source_dataset_id=str(args.dataset_id),
                )
                try:
                    if args.headless:
                        _print(run_headless(session))
                    else:
                        run_gui(session)
                finally:
                    session.close()
                return 0
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
        if args.command == "data" and args.operation == "inspect":
            from quantlab.historical import HistoricalAdapter, HistoricalProfile

            report = HistoricalAdapter(HistoricalProfile.load(args.config)).inspect(
                args.dataset_path
            )
            _print(report)
            return 0 if report["valid"] else 1
        dsn = os.environ.get("QUANTLAB_DATABASE_URL")
        if not dsn:
            raise ValueError("set QUANTLAB_DATABASE_URL to your PostgreSQL connection URL")
        return _execute(args, dsn)
    except KeyboardInterrupt:
        print("QuantLab: interrupted; repeat the same source command to recover", file=sys.stderr)
        return 130
    except ImportError:
        print(
            'QuantLab: install the required extras: pip install -e ".[storage,export]"',
            file=sys.stderr,
        )
        return 2
    except (OSError, ValueError) as exc:
        print(f"QuantLab storage: {exc}", file=sys.stderr)
        return 2
