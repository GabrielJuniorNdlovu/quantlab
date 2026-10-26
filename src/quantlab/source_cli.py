"""Offline qualification commands, dispatched before database configuration."""

import argparse
from pathlib import Path

from quantlab.historical import HistoricalProfile
from quantlab.source_inspection import inspect_sample, write_report
from quantlab.source_registry import STATE_MEANINGS, SourceRegistry


def add_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--registry", type=Path, help="reviewed source_registry.v1 JSON override")
    operations = parser.add_subparsers(dest="operation", required=True)
    operations.add_parser("list", help="list reviewed sources and qualification states")
    describe = operations.add_parser(
        "describe", help="show source capabilities and unresolved mappings"
    )
    describe.add_argument("source_id")
    report = operations.add_parser("report", help="emit the documentation qualification report")
    report.add_argument("source_id")
    report.add_argument("--output", type=Path)
    inspect = operations.add_parser("inspect", help="inspect an authorized local recording")
    inspect.add_argument("sample_path", type=Path)
    inspect.add_argument("--source-id", required=True)
    inspect.add_argument("--format", choices=("csv", "jsonl", "json"))
    inspect.add_argument(
        "--config",
        "--profile",
        dest="config",
        type=Path,
        help="optional complete historical.v1 profile for F01 compatibility",
    )
    inspect.add_argument("--event-field")
    inspect.add_argument("--price-field")
    inspect.add_argument("--instrument-field")
    inspect.add_argument(
        "--timestamp", choices=("uninterpreted", "iso8601", "unix_seconds", "unix_milliseconds")
    )
    inspect.add_argument(
        "--sample-kind", choices=("unknown", "genuine", "synthetic"), default="unknown"
    )
    inspect.add_argument(
        "--authorization-reference",
        required=True,
        help="caller-declared permission or license reference; not independently verified",
    )
    inspect.add_argument("--original-reference")
    inspect.add_argument("--acquired-at", help="known acquisition time as explicitly zoned ISO8601")
    inspect.add_argument("--output", type=Path, help="exclusive report path; never overwritten")


def execute(args: argparse.Namespace) -> tuple[dict[str, object], int]:
    try:
        registry = SourceRegistry.load(args.registry)
        if args.operation == "list":
            return {
                "schema_version": "source_registry_listing.v1",
                "registry_sha256": registry.sha256,
                "states": STATE_MEANINGS,
                "sources": [
                    {
                        "source_id": s.source_id,
                        "name": s.metadata["name"],
                        "qualification": s.status.value,
                    }
                    for s in registry.sources
                ],
            }, 0
        source = registry.get(args.source_id)
        if args.operation in ("describe", "report"):
            result = source.report() if args.operation == "report" else source.describe()
            result["registry_sha256"] = registry.sha256
            if args.operation == "report" and args.output is not None:
                write_report(args.output, result)
            return result, 0
        profile = HistoricalProfile.load(args.config) if args.config else None
        format = args.format or (
            ("csv" if profile.format == "csv" else "jsonl") if profile else None
        )
        if format is None:
            raise ValueError("provide --format or a complete --config historical.v1 profile")
        fields = dict(profile.fields) if profile else {}
        for role, value in (
            ("event_time", args.event_field),
            ("price", args.price_field),
            ("instrument_id", args.instrument_field),
        ):
            if value is not None:
                if profile is not None and fields.get(role) != value:
                    raise ValueError("explicit inspection fields must match the supplied profile")
                fields[role] = value
        timestamp = args.timestamp or (profile.timestamp if profile else "uninterpreted")
        if profile is not None and timestamp != profile.timestamp:
            raise ValueError("timestamp declaration must match the supplied profile")
        result = inspect_sample(
            args.sample_path,
            source,
            format=format,
            fields=fields,
            profile=profile,
            authorization_reference=args.authorization_reference,
            sample_kind=args.sample_kind,
            timestamp=timestamp,
            original_reference=args.original_reference,
            acquired_at=args.acquired_at,
        )
        result["registry_sha256"] = registry.sha256
        if args.output is not None:
            write_report(args.output, result)
        compatibility = result["canonical_compatibility"]
        invalid = isinstance(compatibility, dict) and compatibility.get("status") in (
            "INVALID",
            "BLOCKED",
        )
        observation = result["observation"]
        invalid_values = isinstance(observation, dict) and (
            observation["price_validation_errors"] or observation["timestamp_validation_errors"]
        )
        return result, 1 if result["errors"] or invalid or invalid_values else 0
    except (OSError, ValueError) as exc:
        return {
            "schema_version": "source_error.v1",
            "error": {"code": "SOURCE_OPERATION_FAILED", "description": str(exc)[:512]},
        }, 2
