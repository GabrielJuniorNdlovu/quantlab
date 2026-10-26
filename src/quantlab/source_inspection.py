"""Local schema observation and optional F01/F04 compatibility, without acquisition."""

import csv
import hashlib
import os
import re
import stat
import tempfile
from collections import Counter
from collections.abc import Iterator, Mapping
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import BinaryIO

from quantlab.csv_syntax import CSVRecordLines, is_csv_record
from quantlab.historical import HistoricalAdapter, HistoricalProfile
from quantlab.historical_json import strict_json
from quantlab.models import MAX_RAW_BYTES
from quantlab.parsing import parse_price, parse_timestamp, required_text
from quantlab.quality import ContractError
from quantlab.source_registry import SourceDefinition, encode_report

MAX_JSON_DOCUMENT = 16 * 1024 * 1024
MAX_FIELDS = 128
INSPECTION_ROLES = frozenset(
    {
        "event_time",
        "price",
        "instrument_id",
        "provider_symbol",
        "pip_size",
        "received_at_utc",
        "provider_tick_id",
        "price_basis",
    }
)


def _rows(stream: BinaryIO, format: str) -> Iterator[dict[str, object]]:
    if format == "csv":
        lines = CSVRecordLines(stream, MAX_RAW_BYTES)
        reader = csv.reader(lines, strict=True)
        header = next(reader, None)
        if not header or len(header) != len(set(header)):
            raise ValueError("CSV requires a nonempty unique header")
        if len(header) > MAX_FIELDS or not is_csv_record(lines.record.decode("utf-8")):
            raise ValueError("CSV header exceeds 128 fields or has invalid quote placement")
        while True:
            lines.record.clear()
            values = next(reader, None)
            if values is None:
                break
            if len(values) != len(header) or not is_csv_record(lines.record.decode("utf-8")):
                raise ValueError("CSV row does not match header or quote grammar")
            yield dict(zip(header, values, strict=True))
    elif format == "jsonl":
        while payload := stream.readline(MAX_RAW_BYTES + 1):
            if len(payload) > MAX_RAW_BYTES:
                raise ValueError("JSON line exceeds 65536 bytes")
            value = strict_json(payload)
            if not isinstance(value, dict):
                raise ValueError("JSON Lines record must be an object")
            yield value
    else:
        payload = stream.read(MAX_JSON_DOCUMENT + 1)
        if len(payload) > MAX_JSON_DOCUMENT:
            raise ValueError("JSON document exceeds 16 MiB; use an authorized JSON Lines export")
        value = strict_json(payload)
        if isinstance(value, dict):
            yield value
        elif isinstance(value, list):
            for row in value:
                if not isinstance(row, dict):
                    raise ValueError("JSON array elements must be objects")
                yield row
        else:
            raise ValueError("JSON document must contain an object or an array of objects")


def _kind(value: object) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, str):
        return "text"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, Decimal):
        return "decimal_number"
    return "object" if isinstance(value, dict) else "array"


def _fields(value: object, path: str = "", depth: int = 0) -> Iterator[tuple[str, object]]:
    if depth > 8:
        raise ValueError("sample schema exceeds eight nested levels")
    if path:
        yield path, value
    if isinstance(value, dict):
        for key, child in value.items():
            if len(key) > 256:
                raise ValueError("sample field name exceeds 256 characters")
            yield from _fields(child, f"{path}.{key}" if path else key, depth + 1)
    elif isinstance(value, list):
        for child in value:
            yield from _fields(child, path + "[]", depth + 1)


def _values(value: object, path: str) -> Iterator[object]:
    part, separator, rest = path.partition(".")
    array = part.endswith("[]")
    key = part[:-2] if array else part
    if not isinstance(value, Mapping) or key not in value:
        return
    selected = value[key]
    if array and not isinstance(selected, list):
        return
    children = selected if array and isinstance(selected, list) else [selected]
    for child in children:
        if separator:
            yield from _values(child, rest)
        else:
            yield child


class _Summary:
    def __init__(self, fields: Mapping[str, str], timestamp: str, format: str) -> None:
        self.fields, self.timestamp, self.format = fields, timestamp, format
        self.schema: dict[str, Counter[str]] = {}
        self.types: dict[str, Counter[str]] = {role: Counter() for role in fields}
        self.representations: dict[str, list[dict[str, object]]] = {role: [] for role in fields}
        self.iso_fractional_digits: Counter[int] = Counter()
        self.iso_zone_markers: Counter[str] = Counter()
        self.price_exponents: Counter[int] = Counter()
        self.price_errors: Counter[str] = Counter()
        self.time_errors: Counter[str] = Counter()
        self.instruments: list[str] = []
        self.schema_truncated = self.instruments_truncated = False
        self.records = self.time_count = self.decreases = self.equal_times = 0
        self.first_time: datetime | None = None
        self.last_time: datetime | None = None
        self.previous_time: datetime | None = None

    def observe(self, row: dict[str, object]) -> None:
        self.records += 1
        for path, value in _fields(row):
            if path not in self.schema and len(self.schema) >= MAX_FIELDS:
                self.schema_truncated = True
                continue
            self.schema.setdefault(path, Counter())[_kind(value)] += 1
        for role, path in self.fields.items():
            values = [row[path]] if self.format == "csv" and path in row else _values(row, path)
            for value in values:
                self.types[role][_kind(value)] += 1
                text = str(value) if not isinstance(value, (dict, list)) else ""
                representation: dict[str, object] = {
                    "type": _kind(value),
                    "decoded_text": text[:128] if role != "provider_tick_id" else None,
                    "text_truncated": len(text) > 128,
                }
                examples = self.representations[role]
                if len(examples) < 3 and representation not in examples:
                    examples.append(representation)
                if role == "price":
                    try:
                        candidate = Decimal(value) if type(value) is int else value
                        price = parse_price(
                            candidate if isinstance(candidate, (str, Decimal)) else None
                        )
                        self.price_exponents[int(price.as_tuple().exponent)] += 1
                    except ContractError as exc:
                        self.price_errors[exc.code.value] += 1
                elif role == "event_time" and self.timestamp == "iso8601":
                    try:
                        time = parse_timestamp(value if isinstance(value, str) else "INVALID")
                        self.first_time = self.first_time or time
                        self.last_time = time
                        self.time_count += 1
                        fractional = re.search(r"\.(\d+)(?:Z|[+-]\d{2}:\d{2})$", str(value))
                        self.iso_fractional_digits[len(fractional[1]) if fractional else 0] += 1
                        self.iso_zone_markers[
                            "Z" if str(value).endswith("Z") else "numeric_offset"
                        ] += 1
                        if self.previous_time is not None:
                            self.decreases += time < self.previous_time
                            self.equal_times += time == self.previous_time
                        self.previous_time = time
                    except ContractError as exc:
                        self.time_errors[exc.code.value] += 1
                elif role == "instrument_id" and isinstance(value, str):
                    if value not in self.instruments:
                        if len(self.instruments) < 8 and len(value) <= 256:
                            self.instruments.append(value)
                        else:
                            self.instruments_truncated = True

    def report(self) -> dict[str, object]:
        return {
            "record_count": self.records,
            "record_count_semantics": (
                "CSV data rows, JSON Lines objects, or JSON top-level objects/array elements; "
                "not inferred market ticks"
            ),
            "schema": {k: dict(v) for k, v in sorted(self.schema.items())},
            "schema_truncated": self.schema_truncated,
            "field_bindings": dict(self.fields),
            "selected_field_types": {k: dict(v) for k, v in self.types.items()},
            "observed_decoded_representations": self.representations,
            "iso_fractional_digits": {str(k): v for k, v in self.iso_fractional_digits.items()},
            "iso_zone_markers": dict(self.iso_zone_markers),
            "observed_price_exponents": {
                str(k): v for k, v in sorted(self.price_exponents.items())
            },
            "price_validation_errors": dict(self.price_errors),
            "precision_is_provider_guarantee": False,
            "instrument_values": self.instruments,
            "instrument_values_truncated": self.instruments_truncated,
            "timestamp_interpretation": self.timestamp,
            "timestamp_validation_errors": dict(self.time_errors),
            "valid_iso_timestamps": self.time_count,
            "first_event_time_utc": self.first_time.isoformat() if self.first_time else None,
            "last_event_time_utc": self.last_time.isoformat() if self.last_time else None,
            "ordering": {
                "source_order": "retained as recorded; no sorting or deduplication",
                "event_order_evaluated": self.time_count > 0,
                "decreases_between_valid_iso_times": self.decreases,
                "equal_adjacent_valid_iso_times": self.equal_times,
                "numeric_epochs_converted": False,
            },
        }


def inspect_sample(
    path: Path,
    source: SourceDefinition,
    *,
    format: str,
    fields: Mapping[str, str],
    authorization_reference: str,
    sample_kind: str = "unknown",
    timestamp: str = "uninterpreted",
    original_reference: str | None = None,
    acquired_at: str | None = None,
    profile: HistoricalProfile | None = None,
) -> dict[str, object]:
    """Observe original local bytes. Caller declarations never establish genuine provenance."""
    if format not in ("csv", "jsonl", "json"):
        raise ValueError("format requires csv, jsonl or json")
    if sample_kind not in ("unknown", "genuine", "synthetic"):
        raise ValueError("sample_kind requires unknown, genuine or synthetic")
    if timestamp not in ("uninterpreted", "iso8601", "unix_seconds", "unix_milliseconds"):
        raise ValueError("unsupported declared timestamp interpretation")
    if not fields.keys() <= INSPECTION_ROLES:
        raise ValueError("unsupported inspection field role")
    fields = {key: required_text(value, "inspection field") for key, value in fields.items()}
    if any(len(value.split(".")) > 8 for value in fields.values()):
        raise ValueError("inspection field path exceeds eight levels")
    authorization = required_text(authorization_reference, "authorization_reference", limit=2048)
    reference = (
        None
        if original_reference is None
        else required_text(original_reference, "original_reference", limit=2048)
    )
    acquisition = (
        None if acquired_at is None else parse_timestamp(acquired_at, "acquired_at_utc").isoformat()
    )
    summary = _Summary(fields, timestamp, format)
    errors: list[dict[str, object]] = []
    digest = hashlib.sha256()
    compatibility: dict[str, object] = {
        "status": "NOT_ATTEMPTED",
        "reason": "No complete historical.v1 profile supplied",
    }
    flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK
    with os.fdopen(os.open(path, flags), "rb") as stream:
        before = os.fstat(stream.fileno())
        if not stat.S_ISREG(before.st_mode):
            raise ValueError("sample must be a regular file, not a symlink or device")
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
        stream.seek(0)
        complete = False
        try:
            for row in _rows(stream, format):
                summary.observe(row)
            complete = True
            if summary.records == 0:
                errors.append({"code": "EMPTY_SAMPLE", "description": "No records found"})
        except (ValueError, UnicodeError, csv.Error, OverflowError) as exc:
            errors.append({"code": "SOURCE_STRUCTURE_ERROR", "description": str(exc)[:512]})
        if profile is not None:
            expected = "csv" if profile.format == "csv" else "jsonl"
            resolved = {m.external_id: m for m in source.instruments if m.external_id is not None}
            forbidden = source.metadata["non_event_id_fields"]
            if not isinstance(forbidden, tuple):
                raise ValueError("registry non_event_id_fields must be an immutable sequence")
            if format != expected:
                compatibility = {
                    "status": "BLOCKED",
                    "reason": "Profile format does not match sample framing",
                }
            elif profile.fields.get("provider_tick_id") in forbidden:
                compatibility = {
                    "status": "BLOCKED",
                    "reason": "Configured provider_tick_id is documented as a non-event identifier",
                }
            elif resolved and (
                profile.provider_symbol not in resolved
                or resolved[profile.provider_symbol].pip_size not in (None, profile.pip_size)
                or any(
                    external not in resolved or resolved[external].internal_id != internal
                    for external, internal in profile.instruments.items()
                )
            ):
                compatibility = {
                    "status": "BLOCKED",
                    "reason": "Profile contradicts reviewed instrument mapping or precision",
                }
            else:
                compatibility = HistoricalAdapter(profile).inspect(path)
                if (
                    compatibility["source_sha256"] != digest.hexdigest()
                    or compatibility["source_bytes"] != before.st_size
                ):
                    raise ValueError("sample changed between structural and canonical inspection")
                compatibility["status"] = "VALID" if compatibility["valid"] else "INVALID"
                compatibility["mapping_authenticity_verified"] = False
        after = os.fstat(stream.fileno())
        if (before.st_size, before.st_mtime_ns, before.st_ctime_ns) != (
            after.st_size,
            after.st_mtime_ns,
            after.st_ctime_ns,
        ):
            raise ValueError("sample changed during inspection")
    return {
        "schema_version": "source_inspection.v1",
        "source_id": source.source_id,
        "source_qualification": source.status.value,
        "source_definition_sha256": hashlib.sha256(encode_report(source.describe())).hexdigest(),
        "source_file_name": path.name,
        "source_sha256": digest.hexdigest(),
        "source_bytes": before.st_size,
        "format": format,
        "inspection_complete": complete,
        "inspected_at_utc": datetime.now(UTC).isoformat(),
        "provenance": {
            "sample_kind_declared": sample_kind,
            "genuine_origin_independently_verified": False,
            "authorization_reference_declared": authorization,
            "authorization_independently_verified": False,
            "original_source_reference": reference,
            "acquired_at_utc_declared": acquisition,
            "file_mtime_is_not_acquisition_time": True,
        },
        "observation": summary.report(),
        "canonical_compatibility": compatibility,
        "errors": errors,
        "documentation": source.metadata["documentation"],
        "qualification_changed": False,
        "limitations": [
            "Origin and permission references are caller declarations. They are unverified.",
            "Missing field bindings leave the corresponding semantics unassessed.",
            "Epoch units can be declared but numeric epochs are not converted or validated as UTC.",
            "Decoded examples are bounded. Original byte spelling remains in the source.",
            "Timestamp bounds cover individually valid selected ISO values. "
            "Canonical accepted bounds are separate.",
        ],
    }


def write_report(path: Path, report: Mapping[str, object]) -> None:
    """Publish one complete local report exclusively; never overwrite a report or sample."""
    temporary: str | None = None
    try:
        with tempfile.NamedTemporaryFile(
            dir=path.parent, prefix=".source-report-", delete=False
        ) as stream:
            temporary = stream.name
            stream.write(encode_report(report))
            stream.flush()
            os.fsync(stream.fileno())
        os.link(temporary, path, follow_symlinks=False)
        directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if temporary is not None:
            Path(temporary).unlink(missing_ok=True)
