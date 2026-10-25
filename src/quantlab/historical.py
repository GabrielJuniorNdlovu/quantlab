"""Explicit historical source profiles and one incremental adapter facade."""

from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO
from uuid import UUID, uuid4

from quantlab.csv_adapter import iter_csv_records
from quantlab.engine import normalize_mapped_record, validate_acquisition
from quantlab.historical_json import iter_json_records, strict_json
from quantlab.metadata import MetadataValue, freeze_metadata
from quantlab.models import AcquisitionContext, SourceType, TickCandidate
from quantlab.parsing import parse_pip_size, require_instrument_id, required_text

_VERSION = "historical.v1"
_REQUIRED = {"instrument_id", "event_time", "price"}
_OPTIONAL = {"provider_symbol", "pip_size", "provider_tick_id", "received_at_utc"}
_FORMATS = {
    "csv": SourceType.HISTORICAL_CSV,
    "jsonl": SourceType.HISTORICAL_JSON,
    "stream_jsonl": SourceType.WEBSOCKET,
}


@dataclass(frozen=True, slots=True)
class HistoricalProfile:
    """All source interpretation is immutable and included in dataset identity."""

    format: str
    provider: str
    provider_symbol: str
    pip_size: int
    fields: Mapping[str, str]
    instruments: Mapping[str, str]
    timestamp: str = "iso8601"

    def __post_init__(self) -> None:
        if self.format not in _FORMATS or self.timestamp != "iso8601":
            raise ValueError("supported format: csv, jsonl or stream_jsonl; timestamp: iso8601")
        required_text(self.provider, "provider")
        required_text(self.provider_symbol, "provider_symbol")
        parse_pip_size(self.pip_size)
        if type(self.pip_size) is not int:
            raise ValueError("profile pip_size must be an integer")
        if not _REQUIRED <= self.fields.keys() or not self.fields.keys() <= _REQUIRED | _OPTIONAL:
            raise ValueError(
                "fields require instrument_id, event_time, price; unknown fields rejected"
            )
        if len(set(self.fields.values())) != len(self.fields):
            raise ValueError("field bindings must be unique")
        for name in self.fields.values():
            required_text(name, "field binding")
        if not self.instruments or len(self.instruments) > 32:
            raise ValueError("supply 1 to 32 explicit instrument mappings")
        for external, internal in self.instruments.items():
            required_text(external, "external instrument")
            require_instrument_id(internal, "internal instrument")
        # Use the existing bounded immutable metadata contract for serialized profiles.
        from types import MappingProxyType

        object.__setattr__(self, "fields", MappingProxyType(dict(self.fields)))
        object.__setattr__(self, "instruments", MappingProxyType(dict(self.instruments)))
        freeze_metadata(self.metadata())

    @classmethod
    def from_mapping(cls, value: object) -> "HistoricalProfile":
        keys = {
            "version",
            "format",
            "provider",
            "provider_symbol",
            "pip_size",
            "timestamp",
            "fields",
            "instruments",
        }
        if not isinstance(value, dict) or set(value) != keys or value["version"] != _VERSION:
            raise ValueError("profile requires exactly the documented historical.v1 keys")
        for key in ("format", "provider", "provider_symbol", "timestamp"):
            if not isinstance(value[key], str):
                raise ValueError(f"profile {key} must be text")
        for key in ("fields", "instruments"):
            if not isinstance(value[key], dict) or not all(
                isinstance(k, str) and isinstance(v, str) for k, v in value[key].items()
            ):
                raise ValueError(f"profile {key} must map text to text")
        return cls(
            format=value["format"],
            provider=value["provider"],
            provider_symbol=value["provider_symbol"],
            pip_size=value["pip_size"],
            fields=value["fields"],
            instruments=value["instruments"],
            timestamp=value["timestamp"],
        )

    @classmethod
    def load(cls, path: Path) -> "HistoricalProfile":
        with path.open("rb") as stream:
            payload = stream.read(16385)
        if len(payload) > 16384:
            raise ValueError("profile exceeds 16384 bytes")
        return cls.from_mapping(strict_json(payload))

    def metadata(self) -> dict[str, MetadataValue]:
        return {
            "version": _VERSION,
            "format": self.format,
            "provider": self.provider,
            "provider_symbol": self.provider_symbol,
            "pip_size": self.pip_size,
            "timestamp": self.timestamp,
            "fields": dict(self.fields),
            "instruments": dict(self.instruments),
        }


class HistoricalAdapter:
    """Reads original evidence; F01 owns domain validation, F03 owns persistence."""

    def __init__(self, profile: HistoricalProfile) -> None:
        self.profile = profile

    def context(self, capture_id: UUID | None = None) -> AcquisitionContext:
        p = self.profile
        return AcquisitionContext(
            capture_id or uuid4(),
            p.provider,
            _FORMATS[p.format],
            p.provider_symbol,
            p.pip_size,
            source_metadata={"historical_adapter": p.metadata()},
        )

    def candidates(self, stream: BinaryIO, context: AcquisitionContext) -> Iterator[TickCandidate]:
        if context != self.context(context.capture_id):
            raise ValueError("historical adapter requires its own matching acquisition context")
        p = self.profile
        optional_keys = {"provider_tick_id", "received_at_utc"}
        records = (
            iter_csv_records(stream, context, columns=tuple(p.fields.values()))
            if p.format == "csv"
            else iter_json_records(
                stream,
                context,
                tuple(v for k, v in p.fields.items() if k not in optional_keys),
                optional_columns=tuple(v for k, v in p.fields.items() if k in optional_keys),
            )
        )
        has_provider_symbol = "provider_symbol" in p.fields
        has_pip_size = "pip_size" in p.fields
        for raw, fields in records:
            mapped = {name: fields.get(column) for name, column in p.fields.items()}
            yield normalize_mapped_record(
                raw,
                context,
                mapped,
                p.instruments,
                has_provider_symbol=has_provider_symbol,
                has_pip_size=has_pip_size,
            )

    def inspect(self, path: Path) -> dict[str, object]:
        """Scan the whole regular file with bounded error samples and a stable checksum."""
        import hashlib
        import os
        import stat

        digest = hashlib.sha256()
        observed = accepted = rejected = 0
        errors: list[dict[str, object]] = []
        context = self.context()
        from quantlab.quality import QualityError

        flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK
        with os.fdopen(os.open(path, flags), "rb") as stream:
            before = os.fstat(stream.fileno())
            if not stat.S_ISREG(before.st_mode):
                raise ValueError("source must be a regular file")
            while chunk := stream.read(1024 * 1024):
                digest.update(chunk)
            stream.seek(0)
            try:
                for outcome in validate_acquisition(self.candidates(stream, context), context):
                    observed += 1
                    accepted += outcome.accepted
                    rejected += not outcome.accepted
                    for event in outcome.events:
                        if len(errors) < 20:
                            errors.append(
                                {
                                    "code": event.error_code.value,
                                    "position": event.source_position,
                                    "description": event.description,
                                }
                            )
            except QualityError as exc:
                errors.append(
                    {
                        "code": exc.event.error_code.value,
                        "position": exc.row,
                        "description": exc.event.description,
                    }
                )
            after = os.fstat(stream.fileno())
            if (before.st_size, before.st_mtime_ns, before.st_ctime_ns) != (
                after.st_size,
                after.st_mtime_ns,
                after.st_ctime_ns,
            ):
                raise ValueError("source changed during inspection")
        return {
            "source_sha256": digest.hexdigest(),
            "source_bytes": before.st_size,
            "profile": self.profile.metadata(),
            "observed": observed,
            "accepted": accepted,
            "rejected": rejected,
            "errors": errors,
            "valid": observed > 0 and rejected == 0 and not errors,
        }
