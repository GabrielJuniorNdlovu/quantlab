"""Storage configuration and results; canonical market observations remain in models."""

import hashlib
import json
import math
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from uuid import UUID

from quantlab.models import AcquisitionContext, SourceType
from quantlab.parsing import required_text


class StorageError(ValueError):
    """An explicit storage invariant or operation failure."""


class BusyImport(StorageError):
    """Another connection owns this dataset import."""


class IncompleteDataset(StorageError):
    """Replay requires a fully validated completed dataset."""


@dataclass(frozen=True, slots=True)
class ArchivedSource:
    sha256: str
    byte_count: int
    path: Path

    def __post_init__(self) -> None:
        if len(self.sha256) != 64 or any(c not in "0123456789abcdef" for c in self.sha256):
            raise StorageError("source checksum must be lowercase SHA-256")
        if type(self.byte_count) is not int or self.byte_count < 0:
            raise StorageError("source size must be a nonnegative integer")


@dataclass(frozen=True, slots=True)
class ImportRun:
    dataset_id: UUID
    capture_id: UUID
    already_complete: bool = False


@dataclass(frozen=True, slots=True)
class DatasetManifest:
    dataset_id: UUID
    source_sha256: str
    status: str
    capture_id: UUID | None
    source_id: str
    observed: int
    accepted: int
    rejected: int
    error_count: int


@dataclass(frozen=True, slots=True)
class CommitStatus:
    """Durable database truth; acquisition replay/acknowledgement belongs to F04."""

    dataset_id: UUID
    capture_id: UUID
    status: str
    batch_number: int
    source_position: int
    observed: int
    accepted: int
    rejected: int
    error_count: int
    last_event_time_utc: datetime | None
    committed_at_utc: datetime | None
    digest: str | None


@dataclass(frozen=True, slots=True)
class BatchPolicy:
    max_records: int = 100
    max_wait_ms: int = 1000
    queue_capacity: int = 10000

    def __post_init__(self) -> None:
        for name, value, limit in (
            ("max_records", self.max_records, 10000),
            ("max_wait_ms", self.max_wait_ms, 60000),
            ("queue_capacity", self.queue_capacity, 10000),
        ):
            if type(value) is not int or not 1 <= value <= limit:
                raise StorageError(f"{name} must be an integer from 1 to {limit}")


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    max_attempts: int = 3
    backoff_seconds: float = 0.1

    def __post_init__(self) -> None:
        if type(self.max_attempts) is not int or not 1 <= self.max_attempts <= 10:
            raise StorageError("max_attempts must be an integer from 1 to 10")
        if (
            isinstance(self.backoff_seconds, bool)
            or not math.isfinite(self.backoff_seconds)
            or not 0 <= self.backoff_seconds <= 10
        ):
            raise StorageError("backoff_seconds must be finite from 0 to 10")


def json_value(value: object) -> object:
    if isinstance(value, Mapping):
        return {str(key): json_value(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [json_value(item) for item in value]
    return value


def configuration(context: AcquisitionContext, source_id: str) -> dict[str, object]:
    required_text(source_id, "source_id")
    profile = context.source_metadata.get("historical_adapter")
    fields = profile.get("fields") if isinstance(profile, Mapping) else None
    corrected_reception = (
        context.source_type is SourceType.HISTORICAL_CSV
        and isinstance(profile, Mapping)
        and profile.get("version") == "historical.v1"
        and profile.get("format") == "csv"
        and isinstance(fields, Mapping)
        and "received_at_utc" in fields
    )
    return {
        "provider": context.provider,
        "provider_symbol": context.provider_symbol,
        "pip_size": context.pip_size,
        "source_type": context.source_type.value,
        "received_at_utc": context.received_at_utc.isoformat() if context.received_at_utc else None,
        "source_metadata": json_value(context.source_metadata),
        "source_id": source_id,
        "market_tick_schema": "market_tick.v1",
        "raw_observation_schema": "raw_observation.v1",
        "validation_version": "canonical.v2.csv_reception"
        if corrected_reception
        else "canonical.v1",
    }


def configuration_hash(value: dict[str, object]) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    ).hexdigest()


def historical_profile(config: dict[str, object]) -> dict[str, object] | None:
    """Recognize the versioned adapter contract, leaving legacy provider notes alone."""
    metadata = config.get("source_metadata")
    value = metadata.get("historical_adapter") if isinstance(metadata, dict) else None
    version = value.get("version") if isinstance(value, dict) else None
    if isinstance(value, dict) and isinstance(version, str) and version.startswith("historical."):
        return value
    return None
