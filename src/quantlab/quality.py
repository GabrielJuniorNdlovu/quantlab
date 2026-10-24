"""Deterministic, bounded diagnostics without raw payload disclosure."""

from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from uuid import UUID, uuid5


class ErrorCode(str, Enum):
    INVALID_PRICE = "INVALID_PRICE"
    INVALID_TIMESTAMP = "INVALID_TIMESTAMP"
    MISSING_REQUIRED_FIELD = "MISSING_REQUIRED_FIELD"
    UNSUPPORTED_INSTRUMENT = "UNSUPPORTED_INSTRUMENT"
    OUT_OF_ORDER_EVENT = "OUT_OF_ORDER_EVENT"
    INVALID_PRECISION = "INVALID_PRECISION"
    INVALID_SOURCE_POSITION = "INVALID_SOURCE_POSITION"
    INVALID_SOURCE_METADATA = "INVALID_SOURCE_METADATA"
    INVALID_CHECKSUM = "INVALID_CHECKSUM"
    INVALID_CSV = "INVALID_CSV"
    INVALID_CSV_SCHEMA = "INVALID_CSV_SCHEMA"
    EMPTY_DATASET = "EMPTY_DATASET"
    INPUT_TOO_LARGE = "INPUT_TOO_LARGE"
    RECEPTION_BEFORE_EVENT = "RECEPTION_BEFORE_EVENT"
    LEGACY_LIMITED_FIDELITY = "LEGACY_LIMITED_FIDELITY"


class Severity(str, Enum):
    WARNING = "warning"
    ERROR = "error"
    FATAL = "fatal"


class ContractError(ValueError):
    """A domain invariant violation, before acquisition context is available."""

    def __init__(self, code: ErrorCode, field_name: str, description: str) -> None:
        self.code = code
        self.field_name = field_name
        self.description = description
        super().__init__(description)


@dataclass(frozen=True, slots=True)
class DataQualityEvent:
    """A reproducible error or warning; detection time is optional and supplied."""

    event_id: UUID
    capture_id: UUID
    source_position: int | None
    error_code: ErrorCode
    severity: Severity
    description: str
    field_name: str | None = None
    observed_value: str | None = None
    detected_at_utc: datetime | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.event_id, UUID) or not isinstance(self.capture_id, UUID):
            raise ValueError("quality event identifiers must be UUIDs")
        if self.source_position is not None and (
            type(self.source_position) is not int or self.source_position < 1
        ):
            raise ValueError("quality event position must be positive or absent")
        if not isinstance(self.error_code, ErrorCode) or not isinstance(self.severity, Severity):
            raise ValueError("quality event code and severity must be enums")
        for value, limit in (
            (self.description, 512),
            (self.field_name, 64),
            (self.observed_value, 128),
        ):
            if value is not None and (not isinstance(value, str) or len(value) > limit):
                raise ValueError("quality event text exceeds its limit")
        if self.detected_at_utc is not None:
            if self.detected_at_utc.tzinfo is None or self.detected_at_utc.utcoffset() is None:
                raise ValueError("detection time requires an explicit timezone")
            object.__setattr__(
                self, "detected_at_utc", self.detected_at_utc.astimezone(timezone.utc)
            )


def quality_event(
    capture_id: UUID,
    source_position: int | None,
    code: ErrorCode,
    description: str,
    *,
    field_name: str | None = None,
    severity: Severity = Severity.ERROR,
    detected_at_utc: datetime | None = None,
) -> DataQualityEvent:
    """Derive an event ID from stable context; never sample a wall clock."""
    identifier = uuid5(capture_id, f"{source_position}:{code.value}:{field_name}:{description}")
    return DataQualityEvent(
        identifier,
        capture_id,
        source_position,
        code,
        severity,
        description,
        field_name=field_name,
        detected_at_utc=detected_at_utc,
    )


class QualityError(ValueError):
    """A fatal transport/acquisition failure, carrying its structured event."""

    def __init__(self, event: DataQualityEvent) -> None:
        self.event = event
        self.row = event.source_position
        location = f"Row {self.row}" if self.row is not None else "Acquisition"
        super().__init__(f"{location}: {event.error_code.value}: {event.description}")
