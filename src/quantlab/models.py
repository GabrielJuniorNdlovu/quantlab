"""Immutable market-data contracts; constructors enforce domain invariants."""

import hashlib
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import ClassVar
from uuid import UUID

from quantlab.metadata import MetadataValue, freeze_metadata
from quantlab.parsing import (
    INSTRUMENT_ID as INSTRUMENT_ID,
)
from quantlab.parsing import (
    parse_pip_size,
    parse_timestamp,
    require_decimal_price,
    require_instrument_id,
    require_position,
    require_price_precision,
    require_uuid,
    required_text,
)
from quantlab.quality import ContractError, DataQualityEvent, ErrorCode, Severity

INSTRUMENT = "Boom 1000 Index"
MAX_RAW_BYTES = 65536


class SourceType(str, Enum):
    HISTORICAL_CSV = "historical_csv"
    HISTORICAL_JSON = "historical_json"
    WEBSOCKET = "websocket"

    @property
    def expected_format(self) -> "SourceFormat":
        return SourceFormat.CSV if self is SourceType.HISTORICAL_CSV else SourceFormat.JSON


class SourceFormat(str, Enum):
    CSV = "csv"
    JSON = "json"


@dataclass(frozen=True, slots=True)
class AcquisitionContext:
    """Caller-reviewed provider mapping and precision; no inferred defaults."""

    capture_id: UUID
    provider: str
    source_type: SourceType
    provider_symbol: str
    pip_size: int
    received_at_utc: datetime | None = None
    source_metadata: Mapping[str, MetadataValue] = field(default_factory=dict)

    def __post_init__(self) -> None:
        require_uuid(self.capture_id)
        required_text(self.provider, "provider")
        required_text(self.provider_symbol, "provider_symbol")
        if not isinstance(self.source_type, SourceType):
            raise ContractError(
                ErrorCode.INVALID_SOURCE_METADATA, "source_type", "unsupported source_type"
            )
        if type(self.pip_size) is not int:
            raise ContractError(
                ErrorCode.INVALID_PRECISION, "pip_size", "configured pip_size must be an integer"
            )
        parse_pip_size(self.pip_size)
        if self.received_at_utc is not None:
            object.__setattr__(
                self, "received_at_utc", parse_timestamp(self.received_at_utc, "received_at_utc")
            )
        object.__setattr__(self, "source_metadata", freeze_metadata(self.source_metadata))


@dataclass(frozen=True, slots=True)
class RawObservation:
    """Exact acquired bytes in memory, not a claim of durable archival."""

    schema_version: ClassVar[str] = "raw_observation.v1"
    capture_id: UUID
    source_position: int
    provider: str
    source_format: SourceFormat
    raw_payload: bytes
    payload_sha256: str
    received_at_utc: datetime | None = None
    source_metadata: Mapping[str, MetadataValue] = field(default_factory=dict)

    def __post_init__(self) -> None:
        require_uuid(self.capture_id)
        require_position(self.source_position)
        required_text(self.provider, "provider")
        if not isinstance(self.source_format, SourceFormat):
            raise ContractError(
                ErrorCode.INVALID_SOURCE_METADATA, "source_format", "unsupported source_format"
            )
        if (
            not isinstance(self.raw_payload, bytes)
            or not 0 < len(self.raw_payload) <= MAX_RAW_BYTES
        ):
            raise ContractError(
                ErrorCode.INPUT_TOO_LARGE,
                "raw_payload",
                "raw_payload must contain 1 to 65536 bytes",
            )
        if self.payload_sha256 != hashlib.sha256(self.raw_payload).hexdigest():
            raise ContractError(
                ErrorCode.INVALID_CHECKSUM,
                "payload_sha256",
                "payload checksum does not match exact bytes",
            )
        if self.received_at_utc is not None:
            object.__setattr__(
                self, "received_at_utc", parse_timestamp(self.received_at_utc, "received_at_utc")
            )
        object.__setattr__(self, "source_metadata", freeze_metadata(self.source_metadata))


@dataclass(frozen=True, slots=True)
class TickCandidate:
    """Transport-neutral observed fields, still untrusted until validated."""

    raw: RawObservation
    instrument_id: str | None
    provider_symbol: str | None
    event_time: str | datetime | None
    price: str | Decimal | None
    pip_size: str | int | None
    provider_tick_id: str | None = None


@dataclass(frozen=True, slots=True)
class MarketTick:
    """Canonical market_tick.v1, with explicit provider quote decimal places."""

    schema_version: ClassVar[str] = "market_tick.v1"
    instrument_id: str
    provider_symbol: str
    event_time_utc: datetime
    price: Decimal
    pip_size: int
    source_type: SourceType
    capture_id: UUID
    source_position: int
    received_at_utc: datetime | None = None
    provider_tick_id: str | None = None

    def __post_init__(self) -> None:
        require_instrument_id(self.instrument_id)
        required_text(self.provider_symbol, "provider_symbol")
        require_uuid(self.capture_id)
        require_position(self.source_position)
        if not isinstance(self.source_type, SourceType):
            raise ContractError(
                ErrorCode.INVALID_SOURCE_METADATA, "source_type", "unsupported source_type"
            )
        require_decimal_price(self.price)
        if type(self.pip_size) is not int:
            raise ContractError(
                ErrorCode.INVALID_PRECISION, "pip_size", "canonical pip_size must be an integer"
            )
        require_price_precision(self.price, parse_pip_size(self.pip_size))
        if not isinstance(self.event_time_utc, datetime):
            raise ContractError(
                ErrorCode.INVALID_TIMESTAMP,
                "event_time_utc",
                "canonical event time must be datetime",
            )
        object.__setattr__(self, "event_time_utc", parse_timestamp(self.event_time_utc))
        if self.received_at_utc is not None:
            object.__setattr__(
                self, "received_at_utc", parse_timestamp(self.received_at_utc, "received_at_utc")
            )
        if self.provider_tick_id is not None:
            required_text(self.provider_tick_id, "provider_tick_id")

    def market_fields(
        self,
    ) -> tuple[str, str, datetime, tuple[int, tuple[int, ...], int | str], int]:
        """Return canonical market fields independent of transport provenance."""
        return (
            self.instrument_id,
            self.provider_symbol,
            self.event_time_utc,
            self.price.as_tuple(),
            self.pip_size,
        )

    def equivalent_market_fields(self, other: object) -> bool:
        """Compare canonical market fields (including exact Decimal scale) across sources."""
        return isinstance(other, MarketTick) and self.market_fields() == other.market_fields()


@dataclass(frozen=True, slots=True)
class ValidationOutcome:
    """One observation is either a tick with optional warnings or a rejection."""

    raw: RawObservation
    tick: MarketTick | None
    events: tuple[DataQualityEvent, ...] = ()

    def __post_init__(self) -> None:
        has_error = any(event.severity is not Severity.WARNING for event in self.events)
        if (self.tick is None) != has_error:
            raise ValueError("rejections require errors, accepted ticks permit warnings only")

    @property
    def accepted(self) -> bool:
        return self.tick is not None


@dataclass(frozen=True, slots=True)
class LegacyTick:
    """Limited-fidelity legacy CSV value; never a canonical MarketTick."""

    symbol: str
    timestamp_utc: datetime
    price: Decimal
    source: str
    source_row: int


Tick = LegacyTick
