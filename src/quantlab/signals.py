"""Versioned, immutable research arrows; no forecasting or outcome evaluation."""

import json
from collections.abc import Mapping
from dataclasses import dataclass, field, fields
from datetime import datetime
from decimal import Decimal
from typing import ClassVar
from uuid import UUID

from quantlab.metadata import MetadataValue, freeze_metadata
from quantlab.models import MarketTick
from quantlab.parsing import (
    parse_price,
    parse_timestamp,
    require_decimal_price,
    require_instrument_id,
    require_position,
    require_uuid,
    required_text,
)


@dataclass(frozen=True, slots=True)
class Observation:
    """Only the current released input; availability includes previous arrival delays."""

    tick: MarketTick
    available_at_utc: datetime
    source_dataset_id: str

    def __post_init__(self) -> None:
        available = parse_timestamp(self.available_at_utc, "available_at_utc")
        if available < max(
            self.tick.event_time_utc, self.tick.received_at_utc or self.tick.event_time_utc
        ):
            raise ValueError("observation availability precedes source event or reception")
        object.__setattr__(self, "available_at_utc", available)
        required_text(self.source_dataset_id, "source_dataset_id")


@dataclass(frozen=True, slots=True)
class ResearchSignal:
    schema_version: ClassVar[str] = "research_signal.v1"
    signal_id: UUID
    instrument_id: str
    signal_type: str
    issued_at_utc: datetime
    input_cutoff_utc: datetime
    input_cutoff_source_position: int
    bar_open_time_utc: datetime
    observed_price_at_issue: Decimal
    source_dataset_id: str
    producer_version: str
    metadata: Mapping[str, MetadataValue] = field(default_factory=dict)

    def __post_init__(self) -> None:
        require_uuid(self.signal_id, "signal_id")
        require_instrument_id(self.instrument_id)
        if self.signal_type != "FIXTURE_UP":
            raise ValueError("only explicitly synthetic FIXTURE_UP research signals are supported")
        for name in ("issued_at_utc", "input_cutoff_utc", "bar_open_time_utc"):
            object.__setattr__(self, name, parse_timestamp(getattr(self, name), name))
        require_position(self.input_cutoff_source_position)
        require_decimal_price(self.observed_price_at_issue)
        required_text(self.source_dataset_id, "source_dataset_id")
        required_text(self.producer_version, "producer_version")
        if self.issued_at_utc < self.input_cutoff_utc:
            raise ValueError("issuance precedes input event time")
        if self.bar_open_time_utc != self.input_cutoff_utc.replace(second=0, microsecond=0):
            raise ValueError("bar anchor must match the observed input minute")
        object.__setattr__(self, "metadata", freeze_metadata(self.metadata))

    @classmethod
    def issue(
        cls,
        *,
        signal_id: UUID,
        observation: Observation,
        producer_version: str,
        metadata: Mapping[str, MetadataValue] | None = None,
    ) -> "ResearchSignal":
        tick = observation.tick
        return cls(
            signal_id,
            tick.instrument_id,
            "FIXTURE_UP",
            observation.available_at_utc,
            tick.event_time_utc,
            tick.source_position,
            tick.event_time_utc.replace(second=0, microsecond=0),
            tick.price,
            observation.source_dataset_id,
            producer_version,
            metadata or {},
        )

    def check_observation(self, observation: Observation) -> None:
        """The current-input producer API requires an exact, auditable cutoff."""
        tick = observation.tick
        if (
            self.instrument_id != tick.instrument_id
            or self.source_dataset_id != observation.source_dataset_id
            or self.input_cutoff_source_position != tick.source_position
            or self.input_cutoff_utc != tick.event_time_utc
            or self.observed_price_at_issue.as_tuple() != tick.price.as_tuple()
            or self.issued_at_utc != observation.available_at_utc
        ):
            raise ValueError(
                "signal cutoff, price, identity or availability differs from observation"
            )

    def to_dict(self) -> dict[str, object]:
        # Share exact Decimal/UTC/metadata serialization with the existing audit format.
        from quantlab.audit import json_value

        result = {f.name: json_value(getattr(self, f.name)) for f in fields(self)}
        return {"schema_version": self.schema_version, **result}

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), sort_keys=True, separators=(",", ":"), allow_nan=False)

    @classmethod
    def from_json(cls, text: str) -> "ResearchSignal":
        from quantlab.audit import MAX_RECORD_BYTES, decode_json

        data = text.encode("utf-8")
        if len(data) > MAX_RECORD_BYTES:
            raise ValueError("signal exceeds 128 KiB")
        value = decode_json(data)
        if not isinstance(value, dict) or value.pop("schema_version", None) != cls.schema_version:
            raise ValueError("unsupported research signal schema")
        if set(value) != {f.name for f in fields(cls)}:
            raise ValueError("research signal fields differ from schema")
        # Validate JSON types before invoking typed domain parsers.
        for name in (
            "signal_id",
            "instrument_id",
            "signal_type",
            "issued_at_utc",
            "input_cutoff_utc",
            "bar_open_time_utc",
            "observed_price_at_issue",
            "source_dataset_id",
            "producer_version",
        ):
            if not isinstance(value[name], str):
                raise ValueError(f"{name} must be a string")
        return cls(
            UUID(value["signal_id"]),
            value["instrument_id"],
            value["signal_type"],
            parse_timestamp(value["issued_at_utc"]),
            parse_timestamp(value["input_cutoff_utc"]),
            value["input_cutoff_source_position"],
            parse_timestamp(value["bar_open_time_utc"]),
            parse_price(value["observed_price_at_issue"]),
            value["source_dataset_id"],
            value["producer_version"],
            value["metadata"],
        )
