"""Explicit PostgreSQL wire mapping and deterministic batch receipt hashes."""

import hashlib
import json
from collections.abc import Mapping
from dataclasses import fields, is_dataclass
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Any
from uuid import UUID

from quantlab.models import MarketTick, SourceType, ValidationOutcome
from quantlab.storage.contracts import DatasetManifest


def wire_value(value: object) -> Any:
    if isinstance(value, (datetime, UUID, Decimal)):
        return value.isoformat() if isinstance(value, datetime) else str(value)
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, Mapping):
        return {str(key): wire_value(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [wire_value(item) for item in value]
    if is_dataclass(value) and not isinstance(value, type):
        return {
            item.name: wire_value(getattr(value, item.name))
            for item in fields(value)
            if item.name != "raw_payload"
        }
    return value


def batch_digest(outcomes: tuple[ValidationOutcome, ...]) -> str:
    digest = hashlib.sha256()
    for outcome in outcomes:
        digest.update(
            json.dumps(
                wire_value(outcome), sort_keys=True, separators=(",", ":"), allow_nan=False
            ).encode()
        )
        digest.update(b"\n")
    return digest.hexdigest()


def manifest_row(row: dict[str, Any]) -> DatasetManifest:
    return DatasetManifest(**row)


def tick_row(row: dict[str, Any]) -> MarketTick:
    return MarketTick(
        row["instrument_id"],
        row["provider_symbol"],
        row["event_time_utc"],
        Decimal(row["price_text"]),
        row["pip_size"],
        SourceType(row["source_type"]),
        row["capture_id"],
        row["source_position"],
        row["received_at_utc"],
        row["provider_tick_id"],
    )
