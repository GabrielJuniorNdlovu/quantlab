"""Bounded JSON-compatible provenance, copied into deeply immutable values."""

import math
from collections.abc import Mapping
from types import MappingProxyType
from typing import TypeAlias

from quantlab.quality import ContractError, ErrorCode

MetadataValue: TypeAlias = (
    "str | int | float | bool | None | tuple[MetadataValue, ...] | Mapping[str, MetadataValue]"
)
MAX_METADATA_BYTES = 16384


def _freeze(value: object, depth: int, budget: int) -> tuple[MetadataValue, int]:
    budget -= 1
    if depth > 8 or budget < 0:
        raise ValueError("metadata exceeds depth or size limit")
    frozen: MetadataValue
    if value is None or isinstance(value, bool):
        frozen = value
    elif isinstance(value, str):
        if len(value) > budget:
            raise ValueError("metadata exceeds size limit")
        budget -= len(value.encode("utf-8"))
        frozen = value
    elif isinstance(value, int):
        if value.bit_length() > 64:
            raise ValueError("metadata integer exceeds 64 bits")
        budget -= 8
        frozen = value
    elif isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("metadata number must be finite")
        budget -= 8
        frozen = value
    elif isinstance(value, Mapping):
        mapping: dict[str, MetadataValue] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise ValueError("metadata keys must be strings")
            _, budget = _freeze(key, depth + 1, budget)
            mapping[key], budget = _freeze(item, depth + 1, budget)
        frozen = MappingProxyType(mapping)
    elif isinstance(value, (tuple, list)):
        sequence: list[MetadataValue] = []
        for item in value:
            entry, budget = _freeze(item, depth + 1, budget)
            sequence.append(entry)
        frozen = tuple(sequence)
    else:
        raise ValueError("metadata must contain JSON-compatible values")
    if budget < 0:
        raise ValueError("metadata exceeds size limit")
    return frozen, budget


def freeze_metadata(metadata: Mapping[str, object]) -> Mapping[str, MetadataValue]:
    """Reject oversized/deep metadata before retaining arbitrary mutable objects."""
    try:
        if not isinstance(metadata, Mapping):
            raise ValueError("source_metadata must be a mapping")
        result, _ = _freeze(metadata, 0, MAX_METADATA_BYTES)
        assert isinstance(result, Mapping)
        return result
    except (ValueError, UnicodeError) as exc:
        raise ContractError(
            ErrorCode.INVALID_SOURCE_METADATA,
            "source_metadata",
            "invalid, oversized, or deeply nested source_metadata",
        ) from exc
