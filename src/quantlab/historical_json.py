"""Bounded exact JSON Lines framing; no whole-file JSON deserialization."""

import hashlib
import json
from collections.abc import Iterator
from decimal import Decimal, DecimalException
from typing import BinaryIO

from quantlab.models import MAX_RAW_BYTES, AcquisitionContext, RawObservation, SourceFormat
from quantlab.quality import ErrorCode, QualityError, Severity, quality_event


def _object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON object key")
        for text in (key, value):
            if isinstance(text, str):
                text.encode("utf-8")
                if "\x00" in text:
                    raise ValueError("NUL is not supported in source text")
        result[key] = value
    return result


def _constant(value: str) -> object:
    raise ValueError("nonfinite JSON constants are unsupported")


def _number(value: str) -> Decimal:
    if len(value) > 256:
        raise ValueError("JSON number exceeds 256 characters")
    return Decimal(value)


def strict_json(payload: bytes) -> object:
    try:
        return json.loads(
            payload.decode("utf-8"),
            object_pairs_hook=_object,
            parse_float=_number,
            parse_int=int,
            parse_constant=_constant,
        )
    except (UnicodeError, ValueError, RecursionError, DecimalException) as exc:
        raise ValueError("invalid UTF-8 JSON or duplicate keys") from exc


def iter_json_records(
    stream: BinaryIO, context: AcquisitionContext, columns: tuple[str, ...]
) -> Iterator[tuple[RawObservation, dict[str, object]]]:
    position = 0
    offset = 0
    while payload := stream.readline(MAX_RAW_BYTES + 1):
        position += 1

        def fail(code: ErrorCode, message: str, position: int = position) -> QualityError:
            return QualityError(
                quality_event(context.capture_id, position, code, message, severity=Severity.FATAL)
            )

        if len(payload) > MAX_RAW_BYTES:
            raise fail(ErrorCode.INPUT_TOO_LARGE, "JSON line exceeds 65536 bytes")
        try:
            if payload.startswith(b"\xef\xbb\xbf") or b"\r" in payload.replace(b"\r\n", b""):
                raise ValueError("unsupported BOM or line ending")
            fields = strict_json(payload)
        except ValueError as exc:
            raise fail(ErrorCode.INVALID_JSON, "invalid JSON line, UTF-8 or line ending") from exc
        if not isinstance(fields, dict) or any(
            isinstance(v, (dict, list)) for v in fields.values()
        ):
            raise fail(ErrorCode.INVALID_JSON_SCHEMA, "JSON line must be a flat object")
        if not set(columns) <= fields.keys():
            raise fail(ErrorCode.INVALID_JSON_SCHEMA, "JSON object is missing configured fields")
        raw = RawObservation(
            context.capture_id,
            position,
            context.provider,
            SourceFormat.JSON,
            payload,
            hashlib.sha256(payload).hexdigest(),
            context.received_at_utc,
            {
                "provider": context.source_metadata,
                "adapter": {
                    "framing": "json_lines.v1",
                    "byte_offset": offset,
                    "start_line": position,
                    "end_line": position,
                },
            },
        )
        yield raw, fields
        offset += len(payload)
