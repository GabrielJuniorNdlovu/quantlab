"""Bounded UTF-8 CSV transport with exact original record bytes and locations."""

import csv
import hashlib
from collections.abc import Iterator
from typing import BinaryIO

from quantlab.csv_syntax import is_csv_record
from quantlab.models import (
    MAX_RAW_BYTES,
    AcquisitionContext,
    RawObservation,
    SourceFormat,
    SourceType,
    TickCandidate,
)
from quantlab.quality import ContractError, ErrorCode, QualityError, Severity, quality_event

CANONICAL_COLUMNS = ("instrument_id", "provider_symbol", "event_time_utc", "price", "pip_size")
OPTIONAL_COLUMNS = ("provider_tick_id",)


class _RecordLines:
    """Feed csv.reader physical lines while retaining bounded original bytes."""

    def __init__(self, stream: BinaryIO, max_record_bytes: int) -> None:
        self.stream = stream
        self.limit = max_record_bytes
        self.line_number = 0
        self.byte_offset = 0
        self.record = bytearray()

    def __iter__(self) -> "_RecordLines":
        return self

    def __next__(self) -> str:
        remaining = self.limit - len(self.record)
        line = self.stream.readline(remaining + 1)
        if not line:
            raise StopIteration
        if len(line) > remaining:
            raise OverflowError("CSV record exceeds byte limit")
        # LF/CRLF are supported; reject bare CR rather than normalizing evidence.
        if b"\x00" in line:
            raise ValueError("NUL is not supported in CSV text")
        if b"\r" in line.replace(b"\r\n", b""):
            raise ValueError("CSV requires LF or CRLF line endings")
        self.record.extend(line)
        self.line_number += 1
        self.byte_offset += len(line)
        return line.decode("utf-8")


def iter_csv_records(
    stream: BinaryIO,
    context: AcquisitionContext,
    *,
    max_record_bytes: int = MAX_RAW_BYTES,
    columns: tuple[str, ...] | None = None,
) -> Iterator[tuple[RawObservation, dict[str, str]]]:
    """Yield unvalidated observed fields and exact row evidence; never close stream.

    Transport failures abort with a structured fatal event because malformed
    quotes cannot be safely resynchronized. Domain values are not validated here.
    """

    def failure(code: ErrorCode, description: str, position: int | None) -> QualityError:
        return QualityError(
            quality_event(
                context.capture_id,
                position,
                code,
                description,
                severity=Severity.FATAL,
            )
        )

    if context.source_type is not SourceType.HISTORICAL_CSV:
        raise failure(
            ErrorCode.INVALID_SOURCE_METADATA,
            "CSV adapter requires historical_csv acquisition",
            None,
        )
    if type(max_record_bytes) is not int or not 1 <= max_record_bytes <= MAX_RAW_BYTES:
        raise failure(
            ErrorCode.INVALID_SOURCE_METADATA, "max_record_bytes must be from 1 to 65536", None
        )
    lines = _RecordLines(stream, max_record_bytes)
    reader = csv.reader(lines, strict=True)
    position = 1
    try:
        header = next(reader, None)
        if header is None:
            raise failure(ErrorCode.EMPTY_DATASET, "empty input: required CSV header is missing", 1)
        if (
            len(set(header)) != len(header)
            or not set(columns or CANONICAL_COLUMNS) <= set(header)
            or (columns is None and not set(header) <= set(CANONICAL_COLUMNS + OPTIONAL_COLUMNS))
        ):
            raise failure(
                ErrorCode.INVALID_CSV_SCHEMA,
                "required columns: "
                + ", ".join(columns or CANONICAL_COLUMNS)
                + "; optional: provider_tick_id",
                1,
            )
        if not is_csv_record(lines.record.decode("utf-8")):
            raise failure(ErrorCode.INVALID_CSV, "invalid CSV header quote placement", 1)
        header_bytes = bytes(lines.record)
        header_hash = hashlib.sha256(header_bytes).hexdigest()
        seen = False
        while True:
            position = lines.line_number + 1
            offset = lines.byte_offset
            lines.record.clear()
            values = next(reader, None)
            if values is None:
                break
            seen = True
            if not is_csv_record(lines.record.decode("utf-8")):
                raise failure(ErrorCode.INVALID_CSV, "invalid CSV quote placement", position)
            if len(values) != len(header):
                raise failure(
                    ErrorCode.INVALID_CSV, "CSV row has missing or unexpected fields", position
                )
            fields = dict(zip(header, values, strict=True))
            payload = bytes(lines.record)
            raw = RawObservation(
                capture_id=context.capture_id,
                source_position=position,
                provider=context.provider,
                source_format=SourceFormat.CSV,
                raw_payload=payload,
                payload_sha256=hashlib.sha256(payload).hexdigest(),
                received_at_utc=context.received_at_utc,
                source_metadata={
                    "provider": context.source_metadata,
                    "adapter": {
                        "header": header_bytes.decode("utf-8"),
                        "header_sha256": header_hash,
                        "byte_offset": offset,
                        "start_line": position,
                        "end_line": lines.line_number,
                    },
                },
            )
            yield raw, fields
        if not seen:
            raise failure(ErrorCode.EMPTY_DATASET, "empty dataset: no tick records", position)
    except OverflowError as exc:
        raise failure(
            ErrorCode.INPUT_TOO_LARGE, "CSV record exceeds configured byte limit", position
        ) from exc
    except ContractError as exc:
        raise failure(exc.code, exc.description, position) from exc
    except (csv.Error, UnicodeError, ValueError) as exc:
        if isinstance(exc, QualityError):
            raise
        raise failure(
            ErrorCode.INVALID_CSV, "invalid CSV, UTF-8, or unsupported line ending", position
        ) from exc


def iter_csv_candidates(
    stream: BinaryIO, context: AcquisitionContext, *, max_record_bytes: int = MAX_RAW_BYTES
) -> Iterator[TickCandidate]:
    """Compatibility reader for the canonical CSV contract."""
    for raw, fields in iter_csv_records(stream, context, max_record_bytes=max_record_bytes):
        yield TickCandidate(
            raw=raw,
            instrument_id=fields["instrument_id"],
            provider_symbol=fields["provider_symbol"],
            event_time=fields["event_time_utc"],
            price=fields["price"],
            pip_size=fields["pip_size"],
            provider_tick_id=fields.get("provider_tick_id") or None,
        )
