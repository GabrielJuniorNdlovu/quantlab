"""Byte-exact capture and transport schema tests, independent of the engine."""

import hashlib
import io
from dataclasses import replace
from uuid import UUID

import pytest

from quantlab.csv_adapter import iter_csv_candidates
from quantlab.models import AcquisitionContext, SourceType
from quantlab.quality import ErrorCode, QualityError, Severity

HEADER = b"instrument_id,provider_symbol,event_time_utc,price,pip_size\r\n"
ROW = b"BOOM_1000,fixture-symbol,2026-01-01T00:00:00Z,1000.10,2\r\n"


def context() -> AcquisitionContext:
    return AcquisitionContext(
        capture_id=UUID("00000000-0000-4000-8000-000000000001"),
        provider="fixture-provider",
        source_type=SourceType.HISTORICAL_CSV,
        provider_symbol="fixture-symbol",
        pip_size=2,
        source_metadata={"dataset": "engineering-fixture"},
    )


def test_exact_bytes_hash_and_location() -> None:
    stream = io.BytesIO(HEADER + ROW + ROW)
    candidates = iter_csv_candidates(stream, context())
    first = next(candidates)
    assert first.raw.raw_payload == ROW
    assert first.raw.payload_sha256 == hashlib.sha256(ROW).hexdigest()
    assert first.raw.source_position == 2
    assert first.raw.received_at_utc is None
    assert first.raw.source_metadata["provider"] == {"dataset": "engineering-fixture"}
    metadata = first.raw.source_metadata["adapter"]
    assert metadata["byte_offset"] == len(HEADER)
    assert metadata["start_line"] == metadata["end_line"] == 2
    assert metadata["header"] == HEADER.decode()
    assert metadata["header_sha256"] == hashlib.sha256(HEADER).hexdigest()
    second = next(candidates)
    assert second.raw.source_position == 3
    assert stream.getvalue() == HEADER + ROW + ROW
    assert list(candidates) == []
    assert not stream.closed


def test_multiline_utf8_and_optional_provider_id() -> None:
    header = HEADER.rstrip(b"\r\n") + b",provider_tick_id\n"
    row = ROW.rstrip(b"\r\n") + ',"original\r\nidé"\n'.encode()
    candidates = list(
        iter_csv_candidates(io.BytesIO(header + row + ROW.rstrip(b"\r\n") + b",\n"), context())
    )
    first, second = candidates
    assert first.raw.raw_payload == row
    assert first.provider_tick_id == "original\r\nidé"
    assert first.raw.source_position == 2
    assert first.raw.source_metadata["adapter"]["end_line"] == 3
    assert second.raw.source_position == 4
    assert second.provider_tick_id is None


@pytest.mark.parametrize(
    "payload, code",
    [
        (b"", ErrorCode.EMPTY_DATASET),
        (HEADER, ErrorCode.EMPTY_DATASET),
        (b"price\n1\n", ErrorCode.INVALID_CSV_SCHEMA),
        (HEADER.rstrip(b"\r\n") + b",extra\n" + ROW, ErrorCode.INVALID_CSV_SCHEMA),
        (HEADER.replace(b"pip_size", b"price") + ROW, ErrorCode.INVALID_CSV_SCHEMA),
        (HEADER + b"\n", ErrorCode.INVALID_CSV),
        (HEADER + b"one,two\n", ErrorCode.INVALID_CSV),
        (HEADER + b'"unterminated', ErrorCode.INVALID_CSV),
        (HEADER + b"\xff\n", ErrorCode.INVALID_CSV),
        (HEADER + ROW.replace(b"BOOM_1000", b"BOOM\r1000"), ErrorCode.INVALID_CSV),
    ],
)
def test_fatal_transport_errors(payload: bytes, code: ErrorCode) -> None:
    with pytest.raises(QualityError) as error:
        list(iter_csv_candidates(io.BytesIO(payload), context()))
    assert error.value.event.error_code is code
    assert error.value.event.capture_id == context().capture_id
    assert error.value.event.severity is Severity.FATAL
    assert error.value.event.observed_value is None


@pytest.mark.parametrize("row", [b"x" * 500 + b"\n", b'"' + b"x\n" * 200 + b'"\n'])
def test_record_size_limit_includes_all_physical_lines(row: bytes) -> None:
    with pytest.raises(QualityError) as error:
        list(iter_csv_candidates(io.BytesIO(HEADER + row), context(), max_record_bytes=256))
    assert error.value.event.error_code is ErrorCode.INPUT_TOO_LARGE
    assert error.value.event.source_position == 2


def test_adapter_only_parses_business_values() -> None:
    row = ROW.replace(b"1000.10", b"NaN").replace(b"2026-01-01T00:00:00Z", b"bad-time")
    candidate = next(iter_csv_candidates(io.BytesIO(HEADER + row), context()))
    assert candidate.price == "NaN"
    assert candidate.event_time == "bad-time"
    assert candidate.pip_size == "2"


def test_wrong_acquisition_format_rejected() -> None:
    with pytest.raises(QualityError) as error:
        list(
            iter_csv_candidates(
                io.BytesIO(HEADER + ROW), replace(context(), source_type=SourceType.WEBSOCKET)
            )
        )
    assert error.value.event.error_code is ErrorCode.INVALID_SOURCE_METADATA


def test_bare_quote_inside_unquoted_field_is_malformed() -> None:
    header = HEADER.rstrip(b"\r\n") + b",provider_tick_id\n"
    row = ROW.rstrip(b"\r\n") + b',abc"def\n'
    with pytest.raises(QualityError) as error:
        list(iter_csv_candidates(io.BytesIO(header + row), context()))
    assert error.value.event.error_code is ErrorCode.INVALID_CSV


@pytest.mark.parametrize(
    "escaped, expected",
    [
        (b'"a""b"', 'a"b'),
        (b'"a,b"', "a,b"),
        (b'"a"""', 'a"'),
        (b'"a\r\nb"', "a\r\nb"),
    ],
)
def test_valid_quoting_preserves_original_bytes_and_provider_value(
    escaped: bytes, expected: str
) -> None:
    header = HEADER.rstrip(b"\r\n") + b",provider_tick_id\n"
    row = ROW.rstrip(b"\r\n") + b"," + escaped + b"\n"
    observation = next(iter_csv_candidates(io.BytesIO(header + row), context()))
    assert observation.provider_tick_id == expected
    assert observation.raw.raw_payload == row


def test_exact_default_record_limit_is_accepted_without_truncation() -> None:
    header = HEADER.rstrip(b"\r\n") + b",provider_tick_id\n"
    prefix = ROW.rstrip(b"\r\n") + b","
    row = prefix + b"x" * (65536 - len(prefix) - 1) + b"\n"
    observation = next(iter_csv_candidates(io.BytesIO(header + row), context()))
    assert len(observation.raw.raw_payload) == 65536
    assert observation.raw.raw_payload == row
