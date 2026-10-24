"""Failure paths at typed, metadata, and legacy transport boundaries."""

import hashlib
import io
from datetime import datetime, timezone
from decimal import Decimal
from uuid import UUID

import pytest

from quantlab.csv_adapter import iter_csv_candidates
from quantlab.legacy import ValidationError, iter_legacy_csv_ticks
from quantlab.models import (
    AcquisitionContext,
    RawObservation,
    SourceFormat,
    SourceType,
    ValidationOutcome,
)
from quantlab.parsing import parse_price, parse_timestamp
from quantlab.quality import ContractError, ErrorCode, QualityError, Severity, quality_event


@pytest.mark.parametrize(
    "metadata",
    [
        {"large": "x" * 16385},
        {"large": "é" * 10000},
        {"large": 2**65},
        {"bad": "\ud800"},
    ],
)
def test_metadata_limits_and_invalid_unicode(metadata: dict[str, object]) -> None:
    with pytest.raises(ContractError) as error:
        AcquisitionContext(
            UUID(int=1),
            "fixture",
            SourceType.HISTORICAL_CSV,
            "fixture-symbol",
            2,
            source_metadata=metadata,  # type: ignore[arg-type]
        )
    assert error.value.code is ErrorCode.INVALID_SOURCE_METADATA


def test_metadata_accepts_finite_native_json_numbers() -> None:
    context = AcquisitionContext(
        UUID(int=1),
        "fixture",
        SourceType.HISTORICAL_CSV,
        "fixture-symbol",
        2,
        source_metadata={"amount": 1.5, "count": 5, "optional": None},
    )
    assert context.source_metadata == {"amount": 1.5, "count": 5, "optional": None}


@pytest.mark.parametrize("value", [1.2, True, Decimal("sNaN"), Decimal("1e-1001")])
def test_price_never_accepts_floats_or_invalid_native_decimals(value: object) -> None:
    with pytest.raises(ContractError) as error:
        parse_price(value)  # type: ignore[arg-type]
    assert error.value.code is ErrorCode.INVALID_PRICE


def test_timestamp_rejects_wrong_native_type() -> None:
    with pytest.raises(ContractError) as error:
        parse_timestamp(123)  # type: ignore[arg-type]
    assert error.value.code is ErrorCode.INVALID_TIMESTAMP


@pytest.mark.parametrize(
    "payload",
    [
        'symbol,timestamp_utc,price,source\nBoom 1000 Index,2026-01-01T00:00:00Z,1,abc"def\n',
        "symbol,timestamp_utc,price,source\n" + "x" * 65537,
        'symbol,timestamp_utc,price,source\n"' + "x\n" * 40000,
    ],
)
def test_legacy_rejects_quote_and_resource_violations(payload: str) -> None:
    with pytest.raises(ValidationError, match="Row 2: invalid CSV"):
        list(iter_legacy_csv_ticks(io.StringIO(payload)))


def test_wrong_csv_size_limit_is_structured() -> None:
    context = AcquisitionContext(
        UUID(int=1), "fixture", SourceType.HISTORICAL_CSV, "fixture-symbol", 2
    )
    with pytest.raises(QualityError) as error:
        list(iter_csv_candidates(io.BytesIO(), context, max_record_bytes=0))
    assert error.value.event.error_code is ErrorCode.INVALID_SOURCE_METADATA


def test_outcome_cannot_be_rejected_without_error() -> None:
    raw = RawObservation(
        UUID(int=1),
        1,
        "fixture",
        SourceFormat.CSV,
        b"raw",
        hashlib.sha256(b"raw").hexdigest(),
    )
    with pytest.raises(ValueError, match="rejections require errors"):
        ValidationOutcome(raw, None)


def test_quality_event_detection_offset_normalizes_and_warnings_are_distinct() -> None:
    event = quality_event(
        UUID(int=1),
        1,
        ErrorCode.RECEPTION_BEFORE_EVENT,
        "check supplied reception time",
        severity=Severity.WARNING,
        detected_at_utc=datetime.fromisoformat("2026-01-01T02:00:00+02:00"),
    )
    assert event.detected_at_utc == datetime(2026, 1, 1, tzinfo=timezone.utc)
    assert event.severity is Severity.WARNING
