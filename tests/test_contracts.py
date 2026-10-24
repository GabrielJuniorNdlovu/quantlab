"""Source-independent canonical contracts, integrity, and acquisition ordering."""

from collections.abc import Iterator
from dataclasses import FrozenInstanceError, replace
from datetime import datetime, timezone
from decimal import Decimal, localcontext
from hashlib import sha256
from uuid import UUID

import pytest

from quantlab.engine import validate_acquisition, validate_tick
from quantlab.models import (
    INSTRUMENT_ID,
    AcquisitionContext,
    MarketTick,
    RawObservation,
    SourceFormat,
    SourceType,
    TickCandidate,
)
from quantlab.quality import ContractError, ErrorCode, QualityError, Severity

CAPTURE = UUID("11111111-1111-4111-8111-111111111111")
OTHER_CAPTURE = UUID("22222222-2222-4222-8222-222222222222")
DETECTED = datetime(2026, 1, 2, tzinfo=timezone.utc)
EVENT_TIME = datetime(2026, 1, 1, tzinfo=timezone.utc)


def context(**changes: object) -> AcquisitionContext:
    """Create explicitly configured fixture metadata, not a provider mapping."""
    fields: dict[str, object] = {
        "capture_id": CAPTURE,
        "provider": "fixture-provider",
        "source_type": SourceType.HISTORICAL_CSV,
        "provider_symbol": "fixture-boom-1000",
        "pip_size": 2,
    }
    fields.update(changes)
    return AcquisitionContext(**fields)  # type: ignore[arg-type]


def raw(position: int = 1, **changes: object) -> RawObservation:
    fields: dict[str, object] = {
        "capture_id": CAPTURE,
        "source_position": position,
        "provider": "fixture-provider",
        "source_format": SourceFormat.CSV,
        "raw_payload": b"original source observation\r\n",
        "payload_sha256": sha256(b"original source observation\r\n").hexdigest(),
    }
    fields.update(changes)
    return RawObservation(**fields)  # type: ignore[arg-type]


def candidate(position: int = 1, **changes: object) -> TickCandidate:
    fields: dict[str, object] = {
        "raw": raw(position),
        "instrument_id": INSTRUMENT_ID,
        "provider_symbol": "fixture-boom-1000",
        "event_time": "2026-01-01T00:00:00Z",
        "price": "1000.10",
        "pip_size": 2,
    }
    fields.update(changes)
    return TickCandidate(**fields)  # type: ignore[arg-type]


def test_valid_canonical_historical_observation() -> None:
    observation = candidate()
    outcome = validate_tick(observation, context())
    assert outcome.accepted
    assert outcome.events == ()
    assert outcome.raw is observation.raw
    assert outcome.tick is not None
    assert outcome.tick.instrument_id == "BOOM_1000"
    assert outcome.tick.provider_symbol == "fixture-boom-1000"
    assert outcome.tick.price == Decimal("1000.10")
    assert str(outcome.tick.price) == "1000.10"
    assert outcome.tick.event_time_utc == EVENT_TIME
    assert outcome.tick.event_time_utc.tzinfo is timezone.utc
    assert outcome.tick.capture_id == CAPTURE
    assert outcome.tick.source_position == 1
    assert outcome.tick.source_type is SourceType.HISTORICAL_CSV
    assert outcome.tick.pip_size == 2
    assert outcome.tick.received_at_utc is None
    assert outcome.tick.provider_tick_id is None
    assert MarketTick.schema_version == "market_tick.v1"
    assert RawObservation.schema_version == "raw_observation.v1"


@pytest.mark.parametrize(
    "event_time",
    [
        "2026-01-01T02:00:00+02:00",
        "2025-12-31T19:00:00-05:00",
        "2026-01-01T05:30:00+05:30",
        datetime.fromisoformat("2026-01-01T02:00:00+02:00"),
    ],
)
def test_native_and_text_timestamps_normalize_to_utc(event_time: str | datetime) -> None:
    outcome = validate_tick(candidate(event_time=event_time), context())
    assert outcome.tick is not None
    assert outcome.tick.event_time_utc == EVENT_TIME
    assert outcome.tick.event_time_utc.tzinfo is timezone.utc


@pytest.mark.parametrize("price", ["1000.10", Decimal("1000.10"), "1e2", "0.01"])
def test_exact_decimal_prices(price: str | Decimal) -> None:
    outcome = validate_tick(candidate(price=price), context())
    assert outcome.tick is not None
    assert outcome.tick.price == Decimal(price)


def test_decimal_context_does_not_round_validation() -> None:
    price = "1234567890123456789012345678901234567890.12"
    with localcontext() as decimal_context:
        decimal_context.prec = 2
        outcome = validate_tick(candidate(price=price), context())
    assert outcome.tick is not None
    assert str(outcome.tick.price) == price


def test_optional_provider_tick_identifier_is_preserved() -> None:
    outcome = validate_tick(candidate(provider_tick_id="provider-record-42"), context())
    assert outcome.tick is not None
    assert outcome.tick.provider_tick_id == "provider-record-42"


@pytest.mark.parametrize(
    "field, value, code",
    [
        ("instrument_id", "BOOM_500", ErrorCode.UNSUPPORTED_INSTRUMENT),
        ("instrument_id", None, ErrorCode.MISSING_REQUIRED_FIELD),
        ("provider_symbol", None, ErrorCode.MISSING_REQUIRED_FIELD),
        ("provider_symbol", "", ErrorCode.MISSING_REQUIRED_FIELD),
        ("price", None, ErrorCode.MISSING_REQUIRED_FIELD),
        ("price", "", ErrorCode.MISSING_REQUIRED_FIELD),
        ("price", "-1", ErrorCode.INVALID_PRICE),
        ("price", "0", ErrorCode.INVALID_PRICE),
        ("price", "-0.00", ErrorCode.INVALID_PRICE),
        ("price", "NaN", ErrorCode.INVALID_PRICE),
        ("price", "sNaN", ErrorCode.INVALID_PRICE),
        ("price", "Infinity", ErrorCode.INVALID_PRICE),
        ("price", "-Infinity", ErrorCode.INVALID_PRICE),
        ("price", Decimal("NaN"), ErrorCode.INVALID_PRICE),
        ("price", "1_000.00", ErrorCode.INVALID_PRICE),
        ("price", " 1000.10", ErrorCode.INVALID_PRICE),
        ("price", "1e1001", ErrorCode.INVALID_PRICE),
        ("price", "1" * 257, ErrorCode.INVALID_PRICE),
        ("event_time", None, ErrorCode.MISSING_REQUIRED_FIELD),
        ("event_time", "not-a-time", ErrorCode.INVALID_TIMESTAMP),
        ("event_time", "2026-01-01T00:00:00", ErrorCode.INVALID_TIMESTAMP),
        ("event_time", datetime(2026, 1, 1), ErrorCode.INVALID_TIMESTAMP),
        ("event_time", "2026-02-30T00:00:00Z", ErrorCode.INVALID_TIMESTAMP),
        ("event_time", "2026-01-01T00:00:00.1234567Z", ErrorCode.INVALID_TIMESTAMP),
        ("event_time", "2026-01-01T00:00:00+00:60", ErrorCode.INVALID_TIMESTAMP),
        ("event_time", "0001-01-01T00:00:00+01:00", ErrorCode.INVALID_TIMESTAMP),
        ("pip_size", None, ErrorCode.MISSING_REQUIRED_FIELD),
        ("pip_size", -1, ErrorCode.INVALID_PRECISION),
        ("pip_size", 19, ErrorCode.INVALID_PRECISION),
        ("pip_size", True, ErrorCode.INVALID_PRECISION),
        ("pip_size", "2.0", ErrorCode.INVALID_PRECISION),
        ("pip_size", 3, ErrorCode.INVALID_PRECISION),
    ],
)
def test_domain_rejection_has_structured_context(
    field: str, value: object, code: ErrorCode
) -> None:
    observation = candidate(**{field: value})
    outcome = validate_tick(observation, context(), detected_at_utc=DETECTED)
    assert not outcome.accepted
    assert outcome.tick is None
    assert outcome.raw is observation.raw
    assert len(outcome.events) == 1
    event = outcome.events[0]
    assert event.error_code is code
    assert event.severity is Severity.ERROR
    assert event.capture_id == CAPTURE
    assert event.source_position == 1
    assert event.field_name is not None
    assert event.description
    assert event.detected_at_utc == DETECTED


@pytest.mark.parametrize("price", ["1000.101", "1000.100", "1e-3", Decimal("0.001")])
def test_quote_precision_rejects_extra_places_without_rounding(price: str | Decimal) -> None:
    outcome = validate_tick(candidate(price=price), context())
    assert outcome.tick is None
    assert outcome.events[0].error_code is ErrorCode.INVALID_PRECISION


def test_explicit_provider_configuration_is_enforced() -> None:
    outcome = validate_tick(candidate(provider_symbol="unverified-symbol"), context())
    assert not outcome.accepted
    assert outcome.events[0].error_code is ErrorCode.INVALID_SOURCE_METADATA


@pytest.mark.parametrize(
    "raw_changes",
    [
        {"capture_id": OTHER_CAPTURE},
        {"provider": "other-provider"},
        {"source_format": SourceFormat.JSON},
    ],
)
def test_raw_context_mismatch_is_rejected(raw_changes: dict[str, object]) -> None:
    outcome = validate_tick(candidate(raw=raw(**raw_changes)), context())
    assert not outcome.accepted
    assert outcome.events[0].error_code is ErrorCode.INVALID_SOURCE_METADATA


def test_validation_and_errors_are_deterministic() -> None:
    observation = candidate(price="NaN")
    first = validate_tick(observation, context())
    second = validate_tick(observation, context())
    assert first == second
    assert first.events[0].event_id == second.events[0].event_id
    assert first.events[0].detected_at_utc is None
    assert validate_tick(candidate(), context()) == validate_tick(candidate(), context())


def test_untrusted_diagnostics_are_bounded_and_do_not_echo_payload() -> None:
    secret = "SENSITIVE-CONTENT-" * 1000
    outcome = validate_tick(candidate(price=secret), context())
    event = outcome.events[0]
    assert secret not in event.description
    assert len(event.description) <= 512
    assert event.observed_value is None or len(event.observed_value) <= 128


def test_same_timestamp_and_repeated_price_ticks_remain_separate() -> None:
    observations = [candidate(position=position) for position in range(1, 5)]
    outcomes = list(validate_acquisition(observations, context()))
    assert len(outcomes) == 4
    assert all(outcome.accepted for outcome in outcomes)
    assert [outcome.tick.source_position for outcome in outcomes if outcome.tick] == [1, 2, 3, 4]
    assert len({outcome.tick.price for outcome in outcomes if outcome.tick}) == 1


def test_decreasing_timestamp_rejected_but_not_deleted() -> None:
    observations = [candidate(), candidate(2, event_time="2025-12-31T23:59:59Z"), candidate(3)]
    outcomes = list(validate_acquisition(observations, context()))
    assert len(outcomes) == 3
    assert [outcome.accepted for outcome in outcomes] == [True, False, True]
    assert outcomes[1].events[0].error_code is ErrorCode.OUT_OF_ORDER_EVENT
    assert outcomes[1].events[0].source_position == 2


@pytest.mark.parametrize("positions", [(1, 1), (2, 1)])
def test_duplicate_or_decreasing_source_positions(positions: tuple[int, int]) -> None:
    outcomes = list(validate_acquisition([candidate(position=p) for p in positions], context()))
    assert outcomes[0].accepted
    assert not outcomes[1].accepted
    assert outcomes[1].events[0].error_code is ErrorCode.INVALID_SOURCE_POSITION


def test_rejected_source_position_is_still_consumed() -> None:
    outcomes = list(validate_acquisition([candidate(price="NaN"), candidate()], context()))
    assert outcomes[0].events[0].error_code is ErrorCode.INVALID_PRICE
    assert outcomes[1].events[0].error_code is ErrorCode.INVALID_SOURCE_POSITION


def test_ordering_state_is_reset_between_acquisitions() -> None:
    later = list(validate_acquisition([candidate(event_time="2026-02-01T00:00:00Z")], context()))
    earlier = list(
        validate_acquisition(
            [candidate(raw=raw(capture_id=OTHER_CAPTURE))], context(capture_id=OTHER_CAPTURE)
        )
    )
    assert later[0].accepted
    assert earlier[0].accepted


def test_validator_consumes_incrementally() -> None:
    consumed: list[int] = []

    def observations() -> Iterator[TickCandidate]:
        for position in range(1, 4):
            consumed.append(position)
            yield candidate(position)

    outcomes = validate_acquisition(observations(), context())
    assert consumed == []
    assert next(outcomes).accepted
    assert consumed == [1]
    assert next(outcomes).accepted
    assert consumed == [1, 2]
    assert len(list(outcomes)) == 1


def test_empty_acquisition_raises_structured_error() -> None:
    with pytest.raises(QualityError) as error:
        list(validate_acquisition([], context()))
    assert error.value.event.error_code is ErrorCode.EMPTY_DATASET
    assert error.value.event.capture_id == CAPTURE


def test_reception_before_event_is_warning_not_rejection() -> None:
    reception = datetime(2025, 12, 31, tzinfo=timezone.utc)
    outcome = validate_tick(candidate(raw=raw(received_at_utc=reception)), context())
    assert outcome.accepted
    assert outcome.tick is not None
    assert outcome.tick.received_at_utc == reception
    assert outcome.events[0].severity is Severity.WARNING
    assert outcome.events[0].error_code is ErrorCode.RECEPTION_BEFORE_EVENT


@pytest.mark.parametrize(
    "changes, code",
    [
        ({"capture_id": "not-a-uuid"}, ErrorCode.INVALID_SOURCE_METADATA),
        ({"provider": ""}, ErrorCode.MISSING_REQUIRED_FIELD),
        ({"source_type": "unsupported"}, ErrorCode.INVALID_SOURCE_METADATA),
        ({"provider_symbol": ""}, ErrorCode.MISSING_REQUIRED_FIELD),
        ({"pip_size": -1}, ErrorCode.INVALID_PRECISION),
        ({"pip_size": 19}, ErrorCode.INVALID_PRECISION),
        ({"pip_size": True}, ErrorCode.INVALID_PRECISION),
        ({"received_at_utc": datetime(2026, 1, 1)}, ErrorCode.INVALID_TIMESTAMP),
    ],
)
def test_invalid_acquisition_context_is_not_constructible(
    changes: dict[str, object], code: ErrorCode
) -> None:
    with pytest.raises(ContractError) as error:
        context(**changes)
    assert error.value.code is code


@pytest.mark.parametrize(
    "changes, code",
    [
        ({"source_position": 0}, ErrorCode.INVALID_SOURCE_POSITION),
        ({"source_position": -1}, ErrorCode.INVALID_SOURCE_POSITION),
        ({"source_position": True}, ErrorCode.INVALID_SOURCE_POSITION),
        ({"source_position": 1.5}, ErrorCode.INVALID_SOURCE_POSITION),
        ({"capture_id": "not-a-uuid"}, ErrorCode.INVALID_SOURCE_METADATA),
        ({"source_format": "unsupported"}, ErrorCode.INVALID_SOURCE_METADATA),
        ({"provider": ""}, ErrorCode.MISSING_REQUIRED_FIELD),
        ({"payload_sha256": "0" * 64}, ErrorCode.INVALID_CHECKSUM),
        ({"raw_payload": "not bytes"}, ErrorCode.INPUT_TOO_LARGE),
        ({"raw_payload": b"a" * 65537}, ErrorCode.INPUT_TOO_LARGE),
        ({"received_at_utc": datetime(2026, 1, 1)}, ErrorCode.INVALID_TIMESTAMP),
    ],
)
def test_raw_invariants_are_enforced(changes: dict[str, object], code: ErrorCode) -> None:
    with pytest.raises(ContractError) as error:
        raw(**changes)
    assert error.value.code is code


def test_raw_bytes_and_source_metadata_remain_unchanged() -> None:
    metadata = {"provider": {"tags": ["historical", {"verified": True}]}, "offset": 10}
    original = raw(source_metadata=metadata)
    checksum = original.payload_sha256
    metadata["offset"] = 99
    provider_metadata = metadata["provider"]
    assert isinstance(provider_metadata, dict)
    tags = provider_metadata["tags"]
    assert isinstance(tags, list)
    tags.append("new")
    assert original.source_metadata["offset"] == 10
    assert original.source_metadata["provider"]["tags"] == ("historical", {"verified": True})
    assert original.payload_sha256 == checksum == sha256(original.raw_payload).hexdigest()
    with pytest.raises(TypeError):
        original.source_metadata["offset"] = 42  # type: ignore[index]
    with pytest.raises(FrozenInstanceError):
        original.raw_payload = b"modified"  # type: ignore[misc]


@pytest.mark.parametrize(
    "metadata",
    [
        {"arbitrary": object()},
        {"nonfinite": float("nan")},
        {"nonfinite": float("inf")},
        {1: "non-string key"},
        {"unsupported": {1, 2}},
    ],
)
def test_invalid_metadata_is_rejected(metadata: object) -> None:
    with pytest.raises(ContractError) as error:
        raw(source_metadata=metadata)
    assert error.value.code is ErrorCode.INVALID_SOURCE_METADATA


def test_excessive_metadata_nesting_is_rejected() -> None:
    metadata: object = "leaf"
    for _ in range(10):
        metadata = {"child": metadata}
    with pytest.raises(ContractError) as error:
        context(source_metadata=metadata)
    assert error.value.code is ErrorCode.INVALID_SOURCE_METADATA


def test_market_tick_direct_construction_cannot_bypass_validation() -> None:
    outcome = validate_tick(candidate(), context())
    assert outcome.tick is not None
    with pytest.raises(ContractError):
        replace(outcome.tick, price=Decimal("NaN"))
    with pytest.raises(ContractError):
        replace(outcome.tick, instrument_id="BOOM_500")
    with pytest.raises(ContractError):
        replace(outcome.tick, event_time_utc=datetime(2026, 1, 1))
    with pytest.raises(ContractError):
        replace(outcome.tick, source_type="unknown")
    with pytest.raises(ContractError):
        replace(outcome.tick, price=Decimal("1.001"))


@pytest.mark.parametrize("source_type", [SourceType.HISTORICAL_JSON, SourceType.WEBSOCKET])
def test_native_json_candidates_share_canonical_rules(source_type: SourceType) -> None:
    observation = candidate(raw=raw(source_format=SourceFormat.JSON))
    outcome = validate_tick(observation, context(source_type=source_type, received_at_utc=DETECTED))
    assert outcome.accepted
    assert outcome.tick is not None
    assert outcome.tick.received_at_utc == DETECTED
    assert outcome.tick.source_type is source_type


def test_foreign_capture_does_not_consume_active_acquisition_position() -> None:
    observations = [candidate(raw=raw(100, capture_id=OTHER_CAPTURE)), candidate(2)]
    outcomes = list(validate_acquisition(observations, context()))
    assert outcomes[0].events[0].error_code is ErrorCode.INVALID_SOURCE_METADATA
    assert outcomes[1].accepted
