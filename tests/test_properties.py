"""Deterministic property checks for prices, offsets, and repeated ticks."""

from collections.abc import Iterator
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from hashlib import sha256
from uuid import UUID

from hypothesis import given, settings
from hypothesis import strategies as st

from quantlab.engine import validate_acquisition, validate_tick
from quantlab.models import (
    INSTRUMENT_ID,
    AcquisitionContext,
    RawObservation,
    SourceFormat,
    SourceType,
    TickCandidate,
)
from quantlab.quality import ErrorCode

CAPTURE = UUID("33333333-3333-4333-8333-333333333333")
PROPERTY_SETTINGS = settings(database=None, derandomize=True, deadline=None, max_examples=60)


def observation(
    price: Decimal,
    places: int,
    *,
    position: int = 1,
    event_time: str | datetime = "2026-01-01T00:00:00Z",
) -> tuple[TickCandidate, AcquisitionContext]:
    context = AcquisitionContext(
        CAPTURE, "property-fixture", SourceType.HISTORICAL_CSV, "fixture-symbol", places
    )
    payload = b"property-fixture-row"
    raw = RawObservation(
        CAPTURE,
        position,
        "property-fixture",
        SourceFormat.CSV,
        payload,
        sha256(payload).hexdigest(),
    )
    return TickCandidate(raw, INSTRUMENT_ID, "fixture-symbol", event_time, price, places), context


@PROPERTY_SETTINGS
@given(coefficient=st.integers(min_value=1, max_value=10**40), places=st.integers(0, 18))
def test_positive_decimal_value_and_exponent_are_preserved(coefficient: int, places: int) -> None:
    digits = tuple(int(digit) for digit in str(coefficient))
    price = Decimal((0, digits, -places))
    candidate, context = observation(price, places)
    outcome = validate_tick(candidate, context)
    assert outcome.tick is not None
    assert outcome.tick.price.as_tuple() == price.as_tuple()
    assert candidate.price == price


@PROPERTY_SETTINGS
@given(
    utc_time=st.datetimes(
        min_value=datetime(2000, 1, 2),
        max_value=datetime(2099, 12, 30),
        timezones=st.just(timezone.utc),
    ),
    offset_minutes=st.integers(min_value=-23 * 60 - 59, max_value=23 * 60 + 59),
)
def test_timezone_offsets_normalize_exactly_across_days(
    utc_time: datetime, offset_minutes: int
) -> None:
    local = utc_time.astimezone(timezone(timedelta(minutes=offset_minutes)))
    candidate, context = observation(Decimal("1000.10"), 2, event_time=local.isoformat())
    outcome = validate_tick(candidate, context)
    assert outcome.tick is not None
    assert outcome.tick.event_time_utc == utc_time
    assert outcome.tick.event_time_utc.tzinfo is timezone.utc


@PROPERTY_SETTINGS
@given(
    coefficient=st.integers(min_value=1, max_value=10**16),
    places=st.integers(0, 18),
    count=st.integers(min_value=1, max_value=100),
)
def test_repeated_ticks_preserve_every_source_position(
    coefficient: int, places: int, count: int
) -> None:
    price = Decimal((0, tuple(int(digit) for digit in str(coefficient)), -places))
    _, context = observation(price, places)

    def candidates() -> Iterator[TickCandidate]:
        for position in range(1, count + 1):
            candidate, _ = observation(price, places, position=position)
            yield candidate

    outcomes = list(validate_acquisition(candidates(), context))
    assert len(outcomes) == count
    assert all(outcome.accepted for outcome in outcomes)
    assert [outcome.tick.source_position for outcome in outcomes if outcome.tick] == list(
        range(1, count + 1)
    )
    assert all(
        outcome.tick.price.as_tuple() == price.as_tuple() for outcome in outcomes if outcome.tick
    )


@PROPERTY_SETTINGS
@given(places=st.integers(0, 18), coefficient=st.integers(min_value=1, max_value=10**20))
def test_excess_price_precision_always_rejects_without_rounding(
    places: int, coefficient: int
) -> None:
    price = Decimal((0, tuple(int(digit) for digit in str(coefficient)), -(places + 1)))
    candidate, context = observation(price, places)
    outcome = validate_tick(candidate, context)
    assert outcome.tick is None
    assert outcome.events[0].error_code is ErrorCode.INVALID_PRECISION
    assert candidate.price == price
