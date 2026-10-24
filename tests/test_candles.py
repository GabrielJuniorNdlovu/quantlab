"""UTC M1 aggregation preserves Decimal values and observed arrival order."""

from dataclasses import FrozenInstanceError, replace
from datetime import timedelta
from decimal import Decimal

import pytest
from test_replay import tick

from quantlab.candles import CandleBuilder


def test_ohlc_equal_times_and_rollover() -> None:
    builder = CandleBuilder()
    assert builder.current is None
    for position, seconds, price in (
        (2, 5, "1000.10"),
        (3, 5, "1002.20"),
        (4, 59, "999.90"),
        (5, 59, "1001.00"),
    ):
        assert builder.consume(tick(position, seconds, price)) is None
    candle = builder.current
    assert candle is not None
    assert (candle.open, candle.high, candle.low, candle.close) == (
        Decimal("1000.10"),
        Decimal("1002.20"),
        Decimal("999.90"),
        Decimal("1001.00"),
    )
    assert candle.observation_count == 4 and not candle.complete
    assert candle.opened_at_utc == tick(2).event_time_utc
    completed = builder.consume(tick(6, 60))
    assert completed is not None and completed.complete
    assert not candle.complete
    assert builder.current is not None and not builder.current.complete
    assert builder.current.opened_at_utc == tick(2).event_time_utc + timedelta(minutes=1)
    with pytest.raises(FrozenInstanceError):
        candle.close = Decimal(0)  # type: ignore[misc]


def test_missing_minutes_are_not_fabricated_or_finalized_at_eof() -> None:
    builder = CandleBuilder()
    builder.consume(tick(2, 0))
    completed = builder.consume(tick(3, 180))
    assert completed is not None and completed.opened_at_utc == tick(2).event_time_utc
    assert builder.current is not None
    assert builder.current.opened_at_utc == tick(2).event_time_utc + timedelta(minutes=3)
    assert not builder.current.complete


def test_exact_price_beyond_float_precision_is_preserved() -> None:
    builder = CandleBuilder()
    value = "123456789012345678901234567890.12"
    builder.consume(tick(2, price=value))
    assert builder.current is not None
    assert str(builder.current.close) == value


@pytest.mark.parametrize(
    "bad", [tick(2), tick(1), tick(3, -1), replace(tick(3), provider_symbol="other")]
)
def test_standalone_builder_rejects_bad_order_without_mutation(bad: object) -> None:
    builder = CandleBuilder()
    builder.consume(tick(2))
    previous = builder.current
    with pytest.raises(ValueError):
        builder.consume(bad)  # type: ignore[arg-type]
    assert builder.current == previous
