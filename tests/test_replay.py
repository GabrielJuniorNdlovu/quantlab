"""Replay chronology and controls use an injected presentation clock, never sleeps."""

from collections.abc import Iterator
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import UUID

import pytest

from quantlab.models import MarketTick, SourceType
from quantlab.replay import ReplayEngine, ReplayStatus


def tick(position: int, seconds: int = 0, price: str = "1000.00") -> MarketTick:
    return MarketTick(
        "BOOM_1000",
        "fixture-symbol",
        datetime(2026, 1, 1, tzinfo=UTC) + timedelta(seconds=seconds),
        Decimal(price),
        2,
        SourceType.HISTORICAL_CSV,
        UUID("11111111-1111-4111-8111-111111111111"),
        position,
    )


def test_step_is_paused_and_never_exposes_future_ticks() -> None:
    consumed: list[int] = []

    def source() -> Iterator[MarketTick]:
        for value in (tick(2), tick(3), tick(4, 1)):
            consumed.append(value.source_position)
            yield value

    engine = ReplayEngine(source)
    assert engine.current is None and engine.position == 0 and consumed == []
    assert engine.step() == tick(2)
    assert consumed == [2]
    assert engine.position == 2 and engine.status is ReplayStatus.PAUSED
    assert engine.step() == tick(3)
    assert consumed == [2, 3]
    assert engine.step() == tick(4, 1)
    assert engine.step() is None and engine.status is ReplayStatus.ENDED
    assert engine.step() is None
    engine.play()
    assert engine.advance(100) == ()


def test_clock_pause_resume_and_equal_timestamps() -> None:
    engine = ReplayEngine(lambda: iter((tick(2), tick(3), tick(4, 1))))
    assert engine.advance(500) == ()
    engine.play()
    assert engine.advance(0) == (tick(2), tick(3))
    engine.pause()
    assert engine.advance(500) == ()
    assert engine.position == 3
    engine.play()
    assert engine.advance(0.5) == ()
    assert engine.advance(0.5) == (tick(4, 1),)
    assert engine.status is ReplayStatus.ENDED


@pytest.mark.parametrize("speed", [0.1, 1, 3, 1000])
def test_speed_changes_only_presentation_timing(speed: float) -> None:
    values = (tick(2), tick(3), tick(4, 1), tick(5, 10))
    engine = ReplayEngine(lambda: iter(values), speed=speed)
    engine.play()
    assert engine.advance(100 / speed) == values
    assert engine.status is ReplayStatus.ENDED


def test_reset_closes_source_and_returns_same_sequence() -> None:
    closes: list[bool] = []

    def source() -> Iterator[MarketTick]:
        try:
            yield tick(2)
            yield tick(3, 1)
        finally:
            closes.append(True)

    engine = ReplayEngine(source)
    assert engine.step() == tick(2)
    engine.reset()
    assert closes == [True]
    assert engine.current is None and engine.position == 0
    assert engine.status is ReplayStatus.PAUSED
    assert engine.step() == tick(2)
    engine.close()
    assert closes == [True, True]
    engine.close()
    assert closes == [True, True]


@pytest.mark.parametrize("value", [0, -1, float("nan"), float("inf"), 1001, True])
def test_invalid_speed(value: float) -> None:
    with pytest.raises(ValueError, match="speed"):
        ReplayEngine(lambda: iter((tick(2),)), speed=value)


@pytest.mark.parametrize("value", [-1, float("nan"), float("inf"), True])
def test_invalid_elapsed(value: float) -> None:
    engine = ReplayEngine(lambda: iter((tick(2),)))
    engine.play()
    with pytest.raises(ValueError, match="elapsed"):
        engine.advance(value)
    assert engine.current is None


@pytest.mark.parametrize(
    "bad",
    [
        tick(2),
        tick(1),
        tick(3, -1),
        replace(tick(3), capture_id=UUID("22222222-2222-4222-8222-222222222222")),
        replace(tick(3), provider_symbol="other"),
        replace(tick(3), pip_size=3),
        replace(tick(3), source_type=SourceType.HISTORICAL_JSON),
    ],
)
def test_invalid_stream_fails_without_publishing_bad_tick(bad: MarketTick) -> None:
    engine = ReplayEngine(lambda: iter((tick(2), bad)))
    engine.step()
    with pytest.raises(ValueError):
        engine.step()
    assert engine.status is ReplayStatus.FAILED
    assert engine.current == tick(2)
    assert engine.step() is None


def test_empty_dataset_is_explicit_failure() -> None:
    engine = ReplayEngine(lambda: iter(()))
    with pytest.raises(ValueError, match="empty"):
        engine.step()
    assert engine.status is ReplayStatus.FAILED


def test_source_error_marks_run_failed() -> None:
    def source() -> Iterator[MarketTick]:
        yield tick(2)
        raise OSError("corrupt dataset")

    engine = ReplayEngine(source)
    engine.step()
    with pytest.raises(OSError, match="corrupt"):
        engine.step()
    assert engine.status is ReplayStatus.FAILED


def test_pulse_has_bounded_work_and_retains_clock_debt() -> None:
    engine = ReplayEngine(lambda: (tick(i, 1) for i in range(1, 2502)))
    engine.play()
    assert len(engine.advance(100)) == 1000
    assert len(engine.advance(0)) == 1000
    assert len(engine.advance(0)) == 501
    assert engine.status is ReplayStatus.ENDED


def test_fractional_clock_does_not_change_microsecond_order() -> None:
    first = tick(2)
    second = replace(tick(3), event_time_utc=first.event_time_utc + timedelta(microseconds=3))
    engine = ReplayEngine(lambda: iter((first, second)), speed=0.1)
    engine.play()
    assert engine.advance(0) == (first,)
    assert engine.advance(0.00001) == ()
    assert engine.advance(0.00001) == ()
    assert engine.advance(0.00001) == (second,)


def test_failed_batch_does_not_publish_unreturned_observations() -> None:
    engine = ReplayEngine(lambda: iter((tick(2), tick(3), tick(3))))
    engine.play()
    with pytest.raises(ValueError):
        engine.advance(100)
    assert engine.current is None and engine.position == 0
    assert engine.status is ReplayStatus.FAILED


def test_single_observation_pulses_retain_clock_debt_and_order() -> None:
    values = tuple(tick(i, i) for i in range(1, 5))
    engine = ReplayEngine(lambda: iter(values), speed=1000)
    engine.play()
    assert engine.advance(100, max_ticks=1) == values[:1]
    assert engine.current == values[0]
    for expected in values[1:]:
        assert engine.advance(0, max_ticks=1) == (expected,)
        assert engine.current == expected
    assert engine.advance(0, max_ticks=1) == ()
    assert engine.status is ReplayStatus.ENDED


@pytest.mark.parametrize("maximum", [0, -1, 1001, True, 1.5])
def test_invalid_pulse_capacity(maximum: int) -> None:
    engine = ReplayEngine(lambda: iter((tick(2),)))
    engine.play()
    with pytest.raises(ValueError, match="max_ticks"):
        engine.advance(0, max_ticks=maximum)
    assert engine.current is None
