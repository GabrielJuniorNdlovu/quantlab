"""Bounded chronological replay driven by an explicit presentation clock."""

from collections.abc import Callable, Iterator
from datetime import datetime
from decimal import Decimal
from enum import Enum
from math import isfinite

from quantlab.models import MarketTick


class ReplayStatus(str, Enum):
    PAUSED = "PAUSED"
    PLAYING = "PLAYING"
    ENDED = "ENDED"
    FAILED = "FAILED"


def check_tick_order(previous: MarketTick | None, tick: MarketTick) -> None:
    """Reject an incompatible or unordered canonical stream without sorting it."""
    if not isinstance(tick, MarketTick):
        raise ValueError("replay requires validated MarketTick records")
    if previous is None:
        return
    if (
        tick.capture_id != previous.capture_id
        or tick.provider_symbol != previous.provider_symbol
        or tick.pip_size != previous.pip_size
        or tick.source_type != previous.source_type
    ):
        raise ValueError("replay requires one acquisition and consistent provider mapping")
    if (
        tick.source_position <= previous.source_position
        or tick.event_time_utc < previous.event_time_utc
    ):
        raise ValueError("tick event time must not decrease and source position must increase")


def _positive_speed(value: float) -> Decimal:
    if isinstance(value, bool) or not isfinite(value) or not 0 < value <= 1000:
        raise ValueError("speed must be finite, greater than zero and at most 1000")
    return Decimal(str(value))


class ReplayEngine:
    """One acquisition per run, one private lookahead, no retained tick history.

    `advance` emits at most 1000 observations per call, retaining clock debt.
    `step` consumes exactly one tick and rebases the clock to that observation.
    EOF is detected on the next source read; it never fabricates an observation.
    """

    def __init__(
        self, tick_factory: Callable[[], Iterator[MarketTick]], *, speed: float = 1
    ) -> None:
        self._tick_factory = tick_factory
        self._speed = _positive_speed(speed)
        self._iterator: Iterator[MarketTick] | None = None
        self._pending: MarketTick | None = None
        self._current: MarketTick | None = None
        self._origin: datetime | None = None
        self._available: datetime | None = None
        self._clock_us = Decimal(0)
        self._status = ReplayStatus.PAUSED

    @property
    def status(self) -> ReplayStatus:
        return self._status

    @property
    def current(self) -> MarketTick | None:
        return self._current

    @property
    def available_at_utc(self) -> datetime | None:
        return self._available

    def _availability(self, tick: MarketTick) -> datetime:
        return max(
            tick.event_time_utc,
            tick.received_at_utc or tick.event_time_utc,
            self._available or tick.event_time_utc,
        )

    @property
    def position(self) -> int:
        return 0 if self._current is None else self._current.source_position

    @property
    def speed(self) -> float:
        return float(self._speed)

    @speed.setter
    def speed(self, value: float) -> None:
        self._speed = _positive_speed(value)

    def play(self) -> None:
        if self._status is ReplayStatus.PAUSED:
            self._status = ReplayStatus.PLAYING

    def pause(self) -> None:
        if self._status is ReplayStatus.PLAYING:
            self._status = ReplayStatus.PAUSED

    def _release_source(self) -> None:
        iterator, self._iterator = self._iterator, None
        close = getattr(iterator, "close", None)
        if callable(close):
            close()

    def _next(self) -> MarketTick | None:
        if self._pending is not None:
            return self._pending
        try:
            if self._iterator is None:
                self._iterator = iter(self._tick_factory())
            try:
                tick = next(self._iterator)
            except StopIteration:
                if self._current is None:
                    raise ValueError("empty replay dataset") from None
                self._status = ReplayStatus.ENDED
                self._release_source()
                return None
            check_tick_order(self._current, tick)
            self._pending = tick
            if self._origin is None:
                self._origin = self._availability(tick)
            return tick
        except Exception:
            self._status = ReplayStatus.FAILED
            self._release_source()
            raise

    def _offset_us(self, tick: MarketTick) -> int:
        assert self._origin is not None
        delta = self._availability(tick) - self._origin
        return (delta.days * 86400 + delta.seconds) * 1_000_000 + delta.microseconds

    def _consume(self, tick: MarketTick) -> None:
        self._available = self._availability(tick)
        self._current = tick
        self._pending = None

    def step(self) -> MarketTick | None:
        if self._status in (ReplayStatus.ENDED, ReplayStatus.FAILED):
            return None
        self.pause()
        tick = self._next()
        if tick is not None:
            self._consume(tick)
            self._clock_us = Decimal(self._offset_us(tick))
        return tick

    def advance(self, elapsed_seconds: float, *, max_ticks: int = 1000) -> tuple[MarketTick, ...]:
        """Advance a playing clock, retaining debt beyond the chosen pulse capacity."""
        if type(max_ticks) is not int or not 1 <= max_ticks <= 1000:
            raise ValueError("max_ticks must be an integer from 1 to 1000")
        if (
            isinstance(elapsed_seconds, bool)
            or not isfinite(elapsed_seconds)
            or elapsed_seconds < 0
        ):
            raise ValueError("elapsed_seconds must be finite and nonnegative")
        if self._status is not ReplayStatus.PLAYING:
            return ()
        self._clock_us += Decimal(str(elapsed_seconds)) * self._speed * 1_000_000
        emitted: list[MarketTick] = []
        previous = self._current
        previous_available = self._available
        try:
            while len(emitted) < max_ticks:
                tick = self._next()
                if tick is None or self._offset_us(tick) > self._clock_us:
                    break
                self._consume(tick)
                emitted.append(tick)
        except Exception:
            # A failed batch publishes nothing; observers keep the prior committed position.
            self._current = previous
            self._available = previous_available
            self._pending = None
            raise
        return tuple(emitted)

    def reset(self) -> None:
        """Close the old iterator; the factory is reopened lazily on first use."""
        self._release_source()
        self._pending = None
        self._current = None
        self._origin = None
        self._available = None
        self._clock_us = Decimal(0)
        self._status = ReplayStatus.PAUSED

    def close(self) -> None:
        self._release_source()
        self._pending = None
        if self._status is not ReplayStatus.FAILED:
            self._status = ReplayStatus.ENDED
