"""Incremental UTC M1 candles without fabricated gap bars or EOF finalization."""

from dataclasses import dataclass, replace
from datetime import datetime
from decimal import Decimal

from quantlab.models import MarketTick
from quantlab.parsing import (
    parse_timestamp,
    require_decimal_price,
    require_instrument_id,
    require_position,
)
from quantlab.replay import check_tick_order


@dataclass(frozen=True, slots=True)
class Candle:
    instrument_id: str
    opened_at_utc: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    observation_count: int
    complete: bool

    def __post_init__(self) -> None:
        require_instrument_id(self.instrument_id)
        opened = parse_timestamp(self.opened_at_utc)
        if opened.second or opened.microsecond:
            raise ValueError("candle opening time must align to a UTC minute")
        object.__setattr__(self, "opened_at_utc", opened)
        for price in (self.open, self.high, self.low, self.close):
            require_decimal_price(price)
        if not self.low <= min(self.open, self.close) <= max(self.open, self.close) <= self.high:
            raise ValueError("inconsistent candle OHLC bounds")
        require_position(self.observation_count)
        if type(self.complete) is not bool:
            raise ValueError("candle completion must be boolean")


class CandleBuilder:
    """Retain only the forming candle and previous tick for ordering checks."""

    def __init__(self) -> None:
        self._current: Candle | None = None
        self._previous: MarketTick | None = None

    @property
    def current(self) -> Candle | None:
        return self._current

    def consume(self, tick: MarketTick) -> Candle | None:
        """Return a completed prior candle only when a later minute is observed."""
        check_tick_order(self._previous, tick)
        opened = tick.event_time_utc.replace(second=0, microsecond=0)
        current = self._current
        completed = None
        if current is None or current.opened_at_utc != opened:
            if current is not None:
                completed = replace(current, complete=True)
            updated = Candle(
                tick.instrument_id,
                opened,
                tick.price,
                tick.price,
                tick.price,
                tick.price,
                1,
                False,
            )
        else:
            updated = replace(
                current,
                high=max(current.high, tick.price),
                low=min(current.low, tick.price),
                close=tick.price,
                observation_count=current.observation_count + 1,
            )
        self._previous = tick
        self._current = updated
        return completed
