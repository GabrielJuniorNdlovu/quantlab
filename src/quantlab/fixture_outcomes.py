"""Legacy constructed-event demonstration, retained only for compatibility.

No research signal producer or core candle logic depends on these fixture outcomes.
"""

from dataclasses import dataclass, replace
from datetime import datetime

from quantlab.candles import Candle
from quantlab.legacy_alerts import AlertOutcome, IssuedAlert, ReplayAlert
from quantlab.models import MarketTick
from quantlab.parsing import require_position


@dataclass(slots=True)
class _Window:
    anchor: IssuedAlert
    completed: tuple[datetime, ...] = ()
    hit: bool = False


class AlertBook:
    """Finite fixture schedule. Only consumed constructed events can affect outcomes."""

    def __init__(
        self, alerts: tuple[ReplayAlert, ...], *, event_positions: frozenset[int] = frozenset()
    ) -> None:
        if len(alerts) > 1000 or len(event_positions) > 1000:
            raise ValueError("fixture schedule and event annotations are limited to 1000 entries")
        ids = [a.alert_id for a in alerts]
        if len(set(ids)) != len(ids):
            raise ValueError("duplicate alert IDs")
        if any(a.status != "SCHEDULED" for a in alerts):
            raise ValueError("input alerts must be SCHEDULED")
        for position in event_positions:
            require_position(position)
        self._scheduled = iter(sorted(alerts, key=lambda a: a.issued_at_source_position))
        self._next = next(self._scheduled, None)
        self._events = event_positions
        self._issued: list[IssuedAlert] = []
        self._outcomes: list[AlertOutcome] = []
        self._active: list[_Window] = []
        self._last_position = 0
        self._finished = False

    @property
    def issued(self) -> tuple[IssuedAlert, ...]:
        return tuple(self._issued)

    @property
    def outcomes(self) -> tuple[AlertOutcome, ...]:
        return tuple(self._outcomes)

    def consume(
        self, tick: MarketTick, completed: Candle | None
    ) -> tuple[tuple[IssuedAlert, ...], tuple[AlertOutcome, ...]]:
        if self._finished or tick.source_position <= self._last_position:
            raise ValueError("alert book requires increasing observations in an open run")
        if self._next is not None and (
            self._next.issued_at_source_position < tick.source_position
            or (
                self._next.issued_at_source_position == tick.source_position
                and self._next.issued_at_utc != tick.event_time_utc
            )
            or self._next.issued_at_utc < tick.event_time_utc
        ):
            raise ValueError("scheduled issuance does not match the current observation")
        self._last_position = tick.source_position
        resolved: list[AlertOutcome] = []
        remaining: list[_Window] = []
        minute = tick.event_time_utc.replace(second=0, microsecond=0)
        for window in self._active:
            if completed is not None and completed.opened_at_utc > window.anchor.candle_open_utc:
                window.completed += (completed.opened_at_utc,)
            if len(window.completed) == window.anchor.alert.evaluation_horizon:
                resolved.append(
                    AlertOutcome(
                        window.anchor.alert.alert_id,
                        "RESOLVED",
                        window.hit,
                        tick.event_time_utc,
                        tick.source_position,
                        window.completed,
                        "constructed fixture event in window"
                        if window.hit
                        else "no constructed fixture event in window",
                    )
                )
            else:
                remaining.append(window)
        self._active = remaining
        issued: list[IssuedAlert] = []
        while (
            self._next is not None and self._next.issued_at_source_position == tick.source_position
        ):
            # Every alert at a shared position must match the same timestamp.
            if self._next.issued_at_utc != tick.event_time_utc:
                raise ValueError("scheduled issuance does not match the current observation")
            anchor = IssuedAlert(replace(self._next, status="ISSUED"), minute, tick.price)
            issued.append(anchor)
            self._active.append(_Window(anchor))
            self._next = next(self._scheduled, None)
        if tick.source_position in self._events:
            for window in self._active:
                if minute > window.anchor.candle_open_utc:
                    window.hit = True
        self._issued.extend(issued)
        self._outcomes.extend(resolved)
        return tuple(issued), tuple(resolved)

    def finish(self, current: MarketTick) -> tuple[AlertOutcome, ...]:
        if self._finished:
            return ()
        if self._next is not None:
            raise ValueError("dataset ended before a scheduled issuance observation")
        outcomes = tuple(
            AlertOutcome(
                w.anchor.alert.alert_id,
                "EXPIRED",
                None,
                current.event_time_utc,
                current.source_position,
                w.completed,
                "insufficient completed future candles",
            )
            for w in self._active
        )
        self._outcomes.extend(outcomes)
        self._active.clear()
        self._finished = True
        return outcomes
