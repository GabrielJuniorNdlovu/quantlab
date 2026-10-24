"""Causal fixture alert issuance; immutable anchors and separate terminal outcomes."""

from dataclasses import dataclass, replace
from datetime import datetime
from decimal import Decimal
from typing import ClassVar
from uuid import UUID

from quantlab.candles import Candle
from quantlab.models import INSTRUMENT_ID, MarketTick
from quantlab.parsing import (
    parse_price,
    parse_timestamp,
    require_position,
    require_uuid,
    required_text,
)


@dataclass(frozen=True, slots=True)
class ReplayAlert:
    schema_version: ClassVar[str] = "replay_alert.v1"
    alert_id: UUID
    instrument_id: str
    issued_at_utc: datetime
    issued_at_source_position: int
    data_cutoff_position: int
    direction: str = "UP"
    alert_source: str = "DETERMINISTIC_FIXTURE"
    model_version: str = "fixture.v1"
    evaluation_horizon: int = 3
    evaluation_horizon_unit: str = "SUBSEQUENT_COMPLETED_M1_CANDLES"
    status: str = "SCHEDULED"

    def __post_init__(self) -> None:
        require_uuid(self.alert_id)
        require_position(self.issued_at_source_position)
        require_position(self.data_cutoff_position)
        if self.data_cutoff_position > self.issued_at_source_position:
            raise ValueError("alert cutoff cannot reference a future observation")
        object.__setattr__(self, "issued_at_utc", parse_timestamp(self.issued_at_utc))
        if (
            self.instrument_id != INSTRUMENT_ID
            or self.direction != "UP"
            or self.alert_source != "DETERMINISTIC_FIXTURE"
        ):
            raise ValueError("only Boom 1000 UP deterministic fixture alerts are supported")
        required_text(self.model_version, "model_version")
        if type(self.evaluation_horizon) is not int or not 1 <= self.evaluation_horizon <= 1000:
            raise ValueError("evaluation_horizon must be an integer from 1 to 1000")
        if self.evaluation_horizon_unit != "SUBSEQUENT_COMPLETED_M1_CANDLES":
            raise ValueError("unsupported evaluation horizon unit")
        if self.status not in ("SCHEDULED", "ISSUED"):
            raise ValueError("original alerts may only be SCHEDULED or ISSUED")


@dataclass(frozen=True, slots=True)
class IssuedAlert:
    alert: ReplayAlert
    candle_open_utc: datetime
    price: Decimal

    def __post_init__(self) -> None:
        if self.alert.status != "ISSUED":
            raise ValueError("an arrow requires an ISSUED alert")
        opened = parse_timestamp(self.candle_open_utc)
        if opened != self.alert.issued_at_utc.replace(second=0, microsecond=0):
            raise ValueError("arrow anchor must match the issuance minute")
        object.__setattr__(self, "candle_open_utc", opened)
        if not isinstance(self.price, Decimal):
            raise ValueError("anchor price must be Decimal")
        parse_price(self.price)


@dataclass(frozen=True, slots=True)
class AlertOutcome:
    schema_version: ClassVar[str] = "alert_outcome.v1"
    alert_id: UUID
    status: str
    successful: bool | None
    evaluated_at_utc: datetime
    evaluated_at_source_position: int
    window_candles: tuple[datetime, ...]
    reason: str

    def __post_init__(self) -> None:
        require_uuid(self.alert_id)
        require_position(self.evaluated_at_source_position)
        object.__setattr__(self, "evaluated_at_utc", parse_timestamp(self.evaluated_at_utc))
        windows = tuple(parse_timestamp(opened) for opened in self.window_candles)
        if (
            len(windows) > 1000
            or any(t.second or t.microsecond for t in windows)
            or any(a >= b for a, b in zip(windows, windows[1:], strict=False))
            or any(t >= self.evaluated_at_utc for t in windows)
        ):
            raise ValueError("outcome windows must be ordered past UTC minutes")
        object.__setattr__(self, "window_candles", windows)
        if not (
            (self.status == "RESOLVED" and type(self.successful) is bool)
            or (self.status == "EXPIRED" and self.successful is None)
        ):
            raise ValueError("resolved outcomes require bool; expired outcomes are unevaluated")
        required_text(self.reason, "reason")


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
