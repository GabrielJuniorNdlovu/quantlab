"""Causal fixture alert issuance; immutable anchors and separate terminal outcomes."""

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import ClassVar
from uuid import UUID

from quantlab.parsing import (
    parse_timestamp,
    require_decimal_price,
    require_instrument_id,
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
        require_instrument_id(self.instrument_id)
        if self.direction != "UP" or self.alert_source != "DETERMINISTIC_FIXTURE":
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
        require_decimal_price(self.price)

    @property
    def signal_id(self) -> UUID:
        return self.alert.alert_id

    @property
    def issued_at_utc(self) -> datetime:
        return self.alert.issued_at_utc

    @property
    def input_cutoff_source_position(self) -> int:
        return self.alert.issued_at_source_position

    @property
    def bar_open_time_utc(self) -> datetime:
        return self.candle_open_utc

    @property
    def observed_price_at_issue(self) -> Decimal:
        return self.price


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
