"""Source-independent, incremental canonical validation for one acquisition."""

from collections.abc import Iterable, Iterator, Mapping
from datetime import datetime
from decimal import Decimal

from quantlab.models import (
    AcquisitionContext,
    MarketTick,
    RawObservation,
    TickCandidate,
    ValidationOutcome,
)
from quantlab.parsing import parse_pip_size, parse_price, parse_timestamp, required_text
from quantlab.quality import (
    ContractError,
    DataQualityEvent,
    ErrorCode,
    QualityError,
    Severity,
    quality_event,
)


def normalize_mapped_record(
    raw: RawObservation,
    context: AcquisitionContext,
    mapped: Mapping[str, object],
    instruments: Mapping[str, str],
    *,
    has_provider_symbol: bool = False,
    has_pip_size: bool = False,
) -> TickCandidate:
    """Normalize adapter-extracted fields into a TickCandidate for canonical validation."""
    external = mapped.get("instrument_id")
    instrument = instruments.get(external, "UNMAPPED") if isinstance(external, str) else None
    price = mapped.get("price")
    if type(price) is int:
        price = Decimal(price)
    timestamp = mapped.get("event_time")
    symbol = mapped.get("provider_symbol") if has_provider_symbol else context.provider_symbol
    pip = mapped.get("pip_size") if has_pip_size else context.pip_size
    identifier = mapped.get("provider_tick_id")
    if identifier == "":
        identifier = None
    if identifier is not None:
        try:
            identifier = required_text(identifier, "provider_tick_id")
        except ContractError as exc:
            raise QualityError(
                quality_event(
                    context.capture_id,
                    raw.source_position,
                    ErrorCode.INVALID_SOURCE_METADATA,
                    "provider_tick_id must be valid nonblank UTF-8 text or null",
                    field_name="provider_tick_id",
                )
            ) from exc
    # Wrong scalar types stay invalid; they are never coerced to plausible prices/times.
    return TickCandidate(
        raw,
        instrument,
        symbol if isinstance(symbol, str) else None,
        timestamp if isinstance(timestamp, (str, datetime)) or timestamp is None else "INVALID",
        price if isinstance(price, (str, Decimal)) else None,
        pip if isinstance(pip, (str, int)) else None,
        identifier,
    )


def _reject(
    candidate: TickCandidate,
    context: AcquisitionContext,
    code: ErrorCode,
    description: str,
    field_name: str,
    detected_at_utc: datetime | None,
) -> ValidationOutcome:
    return ValidationOutcome(
        candidate.raw,
        None,
        (
            quality_event(
                context.capture_id,
                candidate.raw.source_position,
                code,
                description,
                field_name=field_name,
                detected_at_utc=detected_at_utc,
            ),
        ),
    )


def validate_tick(
    candidate: TickCandidate,
    context: AcquisitionContext,
    *,
    detected_at_utc: datetime | None = None,
) -> ValidationOutcome:
    """Validate one observed candidate; acquisition ordering is checked separately."""
    raw = candidate.raw
    try:
        if (
            raw.capture_id != context.capture_id
            or raw.provider != context.provider
            or raw.source_format is not context.source_type.expected_format
        ):
            raise ContractError(
                ErrorCode.INVALID_SOURCE_METADATA,
                "raw",
                "raw observation does not match acquisition context",
            )
        instrument_id = required_text(candidate.instrument_id, "instrument_id")
        provider_symbol = required_text(candidate.provider_symbol, "provider_symbol")
        if provider_symbol != context.provider_symbol:
            raise ContractError(
                ErrorCode.INVALID_SOURCE_METADATA,
                "provider_symbol",
                "provider_symbol does not match configured mapping",
            )
        pip_size = parse_pip_size(candidate.pip_size)
        if pip_size != context.pip_size:
            raise ContractError(
                ErrorCode.INVALID_PRECISION,
                "pip_size",
                "pip_size does not match configured quote precision",
            )
        tick = MarketTick(
            instrument_id=instrument_id,
            provider_symbol=provider_symbol,
            event_time_utc=parse_timestamp(candidate.event_time),
            price=parse_price(candidate.price),
            pip_size=pip_size,
            source_type=context.source_type,
            capture_id=context.capture_id,
            source_position=raw.source_position,
            received_at_utc=raw.received_at_utc
            if raw.received_at_utc is not None
            else context.received_at_utc,
            provider_tick_id=candidate.provider_tick_id,
        )
    except ContractError as exc:
        return _reject(
            candidate, context, exc.code, exc.description, exc.field_name, detected_at_utc
        )
    events: tuple[DataQualityEvent, ...] = ()
    if tick.received_at_utc is not None and tick.received_at_utc < tick.event_time_utc:
        events = (
            quality_event(
                context.capture_id,
                raw.source_position,
                ErrorCode.RECEPTION_BEFORE_EVENT,
                "supplied reception time precedes event time; check clock alignment",
                field_name="received_at_utc",
                severity=Severity.WARNING,
                detected_at_utc=detected_at_utc,
            ),
        )
    return ValidationOutcome(raw, tick, events)


class AcquisitionValidator:
    """Yield every outcome using O(1) state, scoped to one acquisition.

    Positions strictly increase, including rejected records. Event times of
    accepted ticks never decrease. Equal times retain source-position order.
    """

    def __init__(
        self, context: AcquisitionContext, *, detected_at_utc: datetime | None = None
    ) -> None:
        self.context = context
        self.detected_at_utc = detected_at_utc
        self.last_position = 0
        self.previous_time: datetime | None = None
        self.seen = False

    def validate(self, candidate: TickCandidate) -> ValidationOutcome:
        context, detected_at_utc = self.context, self.detected_at_utc
        self.seen = True
        position = candidate.raw.source_position
        if candidate.raw.capture_id == context.capture_id:
            if position <= self.last_position:
                return _reject(
                    candidate,
                    context,
                    ErrorCode.INVALID_SOURCE_POSITION,
                    "source_position must strictly increase within an acquisition",
                    "source_position",
                    detected_at_utc,
                )
            self.last_position = position
        outcome = validate_tick(candidate, context, detected_at_utc=detected_at_utc)
        if outcome.tick is not None:
            if self.previous_time is not None and outcome.tick.event_time_utc < self.previous_time:
                return _reject(
                    candidate,
                    context,
                    ErrorCode.OUT_OF_ORDER_EVENT,
                    "event_time_utc is out of order within acquisition",
                    "event_time_utc",
                    detected_at_utc,
                )
            self.previous_time = outcome.tick.event_time_utc
        return outcome

    def finish(self) -> None:
        context, detected_at_utc = self.context, self.detected_at_utc
        if not self.seen:
            raise QualityError(
                quality_event(
                    context.capture_id,
                    None,
                    ErrorCode.EMPTY_DATASET,
                    "empty acquisition: no observations",
                    severity=Severity.FATAL,
                    detected_at_utc=detected_at_utc,
                )
            )


def validate_acquisition(
    candidates: Iterable[TickCandidate],
    context: AcquisitionContext,
    *,
    detected_at_utc: datetime | None = None,
) -> Iterator[ValidationOutcome]:
    """Validate one acquisition incrementally with the same state used by streaming."""
    validator = AcquisitionValidator(context, detected_at_utc=detected_at_utc)
    for candidate in candidates:
        yield validator.validate(candidate)
    validator.finish()
