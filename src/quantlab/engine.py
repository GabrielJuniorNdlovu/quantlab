"""Source-independent, incremental canonical validation for one acquisition."""

from collections.abc import Iterable, Iterator
from datetime import datetime

from quantlab.models import (
    AcquisitionContext,
    MarketTick,
    SourceFormat,
    SourceType,
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
        expected_format = (
            SourceFormat.CSV
            if context.source_type is SourceType.HISTORICAL_CSV
            else SourceFormat.JSON
        )
        if (
            raw.capture_id != context.capture_id
            or raw.provider != context.provider
            or raw.source_format is not expected_format
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


def validate_acquisition(
    candidates: Iterable[TickCandidate],
    context: AcquisitionContext,
    *,
    detected_at_utc: datetime | None = None,
) -> Iterator[ValidationOutcome]:
    """Yield every outcome using O(1) state, scoped to this acquisition call.

    Positions strictly increase, including rejected records. Event times of
    accepted ticks never decrease. Equal times retain source-position order.
    Exhaust successfully and inspect every outcome before finalizing a dataset.
    """
    last_position = 0
    previous_time: datetime | None = None
    seen = False
    for candidate in candidates:
        seen = True
        position = candidate.raw.source_position
        if candidate.raw.capture_id == context.capture_id:
            if position <= last_position:
                yield _reject(
                    candidate,
                    context,
                    ErrorCode.INVALID_SOURCE_POSITION,
                    "source_position must strictly increase within an acquisition",
                    "source_position",
                    detected_at_utc,
                )
                continue
            last_position = position
        outcome = validate_tick(candidate, context, detected_at_utc=detected_at_utc)
        if outcome.tick is not None:
            if previous_time is not None and outcome.tick.event_time_utc < previous_time:
                yield _reject(
                    candidate,
                    context,
                    ErrorCode.OUT_OF_ORDER_EVENT,
                    "event_time_utc is out of order within acquisition",
                    "event_time_utc",
                    detected_at_utc,
                )
                continue
            previous_time = outcome.tick.event_time_utc
        yield outcome
    if not seen:
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
