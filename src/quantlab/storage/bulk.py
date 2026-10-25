"""Three bounded COPY passes inside the repository's existing batch transaction."""

from typing import Any

import psycopg
from psycopg.types.json import Jsonb

from quantlab.models import ValidationOutcome
from quantlab.storage.mapping import wire_value


def insert_batch(
    connection: psycopg.Connection[dict[str, Any]], outcomes: tuple[ValidationOutcome, ...]
) -> None:
    """Keep raw rows before dependent ticks; every constraint and trigger still runs."""
    with connection.cursor() as cursor:
        with cursor.copy("""COPY raw_observations(capture_id,source_position,provider,
            source_format,raw_payload,payload_sha256,received_at_utc,source_metadata,accepted_at)
            FROM STDIN""") as copy:
            for outcome in outcomes:
                raw, tick = outcome.raw, outcome.tick
                copy.write_row(
                    (
                        raw.capture_id,
                        raw.source_position,
                        raw.provider,
                        raw.source_format.value,
                        raw.raw_payload,
                        raw.payload_sha256,
                        raw.received_at_utc,
                        Jsonb(wire_value(raw.source_metadata)),
                        tick.event_time_utc if tick else None,
                    )
                )
        with cursor.copy("""COPY market_ticks(instrument_id,provider_symbol,event_time_utc,
            price,price_text,pip_size,source_type,capture_id,source_position,
            received_at_utc,provider_tick_id) FROM STDIN""") as copy:
            for outcome in outcomes:
                tick = outcome.tick
                if tick is not None:
                    copy.write_row(
                        (
                            tick.instrument_id,
                            tick.provider_symbol,
                            tick.event_time_utc,
                            tick.price,
                            str(tick.price),
                            tick.pip_size,
                            tick.source_type.value,
                            tick.capture_id,
                            tick.source_position,
                            tick.received_at_utc,
                            tick.provider_tick_id,
                        )
                    )
        with cursor.copy("""COPY data_quality_events(event_id,capture_id,source_position,
            error_code,severity,description,field_name,observed_value,detected_at_utc)
            FROM STDIN""") as copy:
            for outcome in outcomes:
                for event in outcome.events:
                    copy.write_row(
                        (
                            event.event_id,
                            event.capture_id,
                            event.source_position,
                            event.error_code.value,
                            event.severity.value,
                            event.description,
                            event.field_name,
                            event.observed_value,
                            event.detected_at_utc,
                        )
                    )
