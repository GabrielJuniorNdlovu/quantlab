"""Completed, single-acquisition replay with bounded server-side retrieval."""

from collections.abc import Callable, Iterator
from datetime import datetime
from uuid import UUID

import psycopg
from psycopg.rows import dict_row

from quantlab.models import MarketTick
from quantlab.parsing import parse_timestamp
from quantlab.storage.contracts import IncompleteDataset, StorageError
from quantlab.storage.mapping import tick_row


def tick_factory(
    dsn: str,
    dataset_id: UUID,
    *,
    start_utc: datetime | None = None,
    end_utc: datetime | None = None,
    page_size: int = 1000,
    limit: int | None = None,
) -> Callable[[], Iterator[MarketTick]]:
    if type(page_size) is not int or not 1 <= page_size <= 10000:
        raise StorageError("page_size must be an integer from 1 to 10000")
    if limit is not None and (type(limit) is not int or limit < 1):
        raise StorageError("limit must be positive or absent")
    start = parse_timestamp(start_utc) if start_utc is not None else None
    end = parse_timestamp(end_utc) if end_utc is not None else None
    if start is not None and end is not None and start >= end:
        raise StorageError("range requires start before end")

    def ticks() -> Iterator[MarketTick]:
        with psycopg.connect(
            dsn,
            row_factory=dict_row,
            connect_timeout=5,
            options=(
                "-c timezone=UTC -c default_transaction_read_only=on -c statement_timeout=30000"
            ),
        ) as connection:
            selected = connection.execute(
                "SELECT selected_capture,status FROM dataset_manifests WHERE dataset_id=%s",
                (dataset_id,),
            ).fetchone()
            if selected is None or selected["status"] != "COMPLETE":
                raise IncompleteDataset("replay requires an existing completed dataset")
            capture_id = selected["selected_capture"]
            with connection.cursor(name="quantlab_replay") as cursor:
                cursor.itersize = page_size
                cursor.execute(
                    """SELECT instrument_id,provider_symbol,event_time_utc,price_text,
                    pip_size,source_type,capture_id,source_position,received_at_utc,provider_tick_id
                    FROM market_ticks WHERE capture_id=%s
                    AND (%s::timestamptz IS NULL OR event_time_utc >= %s)
                    AND (%s::timestamptz IS NULL OR event_time_utc < %s)
                    ORDER BY event_time_utc,source_position LIMIT %s""",
                    (capture_id, start, start, end, end, limit),
                )
                while rows := cursor.fetchmany(page_size):
                    for row in rows:
                        yield tick_row(row)

    return ticks
