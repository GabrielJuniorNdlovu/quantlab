"""Completed, single-acquisition replay with bounded server-side retrieval."""

from collections.abc import Callable, Iterator
from datetime import datetime
from uuid import UUID

from quantlab.models import MarketTick, RawObservation, SourceFormat
from quantlab.parsing import parse_timestamp, require_position, require_uuid
from quantlab.storage.contracts import IncompleteDataset, StorageError
from quantlab.storage.mapping import tick_row
from quantlab.storage.queries import read_connection


def tick_factory(
    dsn: str,
    dataset_id: UUID,
    *,
    start_utc: datetime | None = None,
    end_utc: datetime | None = None,
    page_size: int = 1000,
    limit: int | None = None,
) -> Callable[[], Iterator[MarketTick]]:
    require_uuid(dataset_id, "dataset_id")
    if type(page_size) is not int or not 1 <= page_size <= 10000:
        raise StorageError("page_size must be an integer from 1 to 10000")
    if limit is not None and (type(limit) is not int or limit < 1):
        raise StorageError("limit must be positive or absent")
    start = parse_timestamp(start_utc) if start_utc is not None else None
    end = parse_timestamp(end_utc) if end_utc is not None else None
    if start is not None and end is not None and start >= end:
        raise StorageError("range requires start before end")

    def ticks() -> Iterator[MarketTick]:
        with read_connection(dsn) as connection:
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


def observation(dsn: str, dataset_id: UUID, source_position: int) -> MarketTick | None:
    """Lookup within one immutable completed capture; raw identity supplies partition time."""
    require_uuid(dataset_id, "dataset_id")
    require_position(source_position)
    if source_position >= 2**63:
        raise StorageError("source position exceeds PostgreSQL BIGINT")
    with read_connection(dsn) as connection:
        selected = connection.execute(
            "SELECT selected_capture,status FROM dataset_manifests WHERE dataset_id=%s",
            (dataset_id,),
        ).fetchone()
        if selected is None or selected["status"] != "COMPLETE":
            raise IncompleteDataset("lookup requires an existing completed dataset")
        capture = selected["selected_capture"]
        raw = connection.execute(
            "SELECT accepted_at FROM raw_observations WHERE capture_id=%s AND source_position=%s",
            (capture, source_position),
        ).fetchone()
        if raw is None or raw["accepted_at"] is None:
            return None
        row = connection.execute(
            "SELECT * FROM market_ticks WHERE event_time_utc=%s AND capture_id=%s "
            "AND source_position=%s",
            (raw["accepted_at"], capture, source_position),
        ).fetchone()
        if row is None:
            raise StorageError("accepted raw observation has no canonical tick")
        return tick_row(row)


def evidence_factory(
    dsn: str,
    dataset_id: UUID,
    *,
    start_utc: datetime | None = None,
    end_utc: datetime | None = None,
    page_size: int = 1000,
) -> Callable[[], Iterator[tuple[RawObservation, MarketTick]]]:
    """Completed canonical observations with their exact source envelopes for export."""
    require_uuid(dataset_id, "dataset_id")
    if type(page_size) is not int or not 1 <= page_size <= 10000:
        raise StorageError("page_size must be an integer from 1 to 10000")
    start = parse_timestamp(start_utc) if start_utc is not None else None
    end = parse_timestamp(end_utc) if end_utc is not None else None
    if start is not None and end is not None and start >= end:
        raise StorageError("range requires start before end")

    def observations() -> Iterator[tuple[RawObservation, MarketTick]]:
        with read_connection(dsn) as connection:
            selected = connection.execute(
                "SELECT selected_capture,status FROM dataset_manifests WHERE dataset_id=%s",
                (dataset_id,),
            ).fetchone()
            if selected is None or selected["status"] != "COMPLETE":
                raise IncompleteDataset("export requires an existing completed dataset")
            with connection.cursor(name="quantlab_export_evidence") as cursor:
                cursor.itersize = page_size
                cursor.execute(
                    "SELECT t.*,r.provider,r.source_format,r.raw_payload,r.payload_sha256,"
                    "r.source_metadata,r.received_at_utc AS raw_received_at_utc "
                    "FROM market_ticks t JOIN raw_observations r "
                    "ON r.capture_id=t.capture_id AND r.source_position=t.source_position "
                    "AND r.accepted_at=t.event_time_utc WHERE t.capture_id=%s "
                    "AND (%s::timestamptz IS NULL OR t.event_time_utc >= %s) "
                    "AND (%s::timestamptz IS NULL OR t.event_time_utc < %s) "
                    "ORDER BY t.event_time_utc,t.source_position",
                    (selected["selected_capture"], start, start, end, end),
                )
                while rows := cursor.fetchmany(page_size):
                    for row in rows:
                        raw = RawObservation(
                            row["capture_id"],
                            row["source_position"],
                            row["provider"],
                            SourceFormat(row["source_format"]),
                            bytes(row["raw_payload"]),
                            row["payload_sha256"],
                            row["raw_received_at_utc"],
                            row["source_metadata"],
                        )
                        yield raw, tick_row(row)

    return observations
