"""Collection status and checkpoint verification on the existing repository connection."""

from collections.abc import Callable, Iterator
from itertools import islice
from typing import Any
from uuid import UUID

import psycopg
from psycopg.types.json import Jsonb

from quantlab.models import RawObservation, SourceFormat, ValidationOutcome
from quantlab.quality import Severity
from quantlab.storage.contracts import CommitStatus, ImportRun, StorageError
from quantlab.storage.mapping import batch_digest, tick_row, wire_value
from quantlab.storage.queries import read_connection


def resumable(connection: psycopg.Connection[dict[str, Any]], row: dict[str, Any]) -> UUID | None:
    if row["status"] != "IMPORTING" or row["selected_capture"] is None:
        return None
    session = connection.execute(
        "SELECT c.capture_id FROM collection_sessions c JOIN acquisition_runs a USING(capture_id) "
        "WHERE c.capture_id=%s AND a.status='IMPORTING'",
        (row["selected_capture"],),
    ).fetchone()
    return session["capture_id"] if session else None


def begin(
    connection: psycopg.Connection[dict[str, Any]], run: ImportRun, settings: dict[str, object]
) -> tuple[int, dict[str, object]]:
    row = connection.execute(
        "INSERT INTO collection_sessions(capture_id,status,settings) VALUES(%s,'ACTIVE',%s) "
        "ON CONFLICT(capture_id) DO UPDATE SET status='ACTIVE',settings=excluded.settings,"
        "recovery_attempts=collection_sessions.recovery_attempts+"
        "(collection_sessions.settings->>'session_id' IS DISTINCT FROM "
        "excluded.settings->>'session_id')::integer,last_error=NULL,"
        "updated_at=clock_timestamp() RETURNING recovery_attempts,metrics",
        (run.capture_id, Jsonb(settings)),
    ).fetchone()
    assert row is not None
    if not isinstance(row["metrics"], dict):
        raise StorageError("invalid collection metadata; preserve evidence and use a fresh capture")
    return int(row["recovery_attempts"]), row["metrics"]


def checkpoint(
    connection: psycopg.Connection[dict[str, Any]],
    run: ImportRun,
    outcomes: Iterator[ValidationOutcome],
    read_progress: Callable[[UUID], CommitStatus],
) -> CommitStatus:
    """Rebuild validator state from exact receipts; never trust a mutable cursor alone."""
    last = position = observed = accepted = rejected = errors = 0
    last_time = None
    with connection.transaction(), connection.cursor(name="quantlab_checkpoint") as cursor:
        cursor.execute(
            "SELECT * FROM ingestion_batches WHERE capture_id=%s ORDER BY batch_number",
            (run.capture_id,),
        )
        while rows := cursor.fetchmany(100):
            for receipt in rows:
                count = 0
                first = None

                def prefix(
                    limit: int = receipt["observation_count"],
                ) -> Iterator[ValidationOutcome]:
                    nonlocal count, position, first, accepted, rejected, errors, last_time
                    for outcome in islice(outcomes, limit):
                        count += 1
                        position = outcome.raw.source_position
                        if first is None:
                            first = position
                        accepted += outcome.accepted
                        rejected += not outcome.accepted
                        errors += sum(
                            event.severity is not Severity.WARNING for event in outcome.events
                        )
                        if outcome.tick is not None:
                            last_time = outcome.tick.event_time_utc
                        yield outcome

                digest = batch_digest(prefix())
                if (
                    receipt["batch_number"] != last + 1
                    or digest != receipt["digest"]
                    or count != receipt["observation_count"]
                    or first != receipt["first_position"]
                    or position != receipt["last_position"]
                ):
                    raise StorageError("replayed source differs from the durable checkpoint")
                observed += count
                last += 1
    progress = read_progress(run.capture_id)
    if (
        progress.dataset_id != run.dataset_id
        or progress.status != "IMPORTING"
        or (last, position, observed, accepted, rejected, errors, last_time)
        != (
            progress.batch_number,
            progress.source_position,
            progress.observed,
            progress.accepted,
            progress.rejected,
            progress.error_count,
            progress.last_event_time_utc,
        )
    ):
        raise StorageError("checkpoint receipts disagree with acquisition progress")
    return progress


def verify_redelivery(dsn: str, run: ImportRun, outcome: ValidationOutcome) -> None:
    """A writer fence precedes this read; compare all existing source/canonical evidence."""
    raw = outcome.raw
    with read_connection(dsn) as connection:
        row = connection.execute(
            "SELECT * FROM raw_observations WHERE capture_id=%s AND source_position=%s",
            (run.capture_id, raw.source_position),
        ).fetchone()
        if row is None:
            raise StorageError("redelivery has no committed source observation")
        existing_raw = RawObservation(
            row["capture_id"],
            row["source_position"],
            row["provider"],
            SourceFormat(row["source_format"]),
            bytes(row["raw_payload"]),
            row["payload_sha256"],
            row["received_at_utc"],
            row["source_metadata"],
        )
        tick = connection.execute(
            "SELECT * FROM market_ticks WHERE capture_id=%s AND source_position=%s "
            "AND event_time_utc=%s",
            (run.capture_id, raw.source_position, row["accepted_at"]),
        ).fetchone()
        events = connection.execute(
            "SELECT * FROM data_quality_events WHERE capture_id=%s AND source_position=%s "
            "ORDER BY event_id",
            (run.capture_id, raw.source_position),
        ).fetchall()
        if (
            raw.raw_payload != existing_raw.raw_payload
            or wire_value(raw) != wire_value(existing_raw)
            or wire_value(outcome.tick) != (wire_value(tick_row(tick)) if tick else None)
            or wire_value(sorted(outcome.events, key=lambda event: event.event_id))
            != wire_value(events)
        ):
            raise StorageError("redelivery conflicts with committed source evidence")


def update(
    connection: psycopg.Connection[dict[str, Any]],
    run: ImportRun,
    status: str,
    metrics: dict[str, object],
    error: str | None,
) -> None:
    connection.execute(
        "UPDATE collection_sessions SET status=%s,metrics=%s,last_error=%s,"
        "updated_at=clock_timestamp() WHERE capture_id=%s",
        (status, Jsonb(metrics), error, run.capture_id),
    )


def statuses(
    connection: psycopg.Connection[dict[str, Any]], limit: int
) -> tuple[dict[str, Any], ...]:
    if type(limit) is not int or not 1 <= limit <= 10000:
        raise StorageError("limit must be from 1 to 10000")
    return tuple(
        connection.execute(
            "SELECT d.dataset_id,d.source_id,d.status AS dataset_status,a.capture_id,"
            "a.observed,a.accepted,a.rejected,a.error_count,a.last_position,a.last_event_time,"
            "c.status AS collection_status,c.settings,c.metrics,c.recovery_attempts,c.last_error,"
            "c.updated_at,COALESCE(b.number,0) AS checkpoint_batch,"
            "CASE WHEN c.status='ACTIVE' THEN 'liveness_unverified' "
            "ELSE 'not_running' END AS worker_health "
            "FROM collection_sessions c JOIN acquisition_runs a USING(capture_id) "
            "JOIN dataset_manifests d USING(dataset_id) LEFT JOIN LATERAL "
            "(SELECT max(batch_number) AS number FROM ingestion_batches "
            "WHERE capture_id=a.capture_id) b ON true ORDER BY c.updated_at DESC LIMIT %s",
            (limit,),
        ).fetchall()
    )


def quality(dsn: str, dataset_id: UUID, threshold: float | None) -> dict[str, object]:
    from quantlab.continuity import Continuity, TimeRange
    from quantlab.parsing import require_uuid

    require_uuid(dataset_id, "dataset_id")
    continuity = Continuity(TimeRange(), threshold)
    with read_connection(dsn) as connection:
        connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        row = connection.execute(
            "SELECT d.dataset_id,d.source_id,d.source_sha256,d.source_bytes,d.configuration,"
            "a.capture_id,a.status,a.observed,a.accepted,a.rejected,a.error_count,"
            "c.status AS session_status,c.metrics FROM dataset_manifests d "
            "JOIN acquisition_runs a ON a.capture_id=d.selected_capture "
            "LEFT JOIN collection_sessions c USING(capture_id) WHERE d.dataset_id=%s",
            (dataset_id,),
        ).fetchone()
        if row is None:
            raise StorageError("unknown dataset")
        capture = row["capture_id"]
        counts = connection.execute(
            "SELECT count(*) AS valid,min(event_time_utc) AS first_event,"
            "max(event_time_utc) AS last_event FROM market_ticks WHERE capture_id=%s",
            (capture,),
        ).fetchone()
        assert counts is not None
        raw_count = connection.execute(
            "SELECT count(*) AS n FROM raw_observations WHERE capture_id=%s", (capture,)
        ).fetchone()
        assert raw_count is not None
        codes = connection.execute(
            "SELECT error_code,severity,count(*) AS count FROM data_quality_events "
            "WHERE capture_id=%s GROUP BY error_code,severity ORDER BY error_code,severity",
            (capture,),
        ).fetchall()
        diagnostics = connection.execute(
            "SELECT source_position,error_code,severity,description,field_name,observed_value "
            "FROM data_quality_events WHERE capture_id=%s "
            "ORDER BY source_position NULLS FIRST,event_id LIMIT 20",
            (capture,),
        ).fetchall()
        if threshold is not None:
            with connection.cursor(name="quantlab_quality_gaps") as cursor:
                cursor.execute(
                    "SELECT previous,event_time_utc FROM (SELECT event_time_utc,"
                    "lag(event_time_utc) OVER (ORDER BY event_time_utc,source_position) "
                    "AS previous FROM market_ticks WHERE capture_id=%s) t "
                    "WHERE extract(epoch FROM event_time_utc-previous)>%s",
                    (capture, threshold),
                )
                while gaps := cursor.fetchmany(100):
                    for gap in gaps:
                        continuity.suspected += 1
                        if len(continuity.samples) < 20:
                            continuity.samples.append(
                                {
                                    "start": gap["previous"].isoformat(),
                                    "end": gap["event_time_utc"].isoformat(),
                                    "classification": "SUSPECTED",
                                }
                            )
    reconciled = raw_count["n"] == row["observed"] and counts["valid"] == row["accepted"]
    metrics = row["metrics"] or {}
    dropped = metrics.get("dropped", 0)
    classification = (
        "INCONSISTENT"
        if not reconciled
        else "VERIFIED_SOURCE_COMPLETE"
        if row["status"] == "COMPLETE"
        else "INVALID_SOURCE"
        if row["status"] == "FAILED"
        else "PARTIAL"
    )
    return {
        "dataset_id": dataset_id,
        "capture_id": capture,
        "source_id": row["source_id"],
        "provider": row["configuration"]["provider"],
        "source_sha256": row["source_sha256"],
        "source_bytes": row["source_bytes"],
        "status": row["status"],
        "observed": row["observed"],
        "valid": row["accepted"],
        "rejected": row["rejected"],
        "committed": raw_count["n"],
        "error_count": row["error_count"],
        "errors_by_code": codes,
        "diagnostic_samples": diagnostics,
        "first_event": counts["first_event"],
        "last_event": counts["last_event"],
        "counts_reconciled": reconciled,
        "completeness": classification,
        "source_file_complete": row["status"] == "COMPLETE" and reconciled,
        "market_coverage": "UNKNOWN",
        "confirmed_delivery_gaps": dropped,
        "confirmed_delivery_gap_samples": metrics.get("dropped_positions", []),
        "confirmed_market_gaps": [],
        "suspected_gaps": continuity.suspected,
        "suspected_gap_samples": continuity.samples,
        "threshold_seconds": threshold,
        "session_status": row["session_status"],
        "last_process_snapshot": metrics,
        "worker_health": "liveness_unverified"
        if row["session_status"] == "ACTIVE"
        else "not_running",
        "count_basis": "committed source observations; process metrics are last snapshots",
        "interpolation": "none",
    }
