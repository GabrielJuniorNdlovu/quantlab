"""Independent read snapshots of durable progress and scoped acquisition history."""

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any
from uuid import UUID

import psycopg
from psycopg.rows import dict_row

from quantlab.parsing import require_uuid
from quantlab.storage.contracts import CommitStatus, StorageError

_PROGRESS = """SELECT a.dataset_id,a.capture_id,a.status,
 COALESCE(b.batch_number,0) AS batch_number,a.last_position AS source_position,
 a.observed,a.accepted,a.rejected,a.error_count,a.last_event_time AS last_event_time_utc,
 b.committed_at AS committed_at_utc,b.digest
 FROM acquisition_runs a LEFT JOIN LATERAL (
 SELECT batch_number,committed_at,digest FROM ingestion_batches
 WHERE capture_id=a.capture_id ORDER BY batch_number DESC LIMIT 1) b ON true"""


@contextmanager
def read_connection(dsn: str) -> Iterator[psycopg.Connection[dict[str, Any]]]:
    """A writer's uncommitted changes must never become a durable checkpoint."""
    with psycopg.connect(
        dsn,
        row_factory=dict_row,
        connect_timeout=5,
        options="-c timezone=UTC -c default_transaction_read_only=on -c statement_timeout=30000",
    ) as connection:
        yield connection


def commit_status(dsn: str, capture_id: UUID) -> CommitStatus:
    require_uuid(capture_id)
    with read_connection(dsn) as connection:
        row = connection.execute(_PROGRESS + " WHERE a.capture_id=%s", (capture_id,)).fetchone()
    if row is None:
        raise StorageError("acquisition does not exist")
    return CommitStatus(**row)


def acquisitions(dsn: str, dataset_id: UUID, limit: int) -> tuple[CommitStatus, ...]:
    require_uuid(dataset_id, "dataset_id")
    if type(limit) is not int or not 1 <= limit <= 10000:
        raise StorageError("acquisition limit must be from 1 to 10000")
    with read_connection(dsn) as connection:
        rows = connection.execute(
            _PROGRESS + " WHERE a.dataset_id=%s "
            "ORDER BY a.started_at DESC,a.capture_id DESC LIMIT %s",
            (dataset_id, limit),
        ).fetchall()
    return tuple(CommitStatus(**row) for row in rows)


def storage_usage(dsn: str) -> dict[str, int]:
    """Count physical table/index bytes including Timescale chunks, not just its root."""
    with read_connection(dsn) as connection:
        row = connection.execute("""SELECT pg_database_size(current_database()) AS database_bytes,
            h.table_bytes+h.toast_bytes AS market_table_bytes,h.index_bytes AS market_index_bytes,
            h.total_bytes AS market_total_bytes,
            pg_table_size('raw_observations') AS raw_table_bytes,
            pg_indexes_size('raw_observations') AS raw_index_bytes,
            pg_total_relation_size('data_quality_events') AS quality_total_bytes
            FROM hypertable_detailed_size('market_ticks') h""").fetchone()
    assert row is not None
    return {key: int(value or 0) for key, value in row.items()}
