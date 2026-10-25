"""Transactional Timescale persistence, immutable attempts, and publication gates."""

import json
import logging
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import datetime
from typing import TYPE_CHECKING, Any, TypeVar
from uuid import NAMESPACE_URL, UUID, uuid5

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from quantlab.models import (
    AcquisitionContext,
    MarketTick,
    SourceType,
    ValidationOutcome,
)
from quantlab.quality import DataQualityEvent, Severity
from quantlab.storage.contracts import (
    ArchivedSource,
    BusyImport,
    DatasetManifest,
    ImportRun,
    RetryPolicy,
    StorageError,
    configuration,
    configuration_hash,
)
from quantlab.storage.mapping import batch_digest, manifest_row, wire_value
from quantlab.storage.migration_runner import migrate as migrate
from quantlab.storage.replay_repository import tick_factory
from quantlab.storage.source_evidence import verify_complete_source

if TYPE_CHECKING:
    from quantlab.storage.source_archive import SourceArchive

_LOG = logging.getLogger(__name__)
_T = TypeVar("_T")
_MANIFEST = """SELECT d.dataset_id,d.source_sha256,d.status,d.selected_capture AS capture_id,
 d.source_id,COALESCE(a.observed,0) AS observed,COALESCE(a.accepted,0) AS accepted,
 COALESCE(a.rejected,0) AS rejected,COALESCE(a.error_count,0) AS error_count
 FROM dataset_manifests d LEFT JOIN acquisition_runs a ON a.capture_id=d.selected_capture"""


class Repository:
    """One claimed acquisition per instance; caller closes it after its writer joins."""

    def __init__(
        self,
        dsn: str,
        *,
        retry: RetryPolicy | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.dsn = dsn
        self.retry = retry if retry is not None else RetryPolicy()
        self._sleep = sleep
        self._connection: psycopg.Connection[dict[str, Any]] | None = None
        self._lock_key: int | None = None
        self._active: ImportRun | None = None

    def __enter__(self) -> "Repository":
        self._conn()
        return self

    def __exit__(self, *args: object) -> None:
        self.close()

    def close(self) -> None:
        if self._connection is not None:
            self._connection.close()
            self._connection = None

    def _conn(self) -> psycopg.Connection[dict[str, Any]]:
        if self._connection is None or self._connection.closed:
            self._connection = psycopg.connect(
                self.dsn,
                autocommit=True,
                row_factory=dict_row,
                connect_timeout=5,
                options="-c timezone=UTC -c statement_timeout=30000",
            )
            if self._lock_key is not None:
                acquired = self._connection.execute(
                    "SELECT pg_try_advisory_lock(%s) AS acquired", (self._lock_key,)
                ).fetchone()
                if acquired is None or not acquired["acquired"]:
                    self.close()
                    raise BusyImport("another importer acquired the dataset during reconnection")
        return self._connection

    def _retry(self, operation: Callable[[psycopg.Connection[dict[str, Any]]], _T]) -> _T:
        for attempt in range(self.retry.max_attempts):
            try:
                return operation(self._conn())
            except (
                psycopg.OperationalError,
                psycopg.InterfaceError,
                psycopg.errors.SerializationFailure,
                psycopg.errors.DeadlockDetected,
            ) as exc:
                self.close()
                if attempt + 1 == self.retry.max_attempts:
                    raise
                _LOG.warning(
                    json.dumps(
                        {
                            "event": "database_retry",
                            "attempt": attempt + 1,
                            "sqlstate": exc.sqlstate,
                            "exception_type": type(exc).__name__,
                            "dataset_id": str(self._active.dataset_id) if self._active else None,
                            "capture_id": str(self._active.capture_id) if self._active else None,
                        },
                        sort_keys=True,
                    )
                )
                self._sleep(min(10.0, self.retry.backoff_seconds * 2**attempt))
        raise AssertionError("unreachable retry state")

    @contextmanager
    def claim(
        self, source: ArchivedSource, context: AcquisitionContext, source_id: str = "local-csv"
    ) -> Iterator[ImportRun]:
        if self._active is not None:
            raise BusyImport("repository already owns an active import")
        config = configuration(context, source_id)
        config_hash = configuration_hash(config)
        dataset_id = uuid5(
            NAMESPACE_URL, f"quantlab.dataset.v1:{source.sha256}:{config_hash}:{source_id}"
        )
        key = int.from_bytes(dataset_id.bytes[:8], "big", signed=True)
        acquired = (
            self._conn().execute("SELECT pg_try_advisory_lock(%s) AS acquired", (key,)).fetchone()
        )
        if acquired is None or not acquired["acquired"]:
            raise BusyImport("another importer owns this dataset")
        self._lock_key = key
        tried = False

        def register(connection: psycopg.Connection[dict[str, Any]]) -> ImportRun:
            nonlocal tried
            repeated, tried = tried, True
            with connection.transaction():
                connection.execute(
                    """INSERT INTO dataset_manifests(dataset_id,source_sha256,
                 source_bytes,source_path,source_id,configuration,configuration_sha256,status)
                 VALUES(%s,%s,%s,%s,%s,%s,%s,'IMPORTING') ON CONFLICT(dataset_id) DO NOTHING""",
                    (
                        dataset_id,
                        source.sha256,
                        source.byte_count,
                        str(source.path),
                        source_id,
                        Jsonb(config),
                        config_hash,
                    ),
                )
                row = connection.execute(
                    "SELECT * FROM dataset_manifests WHERE dataset_id=%s FOR UPDATE", (dataset_id,)
                ).fetchone()
                assert row is not None
                if row["source_bytes"] != source.byte_count or row["configuration"] != config:
                    raise StorageError(
                        "dataset identity conflicts with stored source configuration"
                    )
                if row["status"] == "COMPLETE":
                    return ImportRun(dataset_id, row["selected_capture"], True)
                existing = connection.execute(
                    "SELECT dataset_id,status FROM acquisition_runs WHERE capture_id=%s",
                    (context.capture_id,),
                ).fetchone()
                if existing is not None:
                    if (
                        repeated
                        and existing["dataset_id"] == dataset_id
                        and existing["status"] == "IMPORTING"
                    ):
                        return ImportRun(dataset_id, context.capture_id)
                    raise StorageError(
                        "capture UUID has already been used; restart with a fresh UUID"
                    )
                connection.execute(
                    "UPDATE acquisition_runs SET status='INTERRUPTED',"
                    "completed_at=clock_timestamp() "
                    "WHERE dataset_id=%s AND status='IMPORTING'",
                    (dataset_id,),
                )
                connection.execute(
                    "INSERT INTO acquisition_runs(capture_id,dataset_id,instrument_id,status) "
                    "VALUES(%s,%s,'BOOM_1000','IMPORTING')",
                    (context.capture_id, dataset_id),
                )
                connection.execute(
                    "UPDATE dataset_manifests SET selected_capture=%s,status='IMPORTING',"
                    "completed_at=NULL WHERE dataset_id=%s",
                    (context.capture_id, dataset_id),
                )
                return ImportRun(dataset_id, context.capture_id)

        try:
            run = self._retry(register)
            self._active = run
            yield run
        finally:
            self._active = None
            self._lock_key = None
            self.close()

    def _require_claim(self, run: ImportRun) -> None:
        if self._active != run or run.already_complete:
            raise StorageError("writes require this repository active import claim")

    def write_batch(
        self, run: ImportRun, batch_number: int, outcomes: tuple[ValidationOutcome, ...]
    ) -> None:
        self._require_claim(run)
        if not outcomes:
            return
        if type(batch_number) is not int or not 1 <= batch_number < 2**63 or len(outcomes) > 10000:
            raise StorageError("batch number/size exceeds supported limits")
        digest = batch_digest(outcomes)

        def write(connection: psycopg.Connection[dict[str, Any]]) -> None:
            with connection.transaction():
                row = connection.execute(
                    """SELECT a.*,d.configuration FROM acquisition_runs a
                 JOIN dataset_manifests d USING(dataset_id) WHERE capture_id=%s FOR UPDATE OF a""",
                    (run.capture_id,),
                ).fetchone()
                assert row is not None
                receipt = connection.execute(
                    "SELECT digest FROM ingestion_batches WHERE capture_id=%s AND batch_number=%s",
                    (run.capture_id, batch_number),
                ).fetchone()
                if receipt is not None:
                    if receipt["digest"] != digest:
                        raise StorageError("batch receipt conflicts with observation contents")
                    return
                if row["status"] != "IMPORTING":
                    raise StorageError("cannot append to a terminal acquisition")
                maximum = connection.execute(
                    "SELECT COALESCE(max(batch_number),0) AS last FROM ingestion_batches "
                    "WHERE capture_id=%s",
                    (run.capture_id,),
                ).fetchone()
                assert maximum is not None
                if batch_number != maximum["last"] + 1:
                    raise StorageError("batch numbers must be contiguous")
                last_position, last_time = row["last_position"], row["last_event_time"]
                rejected = errors = 0
                for outcome in outcomes:
                    last_position, last_time = self._validate(
                        outcome, run, row["configuration"], last_position, last_time
                    )
                    rejected += outcome.tick is None
                    errors += sum(
                        event.severity is not Severity.WARNING for event in outcome.events
                    )
                    self._insert_outcome(connection, outcome)
                connection.execute(
                    """INSERT INTO ingestion_batches(capture_id,batch_number,digest,
                 first_position,last_position,observation_count) VALUES(%s,%s,%s,%s,%s,%s)""",
                    (
                        run.capture_id,
                        batch_number,
                        digest,
                        outcomes[0].raw.source_position,
                        last_position,
                        len(outcomes),
                    ),
                )
                connection.execute(
                    """UPDATE acquisition_runs SET observed=observed+%s,
                 accepted=accepted+%s,rejected=rejected+%s,error_count=error_count+%s,
                 last_position=%s,last_event_time=%s WHERE capture_id=%s""",
                    (
                        len(outcomes),
                        len(outcomes) - rejected,
                        rejected,
                        errors,
                        last_position,
                        last_time,
                        run.capture_id,
                    ),
                )

        self._retry(write)

    @staticmethod
    def _validate(
        outcome: ValidationOutcome,
        run: ImportRun,
        config: dict[str, Any],
        previous: int,
        last_time: Any,
    ) -> tuple[int, Any]:
        raw, tick = outcome.raw, outcome.tick
        source_type = SourceType(config["source_type"])
        if (
            raw.capture_id != run.capture_id
            or raw.provider != config["provider"]
            or raw.source_format != source_type.expected_format
            or not previous < raw.source_position < 2**63
        ):
            raise StorageError("observation context or source position does not match acquisition")
        if any(
            event.capture_id != run.capture_id or event.source_position != raw.source_position
            for event in outcome.events
        ):
            raise StorageError("quality event does not match its raw observation")
        if tick is not None:
            if (
                tick.capture_id != run.capture_id
                or tick.source_position != raw.source_position
                or tick.provider_symbol != config["provider_symbol"]
                or tick.pip_size != config["pip_size"]
                or tick.source_type != source_type
                or (last_time is not None and tick.event_time_utc < last_time)
            ):
                raise StorageError("tick context or event ordering does not match acquisition")
            last_time = tick.event_time_utc
        return raw.source_position, last_time

    @staticmethod
    def _insert_event(
        connection: psycopg.Connection[dict[str, Any]], event: DataQualityEvent
    ) -> None:
        connection.execute(
            """INSERT INTO data_quality_events(event_id,capture_id,source_position,
         error_code,severity,description,field_name,observed_value,detected_at_utc)
         VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
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
            ),
        )

    @classmethod
    def _insert_outcome(
        cls, connection: psycopg.Connection[dict[str, Any]], outcome: ValidationOutcome
    ) -> None:
        raw, tick = outcome.raw, outcome.tick
        connection.execute(
            """INSERT INTO raw_observations(capture_id,source_position,provider,
         source_format,raw_payload,payload_sha256,received_at_utc,source_metadata,accepted_at)
         VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
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
            ),
        )
        if tick is not None:
            connection.execute(
                """INSERT INTO market_ticks(instrument_id,provider_symbol,event_time_utc,
             price,price_text,pip_size,source_type,capture_id,source_position,received_at_utc,provider_tick_id)
             VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
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
                ),
            )
        for event in outcome.events:
            cls._insert_event(connection, event)

    def finish(self, run: ImportRun) -> DatasetManifest:
        self._require_claim(run)

        def finish(connection: psycopg.Connection[dict[str, Any]]) -> None:
            with connection.transaction():
                row = connection.execute(
                    "SELECT * FROM acquisition_runs WHERE capture_id=%s FOR UPDATE",
                    (run.capture_id,),
                ).fetchone()
                assert row is not None
                if row["status"] == "COMPLETE":
                    return
                if row["status"] != "IMPORTING":
                    raise StorageError("cannot finish a terminal failed/interrupted attempt")
                state = (
                    "COMPLETE"
                    if row["observed"] > 0 and row["rejected"] == row["error_count"] == 0
                    else "FAILED"
                )
                if state == "COMPLETE":
                    verify_complete_source(connection, run.capture_id, run.dataset_id)
                connection.execute(
                    "UPDATE acquisition_runs SET status=%s,completed_at=clock_timestamp() "
                    "WHERE capture_id=%s",
                    (state, run.capture_id),
                )
                connection.execute(
                    "UPDATE dataset_manifests SET status=%s,completed_at=clock_timestamp() "
                    "WHERE dataset_id=%s",
                    (state, run.dataset_id),
                )

        self._retry(finish)
        return self.manifest(run.dataset_id)

    def fail(
        self, run: ImportRun, event: DataQualityEvent | None = None, *, interrupted: bool = False
    ) -> None:
        self._require_claim(run)
        if event is not None and (
            event.capture_id != run.capture_id
            or (event.source_position is not None and event.source_position >= 2**63)
        ):
            raise StorageError("failure event does not match acquisition")
        state = "INTERRUPTED" if interrupted else "FAILED"

        def fail(connection: psycopg.Connection[dict[str, Any]]) -> None:
            with connection.transaction():
                row = connection.execute(
                    "SELECT status FROM acquisition_runs WHERE capture_id=%s FOR UPDATE",
                    (run.capture_id,),
                ).fetchone()
                assert row is not None
                if row["status"] == state:
                    return
                if row["status"] != "IMPORTING":
                    raise StorageError("cannot change terminal acquisition status")
                if event is not None:
                    self._insert_event(connection, event)
                connection.execute(
                    "UPDATE acquisition_runs SET status=%s,error_count=error_count+%s,"
                    "completed_at=clock_timestamp() WHERE capture_id=%s",
                    (
                        state,
                        int(event is not None and event.severity is not Severity.WARNING),
                        run.capture_id,
                    ),
                )
                connection.execute(
                    "UPDATE dataset_manifests SET status=%s WHERE dataset_id=%s",
                    (state, run.dataset_id),
                )

        self._retry(fail)

    def validate_dataset(self, dataset_id: UUID, archive: "SourceArchive") -> dict[str, object]:
        from quantlab.storage.dataset_validation import validate_dataset

        return validate_dataset(self.dsn, dataset_id, archive)

    def manifest(self, dataset_id: UUID) -> DatasetManifest:
        row = self._retry(
            lambda con: con.execute(_MANIFEST + " WHERE d.dataset_id=%s", (dataset_id,)).fetchone()
        )
        if row is None:
            raise StorageError("dataset does not exist")
        return manifest_row(row)

    def manifests(self, limit: int = 100) -> tuple[DatasetManifest, ...]:
        if type(limit) is not int or not 1 <= limit <= 10000:
            raise StorageError("manifest limit must be from 1 to 10000")
        rows = self._retry(
            lambda con: con.execute(
                _MANIFEST + " ORDER BY d.created_at DESC LIMIT %s", (limit,)
            ).fetchall()
        )
        return tuple(manifest_row(row) for row in rows)

    def tick_factory(
        self,
        dataset_id: UUID,
        *,
        start_utc: datetime | None = None,
        end_utc: datetime | None = None,
        page_size: int = 1000,
        limit: int | None = None,
    ) -> Callable[[], Iterator[MarketTick]]:
        return tick_factory(
            self.dsn,
            dataset_id,
            start_utc=start_utc,
            end_utc=end_utc,
            page_size=page_size,
            limit=limit,
        )
