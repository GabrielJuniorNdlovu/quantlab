"""One recorded-source coordinator; F01 validates and F03 owns every transaction."""

import asyncio
import json
import logging
import time
from collections.abc import AsyncGenerator, AsyncIterator, Callable, Iterator, Mapping
from contextlib import AbstractContextManager, asynccontextmanager
from dataclasses import dataclass, replace
from datetime import datetime
from functools import partial
from pathlib import Path
from queue import Full
from typing import BinaryIO, TypeVar
from uuid import UUID, uuid4

from quantlab.collection import (
    CollectionSettings,
    ConnectionState,
    DeliveryMetrics,
    HistoricalSource,
    LocalHistoricalSource,
    MarketDataStream,
    SimulatedStream,
    SourceInterrupted,
)
from quantlab.csv_adapter import iter_csv_candidates
from quantlab.engine import AcquisitionValidator
from quantlab.historical import HistoricalAdapter
from quantlab.metadata import MetadataValue
from quantlab.models import AcquisitionContext, SourceType, TickCandidate, ValidationOutcome
from quantlab.quality import ErrorCode, QualityError, Severity, quality_event
from quantlab.storage.batching import BatchWriter
from quantlab.storage.contracts import (
    ArchivedSource,
    BatchPolicy,
    CommitStatus,
    DatasetManifest,
    ImportRun,
    StorageError,
)
from quantlab.storage.mapping import wire_value
from quantlab.storage.repository import Repository
from quantlab.storage.source_archive import SourceArchive

_LOG = logging.getLogger("quantlab.ingestion")
_T = TypeVar("_T")


async def _joined(operation: Callable[[], _T]) -> _T:
    """Cancellation cannot release resources still owned by a blocking operation."""
    task = asyncio.create_task(asyncio.to_thread(operation))
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError as cancellation:
        while not task.done():
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                continue
            except Exception as error:
                cancellation.add_note(f"in-flight operation failed: {type(error).__name__}")
        if not task.cancelled():
            try:
                task.result()
            except Exception as error:
                cancellation.add_note(f"in-flight operation failed: {type(error).__name__}")
        raise


async def _until_stop(
    events: AsyncGenerator[TickCandidate, None],
    stop: asyncio.Event | None,
) -> AsyncGenerator[TickCandidate, None]:
    if stop is None:
        async for candidate in events:
            yield candidate
        return
    stopping = asyncio.create_task(stop.wait())
    pending: asyncio.Task[TickCandidate] | None = None
    try:
        while not stop.is_set():
            pending = asyncio.create_task(anext(events))
            done, _ = await asyncio.wait((pending, stopping), return_when=asyncio.FIRST_COMPLETED)
            if pending not in done:
                return
            try:
                candidate = pending.result()
            except StopAsyncIteration:
                return
            pending = None
            yield candidate
    finally:
        if pending is not None:
            pending.cancel()
            await asyncio.gather(pending, return_exceptions=True)
        stopping.cancel()
        await asyncio.gather(stopping, return_exceptions=True)


@asynccontextmanager
async def _managed(manager: AbstractContextManager[_T]) -> AsyncIterator[_T]:
    """Bridge the archive/claim contexts, whose exit always releases owned resources."""
    entered = False

    def enter() -> _T:
        nonlocal entered
        value = manager.__enter__()
        entered = True
        return value

    try:
        yield await _joined(enter)
    finally:
        if entered:
            await _joined(lambda: manager.__exit__(None, None, None))


@dataclass(frozen=True, slots=True)
class CollectionResult:
    manifest: DatasetManifest
    metrics: dict[str, object]


@dataclass
class _CanonicalCSVSource:
    path: Path
    acquisition: AcquisitionContext

    def register(self, archive: SourceArchive) -> ArchivedSource:
        return archive.preserve(self.path)

    def context(self, capture_id: UUID | None = None) -> AcquisitionContext:
        return replace(self.acquisition, capture_id=capture_id or self.acquisition.capture_id)

    def candidates(self, stream: BinaryIO, context: AcquisitionContext) -> Iterator[TickCandidate]:
        return iter_csv_candidates(stream, context)


class IngestionCoordinator:
    def __init__(
        self,
        repository: Repository,
        archive: SourceArchive,
        policy: BatchPolicy | None = None,
        *,
        settings: CollectionSettings | None = None,
    ) -> None:
        if policy is not None and settings is not None:
            raise ValueError("supply policy or settings, not both")
        self.repository, self.archive = repository, archive
        self.settings = settings or CollectionSettings(
            batching=policy or BatchPolicy(), retry=repository.retry
        )
        self.policy = self.settings.batching
        repository.retry = self.settings.retry

    def _sync(self, source: HistoricalSource, source_id: str) -> DatasetManifest:
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(self._collect(source, source_id=source_id, legacy=True)).manifest
        raise RuntimeError("inside an event loop, await IngestionCoordinator.collect(source)")

    def import_csv(
        self,
        path: Path,
        *,
        provider: str,
        provider_symbol: str,
        pip_size: int,
        source_id: str = "local-csv",
        received_at_utc: datetime | None = None,
        source_metadata: Mapping[str, MetadataValue] | None = None,
    ) -> DatasetManifest:
        context = AcquisitionContext(
            uuid4(),
            provider,
            SourceType.HISTORICAL_CSV,
            provider_symbol,
            pip_size,
            received_at_utc,
            source_metadata or {},
        )
        return self._sync(_CanonicalCSVSource(path, context), source_id)

    def import_historical(
        self, path: Path, *, adapter: HistoricalAdapter, source_id: str = "local-historical"
    ) -> DatasetManifest:
        source = LocalHistoricalSource(path, adapter.profile)
        source.adapter = adapter
        return self._sync(source, source_id)

    def _verify_delivery(
        self,
        source: HistoricalSource,
        receipt: ArchivedSource,
        context: AcquisitionContext,
        candidate: TickCandidate,
        run: ImportRun,
    ) -> None:
        validator = AcquisitionValidator(context)
        with self.archive.open_verified(receipt) as stream:
            for original in source.candidates(stream, context):
                position = original.raw.source_position
                if position < candidate.raw.source_position:
                    validator.validate(original)
                    continue
                if (
                    position != candidate.raw.source_position
                    or wire_value(candidate) != wire_value(original)
                    or candidate.raw.raw_payload != original.raw.raw_payload
                ):
                    raise StorageError("redelivery differs from registered source interpretation")
                self.repository.verify_redelivery(run, validator.validate(original))
                return
        raise StorageError("redelivery has no registered source observation")

    async def collect(
        self,
        source: HistoricalSource,
        *,
        stream: MarketDataStream | None = None,
        source_id: str = "local-historical",
        resume: bool = True,
        stop: asyncio.Event | None = None,
    ) -> CollectionResult:
        return await self._collect(
            source, stream=stream, source_id=source_id, resume=resume, stop=stop
        )

    async def _collect(
        self,
        source: HistoricalSource,
        *,
        stream: MarketDataStream | None = None,
        source_id: str,
        resume: bool = True,
        stop: asyncio.Event | None = None,
        legacy: bool = False,
    ) -> CollectionResult:
        repo, settings = self.repository, self.settings
        started, retries_before = time.monotonic(), repo.retry_count
        delivery = stream or SimulatedStream()
        delivery_before = delivery.metrics
        receipt = await _joined(lambda: source.register(self.archive))
        async with _managed(
            repo.claim(
                receipt,
                source.context(),
                source_id,
                resume=resume and settings.recovery_enabled and not legacy,
            )
        ) as run:
            if run.already_complete:
                manifest = await _joined(lambda: repo.manifest(run.dataset_id))
                return CollectionResult(manifest, {"already_complete": True})
            writer: BatchWriter | None = None
            progress: CommitStatus | None = None
            batch_number = received = validated = accepted = rejected = redeliveries = 0
            prefix_count = acknowledged = batches = high_water = interruptions = reconnects = 0
            unsubmitted = queued_remaining = buffered_remaining = 0
            last_latency = last_wait = max_lag = 0.0
            last_event: datetime | None = None
            recovery = 0
            coverage_history = DeliveryMetrics(ConnectionState.DISCONNECTED)
            coverage_history_loaded = False
            recent: TickCandidate | None = None
            state, error = "PAUSED", None

            def persist(outcomes: tuple[ValidationOutcome, ...]) -> None:
                nonlocal batch_number
                batch_number += 1
                began = time.monotonic()
                repo.write_batch(run, batch_number, outcomes)
                if settings.monitoring:
                    _LOG.info(
                        json.dumps(
                            {
                                "event": "ingestion_batch_committed",
                                "dataset_id": str(run.dataset_id),
                                "capture_id": str(run.capture_id),
                                "batch_number": batch_number,
                                "batch_records": len(outcomes),
                                "commit_latency_seconds": time.monotonic() - began,
                            },
                            sort_keys=True,
                        )
                    )

            async def drain() -> None:
                nonlocal writer, acknowledged, batches, high_water, last_latency, last_wait, max_lag
                nonlocal queued_remaining, buffered_remaining
                active, writer = writer, None
                if active is not None:
                    try:
                        await _joined(active.close)
                    finally:
                        m = active.metrics
                        queued_remaining, buffered_remaining = m.queued, m.buffered
                        acknowledged += m.committed
                        batches += m.batches
                        high_water = max(high_water, m.queue_high_water)
                        last_latency = m.last_commit_latency_seconds or last_latency
                        last_wait = m.last_batch_wait_seconds or last_wait
                        max_lag = max(max_lag, m.max_acknowledgement_lag_seconds)

            def metrics() -> dict[str, object]:
                source_metrics = delivery.metrics
                elapsed = time.monotonic() - started
                committed = progress.observed if progress is not None else 0
                return {
                    "received": received,
                    "validated": validated,
                    "accepted": accepted,
                    "rejected": rejected,
                    "redeliveries": redeliveries,
                    "persisted": committed,
                    "committed": committed,
                    "acknowledged": prefix_count + acknowledged,
                    "unsubmitted": unsubmitted,
                    "unacknowledged": max(0, received - redeliveries - committed),
                    "queue_depth": queued_remaining,
                    "buffered": buffered_remaining,
                    "queue_high_water": high_water,
                    "queue_capacity": settings.batching.queue_capacity,
                    "commit_latency_seconds": last_latency,
                    "batch_wait_seconds": last_wait,
                    "max_acknowledgement_lag_seconds": max_lag,
                    "persistence_lag_seconds": 0.0 if committed >= validated else None,
                    "batches": batches,
                    "elapsed_seconds": elapsed,
                    "throughput": max(0, validated - prefix_count) / max(elapsed, 1e-9),
                    "source_state": source_metrics.state.value,
                    "recovery_attempts": recovery,
                    "retry_count": repo.retry_count - retries_before,
                    "source_interruptions": coverage_history.interruptions + interruptions,
                    "reconnects": reconnects,
                    "dropped": coverage_history.dropped
                    + source_metrics.dropped
                    - delivery_before.dropped,
                    "dropped_positions": tuple(
                        dict.fromkeys(
                            (
                                *coverage_history.dropped_positions,
                                *(
                                    p
                                    for p in source_metrics.dropped_positions
                                    if p not in delivery_before.dropped_positions
                                ),
                            )
                        )
                    )[:20],
                    "source_coverage_guaranteed": (
                        coverage_history.coverage_guaranteed and source_metrics.coverage_guaranteed
                    ),
                    "coverage_history_loaded": coverage_history_loaded,
                    "last_event_time": last_event.isoformat() if last_event else None,
                    "progress_basis": "last independently verified committed receipt",
                }

            async def refresh() -> None:
                nonlocal progress
                progress = await _joined(lambda: repo.commit_status(run.capture_id))

            try:
                recovery, previous = await _joined(
                    lambda: repo.begin_collection(run, wire_value(settings))
                )
                coverage_history = DeliveryMetrics.from_snapshot(previous)
                coverage_history_loaded = True
                first_attempt = True
                fatal_event = None
                while True:
                    context = source.context(run.capture_id)
                    validator = AcquisitionValidator(context)
                    reconnectable = False
                    try:
                        async with _managed(self.archive.open_verified(receipt)) as input_stream:
                            records = source.candidates(input_stream, context)
                            checkpoint = await _joined(
                                partial(repo.checkpoint, run, map(validator.validate, records))
                            )
                            progress = checkpoint
                            batch_number = checkpoint.batch_number
                            if first_attempt:
                                prefix_count = received = validated = checkpoint.observed
                                accepted, rejected = checkpoint.accepted, checkpoint.rejected
                                first_attempt = False
                            last_event = validator.previous_time
                            writer = BatchWriter(persist, settings.batching)
                            events = delivery.events(records)
                            deliveries = _until_stop(events, stop)
                            try:
                                async for candidate in deliveries:
                                    received += 1
                                    if stop is not None and stop.is_set():
                                        unsubmitted += 1
                                        break
                                    if candidate.raw.source_position <= validator.last_position:
                                        try:
                                            if (
                                                recent is not None
                                                and candidate.raw.source_position
                                                == recent.raw.source_position
                                            ):
                                                # Original ownership lasts until commit.
                                                if (
                                                    wire_value(candidate) != wire_value(recent)
                                                    or candidate.raw.raw_payload
                                                    != recent.raw.raw_payload
                                                ):
                                                    raise StorageError("conflicting redelivery")
                                            else:
                                                await drain()
                                                await _joined(
                                                    partial(
                                                        self._verify_delivery,
                                                        source,
                                                        receipt,
                                                        context,
                                                        candidate,
                                                        run,
                                                    )
                                                )
                                                writer = BatchWriter(persist, settings.batching)
                                        except StorageError as exc:
                                            raise QualityError(
                                                quality_event(
                                                    run.capture_id,
                                                    candidate.raw.source_position,
                                                    ErrorCode.INVALID_SOURCE_POSITION,
                                                    "conflicting source-position redelivery",
                                                    field_name="source_position",
                                                    severity=Severity.FATAL,
                                                )
                                            ) from exc
                                        redeliveries += 1
                                        await asyncio.sleep(0)
                                        continue
                                    outcome = validator.validate(candidate)
                                    validated += 1
                                    accepted += outcome.accepted
                                    rejected += not outcome.accepted
                                    last_event = validator.previous_time
                                    active = writer
                                    assert active is not None
                                    try:
                                        active.submit(outcome, timeout=0)
                                    except Full:
                                        await _joined(partial(active.submit, outcome))
                                    recent = candidate
                                    await asyncio.sleep(0)
                                else:
                                    if stop is None or not stop.is_set():
                                        validator.finish()
                                        if (
                                            delivery.metrics.coverage_guaranteed
                                            and coverage_history.coverage_guaranteed
                                        ):
                                            state = "COMPLETE"
                                        else:
                                            error = "SOURCE_COVERAGE_UNCERTAIN"
                            finally:
                                await deliveries.aclose()
                                await events.aclose()
                    except SourceInterrupted as exc:
                        interruptions += 1
                        error = "SOURCE_INTERRUPTED"
                        reconnectable = (
                            exc.replayable
                            and settings.recovery_enabled
                            and delivery.metrics.coverage_guaranteed
                            and reconnects < settings.retry.max_attempts - 1
                            and (stop is None or not stop.is_set())
                        )
                    except QualityError as exc:
                        fatal_event, error, state = exc.event, exc.event.error_code.value, "FAILED"
                    finally:
                        await drain()
                    await refresh()
                    if not reconnectable:
                        break
                    reconnects += 1
                    error = None
                if fatal_event is not None:
                    await _joined(lambda: repo.fail(run, fatal_event))
                elif state == "COMPLETE":
                    await _joined(lambda: self.archive.verify(receipt))
                    manifest = await _joined(lambda: repo.finish(run))
                    state = manifest.status
                await refresh()
                result = metrics()
                await _joined(lambda: repo.update_collection(run, state, result, error))
                manifest = await _joined(lambda: repo.manifest(run.dataset_id))
            except BaseException as exc:
                error = type(exc).__name__
                try:
                    await drain()
                    if legacy:
                        await _joined(lambda: repo.fail(run, interrupted=True))
                    await refresh()
                    state = "COMPLETE" if progress and progress.status == "COMPLETE" else "PAUSED"
                    if coverage_history_loaded:
                        await _joined(lambda: repo.update_collection(run, state, metrics(), error))
                except BaseException as cleanup_error:
                    exc.add_note(f"collection cleanup failed: {type(cleanup_error).__name__}")
                if settings.monitoring:
                    _LOG.error(
                        json.dumps(
                            {
                                "event": "collection_failed",
                                "dataset_id": str(run.dataset_id),
                                "capture_id": str(run.capture_id),
                                "error": error,
                                **metrics(),
                            },
                            sort_keys=True,
                        )
                    )
                raise
            if settings.monitoring:
                _LOG.info(
                    json.dumps(
                        {
                            "event": "collection_stopped",
                            "dataset_id": str(run.dataset_id),
                            "capture_id": str(run.capture_id),
                            "status": state,
                            "error": error,
                            **result,
                        },
                        sort_keys=True,
                    )
                )
            return CollectionResult(manifest, result)
