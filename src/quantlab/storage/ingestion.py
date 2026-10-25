"""Historical ingestion coordinates existing validation and atomic repository batches."""

import json
import logging
import time
from collections.abc import Mapping
from contextlib import AbstractContextManager
from datetime import datetime
from pathlib import Path
from typing import Protocol
from uuid import UUID, uuid4

from quantlab.csv_adapter import iter_csv_candidates
from quantlab.engine import validate_acquisition
from quantlab.metadata import MetadataValue
from quantlab.models import AcquisitionContext, SourceType, ValidationOutcome
from quantlab.quality import DataQualityEvent, QualityError
from quantlab.storage.contracts import (
    ArchivedSource,
    BatchPolicy,
    DatasetManifest,
    ImportRun,
)
from quantlab.storage.source_archive import SourceArchive

_LOG = logging.getLogger("quantlab.ingestion")


class ImportRepository(Protocol):
    def claim(
        self, source: ArchivedSource, context: AcquisitionContext, source_id: str = "local-csv"
    ) -> AbstractContextManager[ImportRun]: ...
    def write_batch(
        self, run: ImportRun, batch_number: int, outcomes: tuple[ValidationOutcome, ...]
    ) -> None: ...
    def finish(self, run: ImportRun) -> DatasetManifest: ...
    def fail(
        self, run: ImportRun, event: DataQualityEvent | None = None, *, interrupted: bool = False
    ) -> None: ...
    def manifest(self, dataset_id: UUID) -> DatasetManifest: ...


def _log(event: str, **values: object) -> None:
    _LOG.info(json.dumps({"event": event, **values}, sort_keys=True, default=str))


class IngestionCoordinator:
    """Synchronous historical batching; the event writer owns independent idle timing."""

    def __init__(
        self,
        repository: ImportRepository,
        archive: SourceArchive,
        policy: BatchPolicy | None = None,
    ) -> None:
        self.repository = repository
        self.archive = archive
        self.policy = policy if policy is not None else BatchPolicy()

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
        started = time.monotonic()
        source = self.archive.preserve(path)
        with self.repository.claim(source, context, source_id) as run:
            if run.already_complete:
                _log("ingestion_already_complete", dataset_id=run.dataset_id)
                return self.repository.manifest(run.dataset_id)
            _log(
                "ingestion_started",
                dataset_id=run.dataset_id,
                capture_id=run.capture_id,
                source_sha256=source.sha256,
                max_batch_records=self.policy.max_records,
            )
            pending: list[ValidationOutcome] = []
            batch_number = 0
            committed = 0
            accepted = 0
            rejected = 0

            def flush() -> None:
                nonlocal batch_number, committed, accepted, rejected
                if not pending:
                    return
                batch_number += 1
                commit_start = time.monotonic()
                self.repository.write_batch(run, batch_number, tuple(pending))
                committed += len(pending)
                accepted += sum(outcome.accepted for outcome in pending)
                rejected += sum(not outcome.accepted for outcome in pending)
                _log(
                    "ingestion_batch_committed",
                    dataset_id=run.dataset_id,
                    capture_id=run.capture_id,
                    batch_number=batch_number,
                    batch_records=len(pending),
                    durable_observations=committed,
                    accepted=accepted,
                    rejected=rejected,
                    buffered=0,
                    queue_utilization=0,
                    commit_latency_seconds=time.monotonic() - commit_start,
                )
                pending.clear()

            try:
                with self.archive.open_verified(source) as stream:
                    try:
                        outcomes = validate_acquisition(
                            iter_csv_candidates(stream, context), context
                        )
                        for outcome in outcomes:
                            pending.append(outcome)
                            if len(pending) >= self.policy.max_records:
                                flush()
                    except QualityError as exc:
                        flush()
                        self.repository.fail(run, exc.event)
                        _log(
                            "ingestion_validation_failed",
                            dataset_id=run.dataset_id,
                            code=exc.event.error_code.value,
                            durable_observations=committed,
                        )
                        return self.repository.manifest(run.dataset_id)
                flush()
                self.archive.verify(source)
                result = self.repository.finish(run)
                elapsed = time.monotonic() - started
                _log(
                    "ingestion_finished",
                    dataset_id=run.dataset_id,
                    capture_id=run.capture_id,
                    status=result.status,
                    durable_observations=result.observed,
                    accepted=result.accepted,
                    rejected=result.rejected,
                    elapsed_seconds=elapsed,
                    observations_per_second=result.observed / elapsed,
                )
                return result
            except BaseException as exc:
                try:
                    self.repository.fail(run, interrupted=True)
                except Exception as marking_error:
                    _log(
                        "ingestion_state_update_failed",
                        dataset_id=run.dataset_id,
                        exception_type=type(marking_error).__name__,
                    )
                    exc.add_note(
                        "Database state could not be updated; inspect and restart the import."
                    )
                _log(
                    "ingestion_interrupted",
                    dataset_id=run.dataset_id,
                    capture_id=run.capture_id,
                    durable_observations=committed,
                    uncommitted_buffered=len(pending),
                    exception_type=type(exc).__name__,
                )
                raise
