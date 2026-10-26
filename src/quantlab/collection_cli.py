"""CLI mapping for the unified coordinator, quality summaries, and exports."""

import argparse
import asyncio

from quantlab.collection import CollectionSettings, LocalHistoricalSource, SimulatedStream
from quantlab.continuity import TimeRange
from quantlab.historical import HistoricalProfile
from quantlab.storage.contracts import BatchPolicy, RetryPolicy
from quantlab.storage.ingestion import IngestionCoordinator
from quantlab.storage.repository import Repository
from quantlab.storage.source_archive import SourceArchive


def execute_collection(
    args: argparse.Namespace, repo: Repository, archive: SourceArchive
) -> tuple[object, int]:
    if args.operation == "status":
        return repo.collection_status(args.limit), 0
    if args.operation == "quality":
        return repo.dataset_quality(
            args.dataset_id, gap_threshold_seconds=args.gap_threshold_seconds
        ), 0
    if args.operation == "export":
        from quantlab.dataset_export import SplitPlan, export_parquet

        return export_parquet(
            repo,
            args.dataset_id,
            args.output,
            splits=SplitPlan(args.train_end, args.validation_end),
            selection=TimeRange(args.start, args.end),
            gap_threshold_seconds=args.gap_threshold_seconds,
            instrument_id=args.instrument,
        ), 0
    settings = CollectionSettings(
        BatchPolicy(args.batch_size, args.max_wait_ms, args.queue_size),
        RetryPolicy(args.max_attempts),
        not args.no_monitoring,
        not args.no_recovery,
    )
    source = LocalHistoricalSource(args.dataset_path, HistoricalProfile.load(args.config))
    stream = (
        SimulatedStream(
            rate=args.rate,
            interrupt_after=args.interrupt_after,
            disconnect_after=args.disconnect_after,
            redeliver_every=args.redeliver_every,
            drop_after=args.drop_after,
        )
        if args.operation == "stream-simulate"
        else SimulatedStream()
    )
    result = asyncio.run(
        IngestionCoordinator(repo, archive, settings=settings).collect(
            source,
            stream=stream,
            source_id=args.source_id,
            resume=not args.no_resume,
        )
    )
    code = (
        0
        if result.manifest.status == "COMPLETE"
        else 1
        if result.manifest.status == "FAILED"
        else 3
    )
    return (
        result.manifest
        if args.operation == "import"
        else {
            "manifest": result.manifest,
            "metrics": result.metrics,
        }
    ), code
