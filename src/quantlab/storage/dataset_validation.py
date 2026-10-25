"""Read-only, streaming revalidation of archived and committed dataset evidence."""

from collections.abc import Callable, Iterator
from datetime import datetime
from pathlib import Path
from typing import Any, BinaryIO
from uuid import UUID

import psycopg
from psycopg.rows import dict_row

from quantlab.csv_adapter import iter_csv_candidates
from quantlab.engine import validate_acquisition
from quantlab.historical import HistoricalAdapter, HistoricalProfile
from quantlab.models import AcquisitionContext, SourceType, TickCandidate
from quantlab.storage.contracts import (
    ArchivedSource,
    IncompleteDataset,
    StorageError,
    configuration_hash,
    historical_profile,
)
from quantlab.storage.mapping import tick_row, wire_value
from quantlab.storage.source_archive import SourceArchive
from quantlab.storage.source_evidence import verify_complete_source


def validate_dataset(dsn: str, dataset_id: UUID, archive: SourceArchive) -> dict[str, object]:
    """Verify source coverage, raw metadata and canonical values in one read snapshot."""
    with psycopg.connect(
        dsn,
        row_factory=dict_row,
        connect_timeout=5,
        options="-c timezone=UTC -c statement_timeout=30000",
    ) as connection:
        connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        source = connection.execute(
            "SELECT * FROM dataset_manifests WHERE dataset_id=%s", (dataset_id,)
        ).fetchone()
        if source is None:
            raise StorageError("unknown dataset")
        if source["status"] != "COMPLETE":
            raise IncompleteDataset(
                f"dataset status is {source['status']}; cannot validate complete evidence"
            )
        capture = source["selected_capture"]
        verify_complete_source(connection, capture, dataset_id)
        config = source["configuration"]
        if configuration_hash(config) != source["configuration_sha256"]:
            raise StorageError("dataset configuration checksum mismatch")
        candidates: Callable[[BinaryIO, AcquisitionContext], Iterator[TickCandidate]]
        profile = historical_profile(config)
        if profile is not None:
            adapter = HistoricalAdapter(HistoricalProfile.from_mapping(profile))
            context = adapter.context(capture)
            candidates = adapter.candidates
        else:
            context = AcquisitionContext(
                capture,
                config["provider"],
                SourceType(config["source_type"]),
                config["provider_symbol"],
                config["pip_size"],
                datetime.fromisoformat(config["received_at_utc"])
                if config["received_at_utc"]
                else None,
                config["source_metadata"],
            )
            candidates = iter_csv_candidates
        receipt = ArchivedSource(
            source["source_sha256"], source["source_bytes"], Path(source["source_path"])
        )
        with (
            archive.open_verified(receipt) as stream,
            connection.cursor(name="quantlab_validation_rows") as cursor,
            connection.cursor(name="quantlab_validation_quality") as quality,
        ):
            cursor.execute(
                "SELECT r.raw_payload,r.payload_sha256,r.source_metadata,r.source_position,"
                "t.* FROM raw_observations r LEFT JOIN market_ticks t "
                "ON t.capture_id=r.capture_id AND t.source_position=r.source_position "
                "AND t.event_time_utc=r.accepted_at "
                "WHERE r.capture_id=%s ORDER BY r.source_position",
                (capture,),
            )
            quality.execute(
                "SELECT * FROM data_quality_events WHERE capture_id=%s "
                "ORDER BY source_position,event_id",
                (capture,),
            )
            quality.itersize = 100
            stored_events = iter(quality)
            cursor.itersize = 100
            stored = iter(cursor)
            count = 0
            for outcome in validate_acquisition(candidates(stream, context), context):
                row: dict[str, Any] | None = next(stored, None)
                if row is None or not outcome.accepted or outcome.tick is None:
                    raise StorageError(
                        "revalidated source does not match committed accepted observations"
                    )
                if (
                    outcome.raw.source_position != row["source_position"]
                    or outcome.raw.raw_payload != bytes(row["raw_payload"])
                    or outcome.raw.payload_sha256 != row["payload_sha256"]
                    or wire_value(outcome.raw.source_metadata) != row["source_metadata"]
                    or outcome.tick != tick_row(row)
                ):
                    raise StorageError("source revalidation differs from committed evidence")
                for event in sorted(outcome.events, key=lambda e: e.event_id):
                    stored_event = next(stored_events, None)
                    if stored_event is None or wire_value(event) != wire_value(stored_event):
                        raise StorageError(
                            "revalidated quality events differ from committed evidence"
                        )
                count += 1
            if next(stored_events, None) is not None:
                raise StorageError("unexpected committed quality events")
            if next(stored, None) is not None:
                raise StorageError("committed observations exceed archived source")
        run = connection.execute(
            "SELECT observed,accepted,rejected,error_count FROM acquisition_runs "
            "WHERE capture_id=%s",
            (capture,),
        ).fetchone()
        if run is None or (
            run["observed"],
            run["accepted"],
            run["rejected"],
            run["error_count"],
        ) != (count, count, 0, 0):
            raise StorageError("acquisition counters differ from revalidated evidence")
    return {
        "dataset_id": str(dataset_id),
        "capture_id": str(capture),
        "valid": True,
        "observed": count,
        "source_sha256": receipt.sha256,
    }
