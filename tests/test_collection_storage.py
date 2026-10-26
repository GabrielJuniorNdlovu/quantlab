"""Collection recovery uses actual committed PostgreSQL receipts, never queue counts."""

import asyncio
import os
from uuid import uuid4

import pytest
from test_historical import profile

from quantlab.collection import CollectionSettings, LocalHistoricalSource, SimulatedStream
from quantlab.storage.contracts import BatchPolicy, IncompleteDataset
from quantlab.storage.ingestion import IngestionCoordinator
from quantlab.storage.repository import Repository
from quantlab.storage.source_archive import SourceArchive


@pytest.fixture
def dsn():
    value = os.environ.get("QUANTLAB_TEST_DATABASE_URL")
    if not value:
        pytest.skip("requires isolated TimescaleDB")
    return value


def local_source(tmp_path, count=7):
    path = tmp_path / "recorded.csv"
    path.write_bytes(b"symbol,time,quote\n" + b"Boom export,2026-01-01T00:00:00Z,1000.10\n" * count)
    return LocalHistoricalSource(path, profile())


def test_interrupted_stream_resumes_same_capture_and_preserves_repeats(dsn, tmp_path):
    async def run():
        source = local_source(tmp_path)
        source_id = str(uuid4())
        archive = SourceArchive(tmp_path / "archive")
        with Repository(dsn) as repo:
            first = await IngestionCoordinator(
                repo, archive, settings=CollectionSettings(batching=BatchPolicy(2, 10, 2))
            ).collect(source, stream=SimulatedStream(interrupt_after=3), source_id=source_id)
            assert first.manifest.status == "IMPORTING" and first.manifest.observed == 3
            assert first.metrics["queue_high_water"] <= 2
            with pytest.raises(IncompleteDataset):
                tuple(repo.tick_factory(first.manifest.dataset_id)())
        with Repository(dsn) as repo:
            second = await IngestionCoordinator(
                repo, archive, settings=CollectionSettings(batching=BatchPolicy(4, 1000, 1))
            ).collect(source, source_id=source_id)
            assert second.manifest.status == "COMPLETE" and second.manifest.accepted == 7
            assert second.manifest.capture_id == first.manifest.capture_id
            assert second.manifest.dataset_id == first.manifest.dataset_id
            assert second.metrics["recovery_attempts"] == 1
            assert len(tuple(repo.tick_factory(second.manifest.dataset_id)())) == 7
            assert repo.validate_dataset(second.manifest.dataset_id, archive)["valid"]
            assert repo.collection_status()[0]["checkpoint_batch"] == 3
            third = await IngestionCoordinator(repo, archive).collect(source, source_id=source_id)
            assert third.manifest == second.manifest

    asyncio.run(run())


def test_interruption_after_final_commit_can_resume_at_eof(dsn, tmp_path):
    async def run():
        source = local_source(tmp_path, 2)
        source_id = str(uuid4())
        with Repository(dsn) as repo:
            coordinator = IngestionCoordinator(repo, SourceArchive(tmp_path / "archive"))
            first = await coordinator.collect(
                source, stream=SimulatedStream(interrupt_after=2), source_id=source_id
            )
            assert first.manifest.status == "IMPORTING" and first.manifest.accepted == 2
            second = await coordinator.collect(source, source_id=source_id)
            assert (
                second.manifest.status == "COMPLETE"
                and second.manifest.capture_id == first.manifest.capture_id
            )

    asyncio.run(run())


def test_reproducible_parquet_export_roundtrip_and_tamper_detection(dsn, tmp_path):
    from datetime import UTC, datetime

    import pyarrow.parquet as pq

    from quantlab.dataset_export import SplitPlan, export_parquet
    from quantlab.storage.contracts import StorageError

    async def run():
        source = local_source(tmp_path)
        source.path.write_bytes(
            b"symbol,time,quote\n"
            + b"".join(
                f"Boom export,2026-01-0{day}T00:00:00Z,1000.10\n".encode() for day in (1, 1, 2, 3)
            )
        )
        with Repository(dsn) as repo:
            result = await IngestionCoordinator(repo, SourceArchive(tmp_path / "archive")).collect(
                source, source_id=str(uuid4())
            )
            plan = SplitPlan(datetime(2026, 1, 2, tzinfo=UTC), datetime(2026, 1, 3, tzinfo=UTC))
            one = export_parquet(
                repo,
                result.manifest.dataset_id,
                tmp_path / "export1",
                splits=plan,
                gap_threshold_seconds=3600,
            )
            two = export_parquet(
                repo,
                result.manifest.dataset_id,
                tmp_path / "export2",
                splits=plan,
                gap_threshold_seconds=3600,
            )
            assert one == two
            assert (
                export_parquet(
                    repo,
                    result.manifest.dataset_id,
                    tmp_path / "export1",
                    splits=plan,
                    gap_threshold_seconds=3600,
                )
                == one
            )
            for name, count in [("train", 2), ("validation", 1), ("test", 1)]:
                table = pq.ParquetFile(tmp_path / "export1" / f"{name}.parquet").read()
                assert table.num_rows == count
                assert table.column("price_text").to_pylist() == ["1000.10"] * count
                assert (tmp_path / "export1" / f"{name}.parquet").read_bytes() == (
                    tmp_path / "export2" / f"{name}.parquet"
                ).read_bytes()
            assert one["manifest"]["continuity"]["suspected_gaps"] == 2
            artifact = tmp_path / "export1" / "train.parquet"
            artifact.chmod(0o644)
            artifact.write_bytes(b"corrupt")
            with pytest.raises(StorageError, match="checksum"):
                export_parquet(
                    repo,
                    result.manifest.dataset_id,
                    tmp_path / "export1",
                    splits=plan,
                    gap_threshold_seconds=3600,
                )

    asyncio.run(run())


def test_checkpoint_rejects_changed_decimal_representation(dsn, tmp_path):
    from dataclasses import replace

    from quantlab.storage.contracts import StorageError

    async def run():
        source = local_source(tmp_path)
        source_id = str(uuid4())
        with Repository(dsn) as repo:
            coordinator = IngestionCoordinator(repo, SourceArchive(tmp_path / "archive"))
            first = await coordinator.collect(
                source, source_id=source_id, stream=SimulatedStream(interrupt_after=2)
            )
            original = source.candidates

            def changed(stream, context):
                for candidate in original(stream, context):
                    yield replace(candidate, price="1000.1")

            source.candidates = changed
            with pytest.raises(StorageError, match="checkpoint"):
                await coordinator.collect(source, source_id=source_id)
            assert repo.manifest(first.manifest.dataset_id).observed == 2

    asyncio.run(run())


def test_cancel_drains_accepted_queue_and_restarts(dsn, tmp_path):
    async def run():
        source = local_source(tmp_path, 20)
        source_id = str(uuid4())
        with Repository(dsn) as repo:
            coordinator = IngestionCoordinator(
                repo,
                SourceArchive(tmp_path / "archive"),
                settings=CollectionSettings(batching=BatchPolicy(2, 10, 1)),
            )
            reached = asyncio.Event()
            original = repo.write_batch
            loop = asyncio.get_running_loop()

            def write(*args):
                original(*args)
                loop.call_soon_threadsafe(reached.set)

            repo.write_batch = write
            task = asyncio.create_task(
                coordinator.collect(source, source_id=source_id, stream=SimulatedStream(rate=100))
            )
            await asyncio.wait_for(reached.wait(), 10)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            repo.write_batch = original
            status = repo.collection_status()[0]
            assert status["collection_status"] == "PAUSED"
            assert 0 < status["observed"] < 20
            second = await coordinator.collect(source, source_id=source_id)
            assert second.manifest.status == "COMPLETE" and second.manifest.accepted == 20
            assert second.manifest.capture_id == status["capture_id"]

    asyncio.run(run())


def test_invalid_record_is_durable_and_cannot_complete(dsn, tmp_path):
    async def run():
        source = local_source(tmp_path, 3)
        source.path.write_bytes(source.path.read_bytes().replace(b"1000.10", b"-1.00", 1))
        with Repository(dsn) as repo:
            result = await IngestionCoordinator(repo, SourceArchive(tmp_path / "archive")).collect(
                source, source_id=str(uuid4())
            )
            assert result.manifest.status == "FAILED"
            assert result.manifest.rejected == 1 and result.manifest.accepted == 2
            assert repo.collection_status()[0]["collection_status"] == "FAILED"
            with pytest.raises(IncompleteDataset):
                tuple(repo.tick_factory(result.manifest.dataset_id)())

    asyncio.run(run())


def test_real_backend_failure_rolls_back_and_recovers_collection(dsn, tmp_path, monkeypatch):
    import psycopg

    async def run():
        source = local_source(tmp_path, 4)
        killed = []
        with Repository(dsn) as repo:
            from quantlab.storage import repository as storage_repository

            insert = storage_repository.insert_batch

            def terminate(connection, outcome):
                insert(connection, outcome)
                if not killed:
                    killed.append(True)
                    with psycopg.connect(dsn, autocommit=True) as admin:
                        admin.execute(
                            "SELECT pg_terminate_backend(%s)", (connection.info.backend_pid,)
                        )

            monkeypatch.setattr(storage_repository, "insert_batch", terminate)
            result = await IngestionCoordinator(repo, SourceArchive(tmp_path / "archive")).collect(
                source, source_id=str(uuid4())
            )
            assert killed and result.manifest.status == "COMPLETE" and result.manifest.accepted == 4

    asyncio.run(run())


def test_hard_kill_resumes_durable_prefix_in_same_capture(dsn, tmp_path):
    import json
    import subprocess
    import sys
    import time
    from uuid import UUID

    source = local_source(tmp_path, 8)
    config = tmp_path / "profile.json"
    config.write_text(json.dumps(profile().metadata()))
    marker = tmp_path / "committed.json"
    source_id = str(uuid4())
    archive = SourceArchive(tmp_path / "archive")
    code = """
import asyncio,json,os,signal,sys
from pathlib import Path
from quantlab.collection import CollectionSettings,LocalHistoricalSource
from quantlab.historical import HistoricalProfile
from quantlab.storage.ingestion import IngestionCoordinator
from quantlab.storage.contracts import BatchPolicy
from quantlab.storage.repository import Repository
from quantlab.storage.source_archive import SourceArchive
class Pause(Repository):
    def write_batch(self,run,number,outcomes):
        super().write_batch(run,number,outcomes)
        Path(sys.argv[4]).write_text(json.dumps({'dataset':str(run.dataset_id),'capture':str(run.capture_id)}))
        while True:
            signal.pause()
with Pause(os.environ['QUANTLAB_TEST_DATABASE_URL']) as repo:
    source=LocalHistoricalSource(Path(sys.argv[1]),HistoricalProfile.load(Path(sys.argv[2])))
    asyncio.run(IngestionCoordinator(repo,SourceArchive(Path(sys.argv[3])),
        settings=CollectionSettings(batching=BatchPolicy(2,10,1))).collect(source,source_id=sys.argv[5]))
"""
    env = dict(os.environ)
    env.pop("PYTHONPATH", None)
    child = subprocess.Popen(
        [
            sys.executable,
            "-c",
            code,
            str(source.path),
            str(config),
            str(archive.root),
            str(marker),
            source_id,
        ],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        deadline = time.monotonic() + 20
        while not marker.exists() and child.poll() is None and time.monotonic() < deadline:
            time.sleep(0.01)
        assert marker.exists(), "child did not reach durable checkpoint"
        child.kill()
        child.wait(timeout=10)
    finally:
        if child.poll() is None:
            child.kill()
        child.communicate(timeout=10)
    ids = json.loads(marker.read_text())
    with Repository(dsn) as repo:
        assert repo.manifest(UUID(ids["dataset"])).observed == 2
        assert repo.collection_status()[0]["worker_health"] == "liveness_unverified"
        result = asyncio.run(
            IngestionCoordinator(repo, archive).collect(source, source_id=source_id)
        )
        assert result.manifest.capture_id == UUID(ids["capture"])
        assert result.manifest.status == "COMPLETE" and result.manifest.accepted == 8
        assert repo.validate_dataset(result.manifest.dataset_id, archive)["valid"]


def test_writer_failure_records_pause_when_status_database_is_available(dsn, tmp_path, monkeypatch):
    import psycopg

    async def run():
        source = local_source(tmp_path, 8)
        source_id = str(uuid4())
        with Repository(dsn) as repo:
            coordinator = IngestionCoordinator(
                repo,
                SourceArchive(tmp_path / "archive"),
                settings=CollectionSettings(batching=BatchPolicy(1, 10, 1)),
            )
            original = repo.write_batch

            def unavailable(*args):
                raise psycopg.OperationalError("simulated persistent writer outage")

            monkeypatch.setattr(repo, "write_batch", unavailable)
            with pytest.raises(psycopg.OperationalError):
                await coordinator.collect(source, source_id=source_id)
            status = repo.collection_status()[0]
            assert (
                status["collection_status"] == "PAUSED"
                and status["last_error"] == "OperationalError"
            )
            monkeypatch.setattr(repo, "write_batch", original)
            result = await coordinator.collect(source, source_id=source_id)
            assert result.manifest.status == "COMPLETE" and result.manifest.accepted == 8

    asyncio.run(run())


def test_lost_session_ack_does_not_invent_recovery_attempt(dsn, tmp_path, monkeypatch):
    import psycopg

    async def run():
        lost = []
        execute = psycopg.Connection.execute

        def uncertain(connection, query, *args, **kwargs):
            result = execute(connection, query, *args, **kwargs)
            if (
                isinstance(query, str)
                and query.startswith("INSERT INTO collection_sessions")
                and not lost
            ):
                lost.append(True)
                raise psycopg.OperationalError("injected lost session registration acknowledgement")
            return result

        monkeypatch.setattr(psycopg.Connection, "execute", uncertain)
        with Repository(dsn) as repo:
            result = await IngestionCoordinator(repo, SourceArchive(tmp_path / "archive")).collect(
                local_source(tmp_path, 2), source_id=str(uuid4())
            )
            assert lost and result.manifest.status == "COMPLETE"
            assert result.metrics["recovery_attempts"] == 0

    asyncio.run(run())
