"""Actual database imports, source proof and replay across repository restarts."""

import json
import os
from dataclasses import replace
from decimal import Decimal
from io import BytesIO
from uuid import uuid4

import pytest
from test_historical import profile

from quantlab.engine import validate_acquisition
from quantlab.historical import HistoricalAdapter
from quantlab.storage.contracts import BatchPolicy, IncompleteDataset, StorageError
from quantlab.storage.ingestion import IngestionCoordinator
from quantlab.storage.repository import Repository
from quantlab.storage.source_archive import SourceArchive


@pytest.fixture
def dsn():
    value = os.environ.get("QUANTLAB_TEST_DATABASE_URL")
    if not value:
        pytest.skip("requires isolated TimescaleDB")
    return value


def source(tmp_path, format):
    path = tmp_path / ("source." + format)
    record = {"symbol": "Boom export", "time": "2026-01-01T00:00:00Z", "quote": "1000.10"}
    payload = (
        b"symbol,time,quote\r\nBoom export,2026-01-01T00:00:00Z,1000.10\r\n"
        b"Boom export,2026-01-01T00:00:00Z,1000.10\r\n"
        if format == "csv"
        else (json.dumps(record) + "\n" + json.dumps(record)).encode()
    )
    path.write_bytes(payload)
    return path, payload


@pytest.mark.parametrize("format", ["csv", "jsonl"])
def test_import_reimport_validate_restart_replay(dsn, tmp_path, format):
    path, payload = source(tmp_path, format)
    adapter = HistoricalAdapter(profile(format))
    archive = SourceArchive(tmp_path / "archive")
    source_id = str(uuid4())
    with Repository(dsn) as repo:
        coordinator = IngestionCoordinator(repo, archive, BatchPolicy(max_records=1))
        first = coordinator.import_historical(path, adapter=adapter, source_id=source_id)
        assert first.status == "COMPLETE" and first.accepted == 2
        assert coordinator.import_historical(path, adapter=adapter, source_id=source_id) == first
        assert repo.validate_dataset(first.dataset_id, archive)["valid"]
    path.unlink()  # replay and validation use immutable evidence, never the original pathname
    with Repository(dsn) as repo:
        factory = repo.tick_factory(first.dataset_id, page_size=1)
        ticks = tuple(factory())
        from quantlab.replay import ReplayEngine

        engine = ReplayEngine(factory)
        assert engine.step() == ticks[0]
        assert engine.step() == ticks[1]
        assert engine.step() is None
        engine.reset()
        assert engine.step() == ticks[0]
        engine.close()
        assert len(ticks) == 2 and ticks[0].price.as_tuple() == Decimal("1000.10").as_tuple()
        assert ticks[0].event_time_utc == ticks[1].event_time_utc
        assert ticks[0].source_position < ticks[1].source_position
        assert repo.validate_dataset(first.dataset_id, archive)["observed"] == 2
    assert next((tmp_path / "archive").glob("*.raw")).read_bytes() == payload


@pytest.mark.parametrize("format", ["csv", "jsonl"])
def test_prefix_cannot_complete_and_interruption_reimports(dsn, tmp_path, format):
    path, payload = source(tmp_path, format)
    adapter = HistoricalAdapter(profile(format))
    archive = SourceArchive(tmp_path / "archive")
    saved = archive.preserve(path)
    ctx = adapter.context()
    rows = tuple(validate_acquisition(adapter.candidates(BytesIO(payload), ctx), ctx))
    source_id = str(uuid4())
    with Repository(dsn) as repo, repo.claim(saved, ctx, source_id) as run:
        repo.write_batch(run, 1, rows[:1])
        with pytest.raises(StorageError, match="source"):
            repo.finish(run)
        repo.fail(run, interrupted=True)
        with pytest.raises(IncompleteDataset):
            tuple(repo.tick_factory(run.dataset_id)())
    with Repository(dsn) as repo:
        result = IngestionCoordinator(repo, archive).import_historical(
            path, adapter=adapter, source_id=source_id
        )
        assert result.status == "COMPLETE" and result.capture_id != ctx.capture_id
        assert result.accepted == 2


@pytest.mark.parametrize("format", ["csv", "jsonl"])
def test_mapping_change_changes_dataset_identity(dsn, tmp_path, format):
    path, _ = source(tmp_path, format)
    archive = SourceArchive(tmp_path / "archive")
    config = profile(format)
    with Repository(dsn) as repo:
        coordinator = IngestionCoordinator(repo, archive)
        source_id = str(uuid4())
        first = coordinator.import_historical(
            path, adapter=HistoricalAdapter(config), source_id=source_id
        )
        second = coordinator.import_historical(
            path, adapter=HistoricalAdapter(replace(config, pip_size=3)), source_id=source_id
        )
        assert first.status == second.status == "COMPLETE"
        assert first.dataset_id != second.dataset_id


def test_truncated_json_cannot_publish_prefix(dsn, tmp_path):
    path, payload = source(tmp_path, "jsonl")
    path.write_bytes(payload + b'\n{"symbol":')
    archive = SourceArchive(tmp_path / "archive")
    with Repository(dsn) as repo:
        result = IngestionCoordinator(repo, archive, BatchPolicy(max_records=1)).import_historical(
            path, adapter=HistoricalAdapter(profile("jsonl")), source_id=str(uuid4())
        )
        assert result.status == "FAILED" and result.accepted == 2 and result.error_count == 1
        with pytest.raises(IncompleteDataset):
            tuple(repo.tick_factory(result.dataset_id)())
        with pytest.raises(IncompleteDataset):
            repo.validate_dataset(result.dataset_id, archive)


def test_historical_disconnect_rolls_back_and_retries(dsn, tmp_path, monkeypatch):
    import psycopg

    path, _ = source(tmp_path, "jsonl")
    archive = SourceArchive(tmp_path / "archive")
    killed = []
    with Repository(dsn) as repo:
        insert = repo._insert_outcome

        def terminate(connection, outcome):
            insert(connection, outcome)
            if not killed:
                killed.append(True)
                with psycopg.connect(dsn, autocommit=True) as admin:
                    admin.execute("SELECT pg_terminate_backend(%s)", (connection.info.backend_pid,))

        monkeypatch.setattr(repo, "_insert_outcome", terminate)
        result = IngestionCoordinator(repo, archive).import_historical(
            path, adapter=HistoricalAdapter(profile("jsonl")), source_id=str(uuid4())
        )
        assert killed and result.status == "COMPLETE" and result.accepted == 2
        assert repo.validate_dataset(result.dataset_id, archive)["valid"]


def test_archived_checksum_mismatch_is_detected(dsn, tmp_path):
    path, _ = source(tmp_path, "jsonl")
    archive = SourceArchive(tmp_path / "archive")
    with Repository(dsn) as repo:
        result = IngestionCoordinator(repo, archive).import_historical(
            path, adapter=HistoricalAdapter(profile("jsonl")), source_id=str(uuid4())
        )
        receipt = archive.preserve(path)
        receipt.path.chmod(0o644)
        receipt.path.write_bytes(b"changed")
        with pytest.raises(StorageError, match="checksum"):
            repo.validate_dataset(result.dataset_id, archive)


def test_jsonl_process_kill_recovers_new_capture(dsn, tmp_path):
    import subprocess
    import sys
    import time
    from uuid import UUID

    path, _ = source(tmp_path, "jsonl")
    marker = tmp_path / "committed.json"
    config = tmp_path / "profile.json"
    config.write_text(json.dumps(profile("jsonl").metadata()))
    source_id = str(uuid4())
    archive = SourceArchive(tmp_path / "archive")
    code = """
import json,os,signal,sys
from pathlib import Path
from quantlab.historical import HistoricalAdapter,HistoricalProfile
from quantlab.storage.contracts import BatchPolicy
from quantlab.storage.ingestion import IngestionCoordinator
from quantlab.storage.repository import Repository
from quantlab.storage.source_archive import SourceArchive
class Pause(Repository):
    def write_batch(self,run,number,outcomes):
        super().write_batch(run,number,outcomes)
        Path(sys.argv[3]).write_text(json.dumps({'dataset':str(run.dataset_id),'capture':str(run.capture_id)}))
        while True:
            signal.pause()
with Pause(os.environ['QUANTLAB_TEST_DATABASE_URL']) as repo:
    IngestionCoordinator(repo,SourceArchive(Path(sys.argv[2])),BatchPolicy(max_records=1)).import_historical(
        Path(sys.argv[1]),adapter=HistoricalAdapter(HistoricalProfile.load(Path(sys.argv[4]))),source_id=sys.argv[5])
"""
    env = dict(os.environ)
    env.pop("PYTHONPATH", None)
    child = subprocess.Popen(
        [
            sys.executable,
            "-c",
            code,
            str(path),
            str(archive.root),
            str(marker),
            str(config),
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
        assert marker.exists(), "child did not commit its first batch"
        child.kill()
        child.wait(timeout=10)
    finally:
        if child.poll() is None:
            child.kill()
        child.communicate(timeout=10)
    ids = json.loads(marker.read_text())
    with Repository(dsn) as repo:
        assert repo.manifest(UUID(ids["dataset"])).status == "IMPORTING"
        with pytest.raises(IncompleteDataset):
            tuple(repo.tick_factory(UUID(ids["dataset"]))())
        completed = IngestionCoordinator(repo, archive).import_historical(
            path, adapter=HistoricalAdapter(profile("jsonl")), source_id=source_id
        )
        assert completed.status == "COMPLETE" and completed.accepted == 2
        assert completed.capture_id != UUID(ids["capture"])
        assert repo.validate_dataset(completed.dataset_id, archive)["valid"]


@pytest.mark.parametrize("format", ["csv", "jsonl"])
def test_unknown_profile_version_cannot_complete(dsn, tmp_path, format):
    path, payload = source(tmp_path, format)
    archive = SourceArchive(tmp_path / "archive")
    adapter = HistoricalAdapter(profile(format))
    ctx = adapter.context()
    rows = tuple(validate_acquisition(adapter.candidates(BytesIO(payload), ctx), ctx))
    metadata = profile(format).metadata()
    metadata["version"] = "historical.UNKNOWN"
    unknown = replace(ctx, source_metadata={"historical_adapter": metadata})
    with Repository(dsn) as repo, repo.claim(archive.preserve(path), unknown, str(uuid4())) as run:
        repo.write_batch(run, 1, rows)
        with pytest.raises(StorageError, match="unsupported"):
            repo.finish(run)
        repo.fail(run, interrupted=True)


@pytest.mark.parametrize("note", ["provider annotation", {"comment": "provider annotation"}, None])
def test_legacy_provider_notes_do_not_become_adapter_profiles(dsn, tmp_path, note):
    from pathlib import Path

    path = Path(__file__).parents[1] / "src/quantlab/data/replay_demo.csv"
    archive = SourceArchive(tmp_path / "archive")
    with Repository(dsn) as repo:
        result = IngestionCoordinator(repo, archive).import_csv(
            path,
            provider="legacy-fixture",
            provider_symbol="fixture-symbol",
            pip_size=2,
            source_id=str(uuid4()),
            source_metadata={"historical_adapter": note},
        )
        assert result.status == "COMPLETE"
        assert repo.validate_dataset(result.dataset_id, archive)["valid"]
