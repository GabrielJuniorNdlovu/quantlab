"""Real PostgreSQL recovery checks; container disruption requires explicit opt-in."""

import json
import os
import shutil
import signal
import subprocess
import sys
import time
from contextlib import contextmanager
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo

from quantlab.models import AcquisitionContext, SourceType
from quantlab.storage.contracts import BatchPolicy, IncompleteDataset
from quantlab.storage.ingestion import IngestionCoordinator
from quantlab.storage.repository import Repository, migrate
from quantlab.storage.source_archive import SourceArchive


class _DatabaseURL(str):
    def __repr__(self):
        return "<isolated database>"


@pytest.fixture
def dsn():
    value = os.environ.get("QUANTLAB_TEST_DATABASE_URL")
    if not value:
        pytest.skip("real TimescaleDB requires QUANTLAB_TEST_DATABASE_URL")
    migrate(value)
    return _DatabaseURL(value)


def fixture_file(tmp_path):
    from importlib.resources import files

    path = tmp_path / "engineering.csv"
    path.write_bytes(files("quantlab").joinpath("data/replay_demo.csv").read_bytes())
    return path


def import_fixture(dsn, path, archive, source_id):
    with Repository(dsn) as repo:
        result = IngestionCoordinator(
            repo, SourceArchive(archive), BatchPolicy(max_records=5)
        ).import_csv(
            path,
            provider="failure-test",
            provider_symbol="fixture-symbol",
            pip_size=2,
            source_id=source_id,
        )
        assert result.status == "COMPLETE" and result.accepted == 34
        rows = tuple(repo.tick_factory(result.dataset_id, page_size=3)())
        assert len(rows) == 34 and rows[-1].source_position == 35
        return result, rows


def destructive_project():
    project = os.environ.get("QUANTLAB_TEST_COMPOSE_PROJECT")
    if os.environ.get("QUANTLAB_DESTRUCTIVE_TESTS") != "1" or project not in {
        "quantlab-feature03-agent",
        "quantlab-ci",
    }:
        pytest.skip("container tests require explicit opt-in to the isolated Feature 03 project")
    return project


def compose_prefix(project):
    return [
        "docker",
        "compose",
        "--env-file",
        os.environ.get("QUANTLAB_TEST_COMPOSE_ENV", ".env.feature03"),
        "-p",
        project,
    ]


def compose(project, *args, **kwargs):
    return subprocess.run(
        [*compose_prefix(project), *args],
        check=True,
        capture_output=True,
        timeout=120,
        **kwargs,
    )


def test_hard_killed_import_restarts_without_duplicate_publication(dsn, tmp_path):
    path = fixture_file(tmp_path)
    archive, marker = tmp_path / "archive", tmp_path / "committed.json"
    source_id = f"process-kill-{uuid4()}"
    code = """
import json, os, signal, sys
from pathlib import Path
from quantlab.storage.contracts import BatchPolicy
from quantlab.storage.ingestion import IngestionCoordinator
from quantlab.storage.repository import Repository
from quantlab.storage.source_archive import SourceArchive
class StopAfterCommit(Repository):
    def write_batch(self, run, batch_number, outcomes):
        super().write_batch(run, batch_number, outcomes)
        marker = Path(sys.argv[3])
        partial = marker.with_suffix('.partial')
        partial.write_text(json.dumps({'dataset_id':str(run.dataset_id),
                                       'capture_id':str(run.capture_id)}))
        os.replace(partial, marker)
        while True:
            signal.pause()
with StopAfterCommit(os.environ['QUANTLAB_TEST_DATABASE_URL']) as repo:
    coordinator = IngestionCoordinator(repo, SourceArchive(Path(sys.argv[2])),
                                       BatchPolicy(max_records=5))
    coordinator.import_csv(
        Path(sys.argv[1]),provider='failure-test',provider_symbol='fixture-symbol',pip_size=2,
        source_id=sys.argv[4])
"""
    environment = dict(os.environ)
    environment.pop("PYTHONPATH", None)
    child = subprocess.Popen(
        [sys.executable, "-c", code, str(path), str(archive), str(marker), source_id],
        env=environment,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        deadline = time.monotonic() + 20
        while not marker.exists() and child.poll() is None and time.monotonic() < deadline:
            time.sleep(0.01)
        assert marker.exists(), "child failed to reach its first durable batch"
        identifiers = json.loads(marker.read_text())
        child.kill()
        child.wait(timeout=10)
        assert child.returncode == -signal.SIGKILL
    finally:
        if child.poll() is None:
            child.kill()
        child.communicate(timeout=10)
    from uuid import UUID

    dataset_id, old_capture = map(UUID, (identifiers["dataset_id"], identifiers["capture_id"]))
    with Repository(dsn) as repo:
        assert repo.manifest(dataset_id).status == "IMPORTING"
        with pytest.raises(IncompleteDataset):
            tuple(repo.tick_factory(dataset_id)())
    completed, rows = import_fixture(dsn, path, archive, source_id)
    assert completed.dataset_id == dataset_id and completed.capture_id != old_capture
    with psycopg.connect(dsn) as connection:
        assert connection.execute(
            "SELECT status,observed FROM acquisition_runs WHERE capture_id=%s", (old_capture,)
        ).fetchone() == ("INTERRUPTED", 5)
        assert connection.execute(
            "SELECT count(*) FROM raw_observations WHERE capture_id=%s", (old_capture,)
        ).fetchone() == (5,)
    assert rows[0].source_position == 2


def test_lost_commit_ack_reconnects_and_reconciles_batch_once(dsn, tmp_path, monkeypatch):
    from quantlab.csv_adapter import iter_csv_candidates
    from quantlab.engine import validate_acquisition

    ctx = AcquisitionContext(
        uuid4(), "failure-test", SourceType.HISTORICAL_CSV, "fixture-symbol", 2
    )
    archive = SourceArchive(tmp_path / "archive")
    source = archive.preserve(fixture_file(tmp_path))
    with Repository(dsn) as repo, repo.claim(source, ctx, f"lost-ack-{uuid4()}") as run:
        with source.path.open("rb") as stream:
            batch = tuple(validate_acquisition(iter_csv_candidates(stream, ctx), ctx))[:5]
        connection = repo._conn()
        original = connection.transaction
        injected = False

        @contextmanager
        def lose_ack(*args, **kwargs):
            nonlocal injected
            with original(*args, **kwargs) as transaction:
                yield transaction
            if not injected:
                injected = True
                connection.close()
                raise psycopg.OperationalError("injected lost COMMIT acknowledgement")

        monkeypatch.setattr(connection, "transaction", lose_ack)
        repo.write_batch(run, 1, batch)
        assert injected
        assert repo.manifest(run.dataset_id).observed == 5
        with psycopg.connect(dsn) as admin:
            assert admin.execute(
                "SELECT count(*) FROM ingestion_batches WHERE capture_id=%s", (run.capture_id,)
            ).fetchone() == (1,)
            assert admin.execute(
                "SELECT count(*) FROM market_ticks WHERE capture_id=%s", (run.capture_id,)
            ).fetchone() == (5,)
        repo.fail(run, interrupted=True)


def test_real_backend_termination_rolls_back_then_retries(dsn, tmp_path, monkeypatch):
    from quantlab.csv_adapter import iter_csv_candidates
    from quantlab.engine import validate_acquisition

    ctx = AcquisitionContext(
        uuid4(), "failure-test", SourceType.HISTORICAL_CSV, "fixture-symbol", 2
    )
    source = SourceArchive(tmp_path / "archive").preserve(fixture_file(tmp_path))
    with Repository(dsn) as repo, repo.claim(source, ctx, f"backend-kill-{uuid4()}") as run:
        with source.path.open("rb") as stream:
            batch = tuple(validate_acquisition(iter_csv_candidates(stream, ctx), ctx))[:5]
        from quantlab.storage import repository as storage_repository

        original = storage_repository.insert_batch
        killed = False

        def terminate(connection, outcome):
            nonlocal killed
            original(connection, outcome)
            if not killed:
                killed = True
                with psycopg.connect(dsn, autocommit=True) as admin:
                    assert admin.execute(
                        "SELECT pg_terminate_backend(%s)", (connection.info.backend_pid,)
                    ).fetchone() == (True,)

        monkeypatch.setattr(storage_repository, "insert_batch", terminate)
        repo.write_batch(run, 1, batch)
        assert killed and repo.manifest(run.dataset_id).observed == 5
        with psycopg.connect(dsn) as admin:
            assert admin.execute(
                "SELECT count(*) FROM raw_observations WHERE capture_id=%s", (run.capture_id,)
            ).fetchone() == (5,)
            assert admin.execute(
                "SELECT count(*) FROM ingestion_batches WHERE capture_id=%s", (run.capture_id,)
            ).fetchone() == (1,)
        repo.fail(run, interrupted=True)


def test_abrupt_database_restart_retains_completed_dataset(dsn, tmp_path):
    project = destructive_project()
    result, before = import_fixture(
        dsn, fixture_file(tmp_path), tmp_path / "archive", f"restart-{uuid4()}"
    )
    with Repository(dsn) as repo:
        checkpoint_before = repo.commit_status(result.capture_id)
    container = compose(project, "ps", "-q", "postgres").stdout.decode().strip()
    assert container
    compose(project, "kill", "-s", "SIGKILL", "postgres")
    subprocess.run(["docker", "wait", container], check=True, capture_output=True, timeout=30)
    compose(project, "up", "-d", "--wait")
    with Repository(dsn) as repo:
        after = tuple(repo.tick_factory(result.dataset_id, page_size=2)())
        assert repo.manifest(result.dataset_id) == result
        assert repo.commit_status(result.capture_id) == checkpoint_before
    assert after == before


def test_logical_backup_restore_including_archive(dsn, tmp_path):
    project = destructive_project()
    archive = tmp_path / "archive"
    result, expected = import_fixture(dsn, fixture_file(tmp_path), archive, f"backup-{uuid4()}")
    settings = conninfo_to_dict(dsn)
    user, database = settings.get("user", "quantlab"), settings["dbname"]
    restored = f"quantlab_restore_{uuid4().hex}"
    backup = tmp_path / "database.dump"
    with backup.open("wb") as stream:
        subprocess.run(
            [
                *compose_prefix(project),
                "exec",
                "-T",
                "postgres",
                "pg_dump",
                "-U",
                user,
                "-d",
                database,
                "-Fc",
                "--no-owner",
                "--no-acl",
            ],
            check=True,
            stdout=stream,
            stderr=subprocess.PIPE,
            timeout=120,
        )
    shutil.copytree(archive, tmp_path / "archive-backup")
    with psycopg.connect(dsn, autocommit=True) as admin:
        admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(restored)))
    try:
        compose(
            project,
            "exec",
            "-T",
            "postgres",
            "psql",
            "-U",
            user,
            "-d",
            restored,
            "-v",
            "ON_ERROR_STOP=1",
            "-c",
            "CREATE EXTENSION IF NOT EXISTS timescaledb; SELECT timescaledb_pre_restore();",
        )
        with backup.open("rb") as stream:
            compose(
                project,
                "exec",
                "-T",
                "postgres",
                "pg_restore",
                "-U",
                user,
                "-d",
                restored,
                "--exit-on-error",
                "--no-owner",
                "--no-acl",
                stdin=stream,
            )
        compose(
            project,
            "exec",
            "-T",
            "postgres",
            "psql",
            "-U",
            user,
            "-d",
            restored,
            "-v",
            "ON_ERROR_STOP=1",
            "-c",
            "SELECT timescaledb_post_restore();",
        )
        shutil.rmtree(archive)
        shutil.copytree(tmp_path / "archive-backup", archive)
        from quantlab.storage.contracts import ArchivedSource

        source_path = archive / f"{result.source_sha256}.raw"
        SourceArchive(archive).verify(
            ArchivedSource(result.source_sha256, source_path.stat().st_size, source_path)
        )
        settings["dbname"] = restored
        with Repository(make_conninfo(**settings)) as repo:
            assert repo.manifest(result.dataset_id) == result
            assert tuple(repo.tick_factory(result.dataset_id, page_size=2)()) == expected
    finally:
        with psycopg.connect(dsn, autocommit=True) as admin:
            admin.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(restored)))
