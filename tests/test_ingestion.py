"""Real-store coordinator tests; all persistence goes to an isolated test instance."""

import os
from pathlib import Path
from uuid import uuid4

import pytest

from quantlab.replay import ReplayEngine
from quantlab.storage.ingestion import IngestionCoordinator
from quantlab.storage.repository import Repository
from quantlab.storage.source_archive import SourceArchive

DSN = os.environ.get("QUANTLAB_TEST_DATABASE_URL")
pytestmark = pytest.mark.skipif(not DSN, reason="requires isolated PostgreSQL/TimescaleDB")
FIXTURE = Path(__file__).parents[1] / "src/quantlab/data/replay_demo.csv"


def ingest(coordinator, path=FIXTURE, *, source_id):
    return coordinator.import_csv(
        path,
        provider="engineering-fixture",
        provider_symbol="fixture-symbol",
        pip_size=2,
        source_id=source_id,
    )


def test_import_idempotence_exact_replay_and_raw_evidence(tmp_path):
    source_id = str(uuid4())
    archive = SourceArchive(tmp_path / "archive")
    with Repository(DSN) as repo:
        coordinator = IngestionCoordinator(repo, archive)
        manifest = ingest(coordinator, source_id=source_id)
        assert manifest.status == "COMPLETE"
        assert (manifest.observed, manifest.accepted, manifest.rejected) == (34, 34, 0)
        renamed = tmp_path / "renamed.csv"
        renamed.write_bytes(FIXTURE.read_bytes())
        repeated = ingest(coordinator, renamed, source_id=source_id)
        assert repeated.dataset_id == manifest.dataset_id
        assert repeated.capture_id == manifest.capture_id
        factory = repo.tick_factory(manifest.dataset_id, page_size=2)
        first = tuple(factory())
        assert len(first) == 34 and first[1].event_time_utc == first[2].event_time_utc
        assert first[2].price.as_tuple().exponent == -2
        assert tuple(factory()) == first
        engine = ReplayEngine(factory)
        assert engine.step() == first[0]
        engine.reset()
        assert engine.step() == first[0]
        engine.close()
        from quantlab.replay import ReplayStatus
        from quantlab.replay_session import ReplaySession

        session = ReplaySession(factory, (), manifest.source_sha256, tmp_path / "audit")
        assert session.snapshot().candles == ()
        initial = session.step()
        assert initial.position == 2 and len(initial.candles) == 1
        assert initial.candles[0].high == initial.candles[0].open
        while session.status is not ReplayStatus.ENDED:
            session.step()
        assert sum(c.observation_count for c in session.snapshot().candles) == 34
        session.reset()
        assert session.step().candles == initial.candles
        session.close()
        receipt = archive.preserve(FIXTURE)
        assert receipt.sha256 == manifest.source_sha256
        assert receipt.path.read_bytes() == FIXTURE.read_bytes()


def test_domain_and_fatal_failures_persist_without_publication(tmp_path):
    with Repository(DSN) as repo:
        coordinator = IngestionCoordinator(repo, SourceArchive(tmp_path / "archive"))
        for text in (
            FIXTURE.read_text().replace("1045.00", "-1.00"),
            FIXTURE.read_text() + '"unterminated',
        ):
            path = tmp_path / "bad.csv"
            path.write_text(text)
            result = ingest(coordinator, path, source_id=str(uuid4()))
            assert result.status == "FAILED" and result.error_count > 0
            with pytest.raises(ValueError):
                tuple(repo.tick_factory(result.dataset_id)())


def test_interrupted_attempt_restarts_preserving_committed_evidence(tmp_path, monkeypatch):
    from quantlab.storage.contracts import BatchPolicy

    source_id = str(uuid4())
    with Repository(DSN) as repo:
        coordinator = IngestionCoordinator(
            repo, SourceArchive(tmp_path / "archive"), BatchPolicy(max_records=2)
        )
        original = repo.write_batch

        def interrupted(*args):
            original(*args)
            raise InterruptedError("injected interruption after durable batch")

        monkeypatch.setattr(repo, "write_batch", interrupted)
        with pytest.raises(InterruptedError):
            ingest(coordinator, source_id=source_id)
        monkeypatch.setattr(repo, "write_batch", original)
        result = ingest(coordinator, source_id=source_id)
        assert result.status == "COMPLETE" and result.observed == 34
        assert len(tuple(repo.tick_factory(result.dataset_id)())) == 34


def test_missing_source_and_invalid_configuration_fail_explicitly(tmp_path):
    with Repository(DSN) as repo:
        coordinator = IngestionCoordinator(repo, SourceArchive(tmp_path / "archive"))
        with pytest.raises(FileNotFoundError):
            ingest(coordinator, tmp_path / "missing", source_id=str(uuid4()))
        with pytest.raises(ValueError):
            coordinator.import_csv(
                FIXTURE, provider="fixture", provider_symbol="fixture-symbol", pip_size=-1
            )
