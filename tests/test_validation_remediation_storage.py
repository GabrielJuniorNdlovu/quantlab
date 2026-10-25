"""Real PostgreSQL regressions for F01 normalization and database-facing diagnostics."""

import hashlib
import json
import os
from dataclasses import replace
from datetime import datetime, timezone
from uuid import uuid4

import pytest
from test_contracts import candidate, context, raw
from test_historical import profile
from test_validation_remediation import FIELDS, observation

from quantlab.engine import validate_tick
from quantlab.historical import HistoricalAdapter
from quantlab.models import ValidationOutcome
from quantlab.quality import ErrorCode, quality_event, redact_observed_value
from quantlab.storage.contracts import BatchPolicy
from quantlab.storage.ingestion import IngestionCoordinator
from quantlab.storage.repository import Repository
from quantlab.storage.source_archive import SourceArchive


@pytest.fixture
def dsn():
    value = os.environ.get("QUANTLAB_TEST_DATABASE_URL")
    if not value:
        pytest.skip("requires isolated PostgreSQL/TimescaleDB")

    class TestDSN(str):
        def __repr__(self):
            return "<isolated-test-dsn>"

    return TestDSN(value)


@pytest.mark.parametrize("format", ["csv", "jsonl", "stream_jsonl"])
def test_reception_roundtrip_archive_revalidation_and_replay(dsn, tmp_path, format):
    payload, _ = observation(format, "2025-12-31T23:59:59.123456Z")
    fields = FIELDS if format != "stream_jsonl" else {k: "tick." + v for k, v in FIELDS.items()}
    adapter = HistoricalAdapter(profile(format, fields=fields))
    path = tmp_path / "recorded"
    path.write_bytes(payload)
    archive = SourceArchive(tmp_path / "archive")
    with Repository(dsn) as repo:
        coordinator = IngestionCoordinator(repo, archive, BatchPolicy(max_records=1))
        source_id = str(uuid4())
        result = coordinator.import_historical(path, adapter=adapter, source_id=source_id)
        assert result.status == "COMPLETE" and result.accepted == 1
        assert coordinator.import_historical(path, adapter=adapter, source_id=source_id) == result
        (tick,) = tuple(repo.tick_factory(result.dataset_id)())
        assert tick.received_at_utc == datetime(
            2025, 12, 31, 23, 59, 59, 123456, tzinfo=timezone.utc
        )
        assert tick.event_time_utc == datetime(2026, 1, 1, 0, 0, 0, 123456, tzinfo=timezone.utc)
        assert str(tick.price) == "1000.10"
        assert repo.validate_dataset(result.dataset_id, archive)["valid"]
        from quantlab.replay import ReplayEngine

        engine = ReplayEngine(repo.tick_factory(result.dataset_id))
        assert engine.step() == tick
        engine.reset()
        assert engine.step() == tick
        engine.close()
    assert archive.preserve(path).path.read_bytes() == payload


@pytest.mark.parametrize("format", ["csv", "jsonl", "stream_jsonl"])
def test_invalid_reception_is_persisted_as_structured_failed_import(dsn, tmp_path, format):
    fields = FIELDS if format != "stream_jsonl" else {k: "tick." + v for k, v in FIELDS.items()}
    record = {
        "symbol": "Boom export",
        "time": "2026-01-01T00:00:00Z",
        "quote": "1000.10",
        "received": "invalid",
    }
    payload = (
        b"symbol,time,quote,received\nBoom export,2026-01-01T00:00:00Z,1000.10,invalid\n"
        if format == "csv"
        else json.dumps({"tick": record} if format == "stream_jsonl" else record).encode()
    )
    path = tmp_path / "invalid"
    path.write_bytes(payload)
    archive = SourceArchive(tmp_path / "archive")
    with Repository(dsn) as repo:
        result = IngestionCoordinator(repo, archive).import_historical(
            path, adapter=HistoricalAdapter(profile(format, fields=fields)), source_id=str(uuid4())
        )
        assert result.status == "FAILED" and result.accepted == 0 and result.error_count == 1
        import psycopg

        with psycopg.connect(dsn) as connection:
            event = connection.execute(
                "SELECT error_code,field_name,severity FROM data_quality_events "
                "WHERE capture_id=%s",
                (result.capture_id,),
            ).fetchone()
            assert event == ("INVALID_TIMESTAMP", "received_at_utc", "fatal")
        with pytest.raises(ValueError):
            tuple(repo.tick_factory(result.dataset_id)())
    assert archive.preserve(path).path.read_bytes() == payload


@pytest.mark.parametrize(
    "bad,payload", [("bad\x00value", b"bad\x00value"), ("bad\ud800value", b"bad\\ud800value")]
)
def test_explicit_diagnostic_redaction_persists_with_exact_original_evidence(
    dsn, tmp_path, bad, payload
):
    ctx = context(capture_id=uuid4())
    source_raw = raw(
        capture_id=ctx.capture_id,
        raw_payload=payload,
        payload_sha256=hashlib.sha256(payload).hexdigest(),
    )
    # Sanitization is explicit and limited to display diagnostics; raw bytes are untouched.
    event = quality_event(
        ctx.capture_id,
        source_raw.source_position,
        ErrorCode.INVALID_PRICE,
        "invalid source price",
        field_name="price",
        observed_value=redact_observed_value(bad),
    )
    path = tmp_path / "evidence"
    path.write_bytes(payload)
    archive = SourceArchive(tmp_path / "archive")
    with Repository(dsn) as repo, repo.claim(archive.preserve(path), ctx, str(uuid4())) as run:
        repo.write_batch(run, 1, (ValidationOutcome(source_raw, None, (event,)),))
        assert repo.finish(run).status == "FAILED"
    import psycopg

    with psycopg.connect(dsn) as connection:
        stored = connection.execute(
            "SELECT r.raw_payload,q.observed_value FROM raw_observations r "
            "JOIN data_quality_events q USING(capture_id,source_position) WHERE capture_id=%s",
            (ctx.capture_id,),
        ).fetchone()
        assert bytes(stored[0]) == payload and stored[1] == event.observed_value
    assert archive.preserve(path).path.read_bytes() == payload


def test_valid_unicode_metadata_and_diagnostics_persist_without_normalization(dsn, tmp_path):
    ctx = context(capture_id=uuid4(), source_metadata={"price_basis": "bid", "note": "café — 東京"})
    source_raw = raw(capture_id=ctx.capture_id, source_metadata=ctx.source_metadata)
    outcome = validate_tick(candidate(raw=source_raw, price="bad"), ctx)
    event = replace(outcome.events[0], description="Prix invalide — 東京", observed_value="café")
    outcome = replace(outcome, events=(event,))
    path = tmp_path / "source"
    path.write_bytes(source_raw.raw_payload)
    archive = SourceArchive(tmp_path / "archive")
    with Repository(dsn) as repo, repo.claim(archive.preserve(path), ctx, str(uuid4())) as run:
        repo.write_batch(run, 1, (outcome,))
        assert repo.finish(run).status == "FAILED"
    import psycopg

    with psycopg.connect(dsn) as connection:
        row = connection.execute(
            "SELECT r.source_metadata,q.description,q.observed_value "
            "FROM raw_observations r JOIN data_quality_events q USING(capture_id,source_position) "
            "WHERE capture_id=%s",
            (ctx.capture_id,),
        ).fetchone()
        assert row == (
            {"price_basis": "bid", "note": "café — 東京"},
            "Prix invalide — 東京",
            "café",
        )


def test_pre_fix_csv_dataset_is_preserved_and_reimported_under_new_identity(
    dsn, tmp_path, monkeypatch
):
    from io import BytesIO

    import quantlab.storage.repository as repository_module
    from quantlab.csv_adapter import iter_csv_records
    from quantlab.engine import validate_acquisition
    from quantlab.models import TickCandidate
    from quantlab.storage.contracts import StorageError, configuration

    payload, _ = observation("csv")
    path = tmp_path / "old-source.csv"
    path.write_bytes(payload)
    adapter = HistoricalAdapter(profile("csv", fields=FIELDS))
    ctx = adapter.context()
    archive = SourceArchive(tmp_path / "archive")
    receipt = archive.preserve(path)
    # Reconstruct the audited pre-fix interpretation without rewriting stored evidence.
    candidates = (
        TickCandidate(raw, "BOOM_1000", ctx.provider_symbol, fields["time"], fields["quote"], 2)
        for raw, fields in iter_csv_records(BytesIO(payload), ctx, columns=tuple(FIELDS.values()))
    )
    old_outcomes = tuple(validate_acquisition(candidates, ctx))

    def legacy_configuration(context, source_id):
        value = configuration(context, source_id)
        value["validation_version"] = "canonical.v1"
        return value

    source_id = str(uuid4())
    with monkeypatch.context() as patch:
        patch.setattr(repository_module, "configuration", legacy_configuration)
        with Repository(dsn) as repo, repo.claim(receipt, ctx, source_id) as run:
            repo.write_batch(run, 1, old_outcomes)
            old = repo.finish(run)
    with Repository(dsn) as repo:
        (old_tick,) = tuple(repo.tick_factory(old.dataset_id)())
        assert old_tick.received_at_utc is None
        with pytest.raises(StorageError, match="legacy validation version"):
            repo.validate_dataset(old.dataset_id, archive)
        current = IngestionCoordinator(repo, archive).import_historical(
            path, adapter=adapter, source_id=source_id
        )
        assert current.status == "COMPLETE" and current.dataset_id != old.dataset_id
        assert repo.manifest(old.dataset_id) == old
        (new_tick,) = tuple(repo.tick_factory(current.dataset_id)())
        assert new_tick.received_at_utc == datetime(
            2026, 1, 1, 0, 0, 0, 123456, tzinfo=timezone.utc
        )
        assert new_tick.market_fields() == old_tick.market_fields()
        assert repo.validate_dataset(current.dataset_id, archive)["valid"]
