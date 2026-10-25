"""Synthetic engineering data; exact persistence, progress, and lookup regressions."""

import os
from dataclasses import replace
from decimal import Decimal
from uuid import uuid4

import pytest
from test_storage_repository import context, outcome, source

from quantlab.storage.contracts import StorageError
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


def test_revalidation_rejects_changed_decimal_representation(dsn, tmp_path):
    ctx = context()
    from quantlab.csv_adapter import iter_csv_candidates
    from quantlab.engine import validate_acquisition

    saved = source(tmp_path, (outcome(ctx, price="1000.00"),))
    archive = SourceArchive(tmp_path / "archive")
    saved = archive.preserve(saved.path)
    with saved.path.open("rb") as stream:
        (original,) = tuple(validate_acquisition(iter_csv_candidates(stream, ctx), ctx))
    changed = replace(original, tick=replace(original.tick, price=Decimal("1000.0")))
    with Repository(dsn) as repo, repo.claim(saved, ctx) as run:
        repo.write_batch(run, 1, (changed,))
        assert repo.finish(run).status == "COMPLETE"
        with pytest.raises(StorageError, match="differs"):
            repo.validate_dataset(run.dataset_id, archive)


def test_atomic_commit_status_and_exact_lookup(dsn, tmp_path):
    ctx = context()
    rows = tuple(outcome(ctx, p, seconds=0) for p in (2, 3, 4))
    saved = source(tmp_path, rows)
    with Repository(dsn) as repo, repo.claim(saved, ctx) as run:
        initial = repo.commit_status(run.capture_id)
        assert initial.observed == initial.source_position == initial.batch_number == 0
        repo.write_batch(run, 1, rows[:2])
        first = repo.commit_status(run.capture_id)
        assert (first.batch_number, first.source_position, first.observed) == (1, 3, 2)
        repo.write_batch(run, 1, rows[:2])
        assert repo.commit_status(run.capture_id) == first
        repo.write_batch(run, 2, rows[2:])
        assert repo.finish(run).accepted == 3
        for row in rows:
            actual = repo.observation(run.dataset_id, source_position=row.raw.source_position)
            assert actual == row.tick and actual.price.as_tuple() == row.tick.price.as_tuple()
        assert repo.observation(run.dataset_id, source_position=999) is None
        attempts = repo.acquisitions(run.dataset_id)
        assert len(attempts) == 1 and attempts[0].capture_id == run.capture_id
        assert attempts[0].status == "COMPLETE" and attempts[0].observed == 3
    with Repository(dsn) as restarted:
        assert restarted.commit_status(run.capture_id) == attempts[0]
        with pytest.raises(StorageError):
            restarted.commit_status(uuid4())


def test_progress_cannot_see_writer_uncommitted_transaction(dsn, tmp_path):
    from quantlab.storage.bulk import insert_batch
    from quantlab.storage.mapping import batch_digest

    ctx = context()
    row = outcome(ctx)
    saved = source(tmp_path, (row,))
    with Repository(dsn) as repo, repo.claim(saved, ctx) as run:
        before = repo.commit_status(run.capture_id)
        connection = repo._conn()
        with connection.transaction():
            insert_batch(connection, (row,))
            connection.execute(
                "INSERT INTO ingestion_batches(capture_id,batch_number,digest,"
                "first_position,last_position,observation_count) VALUES(%s,1,%s,2,2,1)",
                (run.capture_id, batch_digest((row,))),
            )
            connection.execute(
                "UPDATE acquisition_runs SET observed=1,accepted=1,last_position=2,"
                "last_event_time=%s WHERE capture_id=%s",
                (row.tick.event_time_utc, run.capture_id),
            )
            assert repo.commit_status(run.capture_id) == before
        after = repo.commit_status(run.capture_id)
        assert (after.observed, after.batch_number, after.source_position) == (1, 1, 2)
        assert after.digest == batch_digest((row,))
        repo.finish(run)


def test_copy_tick_constraint_failure_rolls_back_raw_receipt_and_progress(dsn, tmp_path):
    from datetime import timedelta

    import psycopg

    ctx = context()
    row = outcome(ctx)
    bad = replace(
        row, tick=replace(row.tick, received_at_utc=row.tick.event_time_utc + timedelta(seconds=1))
    )
    with Repository(dsn) as repo, repo.claim(source(tmp_path, (row,)), ctx) as run:
        before = repo.commit_status(run.capture_id)
        with pytest.raises(psycopg.Error):
            repo.write_batch(run, 1, (bad,))
        assert repo.commit_status(run.capture_id) == before
        with psycopg.connect(dsn) as independent:
            for table in ("raw_observations", "market_ticks", "ingestion_batches"):
                from psycopg import sql

                assert independent.execute(
                    sql.SQL("SELECT count(*) FROM {} WHERE capture_id=%s").format(
                        sql.Identifier(table)
                    ),
                    (run.capture_id,),
                ).fetchone() == (0,)
        repo.write_batch(run, 1, (row,))
        assert repo.finish(run).accepted == 1


@pytest.mark.parametrize(
    "price", [Decimal("1E+1000"), Decimal("9" * 256 + "E+1000"), Decimal("1E-18")]
)
def test_full_native_canonical_decimal_range_roundtrip(dsn, tmp_path, price):
    ctx = replace(context(), pip_size=18)
    row = outcome(ctx)
    # This is a native canonical-value fixture, not a verified provider serialization.
    row = replace(row, tick=replace(row.tick, price=price))
    with Repository(dsn) as repo, repo.claim(source(tmp_path, (row,)), ctx) as run:
        repo.write_batch(run, 1, (row,))
        repo.finish(run)
        read = repo.observation(run.dataset_id, source_position=row.raw.source_position)
        assert read.price.as_tuple() == price.as_tuple()
        assert next(repo.tick_factory(run.dataset_id)()).price.as_tuple() == price.as_tuple()


def test_overlapping_dataset_identities_remain_separate(dsn, tmp_path):
    from uuid import uuid4

    ctx = context()
    original = outcome(ctx)
    saved = source(tmp_path, (original,))
    ids = []
    for number in range(2):
        acquisition = replace(ctx, capture_id=uuid4())
        row = replace(
            original,
            raw=replace(original.raw, capture_id=acquisition.capture_id),
            tick=replace(original.tick, capture_id=acquisition.capture_id),
        )
        with Repository(dsn) as repo, repo.claim(saved, acquisition, f"overlap-{number}") as run:
            repo.write_batch(run, 1, (row,))
            repo.finish(run)
            ids.append((run.dataset_id, run.capture_id))
    assert ids[0][0] != ids[1][0] and ids[0][1] != ids[1][1]
    with Repository(dsn) as repo:
        ticks = [repo.observation(dataset, source_position=2) for dataset, _ in ids]
        assert ticks[0].market_fields() == ticks[1].market_fields()
        assert ticks[0].capture_id != ticks[1].capture_id
        usage = repo.storage_usage()
        assert usage["database_bytes"] >= usage["market_total_bytes"] > 0
        assert usage["market_index_bytes"] > 0 and usage["raw_table_bytes"] > 0


def test_recorded_stream_incremental_writer_to_deterministic_replay(dsn, tmp_path):
    import json

    from test_historical import profile

    from quantlab.engine import validate_acquisition
    from quantlab.historical import HistoricalAdapter
    from quantlab.replay import ReplayEngine
    from quantlab.storage.batching import BatchWriter
    from quantlab.storage.contracts import BatchPolicy

    record = {
        "symbol": "Boom export",
        "time": "2026-01-01T00:00:00.123456Z",
        "quote": "1000.123456789012345678",
        "received": "2026-01-01T00:00:01.234567Z",
    }
    path = tmp_path / "synthetic-stream.jsonl"
    path.write_text((json.dumps({"tick": record}) + "\n") * 20)
    adapter = HistoricalAdapter(
        profile(
            "stream_jsonl",
            pip_size=18,
            fields={
                "instrument_id": "tick.symbol",
                "event_time": "tick.time",
                "price": "tick.quote",
                "received_at_utc": "tick.received",
            },
        )
    )
    ctx = adapter.context()
    archive = SourceArchive(tmp_path / "archive")
    saved = archive.preserve(path)
    with saved.path.open("rb") as stream:
        expected = tuple(validate_acquisition(adapter.candidates(stream, ctx), ctx))
    checkpoints = []
    with Repository(dsn) as repo, repo.claim(saved, ctx, f"recorded-{uuid4()}") as run:
        number = 0

        def sink(values):
            nonlocal number
            number += 1
            repo.write_batch(run, number, values)
            checkpoints.append(repo.commit_status(run.capture_id))

        with BatchWriter(sink, BatchPolicy(max_records=3, queue_capacity=2)) as writer:
            for row in expected:
                writer.submit(row)
        assert writer.metrics.committed == 20 and writer.metrics.queue_high_water <= 2
        assert checkpoints[-1].observed == 20
        assert [s.batch_number for s in checkpoints] == list(range(1, number + 1))
        manifest = repo.finish(run)
        assert (
            manifest.status == "COMPLETE"
            and repo.validate_dataset(run.dataset_id, archive)["valid"]
        )
        engine = ReplayEngine(repo.tick_factory(run.dataset_id, page_size=2))
        for _ in range(2):
            for row in expected:
                actual = engine.step()
                assert actual == row.tick
                assert actual.price.as_tuple() == row.tick.price.as_tuple()
            assert engine.step() is None
            engine.reset()
        engine.close()
    assert saved.path.read_bytes() == path.read_bytes()


def test_exact_lookup_rejects_incomplete_and_invalid_scopes(dsn, tmp_path):
    from quantlab.storage.contracts import IncompleteDataset

    ctx = context()
    with Repository(dsn) as repo, repo.claim(source(tmp_path), ctx) as run:
        with pytest.raises(IncompleteDataset):
            repo.observation(run.dataset_id, source_position=2)
        for position in (0, True, 2**63):
            with pytest.raises(ValueError):
                repo.observation(run.dataset_id, source_position=position)
        with pytest.raises(StorageError):
            repo.acquisitions(run.dataset_id, limit=0)
        assert repo.acquisitions(uuid4()) == ()
        repo.fail(run, interrupted=True)
