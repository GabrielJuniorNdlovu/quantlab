"""Storage boundary and real TimescaleDB integration checks."""

import os
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import uuid4

import pytest

from quantlab.models import AcquisitionContext, SourceType
from quantlab.storage.contracts import BatchPolicy, RetryPolicy, configuration
from quantlab.storage.repository import Repository, migrate


def test_storage_policy_and_configuration_contracts():
    assert BatchPolicy().max_records == 100
    for kwargs in ({"max_records": 0}, {"max_wait_ms": 0}, {"queue_capacity": 10001}):
        with pytest.raises(ValueError):
            BatchPolicy(**kwargs)
    with pytest.raises(ValueError):
        RetryPolicy(max_attempts=0)
    ctx = AcquisitionContext(uuid4(), f"test-{uuid4()}", SourceType.HISTORICAL_CSV, "fixture", 2)
    assert configuration(ctx, "local-csv") == configuration(
        replace(ctx, capture_id=uuid4()), "local-csv"
    )


@pytest.fixture
def dsn():
    value = os.environ.get("QUANTLAB_TEST_DATABASE_URL")
    if not value:
        pytest.skip("real TimescaleDB requires QUANTLAB_TEST_DATABASE_URL")
    migrate(value)

    class TestDSN(str):
        def __repr__(self):
            return "'<isolated-test-dsn>'"

    return TestDSN(value)


HEADER = b"instrument_id,provider_symbol,event_time_utc,price,pip_size\n"


def outcome(ctx, position=2, seconds=0, price="1000.00"):
    import hashlib

    from quantlab.engine import validate_tick
    from quantlab.models import RawObservation, SourceFormat, TickCandidate

    event_time = datetime(2026, 1, 1, tzinfo=UTC) + timedelta(seconds=seconds)
    raw_bytes = (
        f"BOOM_1000,{ctx.provider_symbol},{event_time.isoformat()},{price},{ctx.pip_size}\n"
    ).encode()
    raw = RawObservation(
        ctx.capture_id,
        position,
        ctx.provider,
        SourceFormat.CSV,
        raw_bytes,
        hashlib.sha256(raw_bytes).hexdigest(),
        source_metadata={
            "adapter": {
                "header": HEADER.decode(),
                "header_sha256": hashlib.sha256(HEADER).hexdigest(),
            }
        },
    )
    return validate_tick(
        TickCandidate(raw, "BOOM_1000", ctx.provider_symbol, event_time, price, ctx.pip_size), ctx
    )


def source(tmp_path, rows=None):
    import hashlib

    from quantlab.storage.contracts import ArchivedSource

    if rows is None:
        rows = (outcome(context()),)
    path = tmp_path / f"{uuid4()}.raw"
    path.write_bytes(HEADER + b"".join(row.raw.raw_payload for row in rows))
    return ArchivedSource(hashlib.sha256(path.read_bytes()).hexdigest(), path.stat().st_size, path)


def context():
    return AcquisitionContext(uuid4(), f"test-{uuid4()}", SourceType.HISTORICAL_CSV, "fixture", 2)


def test_migration_idempotent_and_hypertable(dsn):
    import psycopg

    with psycopg.connect(dsn) as connection:
        assert connection.execute(
            "SELECT hypertable_name FROM timescaledb_information.hypertables "
            "WHERE hypertable_name='market_ticks'"
        ).fetchone() == ("market_ticks",)
        assert connection.execute("SELECT version_num FROM alembic_version").fetchone() == (
            "0002_storage_guards",
        )


def test_roundtrip_duplicate_looking_observations_and_completed_idempotence(dsn, tmp_path):
    ctx = context()
    batch = (
        outcome(ctx, 2, 0, "1000.00"),
        outcome(ctx, 3, 0, "1000.00"),
        outcome(ctx, 4, 60, "1000.10"),
    )
    archived = source(tmp_path, batch)
    with Repository(dsn) as repo:
        with repo.claim(archived, ctx) as run:
            repo.write_batch(run, 1, batch)
            repo.write_batch(run, 1, batch)
            result = repo.finish(run)
            assert result.status == "COMPLETE" and result.accepted == 3
            factory = repo.tick_factory(run.dataset_id, page_size=1)
            rows = tuple(factory())
            assert rows == tuple(o.tick for o in batch)
            assert rows[0].price.as_tuple() == Decimal("1000.00").as_tuple()
            assert tuple(factory()) == rows
            assert (
                len(tuple(repo.tick_factory(run.dataset_id, start_utc=rows[-1].event_time_utc)()))
                == 1
            )
        with repo.claim(archived, replace(ctx, capture_id=uuid4())) as duplicate:
            assert duplicate.already_complete and duplicate.capture_id == run.capture_id


def test_partial_import_hidden_and_restart_preserves_old_attempt(dsn, tmp_path):
    from quantlab.storage.contracts import IncompleteDataset

    archived = source(tmp_path)
    ctx = context()
    with Repository(dsn) as repo:
        with repo.claim(archived, ctx) as first:
            repo.write_batch(first, 1, (outcome(ctx),))
        with pytest.raises(IncompleteDataset):
            tuple(repo.tick_factory(first.dataset_id)())
        new = replace(ctx, capture_id=uuid4())
        with repo.claim(archived, new) as restarted:
            assert restarted.capture_id != first.capture_id
            repo.write_batch(restarted, 1, (outcome(new),))
            assert repo.finish(restarted).observed == 1
        import psycopg

        with psycopg.connect(dsn) as con:
            assert con.execute(
                "SELECT status FROM acquisition_runs WHERE capture_id=%s", (first.capture_id,)
            ).fetchone() == ("INTERRUPTED",)
            assert con.execute(
                "SELECT count(*) FROM raw_observations WHERE capture_id=%s", (first.capture_id,)
            ).fetchone() == (1,)


def test_failed_dataset_quality_persisted_never_replayed(dsn, tmp_path):
    from quantlab.storage.contracts import IncompleteDataset

    ctx = context()
    with Repository(dsn) as repo:
        with repo.claim(source(tmp_path), ctx) as run:
            rejected = outcome(ctx, price="-1")
            repo.write_batch(run, 1, (rejected,))
            result = repo.finish(run)
            assert result.status == "FAILED" and result.rejected == result.error_count == 1
            with pytest.raises(IncompleteDataset):
                tuple(repo.tick_factory(run.dataset_id)())


def test_conflicting_batch_rolls_back_and_order_guard(dsn, tmp_path):
    from quantlab.storage.contracts import StorageError

    ctx = context()
    with Repository(dsn) as repo:
        with repo.claim(source(tmp_path), ctx) as run:
            repo.write_batch(run, 1, (outcome(ctx),))
            with pytest.raises(StorageError):
                repo.write_batch(run, 1, (outcome(ctx, price="1001.00"),))
            with pytest.raises(StorageError):
                repo.write_batch(run, 2, (outcome(ctx, 2),))
            assert repo.manifest(run.dataset_id).observed == 1
            repo.fail(run, interrupted=True)


def test_large_exponent_and_normalized_timestamp_exact(dsn, tmp_path):
    ctx = replace(context(), pip_size=0)
    item = outcome(ctx, price="1E+1000")
    with Repository(dsn) as repo:
        with repo.claim(source(tmp_path, (item,)), ctx) as run:
            repo.write_batch(run, 1, (item,))
            repo.finish(run)
            actual = next(repo.tick_factory(run.dataset_id)())
            assert actual.price.as_tuple() == Decimal("1E+1000").as_tuple()
            assert actual.event_time_utc == item.tick.event_time_utc


def test_competing_claim_and_constraints(dsn, tmp_path):
    import psycopg

    from quantlab.storage.contracts import BusyImport

    ctx = context()
    archived = source(tmp_path)
    with Repository(dsn) as first, Repository(dsn) as second:
        with first.claim(archived, ctx) as run:
            with pytest.raises(BusyImport):
                with second.claim(archived, replace(ctx, capture_id=uuid4())):
                    pass
            first.write_batch(run, 1, (outcome(ctx),))
            first.finish(run)
        with psycopg.connect(dsn) as connection:
            with pytest.raises(psycopg.Error):
                connection.execute(
                    "UPDATE raw_observations SET raw_payload=%s WHERE capture_id=%s",
                    (b"changed", ctx.capture_id),
                )


def test_database_rejects_direct_complete_publication_without_validated_rows(dsn, tmp_path):
    import psycopg

    ctx = context()
    with Repository(dsn) as repo:
        with repo.claim(source(tmp_path), ctx) as run:
            with psycopg.connect(dsn, autocommit=True) as con:
                with pytest.raises(psycopg.Error):
                    con.execute(
                        "INSERT INTO acquisition_runs(capture_id,dataset_id,instrument_id,status) "
                        "VALUES(%s,%s,'BOOM_1000','COMPLETE')",
                        (uuid4(), run.dataset_id),
                    )
                with pytest.raises(psycopg.Error):
                    con.execute(
                        "UPDATE dataset_manifests SET status='COMPLETE' WHERE dataset_id=%s",
                        (run.dataset_id,),
                    )
            repo.fail(run, interrupted=True)


def test_whole_batch_rollback_on_second_observation_failure(dsn, tmp_path):
    import psycopg

    from quantlab.storage.contracts import StorageError

    ctx = context()
    with Repository(dsn) as repo:
        with repo.claim(source(tmp_path), ctx) as run:
            wrong = outcome(replace(ctx, capture_id=uuid4()), 3)
            with pytest.raises(StorageError):
                repo.write_batch(run, 1, (outcome(ctx), wrong))
            assert repo.manifest(run.dataset_id).observed == 0
            with psycopg.connect(dsn) as con:
                assert con.execute(
                    "SELECT count(*) FROM raw_observations WHERE capture_id=%s", (ctx.capture_id,)
                ).fetchone() == (0,)
            repo.fail(run, interrupted=True)


def test_lost_commit_acknowledgement_retries_receipt_without_duplicates(dsn, tmp_path, monkeypatch):
    from contextlib import contextmanager

    import psycopg

    ctx = context()
    with Repository(dsn, sleep=lambda delay: None) as repo:
        with repo.claim(source(tmp_path), ctx) as run:
            first_connection = repo._connection
            transaction = psycopg.Connection.transaction
            lost = []

            @contextmanager
            def uncertain(self, *args, **kwargs):
                with transaction(self, *args, **kwargs) as value:
                    yield value
                if self is first_connection and not lost:
                    lost.append(True)
                    raise psycopg.OperationalError("injected lost COMMIT acknowledgement")

            monkeypatch.setattr(psycopg.Connection, "transaction", uncertain)
            repo.write_batch(run, 1, (outcome(ctx),))
            assert lost == [True]
            assert repo.finish(run).accepted == 1
            assert len(tuple(repo.tick_factory(run.dataset_id)())) == 1


def test_hypertable_cannot_reuse_source_identity_at_another_time(dsn, tmp_path):
    import psycopg

    ctx = context()
    with Repository(dsn) as repo:
        with repo.claim(source(tmp_path), ctx) as run:
            repo.write_batch(run, 1, (outcome(ctx),))
            with psycopg.connect(dsn, autocommit=True) as con:
                with pytest.raises(psycopg.errors.ForeignKeyViolation):
                    con.execute(
                        """INSERT INTO market_ticks
                     SELECT instrument_id,provider_symbol,event_time_utc+interval '1 second',
                      price,price_text,pip_size,source_type,capture_id,source_position,
                      received_at_utc,provider_tick_id FROM market_ticks WHERE capture_id=%s""",
                        (ctx.capture_id,),
                    )
            assert repo.finish(run).accepted == 1


def test_direct_decimal_256_coefficient_and_subsecond_range_roundtrip(dsn, tmp_path):
    ctx = replace(context(), pip_size=1)
    price = Decimal("9" * 255 + ".1")
    item = outcome(ctx, seconds=0.123456, price=price)
    with Repository(dsn) as repo:
        with repo.claim(source(tmp_path, (item,)), ctx) as run:
            repo.write_batch(run, 1, (item,))
            repo.finish(run)
            rows = tuple(
                repo.tick_factory(
                    run.dataset_id,
                    start_utc=item.tick.event_time_utc,
                    end_utc=item.tick.event_time_utc + timedelta(microseconds=1),
                )()
            )
            assert rows == (item.tick,)
            assert rows[0].price.as_tuple() == price.as_tuple()
            assert (
                tuple(repo.tick_factory(run.dataset_id, end_utc=item.tick.event_time_utc)()) == ()
            )


def test_bigint_storage_boundary_rejects_without_partial_write(dsn, tmp_path):
    from quantlab.storage.contracts import StorageError

    ctx = context()
    with Repository(dsn) as repo:
        with repo.claim(source(tmp_path), ctx) as run:
            with pytest.raises(StorageError, match="position"):
                repo.write_batch(run, 1, (outcome(ctx, 2**63),))
            assert repo.manifest(run.dataset_id).observed == 0
            repo.fail(run, interrupted=True)


def test_prefix_of_archived_csv_cannot_be_published_as_complete(dsn, tmp_path):
    from importlib.resources import files

    from quantlab.csv_adapter import iter_csv_candidates
    from quantlab.engine import validate_acquisition
    from quantlab.storage.contracts import StorageError
    from quantlab.storage.source_archive import SourceArchive

    ctx = replace(context(), provider_symbol="fixture-symbol")
    path = tmp_path / "full.csv"
    path.write_bytes(files("quantlab").joinpath("data/replay_demo.csv").read_bytes())
    archived = SourceArchive(tmp_path / "raw").preserve(path)
    with archived.path.open("rb") as stream:
        rows = tuple(validate_acquisition(iter_csv_candidates(stream, ctx), ctx))
    with Repository(dsn) as repo, repo.claim(archived, ctx) as run:
        repo.write_batch(run, 1, rows[:5])
        with pytest.raises(StorageError, match="source"):
            repo.finish(run)
        assert repo.manifest(run.dataset_id).status == "IMPORTING"
        repo.write_batch(run, 2, rows[5:])
        assert repo.finish(run).status == "COMPLETE"


def test_populated_first_revision_upgrades_without_losing_evidence(dsn, tmp_path):
    import psycopg
    from alembic import command
    from alembic.config import Config
    from psycopg import sql
    from psycopg.conninfo import make_conninfo
    from sqlalchemy import create_engine
    from sqlalchemy.pool import NullPool

    import quantlab.storage.repository as repository_module

    database = "quantlab_upgrade_" + uuid4().hex
    isolated = make_conninfo(dsn, dbname=database)
    with psycopg.connect(dsn, autocommit=True) as admin:
        admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database)))
    try:
        engine = create_engine(
            "postgresql+psycopg://", creator=lambda: psycopg.connect(isolated), poolclass=NullPool
        )
        try:
            with engine.begin() as connection:
                from pathlib import Path

                cfg = Config()
                cfg.set_main_option(
                    "script_location", str(Path(repository_module.__file__).parent / "migrations")
                )
                cfg.attributes["connection"] = connection
                command.upgrade(cfg, "0001_storage")
        finally:
            engine.dispose()
        ctx = context()
        item = outcome(ctx)
        with Repository(isolated) as repo, repo.claim(source(tmp_path, (item,)), ctx) as run:
            repo.write_batch(run, 1, (item,))
            before = repo.finish(run)
        migrate(isolated)
        with Repository(isolated) as repo:
            assert repo.manifest(run.dataset_id) == before
            assert tuple(repo.tick_factory(run.dataset_id)()) == (item.tick,)
    finally:
        with psycopg.connect(dsn, autocommit=True) as admin:
            admin.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(database)))


def test_retry_budget_and_backoff_are_bounded_without_a_database(monkeypatch, caplog):
    import json

    import psycopg

    delays = []
    attempts = []
    repo = Repository(
        "unused", retry=RetryPolicy(max_attempts=3, backoff_seconds=0.1), sleep=delays.append
    )

    def unavailable():
        attempts.append(1)
        raise psycopg.OperationalError("injected connection unavailable")

    monkeypatch.setattr(repo, "_conn", unavailable)
    with pytest.raises(psycopg.OperationalError):
        repo._retry(lambda connection: None)
    assert len(attempts) == 3
    assert delays == [0.1, 0.2]
    events = [json.loads(record.message) for record in caplog.records]
    assert [event["attempt"] for event in events] == [1, 2]
    assert all(event["event"] == "database_retry" for event in events)


def test_offset_timestamps_and_replay_across_actual_hypertable_chunks(dsn, tmp_path):
    import psycopg

    from quantlab.storage.ingestion import IngestionCoordinator
    from quantlab.storage.source_archive import SourceArchive

    path = tmp_path / "cross-chunk.csv"
    path.write_bytes(
        HEADER
        + b"BOOM_1000,fixture,2026-01-01T02:00:00.123456+02:00,1000.10,2\n"
        + b"BOOM_1000,fixture,2026-01-09T05:00:00.123456+05:00,1000.20,2\n"
    )
    with Repository(dsn) as repo:
        manifest = IngestionCoordinator(repo, SourceArchive(tmp_path / "archive")).import_csv(
            path,
            provider="cross-chunk-fixture",
            provider_symbol="fixture",
            pip_size=2,
            source_id=str(uuid4()),
        )
        rows = tuple(repo.tick_factory(manifest.dataset_id, page_size=1)())
        assert [row.event_time_utc for row in rows] == [
            datetime(2026, 1, 1, 0, 0, 0, 123456, tzinfo=UTC),
            datetime(2026, 1, 9, 0, 0, 0, 123456, tzinfo=UTC),
        ]
        assert tuple(
            repo.tick_factory(manifest.dataset_id, start_utc=rows[1].event_time_utc)()
        ) == (rows[1],)
    with psycopg.connect(dsn) as connection:
        assert (
            connection.execute(
                "SELECT count(*) FROM timescaledb_information.chunks "
                "WHERE hypertable_name='market_ticks'"
            ).fetchone()[0]
            >= 2
        )
