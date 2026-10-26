"""Synthetic recorded-source regressions for the F04 refactor."""

import os
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from test_historical import profile

from quantlab.dataset_export import SplitPlan, export_parquet
from quantlab.historical import HistoricalAdapter
from quantlab.storage.ingestion import IngestionCoordinator
from quantlab.storage.repository import Repository
from quantlab.storage.source_archive import SourceArchive


@pytest.fixture
def dsn():
    value = os.environ.get("QUANTLAB_TEST_DATABASE_URL")
    if not value:
        pytest.skip("requires isolated PostgreSQL/TimescaleDB")
    return value


def received_dataset(repo, tmp_path):
    path = tmp_path / "synthetic.jsonl"
    path.write_text(
        '{"symbol":"Boom export","time":"2026-01-01T00:00:00.123456Z",'
        '"quote":"1000.10","received":"2026-01-01T00:00:01.234567Z"}\n'
    )
    adapter = HistoricalAdapter(
        profile(
            "jsonl",
            fields={
                "instrument_id": "symbol",
                "event_time": "time",
                "price": "quote",
                "received_at_utc": "received",
            },
        )
    )
    return IngestionCoordinator(repo, SourceArchive(tmp_path / "archive")).import_historical(
        path, adapter=adapter, source_id=str(uuid4())
    )


def test_parquet_preserves_source_reception_and_configuration(dsn, tmp_path):
    import pyarrow.parquet as pq

    with Repository(dsn) as repo:
        result = received_dataset(repo, tmp_path)
        export = export_parquet(
            repo,
            result.dataset_id,
            tmp_path / "export",
            splits=SplitPlan(datetime(2026, 1, 2, tzinfo=UTC), datetime(2026, 1, 3, tzinfo=UTC)),
        )
        table = pq.ParquetFile(tmp_path / "export/train.parquet").read()
        assert "received_at_utc" in table.column_names
        assert table.column("received_at_utc").to_pylist() == [
            datetime(2026, 1, 1, 0, 0, 1, 234567, tzinfo=UTC)
        ]
        assert export["manifest"]["request"]["source"]["provider"] == "engineering-fixture"
        assert (
            export["manifest"]["request"]["source"]["configuration"]
            == repo.dataset_info(result.dataset_id)["configuration"]
        )


@pytest.mark.parametrize("damage", ["counts", "last_time", "error_count"])
def test_checkpoint_refuses_corrupted_accepted_rejected_counters(dsn, tmp_path, damage):
    import psycopg
    from test_storage_repository import context, outcome, source

    from quantlab.storage.contracts import StorageError

    ctx = context()
    rows = tuple(outcome(ctx, p) for p in (2, 3))
    with Repository(dsn) as repo, repo.claim(source(tmp_path, rows), ctx) as run:
        repo.write_batch(run, 1, rows)
        with psycopg.connect(dsn) as connection:
            if damage == "counts":
                statement = "UPDATE acquisition_runs SET accepted=0,rejected=2 WHERE capture_id=%s"
            elif damage == "last_time":
                statement = "UPDATE acquisition_runs SET last_event_time=NULL WHERE capture_id=%s"
            else:
                statement = "UPDATE acquisition_runs SET error_count=1 WHERE capture_id=%s"
            connection.execute(statement, (run.capture_id,))
        try:
            with pytest.raises(StorageError, match="checkpoint"):
                repo.checkpoint(run, iter(rows))
        finally:
            with psycopg.connect(dsn) as connection:
                connection.execute(
                    "UPDATE acquisition_runs SET accepted=2,rejected=0,error_count=0,"
                    "last_event_time=%s WHERE capture_id=%s",
                    (rows[-1].tick.event_time_utc, run.capture_id),
                )


def test_historical_stream_reconnect_repeats_replay_and_export_e2e(dsn, tmp_path):
    import asyncio
    import json

    import pyarrow.parquet as pq

    from quantlab.collection import CollectionSettings, LocalHistoricalSource, SimulatedStream
    from quantlab.continuity import TimeRange
    from quantlab.replay import ReplayEngine
    from quantlab.storage.contracts import BatchPolicy

    path = tmp_path / "synthetic-recorded.jsonl"
    path.write_text(
        "".join(
            json.dumps(
                {
                    "symbol": "Boom export",
                    "time": f"2026-01-0{day}T00:00:00.123456Z",
                    "quote": "1000.123456789012345678",
                    "received": f"2026-01-0{day}T00:00:01.234567Z",
                    "id": f"synthetic-{position}",
                    "basis": "bid",
                }
            )
            + "\n"
            for position, day in enumerate([1, 1, 2, 2, 3, 3])
        )
    )
    config = profile(
        "jsonl",
        pip_size=18,
        fields={
            "instrument_id": "symbol",
            "event_time": "time",
            "price": "quote",
            "received_at_utc": "received",
            "provider_tick_id": "id",
            "price_basis": "basis",
        },
    )
    source = LocalHistoricalSource(path, config)
    archive = SourceArchive(tmp_path / "archive")
    settings = CollectionSettings(batching=BatchPolicy(2, 10, 2))
    with Repository(dsn) as repo:
        coordinator = IngestionCoordinator(repo, archive, settings=settings)
        historical = asyncio.run(coordinator.collect(source, source_id=str(uuid4())))
        streamed = asyncio.run(
            coordinator.collect(
                source,
                source_id=str(uuid4()),
                stream=SimulatedStream(disconnect_after=3, redeliver_every=2),
            )
        )
        assert historical.manifest.status == streamed.manifest.status == "COMPLETE"
        assert streamed.metrics["source_interruptions"] == streamed.metrics["reconnects"] == 1
        assert streamed.metrics["redeliveries"] == 3
        assert streamed.metrics["validated"] == streamed.metrics["committed"] == 6
        assert streamed.metrics["received"] == 9
        assert streamed.metrics["queue_high_water"] <= 2
        assert historical.manifest.capture_id != streamed.manifest.capture_id
        expected = tuple(repo.tick_factory(historical.manifest.dataset_id)())
        engine = ReplayEngine(repo.tick_factory(streamed.manifest.dataset_id, page_size=2))
        for _ in range(2):
            for tick in expected:
                actual = engine.step()
                assert actual.market_fields() == tick.market_fields()
                assert actual.received_at_utc == tick.received_at_utc
                assert actual.provider_tick_id == tick.provider_tick_id
            assert engine.step() is None
            engine.reset()
        engine.close()
        assert repo.validate_dataset(streamed.manifest.dataset_id, archive)["valid"]
        quality = repo.dataset_quality(streamed.manifest.dataset_id, gap_threshold_seconds=3600)
        assert quality["completeness"] == "VERIFIED_SOURCE_COMPLETE"
        assert quality["market_coverage"] == "UNKNOWN"
        assert quality["suspected_gaps"] == 2 and quality["confirmed_market_gaps"] == []
        assert quality["counts_reconciled"] and quality["committed"] == 6
        plan = SplitPlan(datetime(2026, 1, 2, tzinfo=UTC), datetime(2026, 1, 3, tzinfo=UTC))
        one = export_parquet(repo, streamed.manifest.dataset_id, tmp_path / "one", splits=plan)
        two = export_parquet(repo, streamed.manifest.dataset_id, tmp_path / "two", splits=plan)
        assert one == two
        for name in ("train", "validation", "test"):
            a, b = tmp_path / "one" / f"{name}.parquet", tmp_path / "two" / f"{name}.parquet"
            assert a.read_bytes() == b.read_bytes()
            table = pq.ParquetFile(a).read()
            assert table.num_rows == 2
            assert all(
                json.loads(item)["quote"]["price_basis"] == "bid"
                for item in table.column("source_metadata_json").to_pylist()
            )
            assert table.column("price_text").to_pylist() == ["1000.123456789012345678"] * 2
            assert all(value is not None for value in table.column("received_at_utc").to_pylist())
        selected = export_parquet(
            repo,
            streamed.manifest.dataset_id,
            tmp_path / "selected",
            splits=plan,
            selection=TimeRange(datetime(2026, 1, 3, tzinfo=UTC)),
        )
        assert [
            selected["manifest"]["files"][f"{name}.parquet"]["records"]
            for name in ("train", "validation", "test")
        ] == [0, 0, 2]


def test_arbitrary_older_redelivery_is_verified_and_conflicts_are_durable(dsn, tmp_path):
    import asyncio
    from dataclasses import replace

    from test_collection_storage import local_source

    from quantlab.collection import ConnectionState, DeliveryMetrics

    class Redelivery:
        metrics = DeliveryMetrics(ConnectionState.CONNECTED)

        def __init__(self, changed=False):
            self.changed = changed

        async def events(self, records):
            first = next(records)
            yield first
            for candidate in records:
                yield candidate
            yield replace(first, price="1000.1") if self.changed else first

    source = local_source(tmp_path, 4)
    with Repository(dsn) as repo:
        coordinator = IngestionCoordinator(repo, SourceArchive(tmp_path / "archive"))
        exact = asyncio.run(
            coordinator.collect(source, stream=Redelivery(), source_id=str(uuid4()))
        )
        assert exact.manifest.status == "COMPLETE" and exact.manifest.observed == 4
        assert exact.metrics["redeliveries"] == 1 and exact.metrics["received"] == 5
        conflict = asyncio.run(
            coordinator.collect(source, stream=Redelivery(changed=True), source_id=str(uuid4()))
        )
        assert conflict.manifest.status == "FAILED" and conflict.manifest.observed == 4
        assert conflict.manifest.error_count == 1
        quality = repo.dataset_quality(conflict.manifest.dataset_id)
        assert quality["errors_by_code"][0]["error_code"] == "INVALID_SOURCE_POSITION"
        assert quality["diagnostic_samples"][0]["source_position"] == 2


def test_declared_loss_never_becomes_complete_and_requires_fresh_attempt(dsn, tmp_path):
    import asyncio

    from test_collection_storage import local_source

    from quantlab.collection import SimulatedStream
    from quantlab.storage.contracts import StorageError

    source = local_source(tmp_path, 5)
    identity = str(uuid4())
    with Repository(dsn) as repo:
        coordinator = IngestionCoordinator(repo, SourceArchive(tmp_path / "archive"))
        lost = asyncio.run(
            coordinator.collect(source, stream=SimulatedStream(drop_after=2), source_id=identity)
        )
        assert lost.manifest.status == "IMPORTING" and lost.manifest.observed == 4
        assert lost.metrics["dropped"] == 1 and lost.metrics["dropped_positions"] == (4,)
        quality = repo.dataset_quality(lost.manifest.dataset_id)
        assert quality["completeness"] == "PARTIAL" and not quality["source_file_complete"]
        assert quality["confirmed_delivery_gaps"] == 1
        with pytest.raises(StorageError, match="checkpoint"):
            asyncio.run(coordinator.collect(source, source_id=identity))
        after_failed_resume = repo.dataset_quality(lost.manifest.dataset_id)
        assert after_failed_resume["confirmed_delivery_gaps"] == 1
        assert not after_failed_resume["last_process_snapshot"]["source_coverage_guaranteed"]
        fresh = asyncio.run(coordinator.collect(source, source_id=identity, resume=False))
        assert fresh.manifest.status == "COMPLETE" and fresh.manifest.observed == 5
        assert fresh.manifest.capture_id != lost.manifest.capture_id


def test_database_lifecycle_does_not_block_event_loop(dsn, tmp_path, monkeypatch):
    import asyncio
    from threading import Event

    from test_collection_storage import local_source

    entered, released = Event(), Event()
    with Repository(dsn) as repo:
        original = repo.begin_collection

        def gated(*args):
            entered.set()
            assert released.wait(2), "database lifecycle blocked the event loop"
            return original(*args)

        monkeypatch.setattr(repo, "begin_collection", gated)
        coordinator = IngestionCoordinator(repo, SourceArchive(tmp_path / "archive"))

        async def run():
            async def heartbeat():
                while not entered.is_set():
                    await asyncio.sleep(0.001)
                released.set()

            result, _ = await asyncio.gather(
                coordinator.collect(local_source(tmp_path), source_id=str(uuid4())), heartbeat()
            )
            assert result.manifest.status == "COMPLETE"

        asyncio.run(run())


def test_stop_accounts_for_acquired_unsubmitted_record_and_recovers(dsn, tmp_path):
    import asyncio

    from test_collection_storage import local_source

    source = local_source(tmp_path, 3)
    identity = str(uuid4())
    with Repository(dsn) as repo:
        coordinator = IngestionCoordinator(repo, SourceArchive(tmp_path / "archive"))

        async def run():
            from quantlab.collection import ConnectionState, DeliveryMetrics

            stop = asyncio.Event()

            class StopAfterAcquisition:
                metrics = DeliveryMetrics(ConnectionState.CONNECTED)

                async def events(self, records):
                    candidate = next(records)
                    stop.set()
                    yield candidate

            paused = await coordinator.collect(
                source, source_id=identity, stop=stop, stream=StopAfterAcquisition()
            )
            assert paused.metrics["received"] == paused.metrics["unsubmitted"] == 1
            assert paused.metrics["committed"] == 0 and paused.manifest.status == "IMPORTING"
            resumed = await coordinator.collect(source, source_id=identity)
            assert resumed.manifest.status == "COMPLETE" and resumed.manifest.observed == 3
            assert resumed.manifest.capture_id == paused.manifest.capture_id

        asyncio.run(run())


def test_redelivery_of_order_rejected_observation_is_idempotent(dsn, tmp_path):
    import asyncio

    from test_collection_storage import local_source

    from quantlab.collection import ConnectionState, DeliveryMetrics

    source = local_source(tmp_path, 3)
    source.path.write_bytes(
        b"symbol,time,quote\n"
        b"Boom export,2026-01-01T00:00:01Z,1000.10\n"
        b"Boom export,2026-01-01T00:00:00Z,1000.10\n"
        b"Boom export,2026-01-01T00:00:02Z,1000.10\n"
    )

    class RedeliverRejected:
        metrics = DeliveryMetrics(ConnectionState.CONNECTED)

        async def events(self, records):
            rows = list(records)
            for row in rows:
                yield row
            yield rows[1]

    with Repository(dsn) as repo:
        result = asyncio.run(
            IngestionCoordinator(repo, SourceArchive(tmp_path / "archive")).collect(
                source, source_id=str(uuid4()), stream=RedeliverRejected()
            )
        )
        assert result.manifest.status == "FAILED" and result.manifest.observed == 3
        assert result.manifest.rejected == result.manifest.error_count == 1
        assert result.metrics["redeliveries"] == 1


def test_graceful_stop_wakes_an_idle_source(dsn, tmp_path):
    import asyncio

    from test_collection_storage import local_source

    from quantlab.collection import ConnectionState, DeliveryMetrics

    async def run():
        entered, stop, forever = asyncio.Event(), asyncio.Event(), asyncio.Event()

        class IdleSource:
            metrics = DeliveryMetrics(ConnectionState.CONNECTED)

            async def events(self, records):
                entered.set()
                await forever.wait()
                yield next(records)

        with Repository(dsn) as repo:
            coordinator = IngestionCoordinator(repo, SourceArchive(tmp_path / "archive"))
            task = asyncio.create_task(
                coordinator.collect(
                    local_source(tmp_path),
                    source_id=str(uuid4()),
                    stream=IdleSource(),
                    stop=stop,
                )
            )
            await asyncio.wait_for(entered.wait(), 2)
            stop.set()
            result = await asyncio.wait_for(task, 2)
            assert result.manifest.status == "IMPORTING" and result.manifest.observed == 0
            assert result.metrics["received"] == 0

    asyncio.run(run())


def test_corrupted_coverage_metadata_is_preserved_and_refused(dsn, tmp_path):
    import asyncio

    import psycopg
    from test_collection_storage import local_source

    from quantlab.collection import SimulatedStream

    source, identity = local_source(tmp_path, 4), str(uuid4())
    with Repository(dsn) as repo:
        coordinator = IngestionCoordinator(repo, SourceArchive(tmp_path / "archive"))
        paused = asyncio.run(
            coordinator.collect(
                source,
                source_id=identity,
                stream=SimulatedStream(interrupt_after=2),
            )
        )
        with psycopg.connect(dsn) as connection:
            connection.execute(
                "UPDATE collection_sessions SET metrics=jsonb_set(metrics,'{dropped}',"
                "'\"bad\"'::jsonb) WHERE capture_id=%s",
                (paused.manifest.capture_id,),
            )
        with pytest.raises(ValueError, match="persisted collection coverage"):
            asyncio.run(coordinator.collect(source, source_id=identity))
        with psycopg.connect(dsn) as connection:
            assert connection.execute(
                "SELECT metrics->>'dropped' FROM collection_sessions WHERE capture_id=%s",
                (paused.manifest.capture_id,),
            ).fetchone() == ("bad",)
        assert repo.manifest(paused.manifest.dataset_id).observed == 2
        recovered = asyncio.run(coordinator.collect(source, source_id=identity, resume=False))
        assert recovered.manifest.status == "COMPLETE" and recovered.manifest.accepted == 4
        assert recovered.manifest.capture_id != paused.manifest.capture_id


def test_immediate_redelivery_keeps_bounded_original_ownership_without_rescans(dsn, tmp_path):
    import asyncio

    from test_collection_storage import local_source

    from quantlab.collection import CollectionSettings, SimulatedStream
    from quantlab.storage.contracts import BatchPolicy

    source = local_source(tmp_path, 100)
    calls = []
    original = source.candidates

    def candidates(*args):
        calls.append(True)
        return original(*args)

    source.candidates = candidates
    with Repository(dsn) as repo:
        coordinator = IngestionCoordinator(
            repo,
            SourceArchive(tmp_path / "archive"),
            settings=CollectionSettings(batching=BatchPolicy(10, 1000, 2)),
        )
        result = asyncio.run(
            coordinator.collect(
                source,
                source_id=str(uuid4()),
                stream=SimulatedStream(redeliver_every=1),
            )
        )
        assert result.manifest.status == "COMPLETE" and result.manifest.observed == 100
        assert result.metrics["received"] == 200 and result.metrics["redeliveries"] == 100
        assert result.metrics["queue_high_water"] <= 2
        assert len(calls) == 1


def test_saturated_queue_waits_without_blocking_event_loop(dsn, tmp_path, monkeypatch):
    import asyncio
    from threading import Event

    from test_collection_storage import local_source

    from quantlab.collection import CollectionSettings, ConnectionState, DeliveryMetrics
    from quantlab.storage.contracts import BatchPolicy

    async def run():
        entered, released = Event(), Event()
        third = asyncio.Event()
        with Repository(dsn) as repo:
            original = repo.write_batch

            def gated(*args):
                if not entered.is_set():
                    entered.set()
                    assert released.wait(3), "backpressure blocked the event loop"
                return original(*args)

            monkeypatch.setattr(repo, "write_batch", gated)

            class Source:
                metrics = DeliveryMetrics(ConnectionState.CONNECTED)

                async def events(self, records):
                    for index, candidate in enumerate(records, 1):
                        if index == 3:
                            third.set()
                        yield candidate

            coordinator = IngestionCoordinator(
                repo,
                SourceArchive(tmp_path / "archive"),
                settings=CollectionSettings(batching=BatchPolicy(1, 1000, 1)),
            )

            async def heartbeat():
                await asyncio.wait_for(third.wait(), 2)
                assert entered.is_set()
                released.set()

            try:
                result, _ = await asyncio.gather(
                    coordinator.collect(
                        local_source(tmp_path, 5),
                        source_id=str(uuid4()),
                        stream=Source(),
                    ),
                    heartbeat(),
                )
                assert result.manifest.status == "COMPLETE" and result.manifest.observed == 5
                assert result.metrics["queue_high_water"] <= 1
            finally:
                released.set()

    asyncio.run(run())


def test_immediately_ready_redelivery_stream_keeps_event_loop_responsive(dsn, tmp_path):
    import asyncio

    from test_collection_storage import local_source

    from quantlab.collection import ConnectionState, DeliveryMetrics

    async def run():
        ready = asyncio.Event()
        heartbeat_ran = False

        class Repeated:
            metrics = DeliveryMetrics(ConnectionState.CONNECTED)

            async def events(self, records):
                original = next(records)
                ready.set()
                yield original
                for _ in range(100):
                    yield original
                assert heartbeat_ran, "ready repeated deliveries starved the event loop"

        async def heartbeat():
            nonlocal heartbeat_ran
            await ready.wait()
            heartbeat_ran = True

        with Repository(dsn) as repo:
            coordinator = IngestionCoordinator(repo, SourceArchive(tmp_path / "archive"))
            result, _ = await asyncio.gather(
                coordinator.collect(
                    local_source(tmp_path, 1),
                    source_id=str(uuid4()),
                    stream=Repeated(),
                ),
                heartbeat(),
            )
            assert result.manifest.status == "COMPLETE" and result.manifest.observed == 1
            assert result.metrics["redeliveries"] == 100

    asyncio.run(run())
