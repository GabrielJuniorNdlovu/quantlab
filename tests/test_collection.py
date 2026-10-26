"""Recorded-source asynchronous collection never changes canonical observations."""

import asyncio
from io import BytesIO

import pytest
from test_historical import profile

from quantlab.collection import (
    CollectionSettings,
    ConnectionState,
    SimulatedStream,
    SourceInterrupted,
)
from quantlab.engine import AcquisitionValidator
from quantlab.historical import HistoricalAdapter
from quantlab.storage.contracts import BatchPolicy


def candidates():
    adapter = HistoricalAdapter(profile())
    ctx = adapter.context()
    payload = (
        b"symbol,time,quote\n"
        b"Boom export,2026-01-01T00:00:01Z,1000.10\n"
        b"Boom export,2026-01-01T00:00:01Z,1000.10\n"
        b"Boom export,2026-01-01T00:00:00Z,1000.10\n"
    )
    return ctx, adapter.candidates(BytesIO(payload), ctx)


def test_async_simulator_preserves_order_and_domain_rejections():
    async def run():
        ctx, records = candidates()
        validator = AcquisitionValidator(ctx)
        stream = SimulatedStream(rate=0)
        rows = [validator.validate(candidate) async for candidate in stream.events(records)]
        validator.finish()
        assert stream.state is ConnectionState.CLOSED
        assert [row.accepted for row in rows] == [True, True, False]
        assert rows[2].events[0].error_code.value == "OUT_OF_ORDER_EVENT"
        assert rows[0].tick.price.as_tuple() == rows[1].tick.price.as_tuple()

    asyncio.run(run())


def test_simulated_interruption_is_distinct_from_eof():
    async def run():
        _, records = candidates()
        stream = SimulatedStream(rate=0, interrupt_after=1)
        with pytest.raises(SourceInterrupted):
            _ = [r async for r in stream.events(records)]
        assert stream.state is ConnectionState.INTERRUPTED

    asyncio.run(run())


@pytest.mark.parametrize("rate", [-1, float("nan"), float("inf"), True])
def test_invalid_rates_rejected(rate):
    with pytest.raises(ValueError):
        SimulatedStream(rate=rate)


def test_settings_reuse_f03_defaults():
    settings = CollectionSettings()
    assert settings.batching == BatchPolicy(100, 1000, 10000)
    assert settings.retry.max_attempts == 3 and settings.monitoring


def test_programmatic_settings_control_actual_retry_policy(tmp_path):
    from quantlab.storage.contracts import RetryPolicy
    from quantlab.storage.ingestion import IngestionCoordinator
    from quantlab.storage.repository import Repository
    from quantlab.storage.source_archive import SourceArchive

    repo = Repository("unused")
    settings = CollectionSettings(retry=RetryPolicy(max_attempts=1))
    IngestionCoordinator(repo, SourceArchive(tmp_path), settings=settings)
    assert repo.retry.max_attempts == 1


def test_archive_integrity_scans_allow_event_loop_heartbeat(tmp_path):
    import time
    from contextlib import contextmanager

    from quantlab.storage.contracts import ArchivedSource
    from quantlab.storage.ingestion import _managed

    class SlowArchive:
        @contextmanager
        def open_verified(self, receipt):
            time.sleep(0.05)
            try:
                yield BytesIO(b"fixture")
            finally:
                time.sleep(0.05)

    async def run():
        beats = 0
        finished = False

        async def work():
            nonlocal finished
            async with _managed(
                SlowArchive().open_verified(ArchivedSource("0" * 64, 7, tmp_path / "source"))
            ) as stream:
                assert stream.read() == b"fixture"
            finished = True

        async def heartbeat():
            nonlocal beats
            while not finished:
                beats += 1
                await asyncio.sleep(0.001)

        await asyncio.gather(work(), heartbeat())
        assert beats > 10

    asyncio.run(run())


@pytest.mark.parametrize(
    "changes",
    [
        {"dropped": 1},
        {"interruptions": -1},
        {"dropped": True},
        {"dropped_positions": [1]},
    ],
)
def test_invalid_delivery_metrics_cannot_claim_trusted_coverage(changes):
    from quantlab.collection import DeliveryMetrics

    with pytest.raises(ValueError):
        DeliveryMetrics(ConnectionState.CONNECTED, **changes)
