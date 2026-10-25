import threading
from queue import Full

import pytest

from quantlab.csv_adapter import iter_csv_candidates
from quantlab.engine import validate_acquisition
from quantlab.replay_fixture import CONTEXT
from quantlab.storage.batching import BatchWriter
from quantlab.storage.contracts import BatchPolicy


def outcome():
    from io import BytesIO

    payload = (
        b"instrument_id,provider_symbol,event_time_utc,price,pip_size\n"
        b"BOOM_1000,fixture-symbol,2026-01-01T00:00:00Z,1000.12,2\n"
    )
    return next(validate_acquisition(iter_csv_candidates(BytesIO(payload), CONTEXT), CONTEXT))


def test_size_threshold_empty_close_and_shutdown_flush():
    batches = []
    writer = BatchWriter(
        batches.append, BatchPolicy(max_records=2, max_wait_ms=60000, queue_capacity=2)
    )
    row = outcome()
    for _ in range(5):
        writer.submit(row)
    writer.close()
    writer.close()
    assert [len(batch) for batch in batches] == [2, 2, 1]
    metrics = writer.metrics
    assert metrics.submitted == metrics.committed == 5
    assert metrics.batches == 3 and metrics.queued == metrics.buffered == 0
    assert metrics.queue_high_water <= 2
    empty = []
    BatchWriter(empty.append).close()
    assert empty == []


def test_idle_deadline_flushes_without_second_observation():
    committed = threading.Event()
    batches = []

    def sink(batch):
        batches.append(batch)
        committed.set()

    writer = BatchWriter(sink, BatchPolicy(max_records=100, max_wait_ms=20))
    try:
        writer.submit(outcome())
        assert committed.wait(2), "idle producer never triggered timed persistence"
        assert len(batches) == 1 and len(batches[0]) == 1
    finally:
        writer.close()


def test_queue_backpressure_timeout_and_durable_metrics():
    entered = threading.Event()
    release = threading.Event()

    def sink(batch):
        entered.set()
        assert release.wait(2)

    writer = BatchWriter(sink, BatchPolicy(max_records=1, max_wait_ms=60000, queue_capacity=1))
    row = outcome()
    try:
        writer.submit(row)
        assert entered.wait(1)
        assert writer.metrics.committed == 0
        writer.submit(row)
        with pytest.raises(Full):
            writer.submit(row, timeout=0)
        assert writer.metrics.submitted == 2 and writer.metrics.queue_high_water == 1
    finally:
        release.set()
        writer.close()
    assert writer.metrics.committed == 2


def test_sink_failure_wakes_waiting_producers_and_close_propagates():
    entered = threading.Event()
    fail = threading.Event()
    raised = threading.Event()

    def sink(batch):
        entered.set()
        assert fail.wait(2)
        raise RuntimeError("database unavailable")

    writer = BatchWriter(sink, BatchPolicy(max_records=1, queue_capacity=1))
    row = outcome()
    writer.submit(row)
    assert entered.wait(1)
    writer.submit(row)
    errors = []

    def blocked():
        try:
            writer.submit(row)
        except RuntimeError as exc:
            errors.append(str(exc))
            raised.set()

    producer = threading.Thread(target=blocked)
    producer.start()
    fail.set()
    assert raised.wait(2)
    producer.join(1)
    assert errors == ["database unavailable"]
    with pytest.raises(RuntimeError, match="database unavailable"):
        writer.close()
    assert writer.metrics.committed == 0
    assert writer.metrics.submitted == 2


def test_context_shutdown_flushes_and_submit_after_close_rejected():
    batches = []
    with BatchWriter(batches.append, BatchPolicy(max_records=100, max_wait_ms=60000)) as writer:
        writer.submit(outcome())
    assert len(batches) == 1
    with pytest.raises(ValueError, match="closed"):
        writer.submit(outcome())


@pytest.mark.parametrize("timeout", [-1, float("nan"), float("inf"), True])
def test_invalid_timeout_rejected(timeout):
    writer = BatchWriter(lambda batch: None)
    try:
        with pytest.raises(ValueError):
            writer.submit(outcome(), timeout=timeout)
    finally:
        writer.close()


def test_injected_clock_deadline_controls_flush():
    now = [10.0]
    started = threading.Event()
    committed = threading.Event()

    def clock():
        started.set()
        return now[0]

    writer = BatchWriter(
        lambda batch: committed.set(), BatchPolicy(max_records=100, max_wait_ms=1000), clock=clock
    )
    try:
        writer.submit(outcome())
        assert started.wait(1)
        # Wake the independent consumer after advancing only the explicit clock.
        with writer._condition:
            now[0] = 11.1
            writer._condition.notify_all()
        assert committed.wait(1)
    finally:
        writer.close()


def test_structured_commit_and_failure_logs_never_claim_unacknowledged_persistence(caplog):
    import json
    import logging

    caplog.set_level(logging.INFO, logger="quantlab.batching")
    writer = BatchWriter(lambda batch: None, BatchPolicy(max_records=1))
    writer.submit(outcome())
    writer.close()
    messages = [
        json.loads(record.getMessage())
        for record in caplog.records
        if record.name == "quantlab.batching"
    ]
    assert len(messages) == 1
    committed = messages[0]
    assert committed["event"] == "batch_committed"
    assert committed["submitted"] == committed["committed"] == 1
    assert committed["buffered"] == committed["queued"] == 0
    assert committed["queue_capacity"] == 10000
    assert committed["commit_latency_seconds"] >= 0
    assert "raw_payload" not in committed

    def fail(batch):
        raise RuntimeError("sensitive connection details must not enter logs")

    writer = BatchWriter(fail, BatchPolicy(max_records=1))
    writer.submit(outcome())
    with pytest.raises(RuntimeError):
        writer.close()
    failure = json.loads(caplog.records[-1].getMessage())
    assert failure["event"] == "batch_failed"
    assert failure["committed"] == 0 and failure["buffered"] == 1
    assert failure["exception_type"] == "RuntimeError"
    assert "sensitive connection details" not in caplog.records[-1].getMessage()
