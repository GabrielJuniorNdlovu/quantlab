"""Bounded, ordered persistence with an idle deadline and explicit backpressure."""

import json
import logging
import math
import time
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass
from queue import Full
from threading import Condition, Thread
from types import TracebackType

from quantlab.models import ValidationOutcome
from quantlab.storage.contracts import BatchPolicy

_DEFAULT_POLICY = BatchPolicy()
_LOG = logging.getLogger("quantlab.batching")


@dataclass(frozen=True, slots=True)
class BatchMetrics:
    submitted: int
    committed: int
    batches: int
    queued: int
    buffered: int
    queue_high_water: int


class BatchWriter:
    """One worker commits existing outcomes; the caller owns validation ordering.

    Submit blocks at queue capacity or raises Full on timeout, leaving ownership
    of the unaccepted observation with the caller. Durability is acknowledged only
    after the sink returns. Sink calls must impose their own I/O timeout. Normal
    close drains and joins; unexpected process exit loses queued/uncommitted work.
    The 10,000-entry default is finite but raw payloads alone can occupy 625 MiB;
    metadata and Python objects add more. Configure a smaller queue for limited memory.
    """

    def __init__(
        self,
        sink: Callable[[tuple[ValidationOutcome, ...]], None],
        policy: BatchPolicy = _DEFAULT_POLICY,
        *,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._sink = sink
        self._policy = policy
        self._clock = clock
        self._queue: deque[ValidationOutcome] = deque()
        self._condition = Condition()
        self._closing = False
        self._failure: BaseException | None = None
        self._submitted = self._committed = self._batches = self._high_water = 0
        self._buffered = 0
        self._worker = Thread(target=self._run, name="quantlab-batch-writer", daemon=True)
        self._worker.start()

    @property
    def metrics(self) -> BatchMetrics:
        with self._condition:
            return BatchMetrics(
                self._submitted,
                self._committed,
                self._batches,
                len(self._queue),
                self._buffered,
                self._high_water,
            )

    def _raise_failed(self) -> None:
        if self._failure is not None:
            raise self._failure

    def submit(self, outcome: ValidationOutcome, timeout: float | None = None) -> None:
        if not isinstance(outcome, ValidationOutcome):
            raise TypeError("batch writer requires a ValidationOutcome")
        if timeout is not None and (
            isinstance(timeout, bool) or not math.isfinite(timeout) or timeout < 0
        ):
            raise ValueError("submit timeout must be finite and nonnegative")
        deadline = None if timeout is None else time.monotonic() + timeout
        with self._condition:
            while True:
                self._raise_failed()
                if self._closing:
                    raise ValueError("batch writer is closed")
                if len(self._queue) < self._policy.queue_capacity:
                    self._queue.append(outcome)
                    self._submitted += 1
                    self._high_water = max(self._high_water, len(self._queue))
                    self._condition.notify_all()
                    return
                remaining = None if deadline is None else deadline - time.monotonic()
                if remaining is not None and remaining <= 0:
                    raise Full("batch queue is full; observation was not accepted")
                self._condition.wait(remaining)

    def _log(
        self,
        event: str,
        *,
        latency: float | None,
        failure: BaseException | None = None,
        capture_id: str | None = None,
    ) -> None:
        metrics = self.metrics
        values = {
            "event": event,
            "capture_id": capture_id,
            "submitted": metrics.submitted,
            "committed": metrics.committed,
            "batches": metrics.batches,
            "queued": metrics.queued,
            "buffered": metrics.buffered,
            "queue_capacity": self._policy.queue_capacity,
            "queue_high_water": metrics.queue_high_water,
            "commit_latency_seconds": latency,
        }
        if failure is not None:
            values["exception_type"] = type(failure).__name__
        _LOG.log(
            logging.ERROR if failure is not None else logging.INFO,
            json.dumps(values, sort_keys=True),
        )

    def _run(self) -> None:
        pending: list[ValidationOutcome] = []
        deadline: float | None = None
        commit_started: float | None = None
        try:
            while True:
                with self._condition:
                    while not self._queue and not self._closing:
                        remaining = None if deadline is None else deadline - self._clock()
                        if remaining is not None and remaining <= 0:
                            break
                        self._condition.wait(remaining)
                    due = deadline is not None and self._clock() >= deadline
                    if self._queue and not due:
                        pending.append(self._queue.popleft())
                        if deadline is None:
                            deadline = self._clock() + self._policy.max_wait_ms / 1000
                        self._buffered = len(pending)
                        self._condition.notify_all()
                    flush = bool(pending) and (
                        len(pending) >= self._policy.max_records
                        or due
                        or (self._closing and not self._queue)
                    )
                    done = self._closing and not self._queue and not pending
                if flush:
                    commit_started = time.monotonic()
                    self._sink(tuple(pending))
                    latency = time.monotonic() - commit_started
                    with self._condition:
                        self._committed += len(pending)
                        self._batches += 1
                        self._buffered = 0
                    capture_id = str(pending[0].raw.capture_id)
                    pending.clear()
                    deadline = None
                    commit_started = None
                    self._log("batch_committed", latency=latency, capture_id=capture_id)
                if done:
                    return
        except BaseException as exc:
            with self._condition:
                self._failure = exc
                self._condition.notify_all()
            self._log(
                "batch_failed",
                latency=None if commit_started is None else time.monotonic() - commit_started,
                failure=exc,
                capture_id=str(pending[0].raw.capture_id) if pending else None,
            )

    def close(self) -> None:
        with self._condition:
            self._closing = True
            self._condition.notify_all()
        self._worker.join()
        with self._condition:
            self._raise_failed()

    def __enter__(self) -> "BatchWriter":
        return self

    def __exit__(
        self,
        kind: type[BaseException] | None,
        error: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()
