"""Replayable recorded sources and deterministic asynchronous delivery."""

import asyncio
import math
from collections.abc import AsyncGenerator, Iterator, Mapping
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import BinaryIO, Protocol
from uuid import UUID

from quantlab.historical import HistoricalAdapter, HistoricalProfile
from quantlab.models import AcquisitionContext, TickCandidate
from quantlab.storage.contracts import ArchivedSource, BatchPolicy, RetryPolicy
from quantlab.storage.source_archive import SourceArchive


@dataclass(frozen=True, slots=True)
class CollectionSettings:
    batching: BatchPolicy = field(default_factory=BatchPolicy)
    retry: RetryPolicy = field(default_factory=RetryPolicy)
    monitoring: bool = True
    recovery_enabled: bool = True

    def __post_init__(self) -> None:
        if not isinstance(self.batching, BatchPolicy) or not isinstance(self.retry, RetryPolicy):
            raise ValueError("collection requires BatchPolicy and RetryPolicy")
        if type(self.monitoring) is not bool or type(self.recovery_enabled) is not bool:
            raise ValueError("monitoring and recovery_enabled must be boolean")


class HistoricalSource(Protocol):
    """A registered immutable snapshot with reproducible source positions and parsing."""

    def register(self, archive: SourceArchive) -> ArchivedSource: ...
    def context(self, capture_id: UUID | None = None) -> AcquisitionContext: ...
    def candidates(
        self, stream: BinaryIO, context: AcquisitionContext
    ) -> Iterator[TickCandidate]: ...


class LocalHistoricalSource:
    def __init__(self, path: Path, profile: HistoricalProfile) -> None:
        self.path = path
        self.adapter = HistoricalAdapter(profile)

    def register(self, archive: SourceArchive) -> ArchivedSource:
        return archive.preserve(self.path)

    def context(self, capture_id: UUID | None = None) -> AcquisitionContext:
        return self.adapter.context(capture_id)

    def candidates(self, stream: BinaryIO, context: AcquisitionContext) -> Iterator[TickCandidate]:
        return self.adapter.candidates(stream, context)


class ConnectionState(str, Enum):
    DISCONNECTED = "DISCONNECTED"
    CONNECTED = "CONNECTED"
    INTERRUPTED = "INTERRUPTED"
    CLOSED = "CLOSED"


class SourceInterrupted(OSError):
    def __init__(self, message: str, *, replayable: bool = False) -> None:
        super().__init__(message)
        self.replayable = replayable


@dataclass(frozen=True, slots=True)
class DeliveryMetrics:
    state: ConnectionState
    interruptions: int = 0
    reconnects: int = 0
    dropped: int = 0
    dropped_positions: tuple[int, ...] = ()
    coverage_guaranteed: bool = True

    def __post_init__(self) -> None:
        if not isinstance(self.state, ConnectionState):
            raise ValueError("delivery state must be a ConnectionState")
        for count in (self.interruptions, self.reconnects, self.dropped):
            if type(count) is not int or count < 0:
                raise ValueError("delivery counters must be nonnegative integers")
        if not isinstance(self.dropped_positions, tuple):
            raise ValueError("delivery loss positions require an immutable tuple")
        if self.dropped and self.coverage_guaranteed:
            raise ValueError("reported delivery drops require uncertain coverage")
        if (
            len(self.dropped_positions) > 20
            or any(type(position) is not int or position < 1 for position in self.dropped_positions)
            or len(set(self.dropped_positions)) > self.dropped
            or type(self.coverage_guaranteed) is not bool
        ):
            raise ValueError("invalid bounded delivery coverage evidence")

    @classmethod
    def from_snapshot(cls, value: Mapping[str, object]) -> "DeliveryMetrics":
        def count(name: str) -> int:
            result = value.get(name, 0)
            if type(result) is not int or result < 0:
                raise ValueError("invalid persisted collection coverage counter")
            return result

        positions = value.get("dropped_positions", ())
        guaranteed = value.get("source_coverage_guaranteed", True)
        if (
            not isinstance(positions, (tuple, list))
            or len(positions) > 20
            or type(guaranteed) is not bool
        ):
            raise ValueError("invalid persisted collection coverage metadata")
        checked: list[int] = []
        for position in positions:
            if type(position) is not int:
                raise ValueError("invalid persisted loss position")
            checked.append(position)
        return cls(
            ConnectionState.DISCONNECTED,
            interruptions=count("source_interruptions"),
            dropped=count("dropped"),
            dropped_positions=tuple(checked),
            coverage_guaranteed=guaranteed,
        )


class MarketDataStream(Protocol):
    @property
    def metrics(self) -> DeliveryMetrics: ...
    def events(
        self, candidates: Iterator[TickCandidate]
    ) -> AsyncGenerator[TickCandidate, None]: ...


class SimulatedStream:
    """Capture-scoped delivery with deterministic faults; use a fresh instance per capture.

    One instance can reconnect within its capture. Fault/coverage counters are cumulative.
    """

    def __init__(
        self,
        *,
        rate: float = 0,
        interrupt_after: int | None = None,
        disconnect_after: int | None = None,
        redeliver_every: int | None = None,
        drop_after: int | None = None,
    ) -> None:
        if isinstance(rate, bool) or not math.isfinite(rate) or rate < 0:
            raise ValueError("rate must be finite and nonnegative; zero means unpaced")
        for name, count in (
            ("interrupt_after", interrupt_after),
            ("disconnect_after", disconnect_after),
            ("drop_after", drop_after),
        ):
            if count is not None and (type(count) is not int or count < 0):
                raise ValueError(f"{name} must be a nonnegative record count")
        if redeliver_every is not None and (
            type(redeliver_every) is not int or redeliver_every < 1
        ):
            raise ValueError("redeliver_every must be a positive record count")
        self.rate = rate
        self.interrupt_after = interrupt_after
        self.disconnect_after = disconnect_after
        self.redeliver_every = redeliver_every
        self.drop_after = drop_after
        self._state = ConnectionState.DISCONNECTED
        self._interruptions = self._reconnects = self._delivered = self._dropped = 0
        self._disconnected = self._has_dropped = False
        self._dropped_positions: tuple[int, ...] = ()

    @property
    def state(self) -> ConnectionState:
        return self._state

    @property
    def metrics(self) -> DeliveryMetrics:
        return DeliveryMetrics(
            self.state,
            self._interruptions,
            self._reconnects,
            self._dropped,
            self._dropped_positions,
            not self._dropped,
        )

    async def events(
        self, candidates: Iterator[TickCandidate]
    ) -> AsyncGenerator[TickCandidate, None]:
        if self.state is ConnectionState.INTERRUPTED:
            self._reconnects += 1
        self._state = ConnectionState.CONNECTED
        seen = 0
        try:
            while True:
                if self.interrupt_after is not None and seen >= self.interrupt_after:
                    raise SourceInterrupted("simulated source stop before verified EOF")
                if (
                    not self._disconnected
                    and self.disconnect_after is not None
                    and self._delivered >= self.disconnect_after
                ):
                    self._disconnected = True
                    raise SourceInterrupted("simulated replayable disconnect", replayable=True)
                candidate = next(candidates, None)
                if candidate is None:
                    self._state = ConnectionState.CLOSED
                    return
                if not self._has_dropped and self.drop_after == self._delivered:
                    self._has_dropped = True
                    self._dropped += 1
                    self._dropped_positions = (candidate.raw.source_position,)
                    continue
                # Zero only yields control; historical event time never throttles a file.
                await asyncio.sleep(1 / self.rate if self.rate else 0)
                seen += 1
                self._delivered += 1
                yield candidate
                if self.redeliver_every and self._delivered % self.redeliver_every == 0:
                    yield candidate
        except SourceInterrupted:
            self._interruptions += 1
            self._state = ConnectionState.INTERRUPTED
            raise
        except BaseException:
            self._state = ConnectionState.INTERRUPTED
            raise
        finally:
            if self.state is ConnectionState.CONNECTED:
                self._state = ConnectionState.CLOSED
