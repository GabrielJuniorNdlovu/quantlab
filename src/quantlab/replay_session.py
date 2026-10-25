"""Coordinate causal domain updates, durable evidence, and read-only rendering."""

from collections import deque
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass, fields, replace
from datetime import datetime
from pathlib import Path
from typing import Literal
from uuid import UUID

from quantlab.alerts import AlertBook, AlertOutcome, ReplayAlert
from quantlab.audit import AuditLog
from quantlab.candles import Candle, CandleBuilder
from quantlab.chart import ChartState
from quantlab.models import MarketTick
from quantlab.parsing import required_text
from quantlab.replay import ReplayEngine, ReplayStatus
from quantlab.signals import Observation, ResearchSignal


@dataclass(frozen=True, slots=True)
class ReplayEvent:
    kind: Literal[
        "tick_observed", "candle_updated", "candle_closed", "signal_issued", "replay_completed"
    ]
    available_at_utc: datetime
    value: Observation | Candle | ResearchSignal | None


def _outcome_payload(outcome: AlertOutcome) -> dict[str, object]:
    return {field.name: getattr(outcome, field.name) for field in fields(outcome)}


class ReplaySession:
    """One owner publishes a snapshot only after its lifecycle records are durable."""

    def __init__(
        self,
        tick_factory: Callable[[], Iterator[MarketTick]],
        alerts: tuple[ReplayAlert, ...],
        dataset_hash: str,
        audit_directory: Path,
        *,
        event_positions: frozenset[int] = frozenset(),
        speed: float = 60,
        source_dataset_id: str | None = None,
        signal_producer: Callable[[Observation], tuple[ResearchSignal, ...]] | None = None,
        on_event: Callable[[ReplayEvent], None] | None = None,
    ) -> None:
        if alerts and signal_producer is not None:
            raise ValueError("use the research producer or the legacy fixture schedule")
        self._dataset_id = dataset_hash if source_dataset_id is None else source_dataset_id
        required_text(self._dataset_id, "source_dataset_id")
        self._producer = signal_producer
        self._on_event = on_event
        self._driving = False
        self._factory = tick_factory
        self._schedule = alerts
        self._dataset_hash = dataset_hash
        self._directory = audit_directory
        self._event_positions = event_positions
        self._engine = ReplayEngine(tick_factory, speed=speed)
        self._start()

    def _start(self) -> None:
        self._book = AlertBook(self._schedule, event_positions=self._event_positions)
        self._signals: dict[UUID, ResearchSignal] = {}
        self._observation: Observation | None = None
        self._terminal_signals: set[UUID] = set()
        self._candles = CandleBuilder()
        self._history: deque[Candle] = deque(maxlen=119)
        self._rendered: set[object] = set()
        self._failed = False
        self._closed = False
        self._market_ended = False
        self._state = ChartState((), (), (), 0, None, ReplayStatus.PAUSED.value)
        self._audit = AuditLog(
            self._directory,
            self._dataset_hash,
            configuration={
                "research_signal_schema": ResearchSignal.schema_version,
                "source_dataset_id": self._dataset_id,
                "availability": "max(previous availability, event time, supplied reception)",
                "producer": "current released observation only; no retained future inputs",
                "ordering": "event_time_utc then source_position; one acquisition",
                "horizon": "observed completed M1 candles strictly after issuance candle",
                "alerts": self._schedule,
                "constructed_event_positions": sorted(self._event_positions),
                "viewport_candles": 120,
            },
        )

    def _append(self, kind: str, time: datetime | None, payload: Mapping[str, object]) -> None:
        try:
            self._audit.append(kind, time, payload)
        except (OSError, ValueError, TypeError):
            self._failed = True
            self._engine.close()
            raise

    @property
    def signals(self) -> tuple[ResearchSignal, ...]:
        return tuple(self._signals.values())

    def _emit(
        self,
        kind: Literal[
            "tick_observed", "candle_updated", "candle_closed", "signal_issued", "replay_completed"
        ],
        value: Observation | Candle | ResearchSignal | None,
    ) -> None:
        if self._on_event is not None:
            available = self._engine.available_at_utc
            assert available is not None
            driving, self._driving = self._driving, True
            try:
                self._on_event(ReplayEvent(kind, available, value))
            except Exception:
                self._failed = True
                self._engine.close()
                raise
            finally:
                self._driving = driving

    def _issue_batch(self, signals: tuple[ResearchSignal, ...]) -> tuple[ResearchSignal, ...]:
        staged: dict[UUID, ResearchSignal] = {}
        for signal in signals:
            previous = self._signals.get(signal.signal_id) or staged.get(signal.signal_id)
            if previous is not None:
                if previous.to_json() != signal.to_json():
                    raise ValueError("conflicting signal ID")
                continue
            if self._closed or self._failed or self._market_ended or self._observation is None:
                raise ValueError("no open released observation for signal issuance")
            signal.check_observation(self._observation)
            staged[signal.signal_id] = signal
            if len(self._signals) + len(staged) > 1000:
                raise ValueError("research run signal capacity exceeded (1000)")
        for signal in staged.values():
            self._append("signal_issued", signal.issued_at_utc, signal.to_dict())
        self._signals.update(staged)
        return tuple(staged.values())

    def issue(self, signal: ResearchSignal) -> bool:
        """Accept an exact current-observation signal; identical redelivery is a no-op."""
        if self._driving:
            raise ValueError("return signals through signal_producer, not event callbacks")
        issued = self._issue_batch((signal,))
        if issued:
            self._state = replace(self._state, arrows=self._book.issued + self.signals)
            self._emit("signal_issued", signal)
        return bool(issued)

    def record_outcome(
        self, signal_id: UUID, *, expired: bool, metadata: dict[str, object]
    ) -> None:
        """Append externally supplied facts; Feature 02 does not evaluate predictions."""
        if self._closed or self._failed or signal_id not in self._signals:
            raise ValueError("outcome requires an issued signal in an open run")
        if signal_id in self._terminal_signals:
            raise ValueError("signal already has a terminal outcome")
        from quantlab.metadata import freeze_metadata

        signal = self._signals[signal_id]
        self._append(
            "signal_expired" if expired else "signal_outcome_recorded",
            self._engine.available_at_utc,
            {
                "signal_id": signal_id,
                "instrument_id": signal.instrument_id,
                "source_dataset_id": signal.source_dataset_id,
                "producer_version": signal.producer_version,
                "source_position": self._engine.position,
                "metadata": freeze_metadata(metadata),
            },
        )
        self._terminal_signals.add(signal_id)

    @property
    def audit_path(self) -> Path:
        return self._audit.path

    @property
    def status(self) -> ReplayStatus:
        return ReplayStatus.FAILED if self._failed else self._engine.status

    @property
    def speed(self) -> float:
        return self._engine.speed

    @speed.setter
    def speed(self, value: float) -> None:
        self._engine.speed = value

    def snapshot(self) -> ChartState:
        return replace(self._state, status=self.status.value)

    def play(self) -> None:
        if not self._failed and not self._closed:
            self._engine.play()

    def pause(self) -> None:
        self._engine.pause()

    def _consume(self, tick: MarketTick) -> None:
        available = self._engine.available_at_utc
        assert available is not None
        self._observation = Observation(tick, available, self._dataset_id)
        if self._schedule and available != tick.event_time_utc:
            raise ValueError(
                "legacy alert schedule requires event-time availability; "
                "use signal_producer for delayed observations"
            )
        completed = self._candles.consume(tick)
        issued, outcomes = self._book.consume(tick, completed)
        for anchor in issued:
            self._append(
                "alert_issued",
                tick.event_time_utc,
                {
                    "alert": anchor.alert,
                    "candle_open_utc": anchor.candle_open_utc,
                    "price": anchor.price,
                },
            )
        for outcome in outcomes:
            self._append("alert_outcome", tick.event_time_utc, _outcome_payload(outcome))
        if completed is not None:
            self._history.append(completed)
        new_signals = (
            self._issue_batch(self._producer(self._observation))
            if self._producer is not None
            else ()
        )
        current = self._candles.current
        assert current is not None
        self._state = ChartState(
            tuple(self._history) + (current,),
            self._book.issued + self.signals,
            self._book.outcomes,
            tick.source_position,
            available,
            self.status.value,
        )
        self._emit("tick_observed", self._observation)
        if completed is not None:
            self._emit("candle_closed", completed)
        self._emit("candle_updated", current)
        for signal in new_signals:
            self._emit("signal_issued", signal)

    def _finish_if_ended(self) -> None:
        if self.status is ReplayStatus.ENDED and not self._market_ended:
            current = self._engine.current
            assert current is not None
            for outcome in self._book.finish(current):
                self._append("alert_outcome", current.event_time_utc, _outcome_payload(outcome))
            self._market_ended = True
            self._emit("replay_completed", None)
            self._state = replace(self._state, outcomes=self._book.outcomes)

    def _drive(self, elapsed: float | None) -> ChartState:
        if self._driving:
            raise ValueError("reentrant replay callbacks are not supported")
        if self._failed or self._closed:
            return self.snapshot()
        self._driving = True
        try:
            if elapsed is None:
                tick = self._engine.step()
                if tick is not None:
                    self._consume(tick)
            else:
                for tick in self._engine.advance(elapsed, max_ticks=1):
                    self._consume(tick)
            self._finish_if_ended()
        except Exception:
            # Fail-stop: a partially updated internal book can never publish another frame.
            self._failed = True
            self._engine.close()
            raise
        finally:
            self._driving = False
        return self.snapshot()

    def step(self) -> ChartState:
        return self._drive(None)

    def advance(self, elapsed_seconds: float) -> ChartState:
        return self._drive(elapsed_seconds)

    def render(
        self, renderer: Callable[[ChartState], None], *, renderer_name: str = "tk_canvas"
    ) -> None:
        if renderer_name not in ("tk_canvas", "headless_geometry"):
            raise ValueError("unsupported renderer name")
        if self._closed or self._failed:
            raise ValueError("cannot render a closed or failed research run")
        state = self.snapshot()
        try:
            renderer(state)
        except Exception as exc:
            self.pause()
            if not self._closed and not self._failed:
                try:
                    self._append(
                        "render_failed", state.replay_time, {"exception_type": type(exc).__name__}
                    )
                except (OSError, ValueError):
                    self._failed = True
                    raise
            raise
        if not state.candles or self._closed or self._failed:
            return
        first, last = state.candles[0].opened_at_utc, state.candles[-1].opened_at_utc
        try:
            for anchor in state.arrows:
                if isinstance(anchor, ResearchSignal):
                    if (
                        anchor.signal_id not in self._rendered
                        and first <= anchor.bar_open_time_utc <= last
                    ):
                        self._append("signal_rendered", state.replay_time, anchor.to_dict())
                        self._rendered.add(anchor.signal_id)
                    continue
                alert = anchor.alert
                if alert.alert_id in self._rendered or not first <= anchor.candle_open_utc <= last:
                    continue
                self._append(
                    "alert_rendered",
                    state.replay_time,
                    {
                        "alert_id": alert.alert_id,
                        "candle_open_utc": anchor.candle_open_utc,
                        "price": anchor.price,
                        "issued_at_utc": alert.issued_at_utc,
                        "issued_at_source_position": alert.issued_at_source_position,
                        "renderer": renderer_name,
                    },
                )
                self._rendered.add(alert.alert_id)
        except (OSError, ValueError):
            self._failed = True
            self._engine.close()
            raise

    def reset(self) -> None:
        if self._driving:
            raise ValueError("reentrant replay callbacks are not supported")
        self.close()
        self._engine.reset()
        self._start()

    def close(self) -> None:
        if self._driving:
            raise ValueError("reentrant replay callbacks are not supported")
        self._engine.close()
        if not self._closed:
            if self._market_ended and not self._failed:
                self._audit.finish("ended", self._state.replay_time)
            else:
                self._audit.close()
            self._closed = True
