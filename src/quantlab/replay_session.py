"""Coordinate causal domain updates, durable evidence, and read-only rendering."""

from collections import deque
from collections.abc import Callable, Iterator
from dataclasses import fields, replace
from pathlib import Path

from quantlab.alerts import AlertBook, AlertOutcome, ReplayAlert
from quantlab.audit import AuditLog
from quantlab.candles import Candle, CandleBuilder
from quantlab.chart import ChartState
from quantlab.models import MarketTick
from quantlab.replay import ReplayEngine, ReplayStatus


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
    ) -> None:
        self._factory = tick_factory
        self._schedule = alerts
        self._dataset_hash = dataset_hash
        self._directory = audit_directory
        self._event_positions = event_positions
        self._engine = ReplayEngine(tick_factory, speed=speed)
        self._start()

    def _start(self) -> None:
        self._book = AlertBook(self._schedule, event_positions=self._event_positions)
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
                "ordering": "event_time_utc then source_position; one acquisition",
                "horizon": "observed completed M1 candles strictly after issuance candle",
                "alerts": self._schedule,
                "constructed_event_positions": sorted(self._event_positions),
                "viewport_candles": 120,
            },
        )

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
        completed = self._candles.consume(tick)
        issued, outcomes = self._book.consume(tick, completed)
        for anchor in issued:
            self._audit.append(
                "alert_issued",
                tick.event_time_utc,
                {
                    "alert": anchor.alert,
                    "candle_open_utc": anchor.candle_open_utc,
                    "price": anchor.price,
                },
            )
        for outcome in outcomes:
            self._audit.append("alert_outcome", tick.event_time_utc, _outcome_payload(outcome))
        if completed is not None:
            self._history.append(completed)
        current = self._candles.current
        assert current is not None
        self._state = ChartState(
            tuple(self._history) + (current,),
            self._book.issued,
            self._book.outcomes,
            tick.source_position,
            tick.event_time_utc,
            self.status.value,
        )

    def _finish_if_ended(self) -> None:
        if self.status is ReplayStatus.ENDED and not self._market_ended:
            current = self._engine.current
            assert current is not None
            for outcome in self._book.finish(current):
                self._audit.append(
                    "alert_outcome", current.event_time_utc, _outcome_payload(outcome)
                )
            self._market_ended = True
            self._state = replace(self._state, outcomes=self._book.outcomes)

    def _drive(self, elapsed: float | None) -> ChartState:
        if self._failed or self._closed:
            return self.snapshot()
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
                    self._audit.append(
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
                alert = anchor.alert
                if alert.alert_id in self._rendered or not first <= anchor.candle_open_utc <= last:
                    continue
                self._audit.append(
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
        self.close()
        self._engine.reset()
        self._start()

    def close(self) -> None:
        self._engine.close()
        if not self._closed:
            if self._market_ended and not self._failed:
                self._audit.finish("ended", self._state.replay_time)
            else:
                self._audit.close()
            self._closed = True
