"""Offline engineering demonstration: python -m quantlab.replay_app --fixture demo."""

import argparse
import json
import sys
import time
from collections.abc import Callable, Sequence
from pathlib import Path
from types import TracebackType
from typing import TYPE_CHECKING

from quantlab.audit import verify_log
from quantlab.chart import ChartState, ChartWindow, chart_geometry
from quantlab.replay import ReplayStatus
from quantlab.replay_fixture import (
    EVENT_POSITIONS,
    dataset_hash,
    demo_alerts,
    demo_signals,
    demo_ticks,
)
from quantlab.replay_session import ReplaySession

if TYPE_CHECKING:
    import tkinter as tk


def _headless_render(frame: ChartState) -> None:
    chart_geometry(frame, 1100, 600)


def run_headless(session: ReplaySession, *, fixture: bool = False) -> dict[str, object]:
    observations = 0
    while session.status not in (ReplayStatus.ENDED, ReplayStatus.FAILED):
        previous = session.snapshot().position
        state = session.step()
        observations += state.position != previous
        session.render(_headless_render, renderer_name="headless_geometry")
    state = session.snapshot()
    session.close()
    report = verify_log(session.audit_path)
    return {
        "fixture": "DETERMINISTIC_FIXTURE" if fixture else None,
        "observations": observations,
        "issued_alerts": len(state.arrows),
        "issued_signals": len(session.signals),
        "signal_timeline": [
            {
                "signal_id": str(s.signal_id),
                "issued_at_utc": s.issued_at_utc.isoformat(),
                "bar_open_time_utc": s.bar_open_time_utc.isoformat(),
                "cutoff_source_position": s.input_cutoff_source_position,
            }
            for s in session.signals
        ],
        "outcomes": [outcome.successful for outcome in state.outcomes],
        "status": state.status,
        "audit_path": str(session.audit_path.resolve()),
        "audit_verified": report["valid"],
        "rendering": "headless_geometry; no native window drawn",
    }


class _Application:
    """Tk timer supplies elapsed presentation time; the domain never sleeps."""

    def __init__(self, root: "tk.Tk", session: ReplaySession) -> None:
        self.root = root
        self.session = session
        self._last_pulse = time.monotonic()
        self._closing = False
        self.window = ChartWindow(
            root,
            on_play=lambda: self._act(session.play),
            on_pause=lambda: self._act(session.pause),
            on_step=lambda: self._act(self._step),
            on_reset=lambda: self._act(session.reset),
            on_speed=lambda value: self._act(lambda: setattr(session, "speed", value)),
            on_close=self.close,
        )
        self.window.speed.set(str(session.speed))
        root.report_callback_exception = self._callback_error
        self._draw()
        root.after(50, self._pulse)

    def _step(self) -> None:
        self.session.step()

    def _draw(self) -> None:
        self.session.render(self.window.render)

    def _error(self, exc: BaseException) -> None:
        self.session.pause()
        message = f"{type(exc).__name__}: {exc}"
        print(message, file=sys.stderr)
        self.window.error(message)

    def _act(self, action: Callable[[], None]) -> None:
        try:
            action()
            self._last_pulse = time.monotonic()
            self._draw()
        except Exception as exc:
            # UI boundary reports the error and pauses; it never rewinds or skips data.
            self._error(exc)

    def _callback_error(
        self, kind: type[BaseException], exc: BaseException, traceback: TracebackType | None
    ) -> None:
        self._error(exc)

    def _pulse(self) -> None:
        if self._closing:
            return
        now = time.monotonic()
        elapsed, self._last_pulse = now - self._last_pulse, now
        if self.session.status is ReplayStatus.PLAYING:
            try:
                self.session.advance(elapsed)
                self._draw()
            except Exception as exc:
                self._error(exc)
        self.root.after(50, self._pulse)

    def close(self) -> None:
        self._closing = True
        try:
            self.session.close()
        finally:
            self.root.destroy()


def run_gui(session: ReplaySession) -> None:
    import tkinter as tk

    try:
        root = tk.Tk()
    except tk.TclError as exc:
        raise ValueError(f"graphical display unavailable: {exc}; use --headless") from exc
    app = _Application(root, session)
    try:
        root.mainloop()
    finally:
        if not app._closing:
            app.close()


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="QuantLab offline deterministic fixture replay")
    parser.add_argument("--fixture", choices=("demo",), default="demo")
    parser.add_argument("--headless", action="store_true")
    parser.add_argument(
        "--research-signals",
        action="store_true",
        help="canonical synthetic signals; no constructed outcome evaluation",
    )
    parser.add_argument(
        "--speed", type=float, default=60, help="presentation seconds per real second"
    )
    parser.add_argument(
        "--horizon", type=int, default=3, help="subsequent observed completed M1 candles"
    )
    parser.add_argument("--audit-dir", type=Path, default=Path("replay-runs"))
    parser.add_argument("--inspect-audit", type=Path)
    args = parser.parse_args(argv)
    try:
        if args.inspect_audit is not None:
            print(json.dumps(verify_log(args.inspect_audit), sort_keys=True))
            return 0
        session = ReplaySession(
            demo_ticks,
            () if args.research_signals else demo_alerts(args.horizon),
            dataset_hash(),
            args.audit_dir,
            event_positions=frozenset() if args.research_signals else EVENT_POSITIONS,
            signal_producer=demo_signals if args.research_signals else None,
            speed=args.speed,
        )
        try:
            if args.headless:
                print(json.dumps(run_headless(session, fixture=True), sort_keys=True))
            else:
                run_gui(session)
        finally:
            session.close()
    except (OSError, ValueError, ImportError) as exc:
        print(f"QuantLab replay: {exc}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
