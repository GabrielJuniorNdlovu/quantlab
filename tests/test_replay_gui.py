"""Opt-in actual desktop controller verification; no screenshot comparisons."""

import os

import pytest

from quantlab.audit import verify_log
from quantlab.replay import ReplayStatus
from quantlab.replay_app import _Application
from quantlab.replay_fixture import EVENT_POSITIONS, dataset_hash, demo_alerts, demo_ticks
from quantlab.replay_session import ReplaySession


@pytest.mark.skipif(
    os.environ.get("QUANTLAB_GUI_TEST") != "1", reason="opt-in native Tk display test"
)
def test_real_controller_step_pause_reset_and_arrow_audit(tmp_path):
    import tkinter as tk

    root = tk.Tk()
    run = ReplaySession(
        demo_ticks,
        demo_alerts(),
        dataset_hash(),
        tmp_path,
        event_positions=EVENT_POSITIONS,
        speed=1000,
    )
    app = _Application(root, run)
    try:
        root.update()
        assert not app.window.canvas.find_withtag("arrow")
        for _ in range(3):
            app.window.buttons["Step forward"].invoke()
            root.update()
        first = run.snapshot()
        assert first.position == 4 and len(first.arrows) == 1 and first.outcomes == ()
        assert len(app.window.canvas.find_withtag("arrow")) == 1
        app.window.buttons["Play"].invoke()
        app.window.buttons["Pause"].invoke()
        frozen = run.snapshot()
        app._pulse()
        assert run.snapshot() == frozen
        while run.status is not ReplayStatus.ENDED:
            app.window.buttons["Step forward"].invoke()
            root.update()
        assert len(app.window.canvas.find_withtag("arrow")) == 3
        assert run.snapshot().arrows[0] == first.arrows[0]
        assert [outcome.successful for outcome in run.snapshot().outcomes] == [True, False, None]
        old = run.audit_path
        app.window.buttons["Reset run"].invoke()
        assert run.audit_path != old
        assert not app.window.canvas.find_withtag("arrow")
        assert verify_log(old)["rendered_alerts"] == 3
        for _ in range(3):
            app.window.buttons["Step forward"].invoke()
        assert run.snapshot().arrows == first.arrows
    finally:
        app.close()
