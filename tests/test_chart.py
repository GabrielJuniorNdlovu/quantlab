"""Deterministic chart geometry proves issuance alignment without screenshots."""

import os
import subprocess
import sys
from dataclasses import FrozenInstanceError, replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import UUID

import pytest

from quantlab.alerts import IssuedAlert, ReplayAlert
from quantlab.candles import Candle
from quantlab.chart import ChartState, chart_geometry
from quantlab.models import INSTRUMENT_ID

BASE = datetime(2026, 1, 1, tzinfo=UTC)


def candle(minute: int = 0, *, low: str = "999.00") -> Candle:
    return Candle(
        INSTRUMENT_ID,
        BASE + timedelta(minutes=minute),
        Decimal("1000.00"),
        Decimal("1002.00"),
        Decimal(low),
        Decimal("1001.00"),
        4,
        False,
    )


def arrow() -> IssuedAlert:
    alert = ReplayAlert(
        alert_id=UUID(int=1),
        instrument_id=INSTRUMENT_ID,
        issued_at_utc=BASE + timedelta(seconds=30),
        issued_at_source_position=2,
        data_cutoff_position=2,
        status="ISSUED",
    )
    return IssuedAlert(alert, BASE, Decimal("1000.00"))


def state(*, arrows: tuple[IssuedAlert, ...] = ()) -> ChartState:
    return ChartState((candle(),), arrows, (), 2, BASE + timedelta(seconds=30), "PAUSED")


def test_empty_chart_has_no_hidden_price_or_arrow() -> None:
    geometry = chart_geometry(ChartState((), (), (), 0, None, "PAUSED"), 900, 500)
    assert geometry.candles == ()
    assert geometry.arrows == ()


def test_arrow_is_on_its_issuance_candle_and_price() -> None:
    geometry = chart_geometry(state(arrows=(arrow(),)), 900, 500)
    assert len(geometry.candles) == len(geometry.arrows) == 1
    assert geometry.arrows[0].x == geometry.candles[0].x
    assert geometry.arrows[0].issued.candle_open_utc == BASE
    assert geometry.arrows[0].issued.price == Decimal("1000.00")
    assert geometry.candles[0].wick_top < geometry.arrows[0].y < geometry.candles[0].wick_bottom


def test_anchor_stays_at_original_event_after_candle_low_changes() -> None:
    original = state(arrows=(arrow(),))
    later = replace(
        original,
        candles=(candle(low="990.00"), candle(1)),
        position=9,
        replay_time=BASE + timedelta(minutes=1, seconds=30),
    )
    a = chart_geometry(original, 900, 500).arrows[0].issued
    b = chart_geometry(later, 900, 500).arrows[0].issued
    assert a == b
    assert original.arrows == later.arrows
    with pytest.raises(FrozenInstanceError):
        b.price = Decimal("990.00")  # type: ignore[misc]


def test_missing_minutes_leave_proportional_empty_chart_space() -> None:
    s = replace(
        state(), candles=(candle(0), candle(1), candle(4)), replay_time=BASE + timedelta(minutes=4)
    )
    items = chart_geometry(s, 900, 500).candles
    assert items[2].x - items[1].x == pytest.approx(3 * (items[1].x - items[0].x))


def test_restart_produces_equivalent_geometry() -> None:
    assert chart_geometry(state(arrows=(arrow(),)), 900, 500) == chart_geometry(
        state(arrows=(arrow(),)), 900, 500
    )


@pytest.mark.parametrize("position,time", [(1, BASE + timedelta(seconds=30)), (2, BASE)])
def test_renderer_rejects_future_issuance(position: int, time: datetime) -> None:
    s = replace(state(arrows=(arrow(),)), position=position, replay_time=time)
    with pytest.raises(ValueError, match="future"):
        chart_geometry(s, 900, 500)


def test_off_viewport_arrow_is_retained_in_state() -> None:
    s = replace(
        state(arrows=(arrow(),)), candles=(candle(1),), replay_time=BASE + timedelta(minutes=1)
    )
    assert chart_geometry(s, 900, 500).arrows == ()
    assert s.arrows == (arrow(),)


def test_unrepresentable_display_prices_fail_explicitly() -> None:
    huge = replace(candle(), high=Decimal("1e1000"))
    with pytest.raises(ValueError, match="display"):
        chart_geometry(replace(state(), candles=(huge,)), 900, 500)


def test_float_collisions_fail_instead_of_drawing_flat_prices() -> None:
    precise = replace(
        candle(),
        open=Decimal("1000.000000000000001"),
        high=Decimal("1000.000000000000002"),
        low=Decimal("1000"),
        close=Decimal("1000.000000000000001"),
    )
    with pytest.raises(ValueError, match="display"):
        chart_geometry(replace(state(), candles=(precise,)), 900, 500)


def test_scheduled_alert_cannot_be_rendered() -> None:
    a = arrow()
    with pytest.raises(ValueError, match="ISSUED"):
        replace(a, alert=replace(a.alert, status="SCHEDULED"))


def test_future_candle_cannot_be_exposed_while_paused() -> None:
    with pytest.raises(ValueError, match="future candle"):
        chart_geometry(replace(state(), candles=(candle(1),)), 900, 500)


@pytest.mark.skipif(
    os.environ.get("QUANTLAB_GUI_TEST") != "1", reason="opt-in native Tk display test"
)
def test_native_window_controls_and_arrow_rendering() -> None:
    import tkinter as tk

    from quantlab.chart import ChartWindow

    root = tk.Tk()
    calls: list[str | float] = []
    try:
        window = ChartWindow(
            root,
            on_play=lambda: calls.append("play"),
            on_pause=lambda: calls.append("pause"),
            on_step=lambda: calls.append("step"),
            on_reset=lambda: calls.append("reset"),
            on_speed=lambda value: calls.append(value),
            on_close=root.destroy,
        )
        root.update()
        assert window.canvas.find_withtag("arrow") == ()
        window.buttons["Play"].invoke()
        window.buttons["Step forward"].invoke()
        window.buttons["Reset run"].invoke()
        assert calls == ["play", "step", "reset"]
        issued = state(arrows=(arrow(),))
        window.render(replace(issued, status="PLAYING"))
        root.update()
        assert len(window.canvas.find_withtag("candle")) == 1
        assert len(window.canvas.find_withtag("arrow")) == 1
        assert "Issued alerts: 1" in window.summary.get()
        assert "issued 00:00:30 UTC" in window.details.get()
        assert "00000001" in window.details.get()
        label = window.canvas.find_withtag("arrow_label")[0]
        assert window.canvas.coords(label)[1] < window.canvas.winfo_height() - 45
        window.buttons["Pause"].invoke()
        window.speed.set("120")
        window.speed.event_generate("<<ComboboxSelected>>")
        root.update()
        assert calls[-2:] == ["pause", 120.0]
        before = issued.arrows[0]
        window.render(
            replace(
                issued,
                candles=(candle(low="990.00"), candle(1)),
                position=9,
                replay_time=BASE + timedelta(minutes=1),
            )
        )
        root.update()
        assert window._state.arrows[0] == before
        assert len(window.canvas.find_withtag("arrow")) == 1
        window.error("test render failure")
        assert "test render failure" in window.error_text.get()
    finally:
        root.destroy()


def test_headless_chart_does_not_import_tkinter() -> None:
    code = """
import importlib.abc
import sys
class BlockTk(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path, target=None):
        if fullname == 'tkinter' or fullname.startswith('tkinter.'):
            raise ImportError('Tk deliberately unavailable')
sys.meta_path.insert(0, BlockTk())
from quantlab.chart import ChartState, chart_geometry
assert chart_geometry(ChartState((), (), (), 0, None, 'PAUSED'), 900, 500).candles == ()
"""
    result = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, check=False
    )
    assert result.returncode == 0, result.stderr
