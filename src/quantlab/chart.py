"""Offline Tk candlesticks and a deterministic, read-only display projection.

Decimal values become binary floats only here. Display prices must be between
1e-15 and 1e15, finite, and distinct Decimal values must remain distinguishable.
Canvas pixel coordinates can rescale; the issuance time and price never change.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from math import isfinite
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import tkinter as tk
    from tkinter import ttk

from quantlab.alerts import AlertOutcome, IssuedAlert
from quantlab.candles import Candle

BACKGROUND = "#020617"
PANEL = "#0F172A"
TEXT = "#F8FAFC"
MUTED = "#CBD5E1"
GRID = "#334155"
UP = "#4ADE80"
DOWN = "#FB7185"
FIXTURE = "#FACC15"


@dataclass(frozen=True, slots=True)
class ChartState:
    """Only released observations and already issued alerts enter the renderer."""

    candles: tuple[Candle, ...]
    arrows: tuple[IssuedAlert, ...]
    outcomes: tuple[AlertOutcome, ...]
    position: int
    replay_time: datetime | None
    status: str


@dataclass(frozen=True, slots=True)
class CandleGlyph:
    candle: Candle
    x: float
    wick_top: float
    wick_bottom: float
    body_top: float
    body_bottom: float
    half_width: float


@dataclass(frozen=True, slots=True)
class ArrowGlyph:
    issued: IssuedAlert
    x: float
    y: float


@dataclass(frozen=True, slots=True)
class ChartGeometry:
    candles: tuple[CandleGlyph, ...]
    arrows: tuple[ArrowGlyph, ...]
    price_ticks: tuple[tuple[float, float], ...] = ()
    time_ticks: tuple[tuple[float, str], ...] = ()


def _display_price(price: Decimal) -> float:
    result = float(price)
    if not isfinite(result) or not 1e-15 <= result <= 1e15:
        raise ValueError("price outside supported display range 1e-15 to 1e15")
    return result


def chart_geometry(state: ChartState, width: int, height: int) -> ChartGeometry:
    """Map UTC minute anchors and observed OHLC into a finite canvas scene."""
    for issued in state.arrows:
        alert = issued.alert
        if alert.status != "ISSUED":
            raise ValueError("renderer requires an issued alert")
        if (
            state.replay_time is None
            or alert.issued_at_utc > state.replay_time
            or alert.issued_at_source_position > state.position
        ):
            raise ValueError("renderer cannot expose a future alert")
        if issued.candle_open_utc != alert.issued_at_utc.replace(second=0, microsecond=0):
            raise ValueError("arrow anchor must match its issuance candle")
    if any(state.replay_time is None or c.opened_at_utc > state.replay_time for c in state.candles):
        raise ValueError("renderer cannot expose a future candle")
    if not state.candles:
        return ChartGeometry((), ())
    left, right, top, bottom = 76.0, max(180.0, width - 24.0), 42.0, max(130.0, height - 45.0)
    first = state.candles[0].opened_at_utc
    last = state.candles[-1].opened_at_utc
    if any(
        a.opened_at_utc >= b.opened_at_utc
        for a, b in zip(state.candles, state.candles[1:], strict=False)
    ):
        raise ValueError("chart candles must be ordered without duplicate minutes")
    minute_span = (last - first).total_seconds() / 60 + 1
    spacing = (right - left) / minute_span

    def x_at(time: datetime) -> float:
        return left + ((time - first).total_seconds() / 60 + 0.5) * spacing

    visible = tuple(a for a in state.arrows if first <= a.candle_open_utc <= last)
    prices = {p for c in state.candles for p in (c.open, c.high, c.low, c.close)}
    prices.update(a.price for a in visible)
    displayed = {_display_price(price) for price in prices}
    if len(displayed) != len(prices):
        raise ValueError("distinct prices collide at binary float display precision")
    minimum, maximum = min(displayed), max(displayed)
    padding = max((maximum - minimum) * 0.15, abs(minimum) * 0.0001, 1e-15)
    minimum -= padding
    maximum += padding

    def y_at(price: Decimal) -> float:
        return bottom - (_display_price(price) - minimum) / (maximum - minimum) * (bottom - top)

    glyphs = tuple(
        CandleGlyph(
            c,
            x_at(c.opened_at_utc),
            y_at(c.high),
            y_at(c.low),
            min(y_at(c.open), y_at(c.close)),
            max(y_at(c.open), y_at(c.close)),
            max(0.25, min(10.0, spacing * 0.25)),
        )
        for c in state.candles
    )
    arrows = tuple(ArrowGlyph(a, x_at(a.candle_open_utc), y_at(a.price)) for a in visible)
    price_ticks = tuple(
        (bottom - i * (bottom - top) / 4, minimum + i * (maximum - minimum) / 4) for i in range(5)
    )
    time_ticks = tuple(
        (g.x, g.candle.opened_at_utc.strftime("%H:%M"))
        for i, g in enumerate(glyphs)
        if i % max(1, len(glyphs) // 7) == 0
    )
    return ChartGeometry(glyphs, arrows, price_ticks, time_ticks)


def draw_chart(canvas: tk.Canvas, state: ChartState) -> None:
    """Draw a complete visible frame; failure cannot mutate the replay state."""
    width, height = max(canvas.winfo_width(), 300), max(canvas.winfo_height(), 200)
    geometry = chart_geometry(state, width, height)
    canvas.delete("all")
    canvas.create_text(
        20,
        18,
        text="BOOM 1000 / M1   ·   UTC   ·   TEST OBSERVATIONS",
        fill=MUTED,
        anchor="w",
        font=("TkDefaultFont", 11),
    )
    if not geometry.candles:
        canvas.create_text(
            width / 2,
            height / 2,
            text="Paused before the first observation.\nStep forward or press Play.",
            fill=TEXT,
            font=("TkDefaultFont", 14),
            justify="center",
        )
        return
    for y, price in geometry.price_ticks:
        canvas.create_line(76, y, width - 24, y, fill=GRID)
        canvas.create_text(66, y, text=f"{price:.7g}", fill=MUTED, anchor="e")
    for x, label in geometry.time_ticks:
        canvas.create_text(x, height - 22, text=label, fill=MUTED)
    for g in geometry.candles:
        color = UP if g.candle.close >= g.candle.open else DOWN
        canvas.create_line(g.x, g.wick_top, g.x, g.wick_bottom, fill=color, width=2)
        canvas.create_rectangle(
            g.x - g.half_width,
            g.body_top,
            g.x + g.half_width,
            max(g.body_top + 1, g.body_bottom),
            fill=color,
            outline=TEXT if not g.candle.complete else color,
            width=1 if g.candle.complete else 2,
            tags=("candle",),
        )
    for a in geometry.arrows:
        key = f"alert:{a.issued.alert.alert_id}"
        canvas.create_line(
            a.x,
            a.y + 29,
            a.x,
            a.y + 3,
            fill=FIXTURE,
            width=3,
            arrow="last",
            arrowshape=(10, 12, 5),
            tags=("arrow", key),
        )
        label_y = a.y + 43 if a.y + 60 <= height - 45 else a.y - 33
        canvas.create_text(
            a.x,
            label_y,
            text=f"FIXTURE UP\n{a.issued.alert.issued_at_utc:%H:%M:%S}",
            fill=FIXTURE,
            font=("TkDefaultFont", 9),
            tags=("arrow_label", key),
        )


class ChartWindow:
    """Native controls and chart, with callbacks supplied by the application."""

    def __init__(
        self,
        root: tk.Tk,
        *,
        on_play: Callable[[], None],
        on_pause: Callable[[], None],
        on_step: Callable[[], None],
        on_reset: Callable[[], None],
        on_speed: Callable[[float], None],
        on_close: Callable[[], None],
    ) -> None:
        import tkinter as tk
        from tkinter import ttk

        self.root = root
        self._state = ChartState((), (), (), 0, None, "PAUSED")
        root.title("QuantLab | Deterministic replay")
        root.geometry("1120x860")
        root.minsize(840, 800)
        root.configure(background=BACKGROUND)
        style = ttk.Style(root)
        style.theme_use("clam")
        style.configure("TButton", font=("TkDefaultFont", 11), padding=(16, 10))
        style.configure("TCombobox", padding=8)
        header = tk.Frame(root, background=PANEL, padx=24, pady=18)
        header.pack(fill="x")
        tk.Label(
            header,
            text="QUANTLAB",
            background=PANEL,
            foreground=TEXT,
            font=("TkDefaultFont", 21, "bold"),
        ).pack(side="left")
        tk.Label(
            header,
            text="DETERMINISTIC FIXTURE  /  RESEARCH REPLAY",
            background=PANEL,
            foreground=FIXTURE,
            font=("TkDefaultFont", 11),
        ).pack(side="right")
        controls = tk.Frame(root, background=BACKGROUND, padx=24, pady=12)
        controls.pack(fill="x")
        self.buttons: dict[str, ttk.Button] = {}
        for label, callback in (
            ("Play", on_play),
            ("Pause", on_pause),
            ("Step forward", on_step),
            ("Reset run", on_reset),
        ):
            button = ttk.Button(controls, text=label, command=callback, takefocus=True)
            button.pack(side="left", padx=(0, 8))
            self.buttons[label] = button
        tk.Label(
            controls,
            text="Replay speed",
            background=BACKGROUND,
            foreground=TEXT,
            font=("TkDefaultFont", 11),
        ).pack(side="left", padx=(16, 8))
        self.speed = ttk.Combobox(
            controls,
            values=("1", "10", "30", "60", "120", "300", "1000"),
            state="readonly",
            width=6,
            takefocus=True,
        )
        self.speed.set("60")
        self.speed.pack(side="left")
        self.speed.bind("<<ComboboxSelected>>", lambda event: on_speed(float(self.speed.get())))
        tk.Label(controls, text="×", background=BACKGROUND, foreground=TEXT).pack(
            side="left", padx=4
        )
        self.summary = tk.StringVar(root)
        tk.Label(
            root,
            textvariable=self.summary,
            background=BACKGROUND,
            foreground=TEXT,
            anchor="w",
            padx=24,
            font=("TkFixedFont", 11),
        ).pack(fill="x", pady=(0, 8))
        self.canvas = tk.Canvas(root, background=BACKGROUND, highlightthickness=0, height=400)
        self.canvas.pack(fill="both", expand=True, padx=12)
        self.canvas.bind("<Configure>", self._resize)
        self.details = tk.StringVar(root)
        history = tk.Frame(root, background=PANEL, padx=24, pady=12)
        history.pack(fill="x")
        self.history = tk.Text(
            history,
            height=5,
            wrap="word",
            background=PANEL,
            foreground=TEXT,
            insertbackground=TEXT,
            font=("TkDefaultFont", 11),
            relief="flat",
            highlightthickness=1,
            highlightcolor=TEXT,
            state="disabled",
            takefocus=True,
        )
        scroll = ttk.Scrollbar(history, command=self.history.yview)
        self.history.configure(yscrollcommand=scroll.set)
        scroll.pack(side="right", fill="y")
        self.history.pack(side="left", fill="both", expand=True)
        self.error_text = tk.StringVar(root)
        tk.Label(
            root,
            textvariable=self.error_text,
            background=BACKGROUND,
            foreground=DOWN,
            anchor="w",
            padx=24,
            wraplength=1000,
        ).pack(fill="x")
        tk.Label(
            root,
            text=(
                "Green / rose = observed candles   |   White outline = forming   |   "
                "Gold UP = fixture alert\nPredetermined test data. Outcomes describe "
                "constructed events, not forecasting accuracy."
            ),
            background=BACKGROUND,
            foreground=MUTED,
            justify="left",
            anchor="w",
            padx=24,
            pady=12,
            font=("TkDefaultFont", 10),
        ).pack(fill="x")
        root.protocol("WM_DELETE_WINDOW", on_close)
        self.render(self._state)

    def _resize(self, event: tk.Event[tk.Misc]) -> None:
        draw_chart(self.canvas, self._state)

    def render(self, state: ChartState) -> None:
        draw_chart(self.canvas, state)
        self._state = state
        self.error_text.set("")
        time = state.replay_time.isoformat() if state.replay_time is not None else "not started"
        self.summary.set(
            f"{state.status}   |   Time: {time}   |   Source position: {state.position}"
            f"   |   Issued alerts: {len(state.arrows)}"
        )
        terminal = {o.alert_id: o for o in state.outcomes}
        active = [a for a in state.arrows if a.alert.alert_id not in terminal]
        lines = [f"Active alerts: {len(active)}"]
        for issued in state.arrows:
            alert = issued.alert
            outcome = terminal.get(alert.alert_id)
            result = "ACTIVE / awaiting completed future candles"
            if outcome is not None:
                result = (
                    "RESOLVED / constructed event present"
                    if outcome.successful is True
                    else "RESOLVED / no constructed event"
                    if outcome.successful is False
                    else "EXPIRED / insufficient completed future candles"
                )
            lines.append(
                f"{str(alert.alert_id)[-8:]}  UP  issued {alert.issued_at_utc:%H:%M:%S} UTC"
                f"  at #{alert.issued_at_source_position} / {issued.price}  |  {result}"
            )
        if not state.arrows:
            lines.append("No alert has been issued.")
        details = "\n".join(lines)
        if details != self.details.get():
            self.details.set(details)
            self.history.configure(state="normal")
            self.history.delete("1.0", "end")
            self.history.insert("1.0", details)
            self.history.configure(state="disabled")
        ended = state.status in ("ENDED", "FAILED")
        self.buttons["Play"].configure(state="disabled" if ended else "normal")
        self.buttons["Step forward"].configure(state="disabled" if ended else "normal")
        self.buttons["Pause"].configure(state="normal" if state.status == "PLAYING" else "disabled")

    def error(self, message: str) -> None:
        self.error_text.set(f"Replay paused. {message}")
