"""Absolute Feature 02 timings on repeated 34-tick engineering fixture replays."""

import argparse
import cProfile
import hashlib
import json
import os
import platform
import pstats
import resource
import statistics
import sys
import tempfile
import time
import tracemalloc
from collections.abc import Callable
from pathlib import Path

from quantlab.alerts import AlertBook
from quantlab.audit import verify_log
from quantlab.candles import CandleBuilder
from quantlab.chart import ChartState, chart_geometry, draw_chart
from quantlab.replay import ReplayEngine, ReplayStatus
from quantlab.replay_fixture import EVENT_POSITIONS, dataset_hash, demo_alerts, demo_ticks
from quantlab.replay_session import ReplaySession


def measure(work: Callable[[], int], iterations: int) -> dict[str, float | int]:
    cpu, wall = time.process_time(), time.perf_counter()
    count = sum(work() for _ in range(iterations))
    duration, cpu_time = time.perf_counter() - wall, time.process_time() - cpu
    assert count > 0
    return {
        "seconds": duration,
        "cpu_seconds": cpu_time,
        "work_count": count,
        "iterations": iterations,
        "work_per_second": count / duration,
        "seconds_per_work": duration / count,
        "errors": 0,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gui", action="store_true", help="measure actual Tk Canvas on DISPLAY")
    parser.add_argument("--output", type=Path, default=Path("benchmarks/replay-results.json"))
    args = parser.parse_args()
    source_hashes = {
        p.name: hashlib.sha256(p.read_bytes()).hexdigest()
        for p in Path("src/quantlab").glob("*.py")
    }
    ticks = tuple(demo_ticks())
    assert len(ticks) == 34 and ticks[-1].source_position == 35
    alerts = demo_alerts()
    builder, book = CandleBuilder(), AlertBook(alerts, event_positions=EVENT_POSITIONS)
    completions, states, history = [], [], []
    for tick in ticks:
        completed = builder.consume(tick)
        completions.append(completed)
        book.consume(tick, completed)
        if completed:
            history.append(completed)
        assert builder.current is not None
        states.append(
            ChartState(
                tuple(history) + (builder.current,),
                book.issued,
                book.outcomes,
                tick.source_position,
                tick.event_time_utc,
                "PAUSED",
            )
        )
    assert len(history) == 10 and builder.current.opened_at_utc.minute == 11

    def replay() -> int:
        engine = ReplayEngine(lambda: iter(ticks))
        seen = []
        while (tick := engine.step()) is not None:
            seen.append(tick)
        assert tuple(seen) == ticks and engine.status is ReplayStatus.ENDED
        return len(seen)

    def candles() -> int:
        aggregation = CandleBuilder()
        closed = [aggregation.consume(t) for t in ticks]
        assert closed == completions and aggregation.current == builder.current
        return len(ticks)

    def lifecycle() -> int:
        lifecycle = AlertBook(alerts, event_positions=EVENT_POSITIONS)
        for tick, completed in zip(ticks, completions, strict=True):
            lifecycle.consume(tick, completed)
        lifecycle.finish(ticks[-1])
        assert len(lifecycle.issued) == 3
        assert [o.successful for o in lifecycle.outcomes] == [True, False, None]
        return len(ticks)

    def geometry() -> int:
        for state in states:
            scene = chart_geometry(state, 1100, 450)
            assert len(scene.candles) == len(state.candles)
            assert len(scene.arrows) == len(state.arrows)
        return len(states)

    root = None
    cases: dict[str, tuple[Callable[[], int], int]] = {
        "replay_ticks": (replay, 1000),
        "candle_ticks": (candles, 500),
        "alert_ticks": (lifecycle, 500),
        "chart_geometry_updates": (geometry, 100),
    }
    if args.gui:
        import tkinter as tk

        root = tk.Tk()
        canvas = tk.Canvas(root, width=1100, height=450)
        canvas.pack()
        root.update()

        def native() -> int:
            assert root is not None
            for state in states:
                draw_chart(canvas, state)
                root.update_idletasks()
                assert len(canvas.find_withtag("candle")) == len(state.candles)
                assert len(canvas.find_withtag("arrow")) == len(state.arrows)
            return len(states)

        cases["tk_canvas_updates"] = (native, 5)
    rss_samples = {}
    rss_high_water = {}

    def sample_rss(label: str) -> None:
        if sys.platform == "linux":
            status = Path("/proc/self/status").read_text()
            rss_samples[label] = int(status.split("VmRSS:")[1].split()[0])
            rss_high_water[label] = int(status.split("VmHWM:")[1].split()[0])

    sample_rss("setup")
    load_before = os.getloadavg()
    with tempfile.TemporaryDirectory(
        prefix=".quantlab-replay-benchmark-", dir=Path.cwd()
    ) as directory:
        audit_directory = Path(directory)

        def integrated() -> int:
            session = ReplaySession(
                demo_ticks, alerts, dataset_hash(), audit_directory, event_positions=EVENT_POSITIONS
            )
            released = 0
            try:
                while session.status is not ReplayStatus.ENDED:
                    state = session.step()
                    if session.status is not ReplayStatus.ENDED:
                        released += 1
                    session.render(
                        lambda s: chart_geometry(s, 1100, 450), renderer_name="headless_geometry"
                    )
                session.close()
                result = verify_log(session.audit_path)
                assert released == 34
                assert (
                    result["issued_alerts"] == result["rendered_alerts"] == result["outcomes"] == 3
                )
                assert [o.successful for o in state.outcomes] == [True, False, None]
                return released
            finally:
                session.close()

        cases["integrated_ticks"] = (integrated, 5)
        trials: dict[str, list[dict[str, float | int]]] = {name: [] for name in cases}
        for _ in range(5):
            for name, (work, iterations) in cases.items():
                trials[name].append(measure(work, iterations))
                sample_rss(f"round{_}:{name}")
        memory = {}
        for name, (work, _) in cases.items():
            tracemalloc.start()
            assert work() > 0
            _, peak = tracemalloc.get_traced_memory()
            tracemalloc.stop()
            memory[name] = peak
            sample_rss(f"memory:{name}")
        with args.output.with_name("replay-profile.txt").open("w") as stream:
            for name, (work, _) in cases.items():
                profile = cProfile.Profile()
                profile.enable()
                assert work() > 0
                profile.disable()
                stream.write(f"\n{name} (independent diagnostic, excluded from timings)\n")
                pstats.Stats(profile, stream=stream).strip_dirs().sort_stats("tottime").print_stats(
                    15
                )
    profile_path = args.output.with_name("replay-profile.txt")
    profile_path.write_text(profile_path.read_text().rstrip() + "\n")
    sample_rss("after_profile")
    if root is not None:
        root.destroy()
    sample_rss("after_tk_destroy")
    summaries = {}
    for name, rows in trials.items():
        elapsed = [float(r["seconds_per_work"]) for r in rows]
        summaries[name] = {
            "median_seconds_per_work": statistics.median(elapsed),
            "range_seconds_per_work": [min(elapsed), max(elapsed)],
            "median_work_per_second": 1 / statistics.median(elapsed),
            "separate_traced_peak_bytes": memory[name],
        }
    result = {
        "claim": "Absolute local replay and rendering costs; no baseline speedup claim.",
        "python": sys.version,
        "platform": platform.platform(),
        "cpu_count": os.cpu_count(),
        "display": os.environ.get("DISPLAY") if args.gui else None,
        "load_before": load_before,
        "load_after": os.getloadavg(),
        "dataset_sha256": dataset_hash(),
        "fixture_ticks": len(ticks),
        "audit_directory_parent": str(Path.cwd()),
        "source_sha256": source_hashes,
        "rss_peak_kib_process_lifetime": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
        "rss_samples_kib": rss_samples,
        "linux_rss_high_water_kib": rss_high_water,
        "summaries": summaries,
        "trials": trials,
    }
    assert source_hashes == {
        p.name: hashlib.sha256(p.read_bytes()).hexdigest()
        for p in Path("src/quantlab").glob("*.py")
    }, "source changed during benchmark"
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(summaries, indent=2))


if __name__ == "__main__":
    main()
