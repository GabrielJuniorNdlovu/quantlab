"""Absolute costs on generated canonical ticks, including source construction.

No speedup claim. Five interleaved trials, separate profiling and traced memory.
Large-view render samples every 1000 observations, preserving every underlying tick.
"""

import argparse
import cProfile
import hashlib
import json
import os
import platform
import pstats
import resource
import statistics
import tempfile
import time
import tracemalloc
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from uuid import NAMESPACE_URL, uuid5

from quantlab.audit import AuditLog, verify_log
from quantlab.candles import CandleBuilder
from quantlab.chart import chart_geometry, draw_chart
from quantlab.replay import ReplayEngine, ReplayStatus
from quantlab.replay_fixture import demo_ticks
from quantlab.replay_session import ReplaySession
from quantlab.signals import ResearchSignal

TEMPLATE = next(demo_ticks())
BASE = TEMPLATE.event_time_utc


def generated_hash(size):
    return hashlib.sha256(
        json.dumps(
            {
                "generator": "large.synthetic.v1",
                "ticks": size,
                "interval_ms": 500,
                "base": BASE.isoformat(),
                "price": str(TEMPLATE.price),
                "provider_symbol": TEMPLATE.provider_symbol,
                "pip_size": TEMPLATE.pip_size,
                "capture_id": str(TEMPLATE.capture_id),
            },
            sort_keys=True,
        ).encode()
    ).hexdigest()


def source(size):
    for position in range(1, size + 1):
        yield replace(
            TEMPLATE,
            event_time_utc=BASE + timedelta(milliseconds=500 * (position - 1)),
            source_position=position,
        )


def fixture(observation):
    if observation.tick.source_position % 1000:
        return ()
    return (
        ResearchSignal.issue(
            signal_id=uuid5(
                NAMESPACE_URL,
                f"large.synthetic.v1:{observation.source_dataset_id}:{observation.tick.source_position}",
            ),
            observation=observation,
            producer_version="large.synthetic.v1",
        ),
    )


def work(mode, size, parent, canvas=None, root=None):
    def factory():
        return source(size)

    if mode == "engine":
        engine = ReplayEngine(factory)
        count = 0
        while engine.step() is not None:
            count += 1
        assert count == size and engine.position == size
        return count
    if mode == "candles":
        builder = CandleBuilder()
        closed = sum(builder.consume(t) is not None for t in factory())
        assert closed == (size - 1) // 120
        assert builder.current is not None and not builder.current.complete
        return size
    with tempfile.TemporaryDirectory(dir=parent) as directory:
        if mode == "audit":
            log = AuditLog(Path(directory), generated_hash(size))
            for position in range(1000):
                log.append("benchmark_diagnostic", BASE, {"position": position})
            log.close()
            assert verify_log(log.path)["records"] == 1002
            assert log.path.stat().st_size > 1000
            return 1000
        run = ReplaySession(
            factory,
            (),
            generated_hash(size),
            Path(directory),
            signal_producer=None if mode == "session" else fixture,
        )
        count = frames = 0
        try:
            while run.status is not ReplayStatus.ENDED:
                previous = run.snapshot().position
                state = run.step()
                if state.position != previous:
                    count += 1
                if (
                    mode in ("geometry", "native")
                    and count
                    and count % 1000 == 0
                    and state.position != previous
                ):
                    if mode == "native":
                        run.render(lambda s: draw_chart(canvas, s))
                        root.update_idletasks()
                        assert len(canvas.find_withtag("candle")) == len(state.candles)
                    else:
                        run.render(
                            lambda s: chart_geometry(s, 1100, 600),
                            renderer_name="headless_geometry",
                        )
                    frames += 1
                assert len(state.candles) <= 120
            assert count == size
            assert len(run.signals) == (0 if mode == "session" else size // 1000)
            run.close()
            result = verify_log(run.audit_path)
            assert result["issued_signals"] == len(run.signals)
            if mode in ("geometry", "native"):
                assert frames >= size // 1000
            return count
        finally:
            run.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ticks", type=int, default=100_000)
    parser.add_argument("--gui", action="store_true")
    parser.add_argument(
        "--output", type=Path, default=Path("benchmarks/causal-replay-results.json")
    )
    args = parser.parse_args()
    if not 1000 <= args.ticks <= 1_000_000:
        parser.error("ticks must be 1000..1000000 (1000 signal capacity)")
    source_hashes = {
        p.name: hashlib.sha256(p.read_bytes()).hexdigest()
        for p in Path("src/quantlab").glob("*.py")
    }
    cases = ["engine", "candles", "session", "signals", "audit", "geometry"]
    root = canvas = None
    if args.gui:
        import tkinter as tk

        root = tk.Tk()
        canvas = tk.Canvas(root, width=1100, height=600)
        canvas.pack()
        root.update()
        cases.append("native")
    trials = {mode: [] for mode in cases}
    load_before = os.getloadavg()
    with tempfile.TemporaryDirectory(prefix="quantlab-causal-benchmark-") as parent:
        for _ in range(5):
            for mode in cases:
                start, cpu = time.perf_counter(), time.process_time()
                count = work(mode, args.ticks, parent, canvas, root)
                seconds = time.perf_counter() - start
                trials[mode].append(
                    {
                        "seconds": seconds,
                        "cpu_seconds": time.process_time() - cpu,
                        "work_count": count,
                        "work_per_second": count / seconds,
                        "errors": 0,
                    }
                )
                print(f"trial={_ + 1} mode={mode} seconds={seconds:.3f}", flush=True)
        memory = {}
        for size in (10_000, args.ticks):
            tracemalloc.start()
            assert work("geometry", size, parent) == size
            memory[str(size)] = tracemalloc.get_traced_memory()[1]
            tracemalloc.stop()
        profile = cProfile.Profile()
        profile.runcall(work, "geometry", 10_000, parent)
        profile_path = args.output.with_suffix(".profile.txt")
        with profile_path.open("w") as stream:
            pstats.Stats(profile, stream=stream).strip_dirs().sort_stats("tottime").print_stats(20)
    profile_path.write_text(profile_path.read_text().rstrip() + "\n")
    if root is not None:
        root.destroy()
    summaries = {}
    for mode, rows in trials.items():
        rates = [r["work_per_second"] for r in rows]
        summaries[mode] = {
            "median_work_per_second": statistics.median(rates),
            "range_work_per_second": [min(rates), max(rates)],
        }
    assert source_hashes == {
        p.name: hashlib.sha256(p.read_bytes()).hexdigest()
        for p in Path("src/quantlab").glob("*.py")
    }
    args.output.write_text(
        json.dumps(
            {
                "source_sha256": source_hashes,
                "claim": "Absolute local costs; no speedup or production capacity claim",
                "python": platform.python_version(),
                "platform": platform.platform(),
                "cpu_count": os.cpu_count(),
                "generated_dataset_sha256": generated_hash(args.ticks),
                "process_lifetime_peak_rss_kib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
                "load_before": load_before,
                "load_after": os.getloadavg(),
                "ticks": args.ticks,
                "ordering": "500ms event interval; increasing source position",
                "source_identity": "SHA-256 of frozen generator configuration; synthetic",
                "source_generation_included": True,
                "render_sample_every_ticks": 1000,
                "audit_policy": "flush+fsync per record; temporary files removed after verification",  # noqa: E501
                "memory_separate_traced_peak_bytes": memory,
                "summaries": summaries,
                "trials": trials,
            },
            indent=2,
        )
        + "\n"
    )
    print(json.dumps(summaries, indent=2))


if __name__ == "__main__":
    main()
