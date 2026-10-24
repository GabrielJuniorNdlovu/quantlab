"""Measure fully consumed validation; generate temporary engineering input only."""

import argparse
import cProfile
import hashlib
import io
import json
import os
import platform
import pstats
import resource
import statistics
import subprocess
import sys
import tempfile
import time
import tracemalloc
from collections.abc import Iterator
from contextlib import redirect_stdout
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from uuid import UUID

from quantlab.cli import main as cli_main
from quantlab.csv_adapter import iter_csv_candidates
from quantlab.engine import validate_acquisition
from quantlab.models import (
    AcquisitionContext,
    RawObservation,
    SourceFormat,
    SourceType,
    TickCandidate,
)
from quantlab.quality import ErrorCode

CAPTURE_ID = UUID("00000000-0000-4000-8000-000000000001")
CONTEXT = AcquisitionContext(
    CAPTURE_ID, "fixture-provider", SourceType.HISTORICAL_CSV, "fixture-symbol", 2
)
HEADER = b"instrument_id,provider_symbol,event_time_utc,price,pip_size\n"
ROW = b"BOOM_1000,fixture-symbol,2026-01-01T00:00:00Z,1000.10,2\n"
PRICE = Decimal("1000.10")
TIME = datetime(2026, 1, 1, tzinfo=timezone.utc)


def candidates(count: int, reject: bool) -> Iterator[TickCandidate]:
    """Include fresh immutable envelopes in core timing, without retaining inputs."""
    for position in range(1, count + 1):
        bad = reject and position % 10 == 0
        payload = ROW.replace(b"1000.10", b"NaN") if bad else ROW
        raw = RawObservation(
            CAPTURE_ID,
            position,
            CONTEXT.provider,
            SourceFormat.CSV,
            payload,
            hashlib.sha256(payload).hexdigest(),
        )
        yield TickCandidate(raw, "BOOM_1000", "fixture-symbol", TIME, "NaN" if bad else PRICE, 2)


def workload(mode: str, count: int, path: Path, reject: bool) -> tuple[int, int]:
    """Assert counts and content within the measured work, including rejections."""
    if mode == "cli":
        output = io.StringIO()
        with redirect_stdout(output):
            status = cli_main(
                [
                    str(path),
                    "--format",
                    "canonical",
                    "--capture-id",
                    str(CAPTURE_ID),
                    "--provider",
                    CONTEXT.provider,
                    "--provider-symbol",
                    CONTEXT.provider_symbol,
                    "--pip-size",
                    "2",
                ]
            )
        assert status == 0 and f"Validated records: {count}\n" in output.getvalue()
        assert "Minimum price: 1000.10\n" in output.getvalue()
        return count, 0

    def consume(source: Iterator[TickCandidate]) -> tuple[int, int]:
        accepted = rejected = total = 0
        for total, outcome in enumerate(validate_acquisition(source, CONTEXT), start=1):
            if outcome.tick is None:
                rejected += 1
                assert reject and total % 10 == 0
                assert outcome.events[0].error_code is ErrorCode.INVALID_PRICE
            else:
                accepted += 1
                assert outcome.tick.price == PRICE and outcome.tick.event_time_utc == TIME
                assert outcome.tick.source_position == total + (1 if mode == "csv" else 0)
        expected_rejects = count // 10 if reject else 0
        assert total == count and rejected == expected_rejects
        assert accepted == count - expected_rejects
        return accepted, rejected

    if mode == "csv":
        with path.open("rb") as stream:
            return consume(iter_csv_candidates(stream, CONTEXT))
    return consume(candidates(count, reject))


def worker(args: argparse.Namespace) -> None:
    if args.memory:
        tracemalloc.start()
    if args.profile:
        profiler = cProfile.Profile()
        profiler.enable()
    started = time.perf_counter()
    accepted, rejected = workload(args.mode, args.count, args.path, args.reject)
    elapsed = time.perf_counter() - started
    peak = tracemalloc.get_traced_memory()[1] if args.memory else None
    if args.memory:
        tracemalloc.stop()
    if args.profile:
        profiler.disable()
        with args.profile.open("w") as output:
            pstats.Stats(profiler, stream=output).strip_dirs().sort_stats("tottime").print_stats(25)
    print(
        json.dumps(
            {
                "seconds": elapsed,
                "accepted": accepted,
                "rejected": rejected,
                "processed_per_second": args.count / elapsed,
                "validated_per_second": accepted / elapsed,
                "tracemalloc_peak_bytes": peak,
                "peak_rss_kib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
            }
        )
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sizes", type=int, nargs="+", default=[10000, 100000])
    parser.add_argument("--runs", type=int, default=5)
    parser.add_argument("--output", type=Path, default=Path("benchmarks/results.json"))
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument(
        "--mode", choices=("core", "csv", "cli"), default="csv", help=argparse.SUPPRESS
    )
    parser.add_argument("--count", type=int, default=10000, help=argparse.SUPPRESS)
    parser.add_argument("--path", type=Path, help=argparse.SUPPRESS)
    parser.add_argument("--reject", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--memory", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--profile", type=Path, help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.worker:
        worker(args)
        return
    if args.runs < 1 or any(size < 1 for size in args.sizes):
        parser.error("sizes and runs must be positive")
    claims = (
        "Observed throughput, rejection overhead, and bounded memory by input size; "
        "no speedup or SLO claim."
    )
    print(claims, flush=True)
    cases = [
        (mode, reject, size)
        for size in args.sizes
        for mode, reject in (
            ("core", False),
            ("core", True),
            ("csv", False),
            ("csv", True),
            ("cli", False),
        )
    ]
    trials: dict[str, list[dict[str, object]]] = {}
    memory: dict[str, dict[str, object]] = {}
    load_before = os.getloadavg()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="quantlab-engineering-") as temporary:
        paths: dict[tuple[int, bool], Path] = {}
        for size in args.sizes:
            for reject in (False, True):
                path = Path(temporary) / f"{size}-{reject}.csv"
                with path.open("wb") as stream:
                    stream.write(HEADER)
                    for index in range(1, size + 1):
                        stream.write(
                            ROW.replace(b"1000.10", b"NaN") if reject and index % 10 == 0 else ROW
                        )
                paths[size, reject] = path

        def invoke(mode: str, reject: bool, size: int, *extra: str) -> dict[str, object]:
            command = [
                sys.executable,
                "-m",
                "benchmarks.validate",
                "--worker",
                "--mode",
                mode,
                "--count",
                str(size),
                "--path",
                str(paths[size, reject]),
                *extra,
            ]
            if reject:
                command.append("--reject")
            started = time.perf_counter()
            result = subprocess.run(command, text=True, capture_output=True, check=True)
            process_elapsed = time.perf_counter() - started
            data: dict[str, object] = json.loads(result.stdout)
            data["process_seconds"] = process_elapsed
            return data

        for repetition in range(args.runs):
            for mode, reject, size in cases:
                key = f"{mode}-{'10pct-reject' if reject else 'valid'}-{size}"
                result = invoke(mode, reject, size)
                trials.setdefault(key, []).append(result)
                print(
                    f"run {repetition + 1}/{args.runs}: {key}: {result['seconds']:.3f}s", flush=True
                )
        for mode, reject, size in cases:
            key = f"{mode}-{'10pct-reject' if reject else 'valid'}-{size}"
            memory[key] = invoke(mode, reject, size, "--memory")
            print(f"memory: {key}: {memory[key]['tracemalloc_peak_bytes']} bytes", flush=True)
        profile = args.output.with_name("profile.txt")
        invoke("csv", False, max(args.sizes), "--profile", str(profile.resolve()))

    summaries = {}
    for key, observations in trials.items():
        seconds = [float(trial["seconds"]) for trial in observations]
        summaries[key] = {
            "median_seconds": statistics.median(seconds),
            "min_seconds": min(seconds),
            "max_seconds": max(seconds),
            "median_processed_per_second": statistics.median(
                float(trial["processed_per_second"]) for trial in observations
            ),
            "median_validated_per_second": statistics.median(
                float(trial["validated_per_second"]) for trial in observations
            ),
            "memory": memory[key],
        }
    report = {
        "claim": claims,
        "measured_at_utc": datetime.now(timezone.utc).isoformat(),
        "python": sys.version,
        "platform": platform.platform(),
        "logical_cpus": os.cpu_count(),
        "load_before": load_before,
        "load_after": os.getloadavg(),
        "runs": args.runs,
        "source_sha256": {
            str(path): hashlib.sha256(path.read_bytes()).hexdigest()
            for path in sorted(Path("src/quantlab").glob("*.py"))
        },
        "method": (
            "Fresh worker per case. Timings exclude imports/input-file generation. "
            "Separate memory tracing. CLI process_seconds includes startup. "
            "Interleaved scenarios. OS cache uncontrolled."
        ),
        "summaries": summaries,
        "trials": trials,
    }
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(f"Saved {args.output}")


if __name__ == "__main__":
    main()
