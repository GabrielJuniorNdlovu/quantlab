"""Real TimescaleDB ingestion measurements using bounded engineering CSV streams.

Requires an isolated QUANTLAB_TEST_DATABASE_URL; writes are retained intentionally.
No existing database or source evidence is deleted by this benchmark.
"""

import argparse
import cProfile
import hashlib
import json
import os
import platform
import pstats
import statistics
import time
import tracemalloc
from collections.abc import Iterator
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import psycopg

from quantlab.csv_adapter import iter_csv_candidates
from quantlab.engine import validate_acquisition
from quantlab.models import AcquisitionContext, MarketTick, SourceType, ValidationOutcome
from quantlab.replay import ReplayEngine
from quantlab.storage.batching import BatchWriter
from quantlab.storage.contracts import BatchPolicy, DatasetManifest, ImportRun
from quantlab.storage.ingestion import IngestionCoordinator
from quantlab.storage.repository import Repository, migrate
from quantlab.storage.source_archive import SourceArchive

BASE = datetime(2026, 1, 1, tzinfo=UTC)


def source_file(directory: Path, count: int) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"engineering-{count}.csv"
    with path.open("x") as stream:
        stream.write("instrument_id,provider_symbol,event_time_utc,price,pip_size\n")
        for index in range(count):
            event = BASE + timedelta(microseconds=(index // 3) * 100000)
            stream.write(f"BOOM_1000,fixture-symbol,{event.isoformat()},1000.{index % 7:02d},2\n")
    return path


def outcomes(path: Path, context: AcquisitionContext) -> Iterator[ValidationOutcome]:
    with path.open("rb") as stream:
        yield from validate_acquisition(iter_csv_candidates(stream, context), context)


def check_tick(tick: MarketTick, index: int) -> None:
    assert tick.source_position == index + 2
    assert tick.event_time_utc == BASE + timedelta(microseconds=(index // 3) * 100000)
    assert tick.price == Decimal(f"1000.{index % 7:02d}")
    assert tick.price.as_tuple().exponent == -2


class TimedRepository(Repository):
    def __init__(self, dsn: str, *, record_commits: bool = True) -> None:
        super().__init__(dsn)
        self.record_commits = record_commits
        self.commit_count = 0
        self.commit_seconds: list[float] = []

    def write_batch(
        self, run: ImportRun, batch_number: int, values: tuple[ValidationOutcome, ...]
    ) -> None:
        started = time.perf_counter()
        super().write_batch(run, batch_number, values)
        self.commit_count += 1
        if self.record_commits:
            self.commit_seconds.append(time.perf_counter() - started)


def memory_status() -> dict[str, int]:
    if not Path("/proc/self/status").exists():
        return {}
    values = {}
    for line in Path("/proc/self/status").read_text().splitlines():
        key = line.split(":", 1)[0]
        if key in ("VmRSS", "VmHWM"):
            values[key + "_kib"] = int(line.split()[1])
    return values


def database_size(dsn: str) -> int:
    with psycopg.connect(dsn) as connection:
        row = connection.execute("SELECT pg_database_size(current_database())").fetchone()
        assert row is not None
        return int(row[0])


def verify_replay(repo: Repository, manifest: DatasetManifest, count: int) -> dict[str, float]:
    started = time.perf_counter()
    replay = ReplayEngine(repo.tick_factory(manifest.dataset_id, page_size=100))
    seen = 0
    while (tick := replay.step()) is not None:
        check_tick(tick, seen)
        seen += 1
    elapsed = time.perf_counter() - started
    assert seen == count
    start, end = BASE + timedelta(seconds=10), BASE + timedelta(seconds=20)
    began = time.perf_counter()
    selected = 0
    for tick in repo.tick_factory(
        manifest.dataset_id, start_utc=start, end_utc=end, page_size=100
    )():
        check_tick(tick, selected + 300)
        assert start <= tick.event_time_utc < end
        selected += 1
    duration = time.perf_counter() - began
    assert selected == min(max(count - 300, 0), 300)
    return {
        "replay_seconds": elapsed,
        "replay_ticks_per_second": count / elapsed,
        "range_seconds": duration,
        "range_rows": selected,
    }


def import_once(
    dsn: str,
    archive: SourceArchive,
    path: Path,
    count: int,
    batch: int,
    source_id: str,
    *,
    check: bool = True,
) -> dict[str, object]:
    before = database_size(dsn)
    with TimedRepository(dsn, record_commits=check) as repo:
        started, cpu = time.perf_counter(), time.process_time()
        result = IngestionCoordinator(repo, archive, BatchPolicy(max_records=batch)).import_csv(
            path,
            provider="engineering-benchmark",
            provider_symbol="fixture-symbol",
            pip_size=2,
            source_id=source_id,
        )
        seconds, cpu_seconds = time.perf_counter() - started, time.process_time() - cpu
        assert result.status == "COMPLETE" and result.accepted == result.observed == count
        assert result.rejected == result.error_count == 0
        assert repo.commit_count == (count + batch - 1) // batch
        row: dict[str, object] = {
            "dataset_id": str(result.dataset_id),
            "batch_records": batch,
            "observations": count,
            "seconds": seconds,
            "cpu_seconds": cpu_seconds,
            "observations_per_second": count / seconds,
            "batch_transactions": repo.commit_count,
            "commit_seconds": repo.commit_seconds,
            "commit_median_seconds": statistics.median(repo.commit_seconds) if check else None,
            "batch_write_observations_per_second": count / sum(repo.commit_seconds)
            if check
            else None,
            "errors": 0,
        }
        if check:
            row.update(verify_replay(repo, result, count))
    row["database_bytes_growth"] = database_size(dsn) - before
    return row


def queue_once(
    dsn: str, archive: SourceArchive, path: Path, count: int, source_id: str
) -> dict[str, object]:
    context = AcquisitionContext(
        uuid4(), "engineering-benchmark", SourceType.HISTORICAL_CSV, "fixture-symbol", 2
    )
    with Repository(dsn) as repo, repo.claim(archive.preserve(path), context, source_id) as run:
        batch_number = 0

        def persist(batch: tuple[ValidationOutcome, ...]) -> None:
            nonlocal batch_number
            batch_number += 1
            repo.write_batch(run, batch_number, batch)

        started, submit_seconds = time.perf_counter(), 0.0
        with BatchWriter(persist, BatchPolicy(max_records=100, queue_capacity=100)) as writer:
            for item in outcomes(path, context):
                assert item.accepted
                begin = time.perf_counter()
                writer.submit(item)
                submit_seconds += time.perf_counter() - begin
        elapsed = time.perf_counter() - started
        metrics = writer.metrics
        assert metrics.submitted == metrics.committed == count
        assert metrics.queued == metrics.buffered == 0 and metrics.queue_high_water <= 100
        assert repo.finish(run).accepted == count
        return {
            **asdict(metrics),
            "seconds": elapsed,
            "submit_seconds": submit_seconds,
            "queue_capacity": 100,
            "observations_per_second": count / elapsed,
            "errors": 0,
        }


def summarize(rows: list[dict[str, object]], field: str) -> dict[str, object]:
    values = [float(row[field]) for row in rows]
    return {"median": statistics.median(values), "range": [min(values), max(values)]}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=2000)
    parser.add_argument("--rounds", type=int, default=5)
    parser.add_argument("--memory-sizes", type=int, nargs="*", default=[10000, 100000])
    parser.add_argument("--output", type=Path, default=Path("benchmarks/storage-results.json"))
    args = parser.parse_args()
    if not 1000 <= args.rows <= 100000 or not 1 <= args.rounds <= 20:
        parser.error("rows must be1000..100000 and rounds1..20")
    if any(not 1000 <= size <= 1000000 for size in args.memory_sizes):
        parser.error("memory sizes must be1000..1000000")
    dsn = os.environ.get("QUANTLAB_TEST_DATABASE_URL")
    if not dsn:
        parser.error("set QUANTLAB_TEST_DATABASE_URL to an isolated real TimescaleDB database")
    hashes = {
        str(p): hashlib.sha256(p.read_bytes()).hexdigest()
        for p in Path("src/quantlab").rglob("*.py")
    }
    migrate(dsn)
    with psycopg.connect(dsn) as connection:
        row = connection.execute("""SELECT current_setting('server_version'),extversion,
                                 current_setting('fsync'),current_setting('synchronous_commit')
                                 FROM pg_extension WHERE extname='timescaledb'""").fetchone()
        assert row is not None and row[2:] == ("on", "on")
        server = dict(
            zip(("postgres", "timescaledb", "fsync", "synchronous_commit"), row, strict=True)
        )
    run_id = str(uuid4())
    root = Path(".storage-test/benchmarks") / run_id
    root.mkdir(parents=True)
    archive = SourceArchive(root / "archive")
    path = source_file(root, args.rows)
    started_load = os.getloadavg()
    trials: dict[str, list[dict[str, object]]] = {str(size): [] for size in (1, 100, 500, 1000)}
    validation: list[dict[str, object]] = []
    queue: list[dict[str, object]] = []
    for number in range(args.rounds):
        ctx = AcquisitionContext(
            uuid4(), "engineering-benchmark", SourceType.HISTORICAL_CSV, "fixture-symbol", 2
        )
        began, cpu, count = time.perf_counter(), time.process_time(), 0
        for item in outcomes(path, ctx):
            assert item.tick is not None
            check_tick(item.tick, count)
            count += 1
        duration = time.perf_counter() - began
        assert count == args.rows
        validation.append(
            {
                "seconds": duration,
                "cpu_seconds": time.process_time() - cpu,
                "observations": count,
                "observations_per_second": count / duration,
                "errors": 0,
            }
        )
        for size in (1, 100, 500, 1000):
            trials[str(size)].append(
                import_once(
                    dsn, archive, path, args.rows, size, f"{run_id}:round{number}:batch{size}"
                )
            )
        queue.append(queue_once(dsn, archive, path, args.rows, f"{run_id}:queue{number}"))
        print(f"completed timing round {number + 1}/{args.rounds}", flush=True)
    memory = []
    for size in args.memory_sizes:
        memory_path = source_file(root, size) if size != args.rows else path
        tracemalloc.start()
        row = import_once(
            dsn, archive, memory_path, size, 100, f"{run_id}:memory{size}", check=False
        )
        current, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        memory.append(
            {
                "observations": size,
                "traced_current_bytes": current,
                "traced_peak_bytes": peak,
                "rss": memory_status(),
                "seconds": row["seconds"],
            }
        )
        print(f"completed memory run {size} observations", flush=True)
    profile = cProfile.Profile()
    profile.enable()
    diagnostic = import_once(dsn, archive, path, args.rows, 100, f"{run_id}:profile")
    profile.disable()
    profile_path = args.output.with_name("storage-profile.txt")
    with profile_path.open("w") as stream:
        pstats.Stats(profile, stream=stream).strip_dirs().sort_stats("tottime").print_stats(30)
    profile_path.write_text(profile_path.read_text().rstrip() + "\n")
    result = {
        "claim": "Measured local costs only; no production capacity or baseline speedup claim.",
        "run_id": run_id,
        "python": platform.python_version(),
        "platform": platform.platform(),
        "logical_cpus": os.cpu_count(),
        "load_before": started_load,
        "load_after": os.getloadavg(),
        "server": server,
        "fixture_rows": args.rows,
        "rounds": args.rounds,
        "source_sha256": hashes,
        "benchmark_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "fixture_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        "retained_fixture_root": str(root),
        "rss": memory_status(),
        "memory": memory,
        "validation_trials": validation,
        "queue_trials": queue,
        "trials": trials,
        "summaries": {
            key: {
                field: summarize(rows, field)
                for field in (
                    "observations_per_second",
                    "commit_median_seconds",
                    "range_seconds",
                    "replay_ticks_per_second",
                    "database_bytes_growth",
                )
            }
            for key, rows in trials.items()
        },
        "diagnostic_seconds": diagnostic["seconds"],
    }
    assert hashes == {
        str(p): hashlib.sha256(p.read_bytes()).hexdigest()
        for p in Path("src/quantlab").rglob("*.py")
    }, "runtime source changed during benchmark"
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result["summaries"], indent=2))


if __name__ == "__main__":
    main()
