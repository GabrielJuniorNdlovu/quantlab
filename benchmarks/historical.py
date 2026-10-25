"""Deterministic local source, validation, persistence, replay and memory measurements.

Writes only to an explicitly selected test database; retains every acquisition.
Run as: python -m benchmarks.historical --records 1000 --rounds 5
"""

import argparse
import cProfile
import io
import json
import os
import platform
import pstats
import statistics
import time
import tracemalloc
from pathlib import Path
from uuid import uuid4

from benchmarks.storage import TimedRepository, memory_status
from quantlab.engine import validate_acquisition
from quantlab.historical import HistoricalAdapter, HistoricalProfile
from quantlab.replay import ReplayEngine
from quantlab.storage.contracts import BatchPolicy
from quantlab.storage.ingestion import IngestionCoordinator
from quantlab.storage.source_archive import SourceArchive


def source_file(root: Path, count: int, format: str) -> Path:
    path = root / f"{count}.{format}"
    with path.open("w", encoding="utf-8", newline="") as stream:
        if format == "csv":
            stream.write("symbol,time,quote\n")
        for index in range(count):
            # Repeated-looking observations deliberately retained in source order.
            row = {
                "symbol": "Boom export",
                "time": "2026-01-01T00:00:00.123456Z",
                "quote": f"1000.{index % 7:02d}",
            }
            stream.write((",".join(row.values()) if format == "csv" else json.dumps(row)) + "\n")
    return path


def adapter(format: str) -> HistoricalAdapter:
    return HistoricalAdapter(
        HistoricalProfile(
            format,
            "engineering-benchmark",
            "BOOM-export",
            2,
            {"instrument_id": "symbol", "event_time": "time", "price": "quote"},
            {"Boom export": "BOOM_1000"},
        )
    )


def scan(path: Path, source: HistoricalAdapter, *, validate: bool) -> int:
    context = source.context()
    with path.open("rb") as stream:
        candidates = source.candidates(stream, context)
        if validate:
            seen = 0
            for outcome in validate_acquisition(candidates, context):
                assert outcome.accepted and not outcome.events
                assert outcome.tick is not None and outcome.tick.price.as_tuple().exponent == -2
                seen += 1
            return seen
        return sum(1 for _ in candidates)


def summary(values: list[float]) -> dict[str, float]:
    return {"median": statistics.median(values), "min": min(values), "max": max(values)}


def save_profile(profile: cProfile.Profile, path: Path) -> None:
    output = io.StringIO()
    pstats.Stats(profile, stream=output).strip_dirs().sort_stats("cumulative").print_stats(30)
    path.write_text(output.getvalue().rstrip() + "\n")


def profile_import(dsn: str, root: Path, count: int = 1000, batch: int = 100) -> None:
    """Separate diagnostic run, excluded from throughput samples."""
    import psycopg

    root.mkdir(parents=True, exist_ok=True)
    path = root / f"{count}.jsonl"
    if not path.exists():
        path = source_file(root, count, "jsonl")
    profile = cProfile.Profile()
    with TimedRepository(dsn) as repo:
        result = profile.runcall(
            IngestionCoordinator(
                repo, SourceArchive(root / "archive"), BatchPolicy(max_records=batch)
            ).import_historical,
            path,
            adapter=adapter("jsonl"),
            source_id="profile-" + uuid4().hex,
        )
        assert result.status == "COMPLETE" and result.accepted == count
    save_profile(profile, Path("benchmarks/historical-import-profile.txt"))
    with psycopg.connect(dsn) as connection:
        row = connection.execute(
            "SELECT version(),extversion,current_setting('fsync'),"
            "current_setting('synchronous_commit') FROM pg_extension WHERE extname='timescaledb'"
        ).fetchone()
        assert row is not None
    Path("benchmarks/historical-diagnostics.json").write_text(
        json.dumps(
            {
                "postgresql": row[0],
                "timescaledb": row[1],
                "fsync": row[2],
                "synchronous_commit": row[3],
                "profiled_records": count,
                "batch_records": batch,
                "errors": 0,
            },
            indent=2,
        )
        + "\n"
    )


def run(args: argparse.Namespace) -> dict[str, object]:
    dsn = os.environ["QUANTLAB_TEST_DATABASE_URL"]
    root = Path(".storage-test") / ("historical-" + uuid4().hex)
    root.mkdir(parents=True)
    archive = SourceArchive(root / "archive")
    samples: dict[str, list[dict[str, float]]] = {f: [] for f in ("csv", "jsonl")}
    paths = {f: source_file(root, args.records, f) for f in samples}
    for _ in range(args.rounds):
        for format in samples:
            source = adapter(format)
            path = paths[format]
            measures = {}
            for validate, name in ((False, "source"), (True, "validation")):
                started = time.perf_counter()
                assert scan(path, source, validate=validate) == args.records
                measures[name + "_seconds"] = time.perf_counter() - started
                measures[name + "_records_per_second"] = args.records / measures[name + "_seconds"]
            with TimedRepository(dsn) as repo:
                started, cpu = time.perf_counter(), time.process_time()
                result = IngestionCoordinator(
                    repo, archive, BatchPolicy(max_records=args.batch)
                ).import_historical(
                    path, adapter=source, source_id="historical-benchmark-" + uuid4().hex
                )
                measures["import_seconds"] = time.perf_counter() - started
                measures["import_cpu_seconds"] = time.process_time() - cpu
                assert result.status == "COMPLETE" and result.accepted == args.records
                assert (
                    result.observed == args.records and result.error_count == result.rejected == 0
                )
                assert repo.commit_count == (args.records + args.batch - 1) // args.batch
                measures["import_records_per_second"] = args.records / measures["import_seconds"]
                measures["persistence_records_per_second"] = args.records / sum(repo.commit_seconds)
                started = time.perf_counter()
                replay = ReplayEngine(repo.tick_factory(result.dataset_id, page_size=100))
                seen = 0
                while (tick := replay.step()) is not None:
                    assert str(tick.price) == f"1000.{seen % 7:02d}"
                    assert tick.source_position == seen + (2 if format == "csv" else 1)
                    seen += 1
                replay.close()
                assert seen == args.records
                measures["replay_records_per_second"] = seen / (time.perf_counter() - started)
                measures["errors"] = 0
            samples[format].append(measures)
    memory = []
    for format in samples:
        for count in (10000, 100000):
            path = source_file(root, count, format)
            tracemalloc.start()
            assert scan(path, adapter(format), validate=True) == count
            _, peak = tracemalloc.get_traced_memory()
            tracemalloc.stop()
            memory.append(
                {"format": format, "records": count, "peak_traced_bytes": peak, **memory_status()}
            )
    import_memory = []
    for format in samples:
        for count in (1000, 10000):
            path = root / f"{count}.{format}"
            if not path.exists():
                path = source_file(root, count, format)
            tracemalloc.start()
            with TimedRepository(dsn, record_commits=False) as repo:
                result = IngestionCoordinator(
                    repo, archive, BatchPolicy(max_records=args.batch)
                ).import_historical(
                    path, adapter=adapter(format), source_id="memory-" + uuid4().hex
                )
                assert result.status == "COMPLETE" and result.accepted == count
            _, peak = tracemalloc.get_traced_memory()
            tracemalloc.stop()
            import_memory.append(
                {"format": format, "records": count, "peak_traced_bytes": peak, **memory_status()}
            )
    profile = cProfile.Profile()
    profile.runcall(scan, paths["jsonl"], adapter("jsonl"), validate=True)
    save_profile(profile, Path("benchmarks/historical-profile.txt"))
    profile_import(dsn, root, args.records, args.batch)
    return {
        "python": platform.python_version(),
        "platform": platform.platform(),
        "records_per_import": args.records,
        "rounds": args.rounds,
        "batch_records": args.batch,
        "source_bytes": {f: p.stat().st_size for f, p in paths.items()},
        "samples": samples,
        "summary": {
            f: {k: summary([s[k] for s in rows]) for k in rows[0]} for f, rows in samples.items()
        },
        "memory": memory,
        "import_memory": import_memory,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--records", type=int, default=1000)
    parser.add_argument("--rounds", type=int, default=5)
    parser.add_argument("--batch", type=int, default=100)
    args = parser.parse_args()
    if args.records < 1 or args.rounds < 1:
        parser.error("records and rounds must be positive")
    output = run(args)
    Path("benchmarks/historical-results.json").write_text(json.dumps(output, indent=2) + "\n")
    print(json.dumps(output["summary"], indent=2))


if __name__ == "__main__":
    main()
