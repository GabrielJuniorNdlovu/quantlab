"""Measured F03 imports, incremental writes, replay, queries, and resource use.

Use only an isolated QUANTLAB_TEST_DATABASE_URL. Data is deliberately retained.
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
import time
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import psycopg

from quantlab.csv_adapter import iter_csv_candidates
from quantlab.engine import validate_acquisition
from quantlab.models import AcquisitionContext, SourceType
from quantlab.replay import ReplayEngine
from quantlab.storage.batching import BatchWriter
from quantlab.storage.contracts import BatchPolicy
from quantlab.storage.ingestion import IngestionCoordinator
from quantlab.storage.repository import Repository, migrate
from quantlab.storage.source_archive import SourceArchive

BASE = datetime(2026, 1, 1, microsecond=123456, tzinfo=UTC)


def source_file(parent, count):
    path = parent / f"synthetic-{count}.csv"
    with path.open("x") as stream:
        stream.write("instrument_id,provider_symbol,event_time_utc,price,pip_size\n")
        for index in range(count):
            event = BASE + timedelta(seconds=(index // 3) * 3)
            stream.write(
                f"BOOM_1000,fixture-symbol,{event.isoformat()},1000.{index // 3 % 7:02d},2\n"
            )
    return path


def check_tick(tick, index):
    assert tick.source_position == index + 2
    assert tick.event_time_utc == BASE + timedelta(seconds=(index // 3) * 3)
    assert str(tick.price) == f"1000.{index // 3 % 7:02d}"
    assert tick.received_at_utc is None and tick.provider_tick_id is None
    assert tick.instrument_id == "BOOM_1000" and tick.provider_symbol == "fixture-symbol"
    assert tick.pip_size == 2 and tick.source_type is SourceType.HISTORICAL_CSV


class MeasuredRepository(Repository):
    def __init__(self, dsn):
        super().__init__(dsn)
        self.latencies = []

    def write_batch(self, run, batch_number, outcomes):
        start = time.perf_counter()
        super().write_batch(run, batch_number, outcomes)
        self.latencies.append(time.perf_counter() - start)


def read_verify(repo, manifest, count):
    start = time.perf_counter()
    engine = ReplayEngine(repo.tick_factory(manifest.dataset_id, page_size=1000))
    seen = 0
    while (tick := engine.step()) is not None:
        check_tick(tick, seen)
        assert tick.capture_id == manifest.capture_id
        seen += 1
    elapsed = time.perf_counter() - start
    assert seen == count
    first = BASE + timedelta(seconds=30)
    last = first + timedelta(seconds=300)
    ranges = []
    for _ in range(5):
        began = time.perf_counter()
        rows = tuple(repo.tick_factory(manifest.dataset_id, start_utc=first, end_utc=last)())
        ranges.append(time.perf_counter() - began)
        assert len(rows) == 300
        for index, tick in enumerate(rows, 30):
            check_tick(tick, index)
    began = time.perf_counter()
    exact = repo.observation(manifest.dataset_id, source_position=count // 2 + 2)
    lookup = time.perf_counter() - began
    check_tick(exact, count // 2)
    return {
        "replay_seconds": elapsed,
        "replay_ticks_per_second": count / elapsed,
        "range_rows": 300,
        "range_seconds": ranges,
        "lookup_seconds": lookup,
    }


def run_case(dsn, archive, path, count, batch_size, source_id, incremental=False):
    with MeasuredRepository(dsn) as repo:
        before = repo.storage_usage()
        began, cpu = time.perf_counter(), time.process_time()
        metrics = None
        if not incremental:
            manifest = IngestionCoordinator(
                repo, archive, BatchPolicy(max_records=batch_size)
            ).import_csv(
                path,
                provider="synthetic-engineering",
                provider_symbol="fixture-symbol",
                pip_size=2,
                source_id=source_id,
            )
        else:
            ctx = AcquisitionContext(
                uuid4(), "synthetic-engineering", SourceType.HISTORICAL_CSV, "fixture-symbol", 2
            )
            source = archive.preserve(path)
            with repo.claim(source, ctx, source_id) as run:
                number = 0

                def sink(values):
                    nonlocal number
                    number += 1
                    repo.write_batch(run, number, values)

                with BatchWriter(
                    sink, BatchPolicy(max_records=batch_size, queue_capacity=batch_size)
                ) as writer:
                    with archive.open_verified(source) as stream:
                        for outcome in validate_acquisition(iter_csv_candidates(stream, ctx), ctx):
                            assert outcome.accepted
                            writer.submit(outcome)
                metrics = asdict(writer.metrics)
                assert metrics["submitted"] == metrics["committed"] == count
                assert metrics["queue_high_water"] <= batch_size and metrics["failures"] == 0
                manifest = repo.finish(run)
        seconds, cpu_seconds = time.perf_counter() - began, time.process_time() - cpu
        assert manifest.status == "COMPLETE" and manifest.accepted == manifest.observed == count
        assert manifest.rejected == manifest.error_count == 0
        assert len(repo.latencies) == (count + batch_size - 1) // batch_size
        progress = repo.commit_status(manifest.capture_id)
        assert progress.source_position == count + 1 and progress.observed == count
        after = repo.storage_usage()
        result = {
            "dataset_id": str(manifest.dataset_id),
            "capture_id": str(manifest.capture_id),
            "count": count,
            "batch_size": batch_size,
            "incremental": incremental,
            "seconds": seconds,
            "cpu_seconds": cpu_seconds,
            "ticks_per_second": count / seconds,
            "batch_write_median_seconds": statistics.median(repo.latencies),
            "batch_write_p95_seconds": sorted(repo.latencies)[
                int((len(repo.latencies) - 1) * 0.95)
            ],
            "batch_count": len(repo.latencies),
            "source_sha256": manifest.source_sha256,
            "storage_before": before,
            "storage_after": after,
            "client_lifetime_peak_rss_kib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
            "metrics": metrics,
            "errors": 0,
        }
        result.update(read_verify(repo, manifest, count))
        return result


def plans(dsn, row):
    with psycopg.connect(dsn) as connection:
        statements = {
            "range": (
                "SELECT * FROM market_ticks WHERE capture_id=%s AND event_time_utc >= %s "
                "AND event_time_utc < %s ORDER BY event_time_utc,source_position",
                (row["capture_id"], BASE + timedelta(seconds=30), BASE + timedelta(seconds=330)),
            ),
            "raw_identity": (
                "SELECT accepted_at FROM raw_observations WHERE capture_id=%s "
                "AND source_position=%s",
                (row["capture_id"], row["count"] // 2 + 2),
            ),
            "tick_identity": (
                "SELECT * FROM market_ticks WHERE event_time_utc=%s "
                "AND capture_id=%s AND source_position=%s",
                (
                    BASE + timedelta(seconds=(row["count"] // 2 // 3) * 3),
                    row["capture_id"],
                    row["count"] // 2 + 2,
                ),
            ),
        }
        connection.execute("ANALYZE market_ticks")
        connection.execute("ANALYZE raw_observations")
        return {
            name: [
                item[0]
                for item in connection.execute(
                    "EXPLAIN (ANALYZE, BUFFERS) " + statement, values
                ).fetchall()
            ]
            for name, (statement, values) in statements.items()
        }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sizes", nargs="+", type=int, default=[100000, 1000000, 2000000])
    parser.add_argument("--rounds", type=int, default=5)
    parser.add_argument(
        "--output", type=Path, default=Path("benchmarks/unified-storage-results.json")
    )
    args = parser.parse_args()
    if args.rounds < 1 or any(n < 1000 for n in args.sizes):
        parser.error("positive rounds and at least 1000 observations per size are required")
    dsn = os.environ["QUANTLAB_TEST_DATABASE_URL"]
    migrate(dsn)
    with psycopg.connect(dsn) as connection:
        version = connection.execute("SELECT version()").fetchone()[0]
        extension = connection.execute(
            "SELECT extversion FROM pg_extension WHERE extname='timescaledb'"
        ).fetchone()[0]
        settings = {
            key: connection.execute("SHOW " + key).fetchone()[0]
            for key in ("fsync", "synchronous_commit", "shared_buffers", "max_connections")
        }
    parent = Path(".storage-test/f03-refactor") / uuid4().hex
    parent.mkdir(parents=True)
    paths = {size: source_file(parent, size) for size in args.sizes}
    archive = SourceArchive(parent / "archive")
    source_hashes = {
        str(p): hashlib.sha256(p.read_bytes()).hexdigest()
        for p in Path("src/quantlab").rglob("*.py")
    }
    result = {
        "claim": "Absolute local costs; no cross-version speedup or production SLO claim",
        "python": platform.python_version(),
        "platform": platform.platform(),
        "cpus": os.cpu_count(),
        "load_before": os.getloadavg(),
        "database_version": version,
        "timescaledb_version": extension,
        "settings": settings,
        "source_sha256": source_hashes,
        "artifacts": str(parent),
        "batch_trials": [],
        "scale_trials": [],
    }

    def save():
        args.output.write_text(json.dumps(result, indent=2) + "\n")

    first = args.sizes[0]
    for round_number in range(args.rounds):
        for size, incremental in ((100, False), (1000, False), (10000, False), (1000, True)):
            row = run_case(
                dsn,
                archive,
                paths[first],
                first,
                size,
                f"{parent}:round{round_number}:batch{size}:queue{incremental}",
                incremental,
            )
            result["batch_trials"].append(row)
            print(
                f"round={round_number + 1} batch={size} incremental={incremental} "
                f"rows={first} rate={row['ticks_per_second']:.1f}/s",
                flush=True,
            )
            save()
    for size in args.sizes[1:]:
        row = run_case(dsn, archive, paths[size], size, 1000, f"{parent}:scale{size}")
        result["scale_trials"].append(row)
        print(
            f"scale={size} rate={row['ticks_per_second']:.1f}/s "
            f"rss_peak={row['client_lifetime_peak_rss_kib']}KiB",
            flush=True,
        )
        save()
    largest = result["scale_trials"][-1] if result["scale_trials"] else result["batch_trials"][-1]
    result["plans"] = plans(dsn, largest)
    profile = cProfile.Profile()
    # A separate 1000-row diagnostic excludes profiler overhead from all reported trials.
    profile_path = source_file(parent, 1000) if 1000 not in paths else paths[1000]
    profile.runcall(run_case, dsn, archive, profile_path, 1000, 1000, f"{parent}:profile")
    with args.output.with_suffix(".profile.txt").open("w") as stream:
        pstats.Stats(profile, stream=stream).strip_dirs().sort_stats("tottime").print_stats(25)
    result["load_after"] = os.getloadavg()
    assert source_hashes == {
        str(p): hashlib.sha256(p.read_bytes()).hexdigest()
        for p in Path("src/quantlab").rglob("*.py")
    }
    save()
    print("Complete; outputs, source counts, replay, and query results verified.", flush=True)


if __name__ == "__main__":
    main()
