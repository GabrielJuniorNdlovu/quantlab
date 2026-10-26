"""F04 actual recorded ingestion, recovery, replay, and Parquet measurements.

Uses an isolated QUANTLAB_TEST_DATABASE_URL and retains all source/database evidence.
"""

import argparse
import asyncio
import cProfile
import hashlib
import json
import os
import platform
import pstats
import resource
import shutil
import statistics
import time
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import psycopg
import pyarrow.parquet as pq

from benchmarks.unified_storage import MeasuredRepository
from quantlab.collection import CollectionSettings, LocalHistoricalSource, SimulatedStream
from quantlab.dataset_export import SplitPlan, export_parquet
from quantlab.historical import HistoricalProfile
from quantlab.replay import ReplayEngine
from quantlab.storage.contracts import BatchPolicy
from quantlab.storage.ingestion import IngestionCoordinator
from quantlab.storage.repository import migrate
from quantlab.storage.source_archive import SourceArchive

BASE = datetime(2026, 1, 1, microsecond=123456, tzinfo=UTC)
HEADER = b"symbol,time,quote,received,basis,id\n"


def event(index):
    return BASE + timedelta(seconds=index // 3 * 3)


def record(index):
    moment = event(index)
    return (
        f"Boom export,{moment.isoformat()},1000.{index // 3 % 7:02d},"
        f"{(moment + timedelta(seconds=1)).isoformat()},bid,synthetic-{index}\n"
    ).encode()


def source_file(parent, size):
    path = parent / f"synthetic-{size}.csv"
    with path.open("xb") as stream:
        stream.write(HEADER)
        for index in range(size):
            stream.write(record(index))
    return path


def source(path):
    return LocalHistoricalSource(
        path,
        HistoricalProfile(
            "csv",
            "synthetic-engineering",
            "fixture-symbol",
            2,
            {
                "instrument_id": "symbol",
                "event_time": "time",
                "price": "quote",
                "received_at_utc": "received",
                "price_basis": "basis",
                "provider_tick_id": "id",
            },
            {"Boom export": "BOOM_1000"},
        ),
    )


def check_tick(tick, index, capture):
    assert tick.capture_id == capture and tick.source_position == index + 2
    assert tick.event_time_utc == event(index)
    assert tick.received_at_utc == event(index) + timedelta(seconds=1)
    assert tick.price.as_tuple() == Decimal(f"1000.{index // 3 % 7:02d}").as_tuple()
    assert tick.provider_tick_id == f"synthetic-{index}"
    assert tick.instrument_id == "BOOM_1000" and tick.provider_symbol == "fixture-symbol"
    assert tick.pip_size == 2


def verify_replay(repo, manifest, count):
    began = time.perf_counter()
    engine = ReplayEngine(repo.tick_factory(manifest.dataset_id, page_size=1000))
    seen = 0
    while (tick := engine.step()) is not None:
        check_tick(tick, seen, manifest.capture_id)
        seen += 1
    engine.close()
    assert seen == count
    return time.perf_counter() - began


def verify_export(output, manifest, count):
    seen = 0
    for name in ("train", "validation", "test"):
        info = manifest["files"][f"{name}.parquet"]
        path = output / f"{name}.parquet"
        with path.open("rb") as stream:
            assert hashlib.file_digest(stream, "sha256").hexdigest() == info["sha256"]
        local = 0
        for batch in pq.ParquetFile(path).iter_batches(batch_size=1000):
            for row in batch.to_pylist():
                assert row["source_position"] == seen + 2
                assert row["capture_id"] == manifest["request"]["capture_id"]
                assert row["provider"] == "synthetic-engineering"
                assert row["instrument_id"] == "BOOM_1000"
                assert row["source_type"] == "historical_csv"
                assert row["event_time_utc"] == event(seen)
                assert row["received_at_utc"] == event(seen) + timedelta(seconds=1)
                assert row["price_text"] == f"1000.{seen // 3 % 7:02d}"
                assert row["provider_tick_id"] == f"synthetic-{seen}"
                assert row["payload_sha256"] == hashlib.sha256(record(seen)).hexdigest()
                assert json.loads(row["source_metadata_json"])["quote"]["price_basis"] == "bid"
                seen += 1
                local += 1
        assert local == info["records"]
    assert seen == count


def run_case(dsn, root, path, count, batch_size, queue_size, mode):
    with MeasuredRepository(dsn) as repo:
        settings = CollectionSettings(BatchPolicy(batch_size, 1000, queue_size), monitoring=False)
        coordinator = IngestionCoordinator(repo, SourceArchive(root / "archive"), settings=settings)
        source_id = f"{root}:{mode}:{uuid4()}"
        before = repo.storage_usage()
        start, cpu = time.perf_counter(), time.process_time()
        recovery_seconds = None
        prefix = 0
        if mode == "recovery":
            paused = asyncio.run(
                coordinator.collect(
                    source(path),
                    source_id=source_id,
                    stream=SimulatedStream(interrupt_after=count // 2),
                )
            )
            assert paused.manifest.status == "IMPORTING"
            prefix = paused.manifest.observed
            assert prefix == count // 2
            resumed = time.perf_counter()
            result = asyncio.run(coordinator.collect(source(path), source_id=source_id))
            recovery_seconds = time.perf_counter() - resumed
            assert result.manifest.capture_id == paused.manifest.capture_id
        else:
            result = asyncio.run(
                coordinator.collect(
                    source(path),
                    source_id=source_id,
                    stream=SimulatedStream(rate=0) if mode == "stream" else None,
                )
            )
        elapsed = time.perf_counter() - start
        cpu_seconds = time.process_time() - cpu
        manifest = result.manifest
        assert manifest.status == "COMPLETE" and manifest.observed == manifest.accepted == count
        assert manifest.rejected == manifest.error_count == 0
        assert result.metrics["queue_high_water"] <= queue_size
        assert result.metrics["committed"] == count and result.metrics["dropped"] == 0
        progress = repo.commit_status(manifest.capture_id)
        assert progress.observed == count and progress.source_position == count + 1
        after = repo.storage_usage()
        replay_seconds = verify_replay(repo, manifest, count)
        destination = root / ("export-" + uuid4().hex)
        export_start = time.perf_counter()
        exported = export_parquet(
            repo,
            manifest.dataset_id,
            destination,
            splits=SplitPlan(BASE + timedelta(days=1), BASE + timedelta(days=2)),
        )
        export_seconds = time.perf_counter() - export_start
        verify_export(destination, exported["manifest"], count)
        assert (
            export_parquet(
                repo,
                manifest.dataset_id,
                destination,
                splits=SplitPlan(BASE + timedelta(days=1), BASE + timedelta(days=2)),
            )
            == exported
        )
        assert (
            asyncio.run(coordinator.collect(source(path), source_id=source_id)).manifest == manifest
        )
        latencies = repo.latencies
        return {
            "mode": mode,
            "records": count,
            "batch_size": batch_size,
            "queue_size": queue_size,
            "dataset_id": str(manifest.dataset_id),
            "capture_id": str(manifest.capture_id),
            "source_sha256": manifest.source_sha256,
            "source_bytes": path.stat().st_size,
            "seconds": elapsed,
            "cpu_seconds": cpu_seconds,
            "records_per_second": count / elapsed,
            "batch_count": len(latencies),
            "commit_median_seconds": statistics.median(latencies),
            "commit_p95_seconds": sorted(latencies)[int((len(latencies) - 1) * 0.95)],
            "metrics": result.metrics,
            "storage_before": before,
            "storage_after": after,
            "replay_seconds": replay_seconds,
            "export_seconds": export_seconds,
            "export_records_per_second": count / export_seconds,
            "export_bytes": sum(info["bytes"] for info in exported["manifest"]["files"].values()),
            "recovery_prefix": prefix,
            "recovery_seconds": recovery_seconds,
            "client_lifetime_peak_rss_kib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
            "errors": 0,
        }


def fingerprint():
    return {
        str(path): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in Path("src/quantlab").rglob("*.py")
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sizes", nargs="+", type=int, default=[100000, 1000000, 2000000])
    parser.add_argument("--tuning-records", type=int, default=10000)
    parser.add_argument("--rounds", type=int, default=5)
    parser.add_argument("--output", type=Path, default=Path("benchmarks/ingestion-results.json"))
    args = parser.parse_args()
    if min(args.sizes) < 1000 or args.rounds < 1 or args.tuning_records < 1000:
        parser.error("sizes must be at least 1000 and rounds positive")
    dsn = os.environ["QUANTLAB_TEST_DATABASE_URL"]
    migrate(dsn)
    root = Path(".storage-test/f04-refactor") / uuid4().hex
    root.mkdir(parents=True)
    paths = {n: source_file(root, n) for n in {*args.sizes, args.tuning_records, 1000}}
    with psycopg.connect(dsn) as connection:
        database = connection.execute("SELECT version()").fetchone()[0]
        extension = connection.execute(
            "SELECT extversion FROM pg_extension WHERE extname='timescaledb'"
        ).fetchone()[0]
        durability = {
            key: connection.execute("SHOW " + key).fetchone()[0]
            for key in ("fsync", "synchronous_commit", "shared_buffers")
        }
    hashes = fingerprint()
    results = {
        "claim": "Local absolute engineering costs; no production SLO or cross-version speedup",
        "python": platform.python_version(),
        "platform": platform.platform(),
        "cpus": os.cpu_count(),
        "load_before": os.getloadavg(),
        "disk_free_before": shutil.disk_usage(root).free,
        "database": database,
        "timescaledb": extension,
        "settings": durability,
        "source_sha256": hashes,
        "artifacts": str(root),
        "tuning": [],
        "scale": [],
    }

    def save():
        args.output.write_text(json.dumps(results, indent=2) + "\n")

    # Warm imports and Arrow initialization before the interleaved configuration comparison.
    run_case(dsn, root, paths[1000], 1000, 100, 1000, "warmup")
    configurations = [(100, 1000), (1000, 64), (1000, 1000)]
    for trial in range(args.rounds):
        order = configurations[trial % 3 :] + configurations[: trial % 3]
        for batch, queue in order:
            row = run_case(
                dsn, root, paths[args.tuning_records], args.tuning_records, batch, queue, "stream"
            )
            row["round"] = trial + 1
            results["tuning"].append(row)
            save()
            print(
                f"round={trial + 1} batch={batch} queue={queue} "
                f"rate={row['records_per_second']:.1f}/s",
                flush=True,
            )
    for index, count in enumerate(args.sizes):
        modes = ("historical", "stream") if index < 2 else ("stream",)
        for mode in modes:
            row = run_case(dsn, root, paths[count], count, 1000, 1000, mode)
            results["scale"].append(row)
            save()
            print(
                f"mode={mode} rows={count} rate={row['records_per_second']:.1f}/s "
                f"export={row['export_records_per_second']:.1f}/s",
                flush=True,
            )
    results["recovery"] = run_case(
        dsn, root, paths[args.sizes[0]], args.sizes[0], 1000, 1000, "recovery"
    )
    profile = cProfile.Profile()
    profile.runcall(run_case, dsn, root, paths[1000], 1000, 1000, 1000, "profile")
    with args.output.with_suffix(".profile.txt").open("w") as stream:
        pstats.Stats(profile, stream=stream).strip_dirs().sort_stats("tottime").print_stats(25)
    profile_path = args.output.with_suffix(".profile.txt")
    profile_path.write_text(profile_path.read_text().rstrip() + "\n")
    assert hashes == fingerprint(), "production code changed during measurement"
    results["load_after"] = os.getloadavg()
    save()
    print(
        "Complete: canonical/source values, counts, replay, export, and recovery verified.",
        flush=True,
    )


if __name__ == "__main__":
    main()
