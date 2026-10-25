# Feature 03 storage benchmark results

Measured on a fresh local PostgreSQL 18.6 / TimescaleDB 2.31.0 database with `fsync=on` and `synchronous_commit=on`. This is an engineering fixture, not a production workload or service-level target. The JSON contains per-run storage, CPU, latency, memory, replay, range, exact-lookup, and query-plan measurements.

Five interleaved 100,000-row rounds; 2 larger scale runs. All imports, source hashes, stored counts, exact values, full replay, range queries, and source-position lookups were asserted.

## 100,000-row batch comparison

| Mode | Batch size | Median rows/s | Range across runs | Median batch ack (ms) |
|---|---:|---:|---:|---:|
| Historical | 100 | 662.9 | 504.9–725.3 | 127.7 |
| Historical | 1,000 | 1,351.7 | 1,165.8–1,463.4 | 551.4 |
| Historical | 10,000 | 1,668.8 | 1,544.8–1,707.5 | 4829.7 |
| Incremental queue | 1,000 | 1,354.8 | 1,223.0–1,367.6 | 679.4 |

## Larger imports

| Rows | Rows/s | Elapsed | Client peak RSS |
|---:|---:|---:|---:|
| 1,000,000 | 1,338.9 | 12.4 min | 88.4 MiB |
| 2,000,000 | 1,253.3 | 26.6 min | 88.4 MiB |

The process lifetime RSS includes all earlier runs in the same benchmark process, so it is a conservative peak rather than an isolated per-case memory measurement. The scale cases are one run each; their rates are descriptive only. Five batch rounds are interleaved to reduce order effects. The script profiles a separate 1,000-row diagnostic case and excludes its profiler overhead from the reported rates.

## Query plans

The harness ran `EXPLAIN (ANALYZE, BUFFERS)` after `ANALYZE` on the largest completed scale case. The 300-row range used the hypertable primary-key index and took 1.625 ms; raw identity used an index-only scan and took 0.293 ms; full tick identity used the dataset-order index and took 0.193 ms. These are single local observations, not latency guarantees. Plans are in the JSON under `plans`; no index or schema change was justified by this fixture.

## Reproduce

With an isolated migrated TimescaleDB test database configured as `QUANTLAB_TEST_DATABASE_URL`:

```bash
.venv/bin/python -m benchmarks.unified_storage
```

The harness retains generated source files, raw archives, database rows, the JSON result, and a cProfile summary for inspection. It does not clear or reuse existing benchmark evidence.
