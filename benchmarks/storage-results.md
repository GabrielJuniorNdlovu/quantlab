# Feature 03 measured storage costs

Five interleaved rounds on the local Docker Desktop deployment measured **256
observations/s with the default 100-record batch**, versus **356 observations/s
with 1000-record batches**. The larger batch has the highest measured median,
but its range overlaps 500-record batches. These results do not identify a unique
production optimum. All imports and output assertions succeeded.

## Reproduce

Use a dedicated TimescaleDB database, export its connection URL as
`QUANTLAB_TEST_DATABASE_URL`, and run from the repository root:

```bash
env -u PYTHONPATH .venv-check/bin/python -m benchmarks.storage
```

The command applies packaged migrations and deliberately retains its engineering
datasets and archive under ignored `.storage-test/benchmarks/RUN_UUID/`. It writes
[raw measurements](storage-results.json) and an independent
[profile](storage-profile.txt). [Additional database wait samples](storage-diagnostics.json)
contain no connection credentials or source payloads. Do not use a valuable database.

## Environment and timed work

The client is CPython 3.14.4 on Linux 7.0.0-38 x86_64/glibc 2.43, with 12 logical
CPUs. The server is PostgreSQL 18.6 with TimescaleDB 2.31.0 in the pinned Compose
image on Docker Desktop 4.96.3/Engine 29.9.2. Both `fsync` and
`synchronous_commit` are `on`. Storage uses the persistent Docker named volume;
archive files use the workspace filesystem. Host load averages changed from
2.94/4.09/4.15 to 3.11/3.56/4.17. Other host activity was not controlled.

Each timing round validates the same 2000-row, 134,653-byte engineering CSV,
then imports it with batch sizes 1, 100, 500, and 1000, followed by the bounded
queue case. Each import declares a new source identity so it actually persists
rows instead of taking the completed-import idempotence shortcut. There is one
writer at a time. Same-timestamp observations, Decimal scale, chronological order,
row counts, completed status, zero rejections, and receipt counts are asserted.
All queries are fully consumed. Twenty batch-comparison imports plus five queue
runs, two separate memory imports, and one profile import persist 162,000 rows.

Ingestion timing includes source preservation/verification, dataset claim, every
validation, raw/tick/receipt/progress write, acknowledged transaction, stored-source
completeness proof, and final publication. It excludes initial repository connection
setup, migrations, input generation, and subsequent output-verification queries.
This is application ingestion timing, not interpreter or CLI startup latency.

The transaction metric wraps the entire `write_batch` call, including receipt
hashing, SQL inserts/constraints/counters, and COMMIT acknowledgement. It is not
isolated PostgreSQL COMMIT-only latency. Reported transaction numbers are medians
of each run's batch-duration median, with ranges across the five run medians.

## Batch results

Rates are median observations per second; ranges are minimum and maximum across
five rounds. There is no rejected-record fast path in these measurements.

- **1 record:** 17.67 observations/s, range 8.52–20.08. Median 2000-row import
  113.20 seconds. Batch transaction median 46.43 ms, range 46.32–68.61 ms.
- **100 records:** 255.82 observations/s, range 193.90–263.21. Median import
  7.82 seconds. Batch transaction median 337.20 ms, range 324.28–408.13 ms.
- **500 records:** 344.57 observations/s, range 258.72–355.53. Median import
  5.80 seconds. Batch transaction median 1.323 s, range 1.253–1.479 s.
- **1000 records:** 355.81 observations/s, range 318.08–364.90. Median import
  5.62 seconds. Batch transaction median 2.554 s, range 2.520–2.699 s.

All four configurations are supported by executed assertions. Keep 100 as the
requested conservative engineering default, not as an asserted throughput optimum.
500/1000 can improve local historical throughput at the cost of larger pending
batches and longer transaction duration. The ranges do not establish a reliable
500-versus-1000 advantage. Allowed sizes above 1000 have not been benchmarked.

## Validation, queries, replay, queue, and storage

Per-observation Feature 01 validation processed a median **13,348 observations/s**,
range 12,418–13,674, including CSV adaptation and output-value assertions.

For datasets imported with 100-record batches, full `ReplayEngine` consumption
measured a median **20,412 ticks/s**, range 12,769–21,001. This includes a fresh
connection, completion check, 100-row server-cursor fetches, canonical rehydration,
engine stepping, and per-tick timestamp/price/position/scale assertions. It draws
no chart. The same retrieval behavior is used for all import-batch variants; their
read-rate differences are not claimed as a batch-size effect.

The UTC `[10 s, 20 s)` query returns exactly 300 observations. Its default-batch
median latency is **45.01 ms**, range 40.94–46.01, including connection and complete
consumption. The fixture's timestamps occupy one Timescale chunk; separate
integration tests verify normalization and replay across actual seven-day chunks.

The event-writer case uses 100-record batches and a deliberately small 100-record
queue. It processed a median **254.93 observations/s**, range 214.24–277.44. Every
round reached queue high-water 100/100, admitted and committed all 2000 outcomes,
created 20 batches, and ended with zero queued or buffered records. Median total
submit-call time is 6.82 seconds, including backpressure and method overhead;
no records were dropped. Default capacity 10,000 was not used to conceal backlog.

Median allocated PostgreSQL database growth per 2000-observation import was
2,531,328 bytes for batch 1; 1,908,736 for 100; 2,064,384 for 500; and 1,875,968 for
1000. The raw artifact retains ranges. `pg_database_size` includes database tables,
indexes, and allocation effects; it excludes WAL, container image, and the archive.
Autovacuum/allocation noise means these are approximate growth measurements, not
an exact per-row storage formula. Immutable envelope metadata and receipts have a
real storage cost. The 2000-row archive object occupies 134,653 bytes once per hash.

## Separate memory and diagnostic runs

Tracing is excluded from throughput comparisons. With 100-record batches:

- **10,000 observations:** 673,046-byte input; traced Python peak 2,038,240 bytes
  or 1.94 MiB; traced current allocations after the run 40,055 bytes.
- **100,000 observations:** 6,730,046-byte input; traced peak 2,415,660 bytes or
  2.30 MiB; traced current after the run 41,245 bytes. The traced run took
  428.99 seconds and is not a throughput timing result.

The whole client process's Linux high-water RSS was **77,048 KiB, about 75.24 MiB**,
including dependencies, earlier cases, profiling/tracing, and driver allocations.
These figures exclude database-server memory and Docker VM/cache overhead. The
memory path disables the benchmark's per-batch timing list so growing measurement
storage cannot masquerade as an ingestion memory leak.

Ten times the input count did not cause proportional client-memory growth. This
is consistent with bounded source copying, batch buffers, and cursor fetches;
it does not establish an arbitrary production memory SLO. Maximum-size payloads
and the default 10,000-record future queue can use substantially more memory than
this fixture. No concurrent-writer or multi-gigabyte benchmark was run.

The independent default-batch profile records 5.938 seconds in psycopg connection
waiting out of an 8.218-second profile interval. Timed client CPU/wall ratios were
about 0.046, 0.157, 0.166, and 0.175 for batch sizes 1/100/500/1000. Separate database
samples observed `IO/WalSync` during timing and `ClientRead` during the traced
memory run. This supports SQL round-trip and durable-storage waiting as the main
measured ingestion costs; pure Python validation is not the limiting stage. Server
CPU/storage internals were not comprehensively profiled, so no universal bottleneck
or tuning claim is made.

## Recovery and limits of the evidence

Executed integration tests hard-kill an importer after five durable observations,
kill/restart the database container, terminate a transaction backend, and inject a
lost COMMIT acknowledgement. Recovery preserves old evidence and publishes the
correct 34-observation attempt without duplicated committed batches. A real
Timescale logical backup and restored archive reproduce the manifest and values.
See the [verification report](../docs/verification/feature-03.md).

No production recovery-time target, real market arrival rate, forecasting result,
exactly-once live delivery, remote-client performance, cold-storage distribution,
or power-loss hardware guarantee follows from these measurements. Runtime source
and benchmark hashes in the raw artifact match the delivered files.
