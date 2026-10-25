# Historical adapter measurements

These are local engineering measurements, not a production capacity guarantee or a claimed
speedup. CSV and JSON Lines import ranges overlap; this run establishes no clear performance
difference between formats.

## Reproduce

Use an isolated database with the existing storage dependencies and migrations. The benchmark
retains acquisitions and archives under `.storage-test/`; it never deletes existing evidence.

```bash
set -a
source .env.feature03.benchmark
set +a
env -u PYTHONPATH .venv-check/bin/python -m benchmarks.historical --records 1000 --rounds 5
```

Reported timing uses five alternating CSV/JSON Lines rounds, 1,000 observations per import,
100 observations per transaction, and 100 per replay fetch. There are ten completed timed
imports and zero rejected observations or errors. Separate memory imports and profile imports
are excluded from timing. An earlier exploratory run overlapped tests and was replaced by this
uncontended run after all database disruption tests finished.

The host had 12 logical cores and load averages 2.39, 1.92, and 2.22 at preflight. The final
run used Python 3.14, Docker Desktop, PostgreSQL 18.6, and TimescaleDB 2.31.0. `fsync` and
`synchronous_commit` were both on. Filesystem caches and the database were warm. This is a
single sequential importer on the existing synchronous repository path, not a concurrency test.
Exact runtime/platform values and all samples are in [the raw JSON](historical-results.json).

## Actual timing

Each rate below gives median and observed minimum–maximum across five runs.

CSV, with a 48,018-byte source:

- Source framing/mapping: 21,389 records/s, range 20,847–21,886. Median source scan 46.8 ms.
- Source plus canonical validation: 12,432 records/s, range 11,956–12,526.
- Complete durable import: 270 records/s, range 249–293. Median 3.698 s, range 3.410–4.012 s.
- Repository batch writes: 304 records/s, range 280–325.
- Replay through Feature 02: 14,684 ticks/s, range 12,746–16,731.

JSON Lines, with an 85,000-byte source:

- Source framing/mapping: 17,572 records/s, range 14,683–18,227. Median source scan 56.9 ms.
- Source plus canonical validation: 11,253 records/s, range 10,849–11,600.
- Complete durable import: 283 records/s, range 250–284. Median 3.530 s, range 3.518–3.992 s.
- Repository batch writes: 313 records/s, range 285–319.
- Replay through Feature 02: 16,107 ticks/s, range 14,321–17,349.

Source timing fully consumes the candidate iterator. Validation timing fully consumes outcomes
and asserts acceptance and decimal scale. Import timing includes preservation, configuration
identity, claims, validation, transactions, and completion proof. Batch-write timing includes
repository validation, serialization, raw/tick SQL, receipts, and commit; it is not isolated
server execution time. Replay timing steps the actual engine, verifies source position and
exact price text for every tick, and closes the engine. Every timed import asserts the expected
transaction count and completed manifest counters.

## Limiter and plausibility

A separate [import profile](historical-import-profile.txt) records 4.725 s total, 4.080 s in
batch writes, and 3.698 s in the PostgreSQL connection wait path. The timing samples consume
about 0.60–0.62 client CPU seconds per 3.53–3.70 wall seconds. The evidence identifies database
round trips and commit waits as the import limiter; it does not identify raw disk bandwidth
or parsing as the limiter. The profile includes roughly two SQL executions per observation,
consistent with raw and tick inserts. No throughput is attributed to skipped writes or retries.

The separate [source profile](historical-profile.txt) places immutable raw-metadata freezing
and canonical validation ahead of JSON decoding. Full validation occupies roughly 2–3% of
the end-to-end import time. Doubling parser speed therefore cannot double import throughput.
Input sizes and reported rates imply only tens of kilobytes per second of source data during
import, far below storage bandwidth. The result is plausible for sequential SQL round trips
through this local Docker environment.

## Bounded memory

Tracing is excluded from reported timings. Source-plus-validation scans use 10,000 and
100,000 records. CSV peak traced allocation is 160,120 and 158,204 bytes; JSON Lines is
156,056 and 142,073 bytes. The tenfold input increase does not retain record history.

Complete imports use batch size 100 and 1,000 then 10,000 records:

- CSV peak traced allocation: 1,502,620 then 1,848,720 bytes, about 1.43 then 1.76 MiB.
- JSON Lines peak traced allocation: 1,539,498 then 2,249,157 bytes, about 1.47 then 2.15 MiB.

This includes archive copying and repository batches. The largest traced import remains below
2.2 MiB despite tenfold more records. Process RSS samples are approximately 46–47 MiB and
include the interpreter, database driver, allocator, and native libraries. Traced allocation
is not total process or database-server memory; RSS samples are snapshots, not a separately
sampled peak. The raw Linux high-water counters are retained for transparency but are not used
as peak-memory evidence because they were not monotonic in this runtime.

## Scope and limits

The fixture deliberately includes repeated timestamps and prices, with deterministic source
positions and decimal representations. It represents small local research imports. No real
provider files, network acquisition, cold-cache performance, multi-importer scaling, or
production durability under host power loss were measured. Database and archive capacity,
backup policy, and operational load need separate sizing.
