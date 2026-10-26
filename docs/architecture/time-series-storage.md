# Durable time-series storage

Feature 03 adds PostgreSQL/Timescale persistence around Feature 01's immutable
canonical contracts. It exposes completed, single-acquisition history to Feature
02 without changing its replay, candle, alert, or visualization interfaces.
The [operations guide](../features/feature-03.md) covers setup and CLI commands;
[ADR-003](../adr/ADR-003-time-series-database.md) records the database decision.
The [unified storage reference](market-data-storage.md) documents the shared
batch contract and committed-progress queries.

## Component ownership

`IngestionCoordinator` archives a source, claims its dataset, runs one continuous
`validate_acquisition` iterator, persists bounded batches, and requests completion.
It contains no SQL. Fatal transport errors preserve available row evidence and
quality details without fabricating an envelope for malformed bytes.

`SourceArchive` owns whole-file evidence: bounded copying, SHA-256, source-change
detection, file synchronization, and exclusive hash-named publication. Read-only
permissions prevent accidental writes. Verification reads through a fresh file
descriptor so previously buffered bytes cannot hide a concurrent mutation.

`Repository` owns SQL, dataset claims, transactional writes, batch idempotency,
manifest state, and retries. `replay_repository` owns bounded retrieval and maps
stored values back into the existing `MarketTick`. Packaged Alembic revisions
own schema initialization and hardening. `BatchWriter` owns the optional event
queue and independent flush deadline; it does not validate or execute SQL.

## Identity and schema ownership

A dataset UUID is derived from the source SHA-256, canonical configuration hash,
and source identity. Configuration includes provider, explicit provider symbol,
pip precision, source type, reception semantics, metadata, source ID, schema
versions, and validation version. It excludes capture UUID and local filename.
Changing canonicalization semantics requires updating the validation version.
The only instrument is `BOOM_1000`; it is not a provider API symbol.

The schema has six application tables:

- `dataset_manifests` owns source hash, byte count, archived path, source identity,
  canonical configuration/hash, lifecycle status, and selected capture. Source
  references live here rather than in a second source-object service.
- `acquisition_runs` owns each attempt's capture UUID, timestamps, status, row and
  error counts, last position, and last accepted event time. A failed or interrupted
  attempt remains evidence when a new attempt starts.
- `raw_observations` owns exact row bytes, matching SHA-256, metadata, reception
  time, and source identity `(capture_id, source_position)`. Its nullable
  `accepted_at` provides the accepted tick's timestamp link. The whole archive
  separately preserves headers and malformed or unparseable trailing bytes.
- `market_ticks` stores the canonical fields plus exact price representation. It
  is a hypertable partitioned by `event_time_utc` into seven-day chunks, with no
  automatic compression or retention policy.
- `data_quality_events` stores deterministic existing event IDs, codes, severity,
  descriptions, optional source positions, and detection time. Fatal transport
  errors link to the acquisition without requiring a nonexistent raw row.
- `ingestion_batches` stores batch number, deterministic contents digest, source
  position range, observation count, and commit timestamp. It resolves uncertain
  commit acknowledgements without broadly ignoring conflicts.

Raw JSON retains Feature 01's per-observation envelope metadata, including
its adapter header and provider metadata. The manifest remains authoritative for
acquisition configuration; those immutable envelope copies have no separate update
policy. This deliberate provenance fidelity has a measurable storage cost.

The ordinary raw table's primary key is `(capture_id, source_position)`. The tick
hypertable's primary key is `(event_time_utc, capture_id, source_position)`, which
includes its partition column. A foreign key to the unique raw triple including
`accepted_at` prevents a second time from being attached to the same source
identity. Price/time pairs are not observation identifiers. Legitimate repeated
prices and timestamps survive imports and replay.

Prices use unconstrained `NUMERIC`, with finite positive-value and precision
checks. `price_text` is bounded to 2048 characters and checked for equality to the
numeric value. Rehydration uses `Decimal(price_text)` to retain coefficient and
exponent, including scientific notation that NUMERIC may normalize. No float
conversion occurs in persistence. TIMESTAMPTZ preserves the canonical contract's
existing microsecond resolution and is returned in UTC.

Storage uses positive signed BIGINT positions and batch numbers, so values must
be below `2**63`. Feature 01's arbitrary positive Python integers are more general.
PostgreSQL TEXT/JSONB cannot represent NUL characters even where an in-memory
canonical string permits them. Such writes fail explicitly; no value is truncated,
normalized away, or falsely reported durable. Raw BYTEA and the whole archive
still represent arbitrary original source bytes. These are storage-boundary
limits, not revisions to the Feature 01 contract.

## Transactions and lifecycle

An attempt starts empty in `IMPORTING`, then ends as `COMPLETE`, `FAILED`, or
`INTERRUPTED`. Terminal attempts are immutable. The manifest selects the current
attempt; only a completed selected attempt can be replayed. Earlier attempts are
never silently removed during recovery.

Each batch transaction writes raw rows, accepted ticks, quality events, a batch
receipt, and counters/checkpoint together. It either commits all those changes
or rolls them back. A matching receipt makes a repeated same-number batch a no-op;
a different digest is an integrity error. Batch numbers are contiguous. Positions
strictly increase across batches, and accepted event times never decrease.

Successful EOF, source re-verification, and reconciled error-free persisted counts
are required before publication. The repository also streams the preserved CSV
header and committed raw row bytes through SHA-256 and compares their complete
byte count and digest to the manifest. Finalizing a valid prefix cannot publish
an incomplete file. This proof fetches at most 100 raw rows at once. Completing
configured JSON Lines uses its explicit versioned source-framing contract. The final transaction completes both acquisition
and manifest. A domain rejection is retained with its original bytes and quality
event but makes the dataset fail. Empty or fatally malformed data cannot become
a completed dataset. Guards reject completed-row mutation, ordinary evidence
updates/deletes, and unreviewed truncation. Administrative owners can still bypass
SQL guards; this is not adversarial tamper-proof storage.

The repository uses one connection for dataset advisory ownership and writes.
A competing importer fails its claim. Connection loss releases ownership; retry
reconnects and must reacquire before further writes. Row state and batch receipts
are rechecked inside the transaction. An unknown COMMIT result is reconciled by
receipt identity rather than blindly replaying inserts. If another owner acquired
the lock, the former owner fails instead of continuing concurrently.

Batch inserts use three bounded PostgreSQL COPY passes inside the existing
transaction: raw evidence, accepted canonical ticks, and quality events. COPY
still runs the same database checks, foreign keys, and append-only guards. The
measured comparison is in the [benchmark results](../../benchmarks/unified-storage-results.md).

Only classified operational/connection, serialization, and deadlock failures are
retried. Defaults are three attempts with exponential backoff starting at 0.1 s.
Integrity and validation errors are not silently retried. Ingestion and replay connections use a five-second connection timeout and a
30-second SQL statement timeout. Replay transactions are read-only. Failure after
exhausting retries propagates; inability to mark a run interrupted is reported explicitly.

## Recovery and raw evidence

The archive copies in fixed 1 MiB chunks, computes a whole-source hash, verifies
the copied bytes and source identity, synchronizes the file, and publishes with a
no-overwrite hard link. Directory synchronization makes publication durable on
the supported local Linux filesystem. An existing hash-named object is verified,
not overwritten. Incomplete `.partial-` files remain identifiable and are ignored
as source objects. A failed database registration can leave a valid unreferenced
archive object; automatic deletion is intentionally absent.

Process death may lose queued or pending memory. Previously committed PostgreSQL
transactions remain subject to WAL and configured fsync/synchronous-commit
semantics. A dataset with an unfinished attempt remains hidden from completed
replay. Reclaiming its dataset lock marks an abandoned importing attempt
`INTERRUPTED` and starts a fresh capture from the beginning of the verified archive.
Committed old evidence stays intact. No mutable-prefix resume or CSV line seeking
is attempted. Failed imports retain their failures as separate attempts.

This is deterministic restart with idempotent sink operations and completed-dataset
registration. It does not establish end-to-end exactly-once live delivery or zero
loss from RAM on abrupt termination. Restart costs additional validation, writes,
and storage for retained failed attempts. Overlapping acquisitions are never
silently merged; each factory selects one dataset and one completed capture.

## Batching and observability

Historical import facades use the same unpaced F04 coordinator and bounded F03 writer
as recorded simulation. Every observation passes F01, batches flush at the configured
size/deadline, and completion drains the EOF tail. Files are not artificially paced;
backpressure reflects storage capacity.

Future event-driven callers can drive one existing validation iterator and submit
its `ValidationOutcome` values to `BatchWriter`. Use one ordered producer per
acquisition. A worker flushes at the size
threshold or when 1000 ms have elapsed since the first pending outcome, even with
no new arrival. Normal close drains, flushes, joins, and propagates worker errors.
Timeout means initiating a flush; database acknowledgement can take longer.

The queue defaults to 10,000 records. Full queues apply blocking backpressure;
an explicit submission timeout raises `queue.Full` without accepting or counting
that observation. The caller must retain it. Queue and pending-batch capacity
bound record count; maximum raw payloads alone occupy 625 MiB at the default queue
capacity, with metadata and Python objects adding more. Smaller queues are
appropriate on limited-memory machines. Policy bounds are 1–10,000 records,
1–60,000 ms, and 1–10,000 queue slots.

Structured logs expose dataset/capture identity, durable and buffered counts,
accepted/rejected counts, batch size, queue occupancy/capacity/high-water,
commit latency, throughput, retries, and lifecycle failures. An accepted or
submitted observation is not called committed before the sink acknowledges it.
Error records include bounded error codes/types without raw payloads or connection
credentials. `BatchWriter.metrics` returns an immutable metrics snapshot. The
[benchmark report](../../benchmarks/storage-results.md) records actual measured
batch sizes, storage, memory, query/replay cost, and recovery behavior.

Source observation frequency, batch commit frequency, and chart rendering frequency
are separate. No minute is treated as a fixed number of ticks.

## Replay and future adapter interface

```python
from pathlib import Path
from quantlab.replay_session import ReplaySession
from quantlab.storage.repository import Repository

with Repository(database_url) as repository:
	manifest = repository.manifest(dataset_id)
	factory = repository.tick_factory(dataset_id, page_size=1000)
	session = ReplaySession(factory, (), manifest.source_sha256, Path("replay-runs"))
	try:
		first_frame = session.step()
	finally:
		session.close()
```

The factory has the existing `Callable[[], Iterator[MarketTick]]` shape. Each call
opens a fresh connection and bounded server-side cursor, checks completion, and
orders `(event_time_utc, source_position)`. UTC ranges use `[start_utc, end_utc)`.
`page_size` is 1–10,000; `limit` is optional and positive. Empty ranges return no
rows without changing manifest state. Generator exhaustion, explicit close, and
replay reset release resources. There is no OFFSET scan or whole-dataset fetch.

A later read-only source adapter must supply authorized original evidence,
reviewed provider mapping, acquisition context, and every observed candidate in
source order. It can reuse continuous Feature 01 validation and the bounded
writer. It must define its own durable source acknowledgement and recovery
protocol before making stronger live-delivery guarantees. Feature 03 supplies the transaction, archive, query, and bounded batching contracts.
The [F04 coordinator](market-data-ingestion.md) supplies historical and recorded-source
scheduling and verified-prefix recovery.

## Migrations and deployment limits

`db init` runs packaged Alembic revisions under a database migration lock.
`0001_storage` creates extensions, tables, the empty hypertable, and initial guards.
`0002_storage_guards` adds source-context, initial-state, publication, and evidence
removal guards without deleting data. Existing revision DDL remains fixed; future
schema changes require new revisions. Destructive downgrades refuse execution.

The Compose deployment is a local developer environment, with ignored credentials,
a localhost-only port, health checks, a persistent named volume, and pinned image.
It does not configure replicated failover, TLS for remote clients, a separate
least-privilege application role, or a production backup schedule. Apply those
operational policies before using another deployment context.

## Backup and recovery boundary

A consistent backup includes the PostgreSQL database and the archive objects
referenced by its manifests. Quiesce importers, record PostgreSQL/Timescale and
schema versions, and preserve the source hashes with both backup parts. Restore
archives at their recorded absolute root unless a reviewed migration explicitly
changes references. Keep backups outside the Git checkout and protect them as
research data.

Follow the [official Timescale logical backup and restore procedure](https://www.tigerdata.com/docs/deploy/self-hosted/backup-and-restore/logical-backup),
including its restore preparation and completion steps. Restore into an isolated
instance, verify archive hashes, reconcile completed manifest counts, and compare
deterministic replay before promoting the restored database. Do not infer recovery
from a successful dump alone. The [Feature 03 verification report](../verification/feature-03.md)
records which interruption, restart, and restore checks actually ran. Local tests
do not certify a deployment's backup retention, recovery-time target, hardware,
or disaster recovery policy.
