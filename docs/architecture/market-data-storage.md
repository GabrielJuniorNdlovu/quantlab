# Unified market-data storage

F03 remains the single PostgreSQL/TimescaleDB persistence layer. F01 owns canonical
validation, F04 owns source interpretation and collection, and F02 consumes
completed datasets. The refactor retains existing schema, archive, manifests,
transaction boundaries, receipt digests, and replay factories.

## Writes share one contract

Historical CSV, historical JSONL, and recorded streaming observations reach
`Repository.write_batch(run, batch_number, outcomes)` as F01 `ValidationOutcome`
objects. The method returns only after the database transaction is acknowledged,
or raises. It never removes legitimate repeated ticks. The asynchronous acquisition
source is outside this layer. `BatchWriter` provides the existing bounded sink
queue, size/deadline flush, backpressure, and drain-on-close behavior.

A claimed repository owns one acquisition and one writer connection. Callers must
join the writer before leaving its repository/claim context. They must not manage
transactions on the repository's private connection. Public progress and history
queries open independent read-only connections and can inspect committed state
without sharing the writer's transaction.

Within each transaction the repository locks the acquisition row, reconciles an
existing receipt, and validates batch context and source order. Three bounded COPY
passes then persist raw rows, accepted ticks, and quality events, in that order.
PostgreSQL still executes all row constraints, foreign keys, and append-only guards.
The receipt and acquisition counters update in the same transaction. A failed
COPY, constraint, or commit cannot publish only part of a batch.

COPY removes per-observation client SQL calls; it does not bypass integrity checks
or remove the original evidence. Row tuples are emitted from the existing bounded
batch, without retaining the dataset or building another full set of row arrays.

## Identity preserves separate evidence

Dataset identity derives from source checksum, interpretation/configuration hash,
and explicit source ID. The selected capture identifies one attempt. Raw observation
identity is `(capture_id, source_position)`. A source position has no meaning
outside that acquisition. Separate source IDs, contexts, or acquisitions retain
separate provenance even if their times and prices overlap.

The hypertable primary key includes event time, capture ID, and source position,
as required by its event-time partitioning. The raw table's unique triple also
includes `accepted_at`; the tick foreign key binds that exact event timestamp to
one raw identity. There is no uniqueness on time and price and no assumed provider
tick ID. Retries reconcile batch number and the full canonical contents digest.
Identical receipts acknowledge already committed work; changed content fails.

Historical restart deliberately creates a fresh capture, preserves the prior
attempt, and marks a previously active attempt interrupted. Completed datasets
are immutable. Reimporting the same completed identity selects its original
capture rather than duplicating observations.

## Durable progress and checkpoint ownership

`Repository.commit_status(capture_id)` returns frozen `CommitStatus` with dataset
and capture identity, lifecycle status, latest committed batch and digest, source
position, counts, last accepted event time, and receipt timestamp. An empty active
capture reports batch/position/count zero and no receipt timestamp or digest.

Progress derives from `acquisition_runs` and `ingestion_batches` in one SQL
statement and one snapshot. A fresh read-only connection prevents visibility of
writer-local uncommitted changes. This is a durable fact, not a second mutable
checkpoint table. `committed_at_utc` retains the existing receipt insertion timestamp:
it becomes visible after commit, but is not the precise commit-completion instant.
The measured batch latency includes the full `write_batch` acknowledgement.

F04 owns advancing its acquisition acknowledgement, replaying a source prefix,
and restoring validation state. It must acknowledge only after `write_batch`
succeeds or after reconciling a durable receipt. Submitted/queued counts are not
checkpoints. A lost acknowledgement is not proof that the transaction failed.

F04 adapts the preserved collection branch's verified-prefix recovery without merging
that branch. `checkpoint` compares source replay with original receipts; `commit_status`
remains the independent durable progress query. Modern collection can continue an
IMPORTING capture, while legacy synchronous imports retain fresh-attempt interruption
semantics. See [the ingestion design](market-data-ingestion.md) for guarantees and limits.

## Exact values and source truth

`NUMERIC` has no declared precision/scale limit in this schema. Finite positive
checks and F01 quote-precision rules remain intact. `price_text` preserves the
Decimal coefficient and exponent, including scientific notation, and rehydration
uses that text. Native canonical values with 256 coefficient digits, exponent
1000, and fractional precision 18 are tested. No float stores canonical prices.

UTC TIMESTAMPTZ preserves supported microsecond event and optional reception
precision. Missing source reception and provider identifiers remain absent.
Schema and interpretation versions live in immutable dataset configuration.
Raw payloads, metadata, hashes, and source positions remain linked to ticks.

Revalidation compares complete serialized canonical values, including Decimal
representation. Numeric equality alone would accept `1000.0` for source `1000.00`;
the new regression demonstrates and repairs that weakness. Canonical domain
validation remains F01's responsibility; F03 does not introduce another parser.

## Scoped retrieval and future references

`tick_factory(dataset_id, start_utc, end_utc, page_size, limit)` returns a restartable
factory over the selected completed capture. UTC range semantics are inclusive
start and exclusive end. A server-side cursor fetches bounded pages and orders
by event time then source position. Equal timestamps retain acquisition order.
There is no invented global ordering across independent captures. Instrument scope
is the canonical `BOOM_1000` enforced by F01 and database constraints.

`observation(dataset_id, source_position=...)` performs exact lookup. It resolves
the completed dataset's selected capture, obtains `accepted_at` through the raw
primary key, and queries the full hypertable key including partition time. It
returns `None` for an absent position and rejects incomplete datasets. Future
research artifacts can retain dataset/capture/position references; no speculative
label, model, signal, or outcome tables are introduced.

`acquisitions(dataset_id, limit=100)` returns bounded committed attempt summaries,
including failed or interrupted attempts. These are diagnostic records, not
completed replay streams. Ordering uses start time then capture UUID to break ties.

## Monitoring and resource limits

Existing ingestion logs report accepted/rejected records, durable observations,
batch duration, completion, retries, and interruption. Batch writer metrics now
also include committed rejected records, quality-event count, failures, last
successful sink latency, and worker state. Quality counts cover batch outcomes;
fatal acquisition-level events recorded separately are represented in acquisition
error counts. Metrics never advance committed counters before the sink returns.

`quantlab db status` reports availability through a real successful query and
storage bytes. Hypertable sizes include Timescale chunks and indexes. Raw table
and index sizes are separate. `ingest status DATASET --attempts` exposes committed
progress. No payload or credentials enter these status records. Database errors
retain the existing bounded CLI diagnostic behavior.

There is no live collector on this branch from which to measure network ingestion
lag. Historical timestamps are not presented as live latency. Local queue depth,
backpressure, flush deadline, and sink acknowledgement latency remain measurable.
Batch and queue limits bound application memory; source verification uses bounded
reads. Disk evidence grows with source volume and retained attempts.

See [recovery procedures](../operations/storage-recovery.md),
[the original schema details](time-series-storage.md),
[measured workloads](../../benchmarks/unified-storage-results.md), and
[the implementation review](../reviews/feature-03-implementation-review.md).
