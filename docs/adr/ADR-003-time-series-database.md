# ADR-003: PostgreSQL and TimescaleDB for durable observations

Status: accepted for Feature 03. Date: 2026-10-25.

## Context

Features 01 and 02 establish exact canonical ticks and deterministic replay, but
neither archives a complete dataset or persists transactional import progress.
Feature 03 requires PostgreSQL, TimescaleDB, psycopg 3, Alembic, and Compose.
The implementation must retain duplicate-looking observations, failed import
evidence, exact price representation, and one-acquisition replay ordering.

The living charter discussed a spike-label ADR using the same proposed number.
This file uses the explicit Feature 03 request's `ADR-003-time-series-database.md`
path. The untracked charter is preserved; no spike-label decision is implied.

## Decision

Use PostgreSQL with TimescaleDB for relational provenance plus a time-partitioned
`market_ticks` table. Use psycopg 3 directly for explicit transactions and bounded
server-side cursors. Alembic owns additive packaged migrations; SQLAlchemy is a
migration dependency, not a competing ORM domain model. Runtime storage packages
are an optional project extra and are pinned in `requirements-storage.lock`; the
development lock includes them for reproducible checks.

The Compose image is pinned to
`timescale/timescaledb:2.31.0-pg18@sha256:0fcff5761363809d1242f2261a3482197b5a5337407f8b97a976fb174206d579`.
Its PostgreSQL version is 18.6. TimescaleDB 2.31.0 was released on 2026-10-20;
the vendor support matrix includes PostgreSQL 18 for 2.31.x. These primary sources
establish the selected compatibility range:

- [TimescaleDB 2.31.0 release](https://github.com/timescale/timescaledb/releases/tag/2.31.0).
- [TimescaleDB supported platforms and PostgreSQL matrix](https://www.tigerdata.com/docs/get-started/timescaledb-supported-platforms).
- [PostgreSQL 18.6 release notes](https://www.postgresql.org/docs/release/18.6/).

The migration explicitly converts an empty relational table through
`create_hypertable(..., by_range('event_time_utc', INTERVAL '7 days'))`. This
supported conversion interface remains documented alongside newer direct
hypertable creation syntax. No compression, columnstore migration, or retention
job is enabled. Unique constraints include the time partition key. See the
[vendor API reference](https://www.tigerdata.com/docs/reference/timescaledb/hypertables/create_hypertable)
and [current creation guide](https://www.tigerdata.com/docs/learn/hypertables/creating-and-configuring-hypertables).
The real selected database, rather than documentation alone, must validate the
foreign keys, triggers, migration behavior, and restart guarantees.

## Recovery design synthesis

Two usage-first design candidates were compared on integrity/recovery, API
simplicity, bounded memory/timing, database constraints, and replay integration.
Candidate A scored 22/25 and continued a verified committed prefix within one
capture. Candidate B scored 23/25 and retained immutable attempts, restarting an
interrupted import from verified archived bytes in a fresh capture.

Choose B's attempt and publication model. It removes validator checkpoint seeding
and CSV seek/resume policy. Keep A's bounded writer accepting existing
`ValidationOutcome` values, so Feature 01 remains the only validation-order owner.
The available runners used separate inherited-model passes; these scores do not
represent independent model-family agreement.

Each dataset identity combines exact bytes, source identity, and canonicalization
configuration. Completed repeated imports reuse the selected attempt. A failed
or interrupted attempt is retained, and a new attempt must validate the whole
source before atomic publication. Batch receipts make uncertain commit retries
idempotent. A dataset advisory lock and transaction state checks prevent concurrent
owners from silently publishing conflicting progress.

## Consequences and alternatives

The relational raw table enforces `(capture_id, source_position)` identity. The
hypertable's timestamp-inclusive composite key and foreign key to raw accepted
time prevent cross-time duplicate identity. Unconstrained NUMERIC plus checked
price text preserves the existing Decimal contract. A fixed NUMERIC precision or
a price/time uniqueness rule would corrupt valid existing semantics.

Restarting from the beginning repeats validation and stores old attempt evidence.
We accept that cost for a simpler recovery guarantee. In-place prefix continuation
can be reconsidered only with a concrete large-history requirement and tests
proving equivalent validation and ambiguous-commit handling.

One transaction for an entire file would hide durable batch progress and create
long-lived transactions. SQLite substitution would fail the requested stack and
real hypertable guarantees. A broker, distributed ingestion service, generic ORM,
or several forwarding manifest/replay services would add unrelated infrastructure.
None is introduced.

The archive and database are separate durability resources: a failure can leave
an unreferenced complete source object, but no partially copied file is published
as valid evidence. PostgreSQL WAL and per-batch commits protect acknowledged work;
RAM queues cannot guarantee zero loss after a hard kill. This design makes no
end-to-end exactly-once delivery claim.

The localhost Compose setup supports reproducible development, not an automatic
production deployment endorsement. Production use requires deployment-specific
access controls, least-privilege roles, tested backup/restore, monitoring, and
capacity evidence. See [architecture and limits](../architecture/time-series-storage.md),
[measured storage results](../../benchmarks/storage-results.md), and
[executed verification](../verification/feature-03.md).
