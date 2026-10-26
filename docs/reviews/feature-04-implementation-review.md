# Feature 04 implementation review

## Verdict

**Implementation present; verification incomplete; not merge-ready.** The shared ingestion
coordinator, recovery, delivery simulation, quality reporting, and deterministic export are
implemented on `refactor/04-market-data-ingestion`. Feature 04 cannot be called complete
until the database integration and scale gates below pass.

## Scope and changes

The refactor uses one `IngestionCoordinator` for historical imports and asynchronous recorded
delivery. Feature 01 remains the canonical validator and Feature 03 remains the persistence
owner. Collection migration `0003` adds session diagnostics. The coordinator supports bounded
backpressure, reconnect and exact redelivery checks, verified-prefix resume, interrupted
collection status, and persistent loss/coverage evidence. CLI commands expose simulation,
status, quality, and export. The Parquet exporter preserves source lineage and metadata, uses
explicit chronological cutoffs, checksums its files, and publishes the manifest last.

Historical profile support adds optional reception-time and price-basis mappings without
guessing values. Existing synchronous import entry points remain, while active asyncio callers
must await the shared asynchronous API. See [the ingestion architecture](../architecture/market-data-ingestion.md),
[collection operations](../operations/data-collection.md), and
[recovery procedures](../operations/collection-recovery.md).

## Files changed

The implementation changes the existing historical profile, storage batch metrics, ingestion
coordinator, repositories, and storage CLI. It adds collection settings/simulation, continuity
classification, Parquet export, collection session migration/state, CLI adapters, regression
tests, architecture/operations guidance, benchmark harness/status, and the current project
status. F01 canonical tick fields and F03 transaction/receipt ownership remain in their
existing layers.

## Verification completed

Executed in this checkout on Python 3.14.4:

| Command | Result |
|---|---|
| `env -u PYTHONPATH .venv-check/bin/pytest -q -rs --tb=short` | **457 passed, 96 skipped** in 3.71s; database and native GUI environments were unavailable |
| Focused Feature 04 and related tests (`test_collection.py`, `test_collection_refactor.py`, `test_collection_cli.py`, `test_dataset_export.py`, `test_historical.py`, `test_storage_batching.py`) | **63 passed, 16 skipped** in 0.58s; skipped cases require isolated PostgreSQL/TimescaleDB |
| `env -u PYTHONPATH .venv-check/bin/ruff check .` | Passed |
| `env -u PYTHONPATH .venv-check/bin/ruff format --check .` | Passed; 92 files already formatted |
| `env -u PYTHONPATH .venv-check/bin/mypy` | Passed; no issues in 50 source files |
| `git diff --check` | Passed |

The full suite was also attempted in the restricted shell and stalled in async socket tests;
that run was interrupted and is not counted. The elevated full run completed and is the result
reported above.

## Required checks still outstanding

Docker Desktop was unavailable: its configured socket path does not exist. No local
PostgreSQL binaries or isolated test-database configuration were present. Therefore these
acceptance checks were not run:

- Feature 03/04 PostgreSQL and TimescaleDB integration, restart, cancellation, and failure
  recovery tests.
- The benchmark harness at 100,000 and 1,000,000 records, including historical/streaming
  collection, interrupted recovery, complete replay, Parquet round-trip, and checksums.
- Installed-wheel database migration and real-database import/export smoke tests.

The benchmark harness is ready at [`benchmarks/ingestion.py`](../../benchmarks/ingestion.py).
Its results status is in [`benchmarks/ingestion-results.md`](../../benchmarks/ingestion-results.md).
No performance values or database success are claimed here.

## Independent review

The final independent code review found no additional concrete Critical, Major, or Minor
defects in the reviewed changes. It confirmed bounded queue submission, cooperative
scheduling, bounded diagnostic samples, and validation of delivery metrics. This review does
not replace the unrun database and scale acceptance checks.

## Compatibility and limits

The canonical `market_tick.v1` shape is unchanged. Synchronous import signatures remain, but
custom implementations of the old internal `ImportRepository` protocol need to migrate to the
F03 `Repository`; synchronous import cannot be called from inside an active event loop. These
changes are described in the architecture documentation. No live broker connectivity,
provider authenticity, or market-history completeness is established. Simulator behavior is
engineering evidence only.

The branch has not been merged or pushed. Final merge-readiness requires running the omitted
database tests and benchmark in an isolated durable TimescaleDB environment, reviewing those
outputs, and updating this report with actual results.
