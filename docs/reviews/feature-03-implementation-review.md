# Feature 03 unified storage refactor review

## Verdict

**PASS for local review on `refactor/03-unified-market-data-storage`.** The canonical repository write path persists bounded batches atomically through PostgreSQL COPY, reports committed progress from an independent read connection, and supports exact observation lookup and bounded acquisition history. Existing schema, migrations, source evidence, and Feature 02 replay interfaces remain compatible. The branch is not merged and no remote push was made.

## Corrections and changes

The refactor confirmed one data-integrity defect: revalidation compared `MarketTick` dataclasses directly, and `Decimal("1000.0") == Decimal("1000.00")` is true. A stored tick whose coefficient/exponent representation differed from the validated source could therefore pass revalidation. The comparison now uses the canonical wire representation, which includes Decimal spelling. A real database regression reproduces the old acceptance and verifies rejection after the fix.

Batch writes now perform three bounded COPY passes for raw evidence, accepted ticks, and quality events inside the existing transaction. Batch receipts, counters, source-order checks, database constraints, triggers, and retry logic remain in that same atomic transaction. COPY constraint failure tests confirm that raw rows and progress roll back together.

Added read-only committed progress, acquisition history, exact source-position lookup, and database/table-size queries. Progress is read on a fresh connection, so it cannot report the writer's uncommitted transaction. CLI commands expose database size and acquisition attempts. Batch metrics include rejected rows, quality events, failures, last commit latency, and lifecycle state without claiming queued data is durable.

No schema or index migration was needed. The existing unconstrained NUMERIC column continues to hold Feature 01's Decimal range; new database tests cover 256-digit coefficients, exponent 1000, and 18 fractional digits. Existing source archives preserve bytes and provenance unchanged.

## Regression and compatibility evidence

The new `tests/test_storage_refactor.py` covers Decimal-exact revalidation, atomic commit progress, uncertain receipt idempotence, reader isolation from uncommitted writes, COPY rollback, extreme Decimal values, overlapping source identities, storage accounting, recorded-stream ingestion through Feature 04 interpretation and Feature 01 validation into F03 and deterministic F02 replay, and invalid/incomplete lookup scopes. The recorded-stream fixture is explicitly synthetic; it is not a verified MT5 message. Existing batch-metric and CLI tests cover the added counters and commands.

Full-suite command, run against a fresh isolated TimescaleDB database with the native virtual display enabled:

```bash
env -u PYTHONPATH .venv-check/bin/python /tmp/run-quantlab-f03-recovery-gui.py -q --tb=short
```

**Result: 508 passed in 134.77 seconds.** This included database migrations, Features 01/02, historical ingestion and persistence, recorded streaming batches, native chart replay, backend interruption, server restart, and logical backup/restore with archive restoration. No tests were skipped. The short-lived initial run had three GUI failures because its Xvfb server had exited; after restarting the bundled Xvfb, the full run passed. A focused synthetic-stream test also exposed a non-unique test source identity on repeated runs; the fixture now uses a fresh source ID and passed independently.

Static checks run after the code and test changes:

```bash
env -u PYTHONPATH .venv-check/bin/python -m ruff check .
env -u PYTHONPATH .venv-check/bin/python -m ruff format --check .
env -u PYTHONPATH .venv-check/bin/python -m mypy
git diff --check
```

Results: Ruff passed, all 80 Python files were formatted, strict Mypy passed on 44 source files, and `git diff --check` passed.

## Scale and query evidence

The benchmark uses deterministic synthetic canonical data and a fresh local PostgreSQL 18.6 / TimescaleDB 2.31.0 instance with `fsync=on` and `synchronous_commit=on`. Five interleaved 100,000-row rounds compared batch sizes 100, 1,000, and 10,000 plus the 1,000-row queued writer. Separate one-run imports covered 1,000,000 and 2,000,000 rows. The harness checked source hashes, exact persisted values, complete counts, full replay, range reads, and exact lookups. It also retained per-run resource data, query plans, and a separate profile run.

Median 100k throughput was 662.9 rows/s at batch 100, 1,351.7 rows/s at batch 1,000, 1,668.8 rows/s at batch 10,000, and 1,354.8 rows/s for the queued 1,000 case. The one-run 1m and 2m cases measured 1,338.9 and 1,253.3 rows/s. These are local measurements, not production service targets. The query plans used existing indexes; no speculative index change was made. Details and raw results are in the [benchmark report](../../benchmarks/unified-storage-results.md) and its JSON companion.

## Downstream compatibility and remaining findings

Feature 02 replay/candle integration, Feature 01 validation, Feature 03 actual database persistence, and the available historical/recorded-stream adapters passed in the full suite. Canonical `MarketTick` fields, Decimal prices, UTC timestamps, metadata, and duplicate observations retain their existing contract. No live broker integration or trade execution was introduced.

The separate `feat/04-data-collection-pipeline` branch remains unmerged and was not automatically integrated. Its collector-specific same-capture checkpoint resume and Parquet pipeline are not certified against this refactor. The new `commit_status` query supplies committed progress but does not implement that collector protocol.

- **Critical:** none confirmed.
- **Major:** none confirmed in the refactor scope.
- **Minor:** hosted CI and Python 3.11 were not run locally. Production capacity, role security, offsite backup policy, and power-loss behavior need deployment-specific validation. The benchmark workload is synthetic and does not establish a production SLO.

## Files changed

Production changes are in `src/quantlab/storage/bulk.py`, `queries.py`, `contracts.py`, `repository.py`, `replay_repository.py`, `batching.py`, `dataset_validation.py`, and `src/quantlab/storage_cli.py`. Regressions are in `tests/test_storage_refactor.py`, `tests/test_storage_batching.py`, `tests/test_storage_cli.py`, `tests/test_storage_failures.py`, and `tests/test_historical_storage.py`. Documentation and measured artifacts are listed in the commit. The pre-existing untracked `goal.md` was preserved and is not part of this branch commit.
