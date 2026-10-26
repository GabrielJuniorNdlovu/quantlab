# Recover an interrupted market-data import

Use this procedure with an approved local database and its original raw archive.
Do not delete evidence or change manifest status manually to make replay succeed.

## Inspect durable progress

Set `QUANTLAB_DATABASE_URL` to the intended database, then run:

```bash
quantlab db status
quantlab ingest status DATASET_UUID
quantlab ingest status DATASET_UUID --attempts --limit 100
```

Check dataset status, capture identity, observed/accepted/rejected/error counts,
last source position, batch number, and digest. Attempt summaries include committed
failed or interrupted prefixes. They do not establish that a collector is alive.
The receipt timestamp is assigned before commit and visible only afterward.

## Recover a historical or recorded-source import

1. Restore database availability. Keep the raw archive path available.
2. Check the source SHA-256 and interpretation profile against the manifest.
3. Run the original import command with the same source ID and profile.
4. Inspect the resulting manifest and attempts.
5. Validate the completed source before replay.

For a configured historical source:

```bash
quantlab data import SOURCE_FILE --config PROFILE_JSON --source-id ORIGINAL_SOURCE_ID
quantlab data validate DATASET_UUID
quantlab replay DATASET_UUID --limit 100
```

For the original canonical CSV interface, reuse its original `ingest import`
provider, provider-symbol, pip-size, and source-ID arguments.

An already completed identical dataset returns its original completed capture.
An incomplete historical attempt is preserved and replaced by a fresh capture;
the source is validated from its beginning. Rows from the prior attempt remain
traceable. Only the newly completed selected capture is replayable. This is not
a same-capture incremental source resume claim.

## Handle uncertain acknowledgements

When a connection fails near commit, do not assume the batch was lost. The
repository reconnects, reacquires its dataset advisory lock, and checks the same
batch number and canonical digest. A matching committed receipt prevents another
copy. A conflicting digest stops the write. Exhausted retries propagate failure.

A collector must retain ownership of unacknowledged observations and reconstruct
its validation state from verified source evidence. Durable `commit_status` can
reconcile committed progress; queue acceptance cannot. The [F04 recovery procedure](collection-recovery.md) describes the adapted
source-prefix resume protocol and the verified collection interface.

## Handle archive failures

The archive first writes a `.partial-` file, flushes and fsyncs it, checks copied
bytes and source identity, then publishes a no-overwrite hash-named hard link and
fsyncs the directory. Only a returned verified receipt enters dataset registration.

A failed copy can leave a partial file. A database failure after publication can
leave an unreferenced complete archive file. Neither is a completed dataset.
Preserve such files for diagnosis. Verify the expected hash and determine all
manifest references before an operator applies a retention policy. Do not claim
byte-exact recovery from a reconstructed or normalized payload.

Database and filesystem publication are not one atomic transaction. Recovery uses
content addressing, verification, retained evidence, and repeatable registration.
Read-only mode prevents accidents, not privileged edits; verification detects
subsequent mutation. Keep both database and original archive in backups.

## Verify recovery on isolated infrastructure

Use a disposable Compose project and database, not valuable local datasets:

```bash
QUANTLAB_DESTRUCTIVE_TESTS=1 \
QUANTLAB_TEST_COMPOSE_PROJECT=quantlab-ci \
QUANTLAB_TEST_COMPOSE_ENV=PATH_TO_ISOLATED_ENV \
python -m pytest -q tests/test_storage_failures.py
```

Set `QUANTLAB_TEST_DATABASE_URL` to that same instance. These tests include
actual killed imports, backend termination, lost acknowledgement injection,
container SIGKILL/restart, and a TimescaleDB-aware logical backup/restore with
raw archive restoration. They must not run against a shared production project.

Run the source-to-replay verification with the same isolated database:

```bash
python -m pytest -q tests/test_storage_refactor.py \
	-k recorded_stream_incremental_writer_to_deterministic_replay
```

The synthetic fixture passes through F04 interpretation, F01 validation, bounded
incremental F03 writes, durable progress inspection, source revalidation, and two
F02 replays. It checks exact prices, microsecond timestamps, count, order, optional
metadata, and original bytes. It represents no verified live provider feed.

## Migrations and durability limits

Run `quantlab db init` to apply packaged migrations under the existing migration
lock. This refactor adds no schema revision or speculative index. Existing
`0001_storage` and `0002_storage_guards` remain authoritative. Tests upgrade a
populated first revision and verify preserved data; revision 0002 deliberately
refuses destructive downgrade. A rollback requiring evidence removal needs a
reviewed forward migration or a tested backup restoration.

With tested `fsync=on` and `synchronous_commit=on`, acknowledged batches survive
the executed process/backend/container failures. The tests do not establish
power-loss behavior of every host filesystem or storage controller. Hashes are
integrity checks, not authentication. No cross-system exactly-once guarantee is
claimed. Statement/connect timeouts and finite retries do not establish a universal
wall-clock shutdown bound during every network failure.
