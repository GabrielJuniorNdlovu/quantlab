# Import and replay durable market datasets

Feature 03 stores locally supplied historical and recorded canonical Boom 1000
observations in PostgreSQL with TimescaleDB. It preserves source evidence,
validates each observation through Feature 01, and exposes completed acquisitions
through Feature 02's replay factory. The durable batch API also accepts already
validated streaming observations; this does not add a live broker connection or
order execution.

## Start the local database

From the repository root, create or activate the environment described in the
[README](../../README.md). Install the pinned development dependencies, which now
include the storage dependencies, then install the project:

```bash
python -m pip install -r requirements-dev.lock
python -m pip install --no-build-isolation --no-deps -e .
cp -n .env.example .env
```

Edit `.env`. Choose a local-only `POSTGRES_PASSWORD` and fill
`QUANTLAB_DATABASE_URL` with the matching username, URL-encoded password, port,
and database. Quote shell-special values. Leave `QUANTLAB_TEST_DATABASE_URL`
empty unless you have prepared a separate disposable test database. The
application does not load `.env` itself; Compose reads it automatically.

Export the values for the Python CLI, then start the database:

```bash
set -a
. ./.env
set +a
docker compose up -d --wait postgres
python -m quantlab db init
```

The final command reports `{"status": "schema_ready"}`. It applies packaged
Alembic revisions through `0002_storage_guards`, including the Timescale hypertable
and integrity guards. Running it again is safe. The installed `quantlab` command
accepts the same arguments as `python -m quantlab`.

The image is pinned to TimescaleDB `2.31.0` on PostgreSQL `18`, with an immutable
image digest. The selected image contains PostgreSQL `18.6`. The default port is
`127.0.0.1:15432`; `QUANTLAB_DB_PORT` can change it. The named `pgdata` volume mounts
`/var/lib/postgresql`, preserving the PostgreSQL 18 image's versioned data layout.
Compose enables `fsync` and `synchronous_commit`, disables Timescale telemetry,
and waits for a database health check. See the [database decision](../adr/ADR-003-time-series-database.md)
for compatibility sources and tradeoffs.

Stop and restart without removing the database volume:

```bash
docker compose stop postgres
docker compose start postgres
```

Keep the volume and the archive directory when preserving research data. Removing
containers is different from deleting their volumes. Take and verify a backup
before changing the deployment or database version.

## Import the engineering fixture

```bash
python -m quantlab ingest import src/quantlab/data/replay_demo.csv \
	--provider engineering-fixture --provider-symbol fixture-symbol --pip-size 2
```

The JSON result includes `dataset_id`, `capture_id`, `status`, and durable row and
error counts. The packaged fixture has 34 observations. `COMPLETE` means the full
source was exhausted, every observation accepted, and completion reconciled in the
database. `fixture-symbol` is deliberately synthetic. It is not a verified Deriv
API symbol.

Repeat the same command. Identical source bytes, source identity, and validation
configuration return the existing completed dataset. A different filename alone
creates no duplicate. Distinct observations with equal timestamps and prices
remain distinct rows.

Use `--batch-size 500` to change the persistence batch threshold. Historical
imports run as fast as validation and database persistence permit. They do not
wait one second between records or batches.

## Inspect status and query one dataset

Substitute the returned UUID for `DATASET_UUID`:

```bash
python -m quantlab ingest status
python -m quantlab ingest status DATASET_UUID
python -m quantlab ingest datasets --limit 100
python -m quantlab replay DATASET_UUID --limit 100
python -m quantlab replay DATASET_UUID \
	--start 2026-01-01T00:01:00Z --end 2026-01-01T00:04:00Z --limit 100
```

Status includes incomplete and failed datasets. The datasets command selects
completed entries from its bounded recent-manifest window; it is not an exhaustive
archive search. Replay emits canonical ticks as JSON Lines, with Decimal prices
as strings and timestamps in UTC. Time ranges include the start and exclude the
end. Equal timestamps use source-position order. Incomplete or missing datasets
fail explicitly instead of exposing partial imports as completed history.

The CLI's normal results go to stdout. Structured ingestion and batching logs go
to stderr. Exit code `0` indicates success, `1` indicates an imported dataset
failed validation, and `2` indicates configuration, file, database, or operational
failure. Ctrl+C interrupts the import; committed batches remain and the source
archive supplies the buffered tail on restart. Database errors omit connection secrets; inspect the database logs for
the underlying diagnostic.

Inspect persisted validation details in a PostgreSQL client using the returned
capture UUID:

```sql
SELECT source_position, error_code, severity, description
FROM data_quality_events
WHERE capture_id = 'CAPTURE_UUID'::uuid
ORDER BY source_position, event_id
LIMIT 100;
```

## Recover an interrupted import

An unexpected exit can leave an `IMPORTING` attempt with some committed batches.
It never makes that partial acquisition replayable. Restart the same import using
the unchanged source and the same provider, symbol, precision, and source identity:

```bash
python -m quantlab ingest import src/quantlab/data/replay_demo.csv \
	--provider engineering-fixture --provider-symbol fixture-symbol --pip-size 2
```

The importer acquires the dataset lock, marks a prior abandoned importing attempt
`INTERRUPTED`, and starts a new capture UUID from the beginning. Old raw records,
ticks, quality events, and batch receipts remain attached to the old attempt.
Only the completed selected attempt becomes replayable. Recovery restarts; it
does not append to an old prefix or silently delete it.

If the original path is unavailable, import the corresponding
`raw-archive/SOURCE_SHA256.raw` object with the original configuration. Whole-file
bytes determine identity, so the `.raw` filename does not change the dataset.
The archive root defaults to `raw-archive`; `QUANTLAB_ARCHIVE_DIR` changes it.
Programmatic imports also require their original reception timestamp and metadata;
use the saved configuration rather than replacing those values with defaults.

A failed validation attempt remains failed. Repeating the same invalid bytes can
only produce another failed attempt under the same configuration. A corrected
file has another checksum and dataset identity. Another active importer reports
a busy error; do not mark it abandoned merely because it is old.

## Use the replay factory or future batching interface

The [architecture reference](../architecture/time-series-storage.md) shows how to
pass `Repository.tick_factory(dataset_id)` into the existing `ReplayEngine` or
`ReplaySession`. Feature 02's alert and chart behavior is unchanged. No duplicate
storage-specific tick model is introduced.

For future event-driven adapters, `BatchWriter` accepts existing
`ValidationOutcome` objects. Its independent worker flushes at 100 pending records
or 1000 ms by default, even if the producer becomes idle. Its queue holds at most
10,000 records. Submit blocks under pressure or raises `queue.Full` on an explicit
timeout; the caller retains an observation that was not accepted. Close drains
accepted work, flushes the tail, and propagates persistence failure. This is a
local batching interface, not an implemented live market-data adapter.

Source arrival, database commit frequency, and chart rendering frequency are
separate. Every observation is validated individually. Batches group database
transactions; replay speed controls later presentation only.

## Verify and measure

```bash
python -m pytest -q
python -m ruff check .
python -m ruff format --check .
python -m mypy
```

Set `QUANTLAB_TEST_DATABASE_URL` only to an isolated Timescale-enabled database
before running database tests. They apply migrations and write retained test
acquisitions. They must not target valuable data. Native chart tests still need
an available display and `QUANTLAB_GUI_TEST=1`. Container restart and restore
checks have additional explicit isolation gates described in the
[verification report](../verification/feature-03.md).

See the [Feature 03 storage measurements](../../benchmarks/unified-storage-results.md)
for repeated batch-size comparisons, 100k/1m/2m import runs, commit latency,
memory, storage, query plans, and replay evidence. Engineering fixture throughput
is not a production SLO.

## Troubleshoot operations

If Compose rejects an empty password, finish editing `.env` and restart the
command. If Python reports missing database configuration, export the file's
values into the current shell. A running Compose service does not export them
into Python automatically.

If connection fails, check `docker compose ps`, `docker compose logs postgres`,
the localhost port, and the database URL. If Docker itself denies access, correct
your local Docker permissions; switching to another database engine is not a
recovery path. If tables or functions are missing, run `python -m quantlab db init`
against the intended database.

A checksum mismatch stops ingestion. Preserve the suspect file and `.partial-`
artifacts for diagnosis; do not overwrite the hash-named archive object to make
the import pass. A source changing during copying is rejected before publication.
No automated archive garbage collection or data-removal command is included.

Backups must include both PostgreSQL and the source archive. Follow the
[backup and recovery boundary](../architecture/time-series-storage.md#backup-and-recovery-boundary)
and verify restoration in an isolated instance. A named Docker volume alone is
not an independently recoverable backup.
