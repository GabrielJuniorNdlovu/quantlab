# Feature 03 engineering verification

## Delivered behavior

Feature 03 adds transactional PostgreSQL/TimescaleDB ingestion and durable source
provenance. It reuses Feature 01 validation and exposes the existing Feature 02
`Callable[[], Iterator[MarketTick]]` replay interface. The implementation includes
bounded historical batches, an independent event-batch deadline, explicit
backpressure, idempotent batch receipts, immutable attempts, publication guards,
structured diagnostics, packaged migrations, and operational commands.

The branch is `feat/03-durable-time-series-storage`, based on accepted Feature 02
merge `0b7829b`. Completed work is committed locally; run `git log -1 --oneline`
for the delivery hash. No feature merge or remote push is performed. The unrelated
untracked `goal.md` is preserved byte-for-byte, SHA-256
`fa403438a403635005649bf32c01f5dc793e409eda753b12f19f8d1d2369a3fe`.

## Grounding and design review

Read the complete request, charter, existing canonical values and validation,
replay input/lifecycle, dependencies, workflow rules, and relevant project skills.
The pre-change suite reported 306 passed and two opt-in native-display skips.
Fetching main verified that Features 01 and 02 were already accepted. Local main
was fast-forwarded before creating the new feature branch.

Applied architect, how, arena, TDD, ponytail, thermo-nuclear-code-quality-review,
benchmark-checklist, principle-explain-the-number, technical-writing, and unslop.
Two independent sketches compared verified-prefix continuation with immutable
attempt restart. The restart design scored 23/25 against 22/25, with the bounded
validated-outcome writer retained from the alternative. Requested external model
families were unavailable; these are independent inherited-model passes, not
agreement between different model families.

The six-table schema separates source/dataset identity, acquisition attempts, raw
rows, the tick hypertable, quality events, and batch receipts. Original source
hashes and exact raw bytes are retained. Timestamp-inclusive hypertable keys plus
an ordinary raw identity foreign key prevent identity duplication at another
time. Unconstrained NUMERIC and checked price text preserve Decimal representation.
The [architecture reference](../architecture/time-series-storage.md) documents
ownership, transitions, transaction boundaries, and limitations.

## Actual environment

Local checks use CPython 3.14.4 on Ubuntu Linux x86_64, 12 logical CPUs, Docker
Desktop 4.96.3, Docker Engine 29.9.2, and Compose 5.6.1. A temporary Xvfb display
runs the existing Tk chart tests. PostgreSQL 18.6 and TimescaleDB 2.31.0 run in the
pinned vendor image with `fsync=on` and `synchronous_commit=on`.

The dedicated Compose project `quantlab-feature03-agent` binds localhost port
16432 and preserves its own named volume. Generated local credentials are mode
0600 in ignored files and are never printed. Application verification uses a
fresh generated database. Benchmarking uses another generated database. No
existing user database, container, or volume is dropped or overwritten.

Official compatibility, hypertable APIs, exact numeric/timestamp behavior,
psycopg transactions/cursors, Alembic, and Timescale logical restore instructions
were checked. The vendor documentation's Markdown responses required a direct
read when the web reader could not parse them. References are linked in
[ADR-003](../adr/ADR-003-time-series-database.md).

## Commands and results

The operational checks include these actual commands and their dependency order:

```bash
git fetch origin main
git merge-base --is-ancestor 2c356b8 origin/main
docker pull timescale/timescaledb:2.31.0-pg18
docker compose --env-file .env.feature03 -p quantlab-feature03-agent up -d --wait
python -m pip install 'psycopg[binary]==3.3.6' 'alembic==1.20.1'
.venv-check/bin/python -m pip install --no-build-isolation --no-deps --no-index -e '.[storage]'
set -a
source .env.feature03
set +a
env -u PYTHONPATH .venv-check/bin/python -m pytest -q --tb=short
```

The final complete suite additionally sets:

```bash
export QUANTLAB_DESTRUCTIVE_TESTS=1
export QUANTLAB_TEST_COMPOSE_PROJECT=quantlab-feature03-agent
export QUANTLAB_TEST_COMPOSE_ENV=.env.feature03
env -u PYTHONPATH DISPLAY=:99 QUANTLAB_GUI_TEST=1 .venv-check/bin/python -m pytest -q --tb=short
```

**355 tests passed with no skips**, including real database and native chart
checks, bounded retry, cross-chunk/offset timestamps, and a full database-backed
Feature 02 session with reset. Combined statement/branch coverage displays 86%; batch writer
coverage is 99%, ingestion 96%, and Feature 01's engine remains 100%. SQL trigger
coverage is established by behavior tests, not inferred from Python line coverage.
CLI subprocess behavior tests do not contribute all child-process lines to that
coverage run. The host's unrelated ROS PYTHONPATH is removed for reproducibility.

Static checks passed:

```bash
env -u PYTHONPATH .venv-check/bin/python -m ruff check .
env -u PYTHONPATH .venv-check/bin/python -m ruff format --check .
env -u PYTHONPATH .venv-check/bin/python -m mypy
git diff --check
```

Ruff checks 61 formatted Python files; strict Mypy checks 36 runtime modules.
The CI YAML and its credential-generation Python were independently executed or
parsed without displaying credentials. CI now provisions the pinned real database,
applies migrations, runs integration tests, and adds isolated crash/restore and
native-chart checks on Python 3.14.

A 0.4.0 wheel was built and installed into a temporary target using:

```bash
.venv-check/bin/python -m pip wheel --no-build-isolation --no-deps --no-index --wheel-dir /tmp/quantlab-feature03-wheels .
.venv-check/bin/python -m pip install --no-index --no-deps --target /tmp/quantlab-feature03-installed /tmp/quantlab-feature03-wheels/quantlab-0.4.0-py3-none-any.whl
```

The package includes migration environment/revisions, schema, operational entry
point, type marker, and the prior replay fixture. The installed wheel successfully applied its packaged migrations, imported all
34 packaged fixture observations, and streamed them back with two-row fetches.
Its core replay fixture and type marker are present. `pip check` reported no
broken requirements. The installed `quantlab --help` entry point also ran.

## Failure and recovery evidence

Tests exercise actual PostgreSQL/TimescaleDB rather than substituting SQLite or
an in-memory store:

- A child importer is SIGKILLed after its first five observations commit. Recovery
  starts a fresh attempt, retains those five raw rows under the interrupted
  capture, and publishes exactly 34 observations in the selected completed attempt.
- Injecting a lost acknowledgement after a real COMMIT reconnects, reacquires
  ownership, checks the receipt, and leaves one batch and one copy of its rows.
- `pg_terminate_backend` during an actual transaction rolls it back; bounded retry
  produces the expected rows and receipt without partial publication.
- SIGKILL of the owned database container followed by WAL recovery retains the
  completed manifest and all canonical values exactly.
- A custom-format `pg_dump` restores into a generated isolated database using
  Timescale pre-restore/post-restore and single-job restore. Removing and restoring
  the test archive from its backup reproduces the hash and full tick sequence.
  Only that generated restore database is dropped afterward.
- A populated `0001_storage` database upgrades to `0002_storage_guards` without
  changing its completed manifest or observations. Destructive downgrade refuses.
- Competing dataset claims, duplicate-looking observations, malformed source,
  invalid domain records, empty input, conflicting receipts, range bounds,
  Decimal exponents through `1E+1000`, 256 coefficient digits, microseconds,
  foreign keys, BIGINT limits, and append-only guards are checked.
- Idle producer timing, size threshold, queue pressure/timeouts, shutdown flushing,
  sink failures, and submitted-versus-committed metrics are exercised separately.

## Confirmed findings and fixes

Critical: direct finalization could publish a valid prefix. A real-database test
first failed `DID NOT RAISE StorageError`. Finalization now streams the preserved
header and row evidence through SHA-256 and checks the entire source byte count
and digest before publication. The same test then appends the rest and completes.

Critical: the first database guard covered updates but allowed a direct INSERT
with completed state. A failing behavior test exposed the gap. Additive revision
0002 enforces empty importing initial state and completion prerequisites on both
insert and update, with context and evidence-removal guards.

Major: buffered file reads could hide mutation during post-read archive
verification. The test first failed `DID NOT RAISE`; verification now uses a fresh
duplicated descriptor reader and compares source identity and checksum.

Major startup race: a Unix-socket health check accepted the temporary initdb
server before its shutdown transition. The Compose health check now uses TCP to
wait for the final server.

Minor diagnostic defects: plain retry messages lost attempt metadata, and the
batch writer initially lacked structured commit/failure records. JSON logs now
retain retry counts, queue metrics, acknowledged commits, and exception types.

Test harness fixes: migration initialization no longer depends on test order in
a fresh database. Restart tests wait for the exact owned container to exit before
starting it again. The child-process checkpoint marker is published atomically
so its parent cannot observe partial JSON; its targeted recovery test also passed
after this test-harness hardening. CI environment generation now writes real newlines, checked
without printing its generated credentials.

Strict review found cohesive ownership, no circular imports, no new tick model,
no SQL in the coordinator, bounded source/batch/cursor memory, parameterized SQL,
no credential-bearing diagnostics, and no file approaching 1000 lines. All
confirmed Critical/Major/Minor implementation findings were fixed and rechecked.

## Performance

[Earlier raw measurements](../../benchmarks/storage-results.md) describe the
initial Feature 03 implementation. The unified batch and scale measurements for
this refactor are in the
[current benchmark report](../../benchmarks/unified-storage-results.md), with
per-run results in its JSON companion. It compares five interleaved 100,000-row
rounds and separately runs 1,000,000 and 2,000,000 rows. The benchmark asserts
source hashes, persisted counts, exact values, replay, range queries, and exact
lookups. Results are local engineering evidence, not production SLOs.

## Remaining boundaries and completion

No confirmed Critical or Major code issue remains from local review. Historical
CSV framing is the implemented completion contract. Other source adapters must
define durable framing and acknowledgement before live publication. RAM buffers
can be lost on abrupt exit, with historical recovery provided by the archive.
PostgreSQL TEXT/JSONB cannot store NUL; BIGINT positions are narrower than Python's
arbitrary integers. The initial maximum queue can occupy substantial memory at
maximum payload size. All these failures or limits are explicit.

The developer Compose role is administrative and the port is localhost-only.
A production deployment still requires least-privilege credentials, remote TLS
policy where applicable, independent backups, retention/restore drills, monitoring,
and capacity targets. Local restart/restore tests do not certify physical power
failure or an offsite disaster-recovery plan.

Python 3.11 and hosted GitHub Actions remain unverified locally. The workflow
covers Python 3.11 and 3.14 after your manual push. Human review and green hosted
CI are required before merge. No broker, account connection, financial execution,
model, or prediction capability is added.

Feature 03's local Definition of Done is satisfied: real database, migrations,
source preservation, bounded batching, idempotence/recovery, completed-only
replay, integration/failure tests, measured performance, observability,
documentation, and code review are complete. Hosted CI and human review remain
release/merge gates; this is not a blanket production-deployment endorsement.

## Changed files

The following 42 files comprise this delivery. After committing, regenerate
the list with `git diff --name-only main...HEAD`.

- `.env.example`
- `.github/workflows/ci.yml`
- `.gitignore`
- `CHANGELOG.md`
- `README.md`
- `benchmarks/storage-diagnostics.json`
- `benchmarks/storage-profile.txt`
- `benchmarks/storage-results.json`
- `benchmarks/storage-results.md`
- `benchmarks/storage.py`
- `compose.yaml`
- `docs/adr/ADR-003-time-series-database.md`
- `docs/architecture/time-series-storage.md`
- `docs/features/feature-03.md`
- `docs/verification/feature-03.md`
- `pyproject.toml`
- `requirements-dev.lock`
- `requirements-storage.lock`
- `src/quantlab/__main__.py`
- `src/quantlab/storage/batching.py`
- `src/quantlab/storage/contracts.py`
- `src/quantlab/storage/ingestion.py`
- `src/quantlab/storage/mapping.py`
- `src/quantlab/storage/migration_runner.py`
- `src/quantlab/storage/migrations/__init__.py`
- `src/quantlab/storage/migrations/env.py`
- `src/quantlab/storage/migrations/versions/0001_storage.py`
- `src/quantlab/storage/migrations/versions/0002_storage_guards.py`
- `src/quantlab/storage/migrations/versions/__init__.py`
- `src/quantlab/storage/replay_repository.py`
- `src/quantlab/storage/repository.py`
- `src/quantlab/storage/schema.py`
- `src/quantlab/storage/source_archive.py`
- `src/quantlab/storage/source_evidence.py`
- `src/quantlab/storage_cli.py`
- `tests/conftest.py`
- `tests/test_ingestion.py`
- `tests/test_source_archive.py`
- `tests/test_storage_batching.py`
- `tests/test_storage_cli.py`
- `tests/test_storage_failures.py`
- `tests/test_storage_repository.py`
