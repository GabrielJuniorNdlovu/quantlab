# QuantLab

QuantLab validates local historical data for offline research on Boom 1000 Index.
Feature 01 separates exact source evidence from canonical `market_tick.v1` ticks
and structured quality events. The single-instrument scope keeps the data
contract precise. Feature 02 adds deterministic offline replay, M1 candlesticks,
immutable versioned research arrows, availability-aware timing, and an independently
inspectable audit log. Feature 03
adds transactional PostgreSQL/TimescaleDB ingestion, original-source archival,
recoverable import attempts, and completed-dataset replay.

The implementation supports local CSV, JSON Lines, and explicitly mapped recorded JSON envelopes. It requires no market credentials
or network connection at runtime. The supplied fixtures are deterministic
engineering examples, not published market observations.

## Set up the environment

Use Python 3.11 or newer from the existing project directory:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements-dev.lock
python -m pip install --no-build-isolation --no-deps -e .
```

On Ubuntu, install the OS `python3-venv` package if environment creation fails.
Dependency installation needs a package index or an available local cache.
Validation and replay run offline. Their runtime dependencies are standard-library
only. The optional storage extra adds psycopg and Alembic and connects to your
configured PostgreSQL instance. Storage integration tests require a local database.
`requirements-dev.lock` records the tested development-tool versions. The
project's development extra declares the supported tool set.

GitHub Actions runs the checks on Python 3.11 and 3.14 when the branch is pushed.
Local verification uses Python 3.14.4. Hosted Feature 03 checks run after your
manual push and include real TimescaleDB integration and recovery tests.

## Run the arrow replay

```bash
python -m quantlab.replay_app --research-signals
python -m quantlab.replay_app --research-signals --headless
python -m quantlab.replay_app --inspect-audit replay-runs/RUN_UUID.jsonl
```

The native Tk chart starts paused. Step forward three times to see a gold
synthetic UP arrow issued at `00:00:20 UTC`, source position `4`, anchored to its
`00:00` M1 candle. The fixture has 34 observations and three unevaluated research
arrows. The headless command verifies geometry and reports the signal timeline;
it draws no window. A graphical display and Python's Tk module are needed for
the native chart. Reset creates a new audit file and preserves the original.

`research_signal.v1` preserves exact issuance time, event cutoff and position,
bar anchor, Decimal price, dataset identity, producer version, and frozen metadata.
Recorded reception delays govern availability without changing candle event time.
These fixtures provide no evidence of forecasting performance.

The original `--fixture demo` command remains compatible, including its
constructed-outcome demonstration. New callers use `--research-signals`.
Persisted datasets can use the existing storage factory and bounded chart:

```bash
quantlab replay DATASET_UUID --show-chart --limit 100000
quantlab data replay DATASET_UUID --show-chart --headless --limit 1000
```

Set `QUANTLAB_DATABASE_URL` for persisted replay. These commands generate no
signals for market datasets. Plain `quantlab replay` retains JSON tick output.
See the [Feature 02 guide](docs/features/feature-02.md),
[causal replay architecture](docs/architecture/causal-replay.md),
[signal contract](docs/architecture/research-signal-contract.md), and
[implementation review](docs/reviews/feature-02-implementation-review.md).
The existing [MT5 prototype](integrations/mt5/README.md) remains uncompiled and
has not been adapted to this new contract.

## Import durable historical datasets

Install the tested storage dependencies and the operational entry point:

```bash
python -m pip install -r requirements-storage.lock
python -m pip install --no-build-isolation --no-deps -e ".[storage]"
```

Create an ignored `.env` from [.env.example](.env.example), choose a local database
password, and set the matching `QUANTLAB_DATABASE_URL`. Docker Compose supplies a
pinned TimescaleDB 2.31.0/PostgreSQL 18 image, a persistent volume, a TCP health
check, and a localhost-only port. It requires Docker Engine and Docker Compose.

```bash
docker compose up -d --wait
set -a
source .env
set +a
quantlab db init
quantlab ingest import src/quantlab/data/replay_demo.csv \
	--provider engineering-fixture --provider-symbol fixture-symbol --pip-size 2
quantlab ingest datasets
quantlab ingest status
quantlab replay DATASET_UUID --limit 100
```

`python -m quantlab` provides the same commands. Substitute the dataset UUID from
the import result. Optional `--start` and `--end` timestamps select a UTC range
with an inclusive start and exclusive end. A replay selects one completed
acquisition and preserves equal-time observations in source-position order.

The importer archives complete original bytes before processing bounded batches.
Repeating identical bytes and source configuration returns the existing completed
dataset. Repeating an interrupted import starts a new acquisition from those
bytes, preserving the earlier attempt. Rejected or interrupted imports are never
published for replay. These guarantees do not imply exactly-once live delivery.

Stop with `docker compose down` to retain the named volume. Back up both the
database and raw archive; deleting the volume or archive loses research evidence.
Read the [Feature 03 operations guide](docs/features/feature-03.md),
[unified storage design](docs/architecture/market-data-storage.md),
[storage architecture](docs/architecture/time-series-storage.md),
[database decision](docs/adr/ADR-003-time-series-database.md), and
[verification evidence](docs/reviews/feature-03-implementation-review.md) before recovery or deployment.

## Collect recorded data and export research partitions

Feature 04 uses one coordinator for unpaced historical imports and asynchronous recorded
simulation. It verifies committed prefixes on resume, retains repeated source observations,
reports file completeness separately from market coverage, and exports exact chronological
Parquet partitions.

```bash
quantlab data stream-simulate data/fixtures/historical/ticks.csv \
	--config data/fixtures/historical/profile-csv.json \
	--source-id synthetic-simulation --disconnect-after 2
quantlab data status --limit 20
quantlab data quality DATASET_UUID --gap-threshold-seconds 60
quantlab data export DATASET_UUID --format parquet --output research-export \
	--train-end 2026-01-01T00:00:00.5Z --validation-end 2026-01-01T00:00:02Z
```

Install `.[storage,export]` or the development lock for Parquet support. Source profiles
remain explicit; no provider identifiers, reception timestamps, or missing ticks are invented.
See the [collection guide](docs/operations/data-collection.md),
[recovery guide](docs/operations/collection-recovery.md),
[ingestion design](docs/architecture/market-data-ingestion.md), and
[F04 review](docs/reviews/feature-04-implementation-review.md).

## Validate the legacy CSV fixtures

The original four-column format remains available through an explicitly named
legacy adapter:

```bash
python -m quantlab.validation data/fixtures/valid_ticks.csv
python -m quantlab.validation data/fixtures/invalid_ticks.csv
```

The valid fixture reports four observations and prices from 999.90 to 1000.20.
Its report marks the result as limited fidelity. The invalid fixture exits with
status 1 and identifies physical line 3. Errors go to stderr. Success summaries
appear only after the entire input passes.

Legacy data does not contain a verified provider symbol, provider quote
precision, or acquisition identity. It produces `LegacyTick`, never
`MarketTick`. The old `Tick` name remains an alias for `LegacyTick`.

## Validate the canonical fixture

Provide an acquisition UUID, provider label, reviewed symbol mapping, and quote
precision explicitly:

```bash
python -m quantlab.validation data/fixtures/canonical_ticks.csv \
	--format canonical \
	--capture-id 11111111-1111-4111-8111-111111111111 \
	--provider fixture-provider \
	--provider-symbol fixture-symbol \
	--pip-size 2
```

The report identifies `market_tick.v1` and the internal instrument `BOOM_1000`.
`fixture-symbol` is test configuration. It is not a Deriv symbol or a claim that
an actual provider mapping has been verified. Review real source documentation
before supplying provider configuration for an authorized dataset.

Each canonical CSV contains these required columns, in any order:

```csv
instrument_id,provider_symbol,event_time_utc,price,pip_size
BOOM_1000,fixture-symbol,2026-01-01T00:00:00Z,1000.10,2
```

`provider_tick_id` is an optional column. Missing required columns, duplicate
headers, unexpected columns, blank records, malformed input, and empty datasets
are errors. Canonical CSV supports UTF-8 with LF or CRLF line endings.

## Consume validated observations incrementally

Use the binary CSV adapter with the source-independent engine:

```python
from pathlib import Path
from uuid import UUID

from quantlab.csv_adapter import iter_csv_candidates
from quantlab.engine import validate_acquisition
from quantlab.models import AcquisitionContext, SourceType

context = AcquisitionContext(
	capture_id=UUID("11111111-1111-4111-8111-111111111111"),
	provider="fixture-provider",
	source_type=SourceType.HISTORICAL_CSV,
	provider_symbol="fixture-symbol",
	pip_size=2,
)
with Path("data/fixtures/canonical_ticks.csv").open("rb") as stream:
	for outcome in validate_acquisition(iter_csv_candidates(stream, context), context):
		if outcome.tick is None:
			print(outcome.events[0].error_code.value)
		else:
			print(outcome.tick.event_time_utc, outcome.tick.price)
```

A domain rejection produces an outcome with structured errors and no tick.
Malformed transport input raises `QualityError` with its structured `.event`.
Consumers must inspect every outcome and exhaust the iterator successfully
before accepting a complete dataset. Early ticks remain provisional.

The engine and CLI keep constant state relative to record count, plus the
bounded current record. The legacy `load_ticks` convenience function still
returns a list and therefore uses O(n) memory. Source bytes remain in memory
only while referenced by a result. This feature does not archive data durably.

## Run the engineering checks

```bash
python -m pytest -q
python -m pytest --cov=quantlab --cov-branch --cov-report=term-missing
python -m ruff check .
python -m ruff format --check .
python -m mypy
python -m benchmarks.replay
.venv/bin/python -m benchmarks.validate --sizes 10000 100000 --runs 5
```

Two native Tk tests are opt-in when a display is available:

```bash
QUANTLAB_GUI_TEST=1 python -m pytest -q tests/test_chart.py tests/test_replay_gui.py
```

On a headless Linux development machine with Xvfb installed, prefix the command
with `xvfb-run -a env`. CI includes this native chart check on Python 3.14.
See [replay measurements](benchmarks/replay-results.md) for the new feature.

Hypothesis tests use deterministic settings and no persistent example database.
Benchmarks create deterministic engineering input, fully consume results, and
check accepted and rejected counts. They are separate from market research.
See [measured benchmark results](benchmarks/results.md) for the environment,
measurement method, run counts, and remaining uncertainty.

## Read the contract and design decisions

The [market-data contract reference](docs/architecture/market-data-contract.md)
defines fields, invariants, precision, ordering, errors, bounds, and compatibility.
[ADR-008](docs/architecture/adr-008-canonical-data-contract.md) records the audit,
architecture alternatives, and selected ownership boundaries.

Current limits include microsecond timestamp resolution, explicit input-size
caps, and caller responsibility for provider mapping authenticity. Feature 03
provides durable raw archival; protect and back up that directory with the database. Passing validation establishes the documented data contract; it
does not establish source authenticity, complete market coverage, or predictive
ability.

Feature 03 exposes a resettable factory of these same canonical ticks for
Feature 02. It does not merge overlapping acquisitions automatically. QuantLab includes no broker connection, account authentication,
order execution, or learned prediction model.

## Historical and recorded streaming exports (Feature 04)

Import local mapped CSV, flat historical JSON Lines (`jsonl`), or recorded streaming JSON Lines (`stream_jsonl` with `websocket` source type) with an explicit source profile:

```bash
quantlab data inspect data/fixtures/historical/ticks.csv --config data/fixtures/historical/profile-csv.json
quantlab data import data/fixtures/historical/ticks.csv --config data/fixtures/historical/profile-csv.json --source-id approved-export-001
quantlab data list --limit 20
quantlab data validate DATASET_UUID
quantlab data replay DATASET_UUID --limit 100
```

Inspection works without a database. Import uses the Feature 03 database/archive configuration,
transactional batches, recovery and idempotence. Profiles explicitly map external instruments
to `BOOM_1000` and supply documented symbol, timestamp and precision semantics. Equivalent observations across CSV, historical JSON, and recorded streaming JSON normalize to identical canonical `MarketTick.market_fields()` while preserving transport-specific provenance (`source_type`, `capture_id`, `source_position`, `received_at_utc`). Original bytes,
repeated observations and source order are retained. Overlapping files stay separate datasets.
See [import instructions and supported formats](docs/features/feature-04.md),
[architecture](docs/architecture/historical-data-adapters.md), and
[verification evidence](docs/verification/feature-04.md).
