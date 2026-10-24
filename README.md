# QuantLab

QuantLab validates local historical data for offline research on Boom 1000 Index.
Feature 01 separates exact source evidence from canonical `market_tick.v1` ticks
and structured quality events. The single-instrument scope keeps the data
contract precise before later research and chart replay.

The implementation supports local CSV files. It requires no market credentials
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
Validation and tests run offline. Runtime dependencies are standard-library only.
`requirements-dev.lock` records the tested development-tool versions. The
project's development extra declares the supported tool set.

GitHub Actions runs the checks on Python 3.11 and 3.14 when the branch is pushed.
Local verification used Python 3.14.4. Hosted CI still needs its first run.

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
.venv/bin/python -m benchmarks.validate --sizes 10000 100000 --runs 5
```

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
caps, caller responsibility for provider mapping authenticity, and no durable
raw archive. Passing validation establishes the documented data contract; it
does not establish source authenticity, complete market coverage, or predictive
ability.

Feature 02 can consume these outcomes for offline arrow replay after successful
validation. No broker connection, account authentication, trading, database,
forecast model, or chart integration is implemented by Feature 01.
