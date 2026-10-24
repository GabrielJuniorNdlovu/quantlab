# QuantLab

QuantLab is a Python platform for offline quantitative research. Feature 01, the
Market Data Integrity Engine, validates local historical tick CSV files before
later research into price behavior, spike events, and prediction replay.
Boom 1000 Index is the sole instrument to keep the first milestone's data
contract precise. Its canonical name is not a broker API symbol.

This is an initial implementation, not a production-ready data service. The
fixtures are small deterministic examples, not published market history. There
are no broker connections, credentials, accounts, or trading functionality.

## Setup and commands

From this project directory, use Python 3.11 or newer:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -e '.[dev]'
python -m pytest -q
python -m quantlab.validation data/fixtures/valid_ticks.csv
```

On Ubuntu, creating an environment requires Python's venv support, typically
provided by `python3-venv`. Installing dependencies may require a package index
or local package cache. Validation and tests require no network or credentials.
There are no runtime dependencies; pytest is a development extra.

The valid example prints four records, the instrument, UTC timestamp bounds,
and minimum/maximum prices of 999.90 and 1000.20. To see rejection:

```bash
python -m quantlab.validation data/fixtures/invalid_ticks.csv
```

This exits with status 1 and reports a non-finite price on physical line 3.
Errors go to stderr; a successful summary goes to stdout, only after the entire
file passes validation.

## CSV contract

Use UTF-8 CSV with exactly these column names, in any order:

```csv
symbol,timestamp_utc,price,source
Boom 1000 Index,2026-01-01T00:00:00Z,1000.10,local-observations
```

- `symbol` must equal `Boom 1000 Index` exactly. `INSTRUMENT` in `models.py`
  centralizes this identifier. Broker-specific mapping is deferred until verified.
- `timestamp_utc` accepts ISO 8601 extended form
  `YYYY-MM-DDTHH:MM:SS[.ffffff]Z` or an explicit `±HH:MM` offset. Fractional
  seconds may have one to six digits. Valid values normalize to aware UTC
  datetimes. Local times without offsets, dates without times, leap seconds,
  and precision exceeding microseconds are rejected instead of silently changed.
- `price` must be a finite, strictly positive decimal number. Scientific notation
  is accepted. `Decimal` conversion is direct, with no intermediate floats or
  rounding. No tick size or instrument price scale is assumed.
- `source` is required and nonblank. Its value is retained exactly as supplied;
  this label is not verified evidence of authenticity.

Rows must have non-decreasing UTC timestamps. Identical timestamps and prices
are legitimate ticks and are preserved. No deduplication, sorting, trimming,
interpolation, or record skipping occurs. Missing/extra fields, duplicate header
names, blank records, invalid CSV, and empty datasets are errors. Quoted multiline
fields are supported: `source_row` identifies the starting physical line, counting
the header. Input files are opened read-only and never rewritten.

## Architecture and Python API

The installable `src/` package separates frozen, slotted `Tick` records from
validation and file I/O. `Tick` is a typed container; use the validator to
establish invariants rather than directly constructing unvalidated records.

```python
from quantlab.validation import load_ticks

ticks = load_ticks("data/fixtures/valid_ticks.csv")
```

`parse_tick(record, source_row)` validates one mapping. `validate_records` adds
ordering and nonempty-dataset checks to an iterable of numbered mappings.
`iter_csv_ticks(stream)` checks CSV structure and reuses these rules.
`load_ticks(path)` owns file I/O and returns a fully validated list or raises
`ValidationError` (with `.row`) or an OS file-access error.

Validation takes O(n) time with O(1) auxiliary state relative to record count,
plus the current CSV record. The list-returning loader and CLI use O(n) memory.
Iterator consumers can process future chunks without rewriting the rules, but
early yields remain provisional: exhaust the iterator successfully before
accepting a dataset or committing downstream results.

## Verification and remaining limitations

The offline tests cover schema and field checks, exact prices, UTC conversion,
ordering, duplicates, provenance, physical line reporting, immutability, input
preservation, CSV/encoding errors, and CLI summaries and exit codes. CLI tests
also invoke the installed package from outside the project directory.

- **Major operational limitation:** the loader materializes the file; bounded
  ingestion, transactional persistence, metrics, and recovery controls are not
  implemented. Iterator consumers need explicit commit handling.
- **Minor limitation:** timestamps have microsecond precision. CSV field sizes
  use Python's default parser limit; higher precision and resource budgets need
  a separately specified contract.
- **Minor limitation:** cadence, gaps, completeness, spike plausibility, tick
  sizes, source authenticity, and cross-file consistency are not checked.
  Passing validation establishes structural integrity, not market truth.

Suggested next feature: a read-only quality report for gaps and sampling cadence,
after verifying the expected observation cadence from data documentation.
No later milestone is implemented here.
