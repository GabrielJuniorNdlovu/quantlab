# Import local historical data

Feature 04 imports authorized local CSV and JSON Lines exports through canonical validation
and durable TimescaleDB storage. The checked-in examples are deterministic engineering
fixtures, not broker history.

**Current status:** The refactor implementation and non-database checks are present on
`refactor/04-market-data-ingestion`. Database integration and required scale benchmarks remain
unverified; see the [implementation review](../reviews/feature-04-implementation-review.md).

## Inspect an export

Run inspection before importing. It reads the entire file and requires no database:

```bash
quantlab data inspect data/fixtures/historical/ticks.csv \
  --config data/fixtures/historical/profile-csv.json
quantlab data inspect data/fixtures/historical/ticks.jsonl \
  --config data/fixtures/historical/profile-jsonl.json
```

A valid example reports three accepted observations, zero rejected observations, the source
checksum, and the explicit profile. An invalid tail makes the report invalid even when the
prefix passes. At most 20 ordinary diagnostics plus one fatal diagnostic are retained.

## Define a source profile

Copy an example profile and supply metadata from your authorized export's documentation.
Every key below is required, and unknown keys are rejected:

```json
{
  "version": "historical.v1",
  "format": "csv",
  "provider": "engineering-fixture",
  "provider_symbol": "BOOM-export",
  "pip_size": 2,
  "timestamp": "iso8601",
  "fields": {
    "instrument_id": "symbol",
    "event_time": "time",
    "price": "quote",
    "provider_tick_id": "id"
  },
  "instruments": {"Boom export": "BOOM_1000"}
}
```

`fields` maps canonical logical names to exact source field names. `instrument_id`,
`event_time`, and `price` are required. `provider_symbol`, `pip_size`, `provider_tick_id`,
`received_at_utc`, and `price_basis` are optional bindings. Bindings must be unique. Instrument mappings require 1–32 explicit
external values; each target must be `BOOM_1000`. An internal-looking source value is rejected
unless explicitly mapped. There is no case folding, trimming, or provider-symbol guessing.

The profile's provider label, provider symbol, and decimal-place precision are supplied
assertions. Use genuinely documented metadata. If symbol or precision fields are bound,
observed values must agree with those assertions. Missing event IDs stay absent, including
blank CSV IDs and missing or null JSON IDs. Present IDs require text. No identifiers or
reception times are generated as provider metadata. A supplied reception timestamp normalizes
through F01 and remains nullable when absent. A supplied price basis is retained as exact
`source_metadata.quote.price_basis` text; no bid/ask/last value is inferred.

Profiles are limited to 16 KiB and the existing canonical metadata budget. NUL and invalid
Unicode source text are rejected before database persistence. Prices are decimal strings
or exact JSON numbers. No binary float conversion or rounding occurs.

## Supported formats

CSV uses UTF-8, comma delimiters, one unique header, and LF or CRLF line endings. Configured
columns must exist. Additional unique columns are preserved in raw bytes but ignored for
canonical interpretation. Row width must match the header. Quoted multiline fields preserve
physical start and end lines. The maximum logical record, including its terminator, is
65,536 bytes. The same byte bound applies to the header, but the preserved header plus
profile must also fit the canonical 16 KiB metadata budget. BOM, bare CR, NUL, and malformed quoting
are rejected.

JSON means **JSON Lines** in this version: one flat UTF-8 object per physical line. LF and
CRLF are supported. A complete final object without a newline is valid. Required configured
fields must exist in each object. Additional scalar fields remain in original evidence.
Objects, arrays, blank lines, BOM, duplicate keys, nonfinite constants, NUL, invalid Unicode,
and truncated objects are rejected. Each line is limited to 65,536 bytes. Numeric price
lexemes retain their original bytes and exact decimal scale.

`timestamp: "iso8601"` requires extended timestamps with seconds, an explicit `Z` or numeric
UTC offset, and at most six fractional digits. Canonical times normalize to UTC without
rounding. Original timestamps remain in raw bytes. Naive dates, epoch numbers, inferred units,
and finer-than-microsecond timestamps are unsupported. Precision is never inferred from prices.

`format: "stream_jsonl"` supports nested recorded envelopes through explicit dot paths,
such as `tick.time` and `tick.quote`. It preserves the recorded source type and does not
establish a live connection. Optional configured IDs, reception times, and price basis can
be absent. Top-level JSON arrays remain unsupported.

## Import and replay

Install the existing storage extra, start the documented database, and set
`QUANTLAB_DATABASE_URL`. Set `QUANTLAB_ARCHIVE_DIR` to your durable archive directory.
The CLI does not automatically load dotenv files. Initialize storage once:

```bash
quantlab db init
quantlab data import data/fixtures/historical/ticks.csv \
  --config data/fixtures/historical/profile-csv.json \
  --source-id approved-export-001 --batch-size 100
quantlab data list --limit 20
quantlab data validate DATASET_UUID
quantlab data replay DATASET_UUID --limit 100
```

Use the UUID returned by import. Substitute the JSON Lines fixture and profile to import
that format. `data list` includes lifecycle statuses, including incomplete datasets.
`data replay` streams canonical JSON Lines in event-time and source-position order. It also
accepts `--start` and `--end` with inclusive start and exclusive end semantics. Existing
`ingest` and `replay` commands remain compatible.

Exit code 0 means success, 1 means inspection/import found invalid data, and 2 means a
configuration, I/O, validation-operation, or database error. Exit 3 means a paused incomplete
collection; keyboard interruption returns 130. Argument errors also return 2.
Import logs are structured JSON on stderr; results are JSON on stdout. Existing lifecycle
logs include dataset/capture identifiers, committed counts, batch status, timing, and error
classification without raw payload dumps or connection secrets.

## Recover an interrupted import

Keep the database and archive together in backups. Rerun the same import with the same
profile and source identifier after restoring database availability. Completed imports return
the same dataset and capture. Modern `data import` resumes a verified committed prefix in
the same capture. `--no-resume` creates a fresh attempt and retains the old evidence. Legacy
`ingest import` and synchronous Python import methods retain fresh-attempt interruption
semantics. Incomplete rows cannot be replayed as a complete dataset. Source checksum
mismatch aborts explicitly. Validation verifies stored evidence independently of completion
status and never repairs or promotes an acquisition.

Valid repeated observations remain separate records. Overlapping files remain independent
datasets under the retain-all reconciliation policy. No interpolation, implicit merge, or
cross-file deduplication occurs.

See [the ingestion architecture](../architecture/market-data-ingestion.md) for contracts and
acknowledgement boundaries, [collection/export commands](../operations/data-collection.md),
[recovery procedures](../operations/collection-recovery.md),
[the implementation review](../reviews/feature-04-implementation-review.md), and
[current benchmark status](../../benchmarks/ingestion-results.md). The original
[adapter reference](../architecture/historical-data-adapters.md) describes framing details.

## Limits

Only local regular files, configured CSV, flat JSON Lines, and explicitly mapped recorded
JSON envelopes are supported. Top-level JSON arrays, network acquisition, compression, live broker connectivity,
account authentication, signals, and order execution are outside this feature. No real provider
history or provider metadata authenticity was verified. A passing contract does not establish
that the source covers every market event.
