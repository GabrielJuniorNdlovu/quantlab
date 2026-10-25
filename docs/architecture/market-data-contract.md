# Market-data contract reference

Feature 01 defines immutable source evidence, canonical market ticks, and
structured validation outcomes for Boom 1000 Index. The runtime is local and
read-only. Transport decoding, domain checks, and file I/O have separate owners.

## Module ownership

- `models.py` defines immutable contracts, source enums (`SourceType.expected_format`), canonical market-field equivalence helpers (`MarketTick.market_fields()`, `MarketTick.equivalent_market_fields()`), and constructor invariants.
- `parsing.py` contains the single authoritative rules for nonblank UTF-8 text without NUL (`required_text`), internal instrument identity (`require_instrument_id`), exact Decimal prices (`parse_price`, `require_decimal_price`), UTC timestamps (`parse_timestamp`), UUIDs, positions, and quote precision (`parse_pip_size`, `require_price_precision`).
- `metadata.py` validates and freezes structured provenance.
- `csv_adapter.py` parses bounded binary CSV into `TickCandidate` values with exact evidence.
- `csv_syntax.py` enforces quote placement shared by both CSV adapters.
- `engine.py` defines `normalize_mapped_record`, `validate_tick`, `AcquisitionValidator`, and `validate_acquisition`, validating candidates independently of files, CSV, networks, or databases.
- `quality.py` defines structured diagnostics (`ErrorCode`, including `INVALID_INSTRUMENT` aliased to `UNSUPPORTED_INSTRUMENT`, and `redact_observed_value`) and fatal transport exceptions.
- `legacy.py` retains the four-column adapter and its limited-fidelity model.
- `cli.py` opens files and calculates streaming summaries.
- `validation.py` preserves the module command and original compatibility imports.

## AcquisitionContext

`AcquisitionContext` contains these required fields:

- `capture_id: UUID` identifies one acquisition and must be an actual UUID object.
- `provider: str` identifies the source with a nonblank label.
- `source_type: SourceType` identifies the acquisition mechanism.
- `provider_symbol: str` supplies the caller-reviewed mapping for Boom 1000 Index.
- `pip_size: int` supplies the configured number of quote decimal places, from 0 through 18.

Its optional fields are `received_at_utc: datetime | None`, defaulting to `None`,
and `source_metadata: Mapping[str, MetadataValue]`, defaulting to an empty mapping.
A supplied reception timestamp requires a timezone and normalizes to UTC.

The supported source types are `historical_csv`, `historical_json`, and
`websocket`. Only a CSV transport adapter exists in this feature. JSON and
WebSocket source types allow native candidates to use the shared domain rules;
they do not imply network adapters or connections.

Configuration values are explicit assertions by the caller. The engine compares
observations against that configuration but does not certify the mapping's
real-world accuracy. `BOOM_1000` is the internal identifier. The legacy display
name and fixture provider symbol are not provider API mappings.

## RawObservation

`RawObservation.schema_version` is `raw_observation.v1`. Required fields are:

- `capture_id: UUID` identifies the acquisition.
- `source_position: int` identifies a positive, traceable acquisition coordinate.
- `provider: str` identifies the provider.
- `source_format: SourceFormat` is `csv` or `json`.
- `raw_payload: bytes` retains the acquired observation representation exactly.
- `payload_sha256: str` is the lowercase hexadecimal SHA-256 digest of those bytes.

`received_at_utc: datetime | None` is optional and defaults to `None`.
`source_metadata` is an optional structured mapping with an empty default.
The constructor checks the checksum, identity, format, position, optional
reception time, payload size, and metadata. A Boolean is not a source position.

Payload bytes are immutable. Metadata mappings become immutable mappings, and
nested lists become tuples. Caller mutation of the original metadata does not
change the observation. Supported metadata values are strings, bounded integers,
finite floats, Booleans, nulls, mappings with string keys, lists, and tuples.
Arbitrary Python objects are rejected.

The CSV adapter uses separate metadata namespaces:

- `provider` contains the acquisition's caller-supplied source metadata.
- `adapter` contains internally generated `header`, `header_sha256`, `byte_offset`,
  `start_line`, and `end_line` values.

`header` preserves the original UTF-8 header text, including its line ending.
Encoding it as UTF-8 reconstructs its exact bytes. Its digest covers those bytes.
Each data row retains its own exact bytes, including original quoting, Unicode,
embedded newlines, and record terminators. The adapter never recreates raw
payloads from parsed dictionaries.

`source_position` is the starting physical line in this CSV adapter, counting
the header. A multiline record can therefore leave gaps between positions.
`byte_offset` is the original row's byte offset within the provided stream,
measured from the adapter's starting position. Consumers provide a stream at the
start of its CSV content. These coordinates are not provider tick identifiers.

The adapter holds evidence in memory. It neither stores an immutable source
file nor writes a durable raw archive. A later storage feature must persist the
raw rows and header, or preserve the original file and verify its identity.
A matching checksum proves byte consistency, not provider authenticity.

## TickCandidate

`TickCandidate` contains an immutable `RawObservation` and these untrusted fields:

- `instrument_id: str | None`.
- `provider_symbol: str | None`.
- `event_time: str | datetime | None`.
- `price: str | Decimal | None`.
- `pip_size: str | int | None`.
- `provider_tick_id: str | None`, optional with a `None` default.

The CSV adapter checks transport structure and leaves value validation to the
engine. Another adapter can construct candidates with native datetimes and
Decimals without duplicating domain checks.

## MarketTick

`MarketTick.schema_version` is `market_tick.v1`. Required fields are:

- `instrument_id: str`, exactly `BOOM_1000`.
- `provider_symbol: str`, nonblank and equal to the acquisition configuration.
- `event_time_utc: datetime`, timezone-aware and normalized to UTC.
- `price: Decimal`, finite and strictly positive.
- `pip_size: int`, quote decimal places matching the acquisition configuration.
- `source_type: SourceType`.
- `capture_id: UUID`.
- `source_position: int`, positive and traceable within the acquisition.

`received_at_utc: datetime | None` and `provider_tick_id: str | None` are optional
and default to `None`. A supplied provider tick ID must be nonblank. The engine
uses the raw observation's supplied reception timestamp first, then the
acquisition timestamp if present. It does not generate a historical reception
time. A reception time preceding event time yields `RECEPTION_BEFORE_EVENT` as a
warning while retaining the valid tick.

Constructors enforce tick-local invariants. The engine checks acquisition
configuration and ordering. Database row IDs and raw-record foreign keys are
absent from this domain contract.

### Price semantics

Price conversion uses `Decimal` directly, without binary floats, quantization,
or rounding. Scientific notation is accepted. The original coefficient and
exponent remain unchanged, including trailing zeros. Decimal context precision
does not determine validation precision.

`pip_size` means quote decimal places in this contract. The represented price
scale must not exceed it. At precision 2, `1000.10` is accepted and `1000.100` is
rejected, even though the values compare equal. `1e-3` requires at least three
quote decimal places. A candidate's precision must match the explicit
configuration. Local text formatting never becomes provider-confirmed precision.

### Timestamp semantics

String timestamps use the ISO 8601 extended representation
`YYYY-MM-DDTHH:MM:SS`, optionally followed by one to six fractional digits, and
an explicit `Z` or `±HH:MM` offset. Native datetimes require timezone awareness.
Values normalize to UTC before comparison. Invalid calendar dates, invalid
offsets, naive values, range overflows, leap seconds, and more than six fractional
digits are rejected.

UTC datetime formatting may display zero fractional digits that were implicit
in the source, but it does not claim additional observed resolution. The raw
payload retains the exact timestamp lexeme. No submicrosecond values are
truncated or rounded.

## DataQualityEvent and validation outcomes

`DataQualityEvent` contains required `event_id: UUID`, `capture_id: UUID`,
`source_position: int | None`, `error_code: ErrorCode`, `severity: Severity`, and
`description: str` fields. Acquisition-level errors can have no source position.
The optional fields are `field_name: str | None`, `observed_value: str | None`,
and `detected_at_utc: datetime | None`.

`quality_event` derives a UUID deterministically from the capture UUID, source
position, error code, field name, and description. It does not sample a clock or
create a random UUID. Detection timestamps are absent unless the caller supplies
one. Identical immutable observations and configuration produce equal validated
results and diagnostics. A different caller-supplied detection time is different
input configuration.

Severity has three values:

- `warning` accompanies an accepted tick with a nonfatal anomaly.
- `error` rejects a candidate and produces no tick.
- `fatal` aborts transport or acquisition processing.

Machine-readable codes include `INVALID_PRICE`, `INVALID_TIMESTAMP`,
`MISSING_REQUIRED_FIELD`, `UNSUPPORTED_INSTRUMENT` (aliased as `INVALID_INSTRUMENT`), `OUT_OF_ORDER_EVENT`,
`INVALID_PRECISION`, `INVALID_SOURCE_POSITION`, `INVALID_SOURCE_METADATA`,
`INVALID_CHECKSUM`, `INVALID_JSON`, `INVALID_JSON_SCHEMA`, `INVALID_CSV`, `INVALID_CSV_SCHEMA`, `EMPTY_DATASET`,
`INPUT_TOO_LARGE`, and `RECEPTION_BEFORE_EVENT`.

Diagnostics do not echo raw payload values. Descriptions have at most 512
characters, field names at most 64, and optional observed values at most 128.
`redact_observed_value(value, *, limit=64)` sanitizes control characters and UTF-16 surrogates and bounds length when callers attach an observed value.
The default engine rejection path leaves `observed_value` absent (`None`).

`ValidationOutcome` contains `raw: RawObservation`, `tick: MarketTick | None`,
and `events: tuple[DataQualityEvent, ...]`. Its `accepted` property is true only
when a tick exists. Rejected outcomes require errors. Accepted outcomes can
contain warnings but cannot contain errors.

`ContractError` identifies invalid direct model construction with `.code`,
`.field_name`, and `.description`. `QualityError` carries a fatal or strict
validation failure in `.event`. Domain validation returns rejected outcomes;
transport failures raise because malformed records cannot always be safely
resynchronized.

## Validation workflow and ordering

`validate_tick(candidate, context, *, detected_at_utc=None)` validates one
candidate independently of ordering. `AcquisitionValidator(context, *, detected_at_utc=None)`
maintains per-acquisition ordering state (`validate(candidate)` and `finish()`) for both
streaming/incremental consumers and batch `validate_acquisition(candidates, context,
*, detected_at_utc=None)`. `normalize_mapped_record(...)` normalizes adapter-mapped
records from CSV, historical JSON Lines, and recorded streaming JSON Lines into
`TickCandidate` instances before validation. Neither function accepts paths, network clients, or database sessions.

Each acquisition call has its own state. Positions must strictly increase;
duplicates and decreasing positions produce `INVALID_SOURCE_POSITION`.
A rejected record with the matching capture identity still consumes its
position. Accepted event times must be non-decreasing. A rejected record does
not advance accepted event-time state. Equal timestamps retain source-position
order, and repeated prices remain separate observations.

Raw capture identity, provider, and format must agree with the acquisition.
Independent files or sessions use independent calls, even when their event
times overlap or go backward relative to another acquisition.

For batches belonging to one acquisition, feed all batches through one iterator
and one `validate_acquisition` call. Starting a new call resets ordering state.
Capture identity and cross-call import deduplication belong to the caller and
the future acquisition manifest, not an unbounded global validator registry.

The validator yields one outcome per candidate and never silently deletes a
record. Empty acquisitions raise a structured error. The CLI stops at the first
rejection and prints no successful summary. Iterator consumers can inspect all
domain rejections, but finalization requires successful exhaustion and an
explicit policy for every rejected record. The validator does not implement
transactional finalization.

The engine uses O(1) ordering state. The adapter retains one bounded record,
header, and bounded metadata. The CLI summarizes incrementally. A consumer that
retains all outcomes incurs its own O(n) memory cost. The legacy `load_ticks`
function intentionally retains O(n) list behavior.

## CSV transport and resource limits

The canonical CSV requires `instrument_id`, `provider_symbol`, `event_time_utc`,
`price`, and `pip_size` headers. `provider_tick_id` is optional. Header order is
arbitrary. Unexpected columns, duplicate headers, missing headers, inconsistent
row widths, blank records, malformed quoting, and invalid UTF-8 are fatal errors.
The supported physical line endings are LF and CRLF. Standalone CR is rejected
instead of normalized. Quoted multiline records remain supported.

Resource bounds are explicit:

- A canonical logical CSV record and raw payload have at most 65,536 bytes.
  `max_record_bytes` can lower the adapter bound but cannot exceed it.
- A legacy logical CSV record has at most 65,536 text characters.
- Price text has at most 256 characters. Decimal coefficients have at most 256
  digits, and the absolute Decimal exponent is at most 1,000.
- Quote precision ranges from 0 through 18 decimal places.
- Required provider and identity labels have at most 256 characters.
- Metadata has an accounting budget of 16,384 units and depth at most 8.
  UTF-8 text charges its byte length, with additional structural and scalar
  charges. This budget is a validation bound, not an exact JSON-file byte limit.
  Metadata integers have at most 64 bits, and floats must be finite.

The raw metadata budget includes both provider metadata and adapter coordinates.
Acquisition configuration must leave space for that combined envelope.

The limits protect this initial contract from oversized input. Changing a limit
requires corresponding tests and compatibility documentation.

## Legacy compatibility

`quantlab.legacy.iter_legacy_csv_ticks` accepts the original four columns
`symbol,timestamp_utc,price,source`. It returns `LegacyTick` with the original
`symbol`, `timestamp_utc`, `price`, `source`, and `source_row` fields. `Tick`
remains its compatibility alias.

The original imports in `quantlab.validation` remain available: `parse_tick`,
`validate_records`, `iter_csv_ticks`, `load_ticks`, `ValidationError`, and
`REQUIRED_COLUMNS`. They use shared price and timestamp rules. Legacy price
formatting cannot establish provider precision, so this path does not enforce
provider-confirmed `pip_size` or produce canonical ticks.

The default CLI stays in legacy mode and explicitly labels successful output
as limited fidelity. Canonical mode requires `--format canonical`,
`--capture-id`, `--provider`, `--provider-symbol`, and `--pip-size`.
Intentional compatibility changes are the successful report's contract label
and explicit resource bounds. Legacy malformed rows still fail with physical
line numbers. The input file is never modified.

## Version and compatibility rules

`raw_observation.v1` and `market_tick.v1` identify the initial contracts.
Schema versions are class-level constants. A caller cannot relabel an instance
through a constructor field. Versions apply to field meanings and validation
semantics, not only field names.

Changes to required fields, accepted precision, ordering semantics, or identity
interpretation require a new contract version and an explicit adapter.
Optional additive fields require a documented compatibility review. Consumers
must reject unsupported versions rather than silently reinterpret them.
The package version is separate from the data-contract version.

There is no database schema or migration framework. A durable canonical
serialization format is also outside this feature. Current determinism means
equal typed results, unchanged Decimal representation, normalized UTC values,
and stable quality-event IDs for identical inputs and configuration. A storage
serializer must preserve these properties and define its own exact bytes.

## Downstream responsibilities

Feature 02 can use canonical validated observations in offline arrow replay,
with replay order determined by acquisition position for equal event times.
Only successfully finalized validation results can establish a complete replay
input. Rejected observations require a visible policy; they cannot disappear
silently from a claimed complete dataset.

Future durable storage must archive raw evidence, record reviewed provider
configuration, handle partial validation safely, and define cross-file dataset
identity. Source authenticity, cadence, gaps, market plausibility, forecasting,
and chart rendering are outside this contract engine.

Commands for installation, validation, tests, static analysis, and benchmarks
are in the [README](../../README.md). Measured performance is recorded in
[benchmark results](../../benchmarks/results.md).
