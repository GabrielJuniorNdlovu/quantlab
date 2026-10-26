# Market-data source qualification contract

F00 owns source evidence and local sample inspection. F01 owns market-data validation.
F03 owns archival and database transactions. F04 owns permitted acquisition orchestration.
F00 does not acquire a feed or add another tick model.

## Existing flow and ownership

F04's `HistoricalProfile` defines a complete explicit mapping. `HistoricalAdapter` frames
original bytes and produces `TickCandidate` values containing `RawObservation` evidence.
F01's `AcquisitionValidator` validates each candidate, preserves equal-time source order,
and returns a `MarketTick` or structured quality events. F03 archives the full source and
persists outcomes with transactional receipts. `IngestionCoordinator` connects those owners.

F00 operates before that ingestion flow. An unresolved source cannot be forced into an
F04 profile: provider symbol, integer quote precision, instrument mappings, and ISO timestamp
interpretation are mandatory there. F00 therefore observes a local file independently and
uses the unchanged F04 inspection path only when a complete profile is supplied.

## Registry representation

`source_registry.py` loads packaged `data/sources.json` with `source_registry.v1`. A caller
can select a reviewed alternative with `quantlab source --registry PATH ...`. Configuration
is bounded to 128 KiB and 32 sources. Each source's immutable metadata uses the existing
16 KiB metadata budget. Unknown top-level source keys and duplicate JSON keys are rejected.
No database, credentials, environment lookup, or external request is involved.

Each definition contains source identity and publisher, interface type, candidate formats,
instrument mappings, historical and streaming claims, timestamp and price semantics,
access constraints, limitations, blockers, and documentation references. References contain
publisher, HTTPS URL, retrieval date, evidence classification, and a bounded claim summary.
`source report` emits that evidence with the registry SHA-256. It does not aggregate unreviewed
sample reports or contact the source.

The qualification states have these meanings:

- `UNASSESSED`: no completed authoritative assessment.
- `DOCUMENTED`: interface capabilities have references; genuine sample compatibility remains unverified.
- `SAMPLE_VERIFIED`: a reviewed authorized genuine sample is compatible and its identity mapping is resolved.
- `QUALIFIED`: reviewed identity, permission, sample compatibility, price semantics, and integration gates are satisfied.
- `BLOCKED`: an identified prerequisite prevents the intended integration.

The loader requires evidence references for resolved metadata, sample-review references for
verified states, and authorization/integration references with no blockers for `QUALIFIED`.
These are reviewed configuration declarations. The loader does not authenticate documents
or independently establish permission. Inspection never mutates these states.

## Instrument mappings

An `InstrumentMapping` contains internal ID, display name, external ID, canonical quote decimal
places, price basis, and evidence-reference IDs. The internal ID is validated by F01 and is
currently only `BOOM_1000`. Unresolved external identity, precision, and basis remain null.
A display name is descriptive metadata, not an importable provider identifier.

After review, a complete existing `historical.v1` profile can bind recorded fields and map
their instrument value to `BOOM_1000`. Its `provider_symbol` must identify the actual source
instrument. Its `pip_size` is integer decimal places, not a fractional tick increment.
Do not generate an executable profile from null registry values. F00 does not provide a
guessed default profile for either initial candidate.

## Local inspection interface

The Python entry point is `source_inspection.inspect_sample(path, source, format=...,
fields=..., authorization_reference=..., ...)`. Field bindings are explicit and optional
for structural observation. The existing `HistoricalProfile` is optional for canonical
compatibility. No alternative market-data value object is introduced.

The inspector reports exact file checksum and size, file name, registry/source-definition
identity, declared source reference and permission, optional known acquisition time,
inspection time, record counts, observed fields/types, bounded selected decoded representations,
price exponents, valid ISO precision/offset statistics, instrument values, and ordering information.
Numeric epochs can be declared but are not converted. Their examples remain observed decoded
values, separate from the declared unit. No acquisition time is inferred from file mtime.

Structural JSON inspection handles an object or an array of objects up to 16 MiB. CSV and
JSON Lines stream with 64 KiB record limits. A JSON object containing history arrays is
one framing record; F00 never calls it one market tick or silently expands it into ticks.
Explicit field paths such as `history.times[]` can summarize array values without pairing
or creating observations. CSV names are literal. JSON paths use dots and `[]`; keys containing
those path delimiters require a future reviewed mapping convention rather than guessed binding.

Schema summaries retain at most 128 paths and eight nesting levels. CSV rejects wider headers.
Other schema overflow is flagged as truncated. Selected decoded examples retain at most three
values of 128 characters per field; provider event identifiers are not copied into examples.
At most eight distinct instrument strings are retained. These are summaries, not a replacement
for unchanged source bytes. JSON numbers use the existing strict Decimal decoder. The displayed
decoded form does not promise the original JSON token's case or spelling.

Price parsing and aware ISO normalization use F01. No alternate positivity, precision, or
timezone rule is added. Individually valid selected ISO values supply observed bounds and
decreasing/equal-time counts in file order, even if another field in that row is invalid.
Canonical accepted bounds are separate in `canonical_compatibility`. Missing role bindings
leave their semantics unassessed.

With a profile, the existing `HistoricalAdapter.inspect` runs F01's complete acquisition
validation and reports machine-readable error codes, counts, and accepted timestamp bounds.
F00 compares its full-file hash/size with that pass. Resolved registry symbol, instrument-binding, or precision
contradictions, framing mismatches, and mapping documented non-event fields as event IDs
block compatibility. Successful canonical syntax does not authenticate caller-supplied mapping
or genuine origin. The report marks those trust limits explicitly.

## File safety and provenance

Inspection accepts regular files with `O_NOFOLLOW` and uses before/after file metadata checks.
File safety and exclusive publication follow the project's POSIX filesystem conventions;
native Windows behavior is outside this untested implementation phase.
It hashes the complete original file even if parsing later stops. Fatal framing or schema
errors mark inspection incomplete and retain the parsed prefix's counts and diagnostics.
The inspector never overwrites, cleans, archives, or normalizes the supplied file on disk.

Reports use `source_inspection.v1` JSON. The inspection timestamp is the current UTC clock;
acquisition time is a caller declaration when known; source event times come only from
explicitly mapped values. They have separate fields. Origin, sample kind, and authorization
references remain caller declarations until reviewed independently. Synthetic or unknown
inputs cannot become genuine samples through successful validation.

`--output` writes a sibling temporary file, flushes/fsyncs it, and publishes exclusively by
hard link. Existing destinations, including source files, are refused. A final directory fsync
completes publication; a failure is an operation error even if the published file already exists.
Without `--output`, JSON goes to stdout. Runtime `source-reports/` is ignored by Git.
Keep private permission evidence, recordings, account identifiers, and credentials out of commits.

## Future F04 adapter requirements

A permitted adapter must provide the following before source integration review:

1. Stable source/configuration identity and evidence-backed instrument mapping.
2. Authorized original bytes and acquisition metadata, with exact payload hashes and an explicit capture scope.
3. A framing rule for each `RawObservation`, provider label, source format, and increasing capture-scoped source position.
4. A documented conversion from provider timestamps to aware UTC with no precision truncation or guessed epoch units.
5. Exact decimal price decoding, explicit quote decimal places, and named price basis where supplied.
6. Candidate fields for F01's existing `TickCandidate`; no acceptance before canonical validation.
7. Optional reception time and genuine source event ID only when available. Subscription IDs and row numbers are not provider event IDs.
8. Structured fatal framing errors and per-record quality events through the existing F01 contracts.
9. Coverage boundaries, retrieval limits, overlap rules, and a policy for partial responses and missing intervals.
10. Reconnect, redelivery, ownership, backpressure, and explicit uncertain-coverage behavior compatible with F04.

Raw payloads remain original evidence. For a history envelope containing multiple observations,
the future adapter must define replayable framing and lineage to the original message without
claiming reconstructed JSON is the source payload. F03's full-source archive and receipts remain
the durability authority. Current F04 collection/resume requires a reproducible immutable
snapshot and stable positions. An indefinite live stream needs a separately reviewed protocol.

See [the F04 ingestion architecture](market-data-ingestion.md) and
[the source assessment](../data-sources/boom1000-source-assessment.md) for current limits.

## Design decision and verification boundary

The architect workflow grounded the existing layers, compared two read-only design candidates,
and used an independent cross-judge. Both favoured a separate observational inspector. The
simpler explicit-parameter design scored 24/25 against 21/25 for an additional sample-profile
type. The synthesis added immutable registry metadata and explicit provenance from the other
candidate, without adding a second profile or domain schema. Requested alternate model
families were unavailable; these were inherited-model reviews, not cross-family consensus.

The small CSV record framer and immutable-metadata JSON conversion were moved to their shared
syntax/metadata modules for reuse. Existing storage serialization imports retain their public
name. Historical inspection adds accepted timestamp bounds; its existing fields remain.

The latest F00 assignment replaces the charter's older F00 repository-audit label. The research
vision and the F01–F04 ownership boundaries remain. Per the explicit task policy, no automated
tests or fixtures were added or run. Static checks and read-only review do not establish runtime
acceptance. The next task must independently exercise registry validation, supported/invalid
sample shapes, canonical compatibility, stable-source checks, report publication, packaging,
and CLI behavior.
