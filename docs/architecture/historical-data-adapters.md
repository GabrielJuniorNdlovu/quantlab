# Historical data adapters

Feature 04 adds source interpretation to the pipeline established by Features 01–03.
A `HistoricalAdapter` takes one immutable `HistoricalProfile`. It exposes acquisition
metadata, incremental candidates, and a full-file inspection operation. The coordinator
accepts a local path and this adapter. No adapter writes SQL or creates another tick model.

## Ownership and data flow

`HistoricalProfile.load(path)` supports `"csv"` (`SourceType.HISTORICAL_CSV`), `"jsonl"` (`SourceType.HISTORICAL_JSON`), and `"stream_jsonl"` (`SourceType.WEBSOCKET`, supporting nested envelope field paths such as `"tick.symbol"` and optional per-message `"received_at_utc"`). It rejects duplicate JSON keys, unknown profile keys,
unsupported versions, ambiguous bindings, unsupported instruments, and absent precision by delegating shared text, instrument, and precision checks to `quantlab.parsing`.
`HistoricalAdapter.candidates(stream, context)` reads the verified archive stream and
delegates candidate normalization to Feature 01's `normalize_mapped_record`, producing `RawObservation` and `TickCandidate` objects. Feature 01's
`AcquisitionValidator` / `validate_acquisition` owns price, precision, timestamp, identity, and ordering validation.
The shared `IngestionCoordinator` orchestrates preservation, validation, batching,
failure status, recovery, and completion. Feature 03 owns its database claims and transactions. Feature 02 receives the existing resettable
`Repository.tick_factory(dataset_id)` directly.

The original canonical CSV reader delegates to the same bounded CSV framing function as
the mapped reader. Its public signature and dataset fingerprint remain compatible.
`import_csv` and `import_historical` share one private ingestion lifecycle. There is no
intermediate conversion file, new canonical schema, or parser dependency. The collection
extension adds session migration `0003_collection`; see the
[ingestion architecture](market-data-ingestion.md).

## Source evidence and completion

The archive stores original bytes under their SHA-256, using Feature 03's verified,
exclusive publication. Source files stay unchanged. Each raw observation retains its own
checksum, exact bytes, source position, physical lines, and byte offset. CSV observations
also retain the exact header. Provider event IDs remain optional.

Only a `historical_adapter` object with a `version` starting with `historical.` is an
adapter contract. Other legacy provider notes remain uninterpreted. Unknown contract versions
fail completion rather than falling back to canonical CSV.

The complete versioned profile is stored in acquisition metadata and therefore included
in Feature 03's configuration fingerprint. Filename and capture UUID do not determine
dataset identity. Identical bytes, profile, and source identifier reuse a completed dataset;
a changed binding or precision produces a different identity.

Completion streams persisted evidence in source order. CSV hashes the preserved header
followed by raw rows. JSON Lines hashes complete original lines including terminators.
Both must match the archived source byte count and SHA-256. Configured imports additionally
require contiguous offsets. Unknown JSON framing fails closed. Legacy canonical CSV
retains its existing header-and-payload completion proof. A valid prefix cannot complete.

Rejected observations persist as Feature 01 outcomes. Malformed framing aborts with a
structured fatal event; the archive still retains the offending bytes. Neither path can
publish a complete dataset. Database interruption uses existing transactions, receipts,
retry bounds, and claim locks. Legacy synchronous imports restart into a fresh capture;
modern collection can verify a committed prefix and continue the same capture. Both retain
older evidence and expose only a completed selected capture for replay.

## Independent validation

`Repository.validate_dataset` uses a separate read-only repeatable-read database snapshot.
It verifies the configuration fingerprint, whole-source coverage, and archive checksum,
then reparses the archive with its stored profile. Ordered server cursors compare raw
payloads, checksums, metadata, canonical ticks, complete quality events, and acquisition
counters. It does not load the dataset into memory or issue a query per tick. It rejects
failed or incomplete acquisitions and never changes their state.

Inspection scans the entire source, reports SHA-256 and counts, and caps diagnostic samples.
It needs no database driver or connection. Validation and inspection time scale with source
size; their retained state does not scale with record count.

## Design decision

Two independent design passes compared separate format adapters with a single configured
facade and shared mapper. The synthesis concentrates source interpretation in one facade,
reuses the existing CSV grammar, and keeps storage orchestration in Feature 03. Both design
passes used the available GPT runtime; they do not establish agreement across model families.

A general JSON array or provider-envelope parser would need bounded token handling plus
original bracket and separator evidence. Version 1 instead documents JSON Lines explicitly.
This keeps whole-source proof auditable without whole-file loading. It also rejects epoch
unit inference and arbitrary date patterns. Additional formats require an explicit versioned
contract and fixtures.

Each file remains a separate dataset. Overlapping files are retained independently and
never merged or deduplicated implicitly. Replay selects one dataset. This retain-all policy
prevents timestamp-and-price deduplication from deleting valid repeated observations.
