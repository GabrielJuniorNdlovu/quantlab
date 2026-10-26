# Recorded market-data ingestion

F04 owns source interpretation, delivery, recovery coordination, quality reporting, and research exports. F01 owns canonical validation. F03 owns PostgreSQL transactions and evidence. F02 consumes completed datasets. All functioning sources here are local recordings or deterministic synthetic fixtures. External market connectivity is unimplemented.

## One coordinator

`storage.ingestion.IngestionCoordinator.collect(source, stream=..., source_id=..., resume=True)` is the shared asynchronous path. Historical collection uses unpaced delivery; `SimulatedStream` adds event pacing and deterministic faults. `import_csv` and `import_historical` retain their synchronous signatures and return `DatasetManifest` through the same core.

```python
source = LocalHistoricalSource(path, HistoricalProfile.load(profile_path))
coordinator = IngestionCoordinator(repository, archive, settings=settings)
result = await coordinator.collect(source, source_id="authorized-recording")
result = await coordinator.collect(
	source,
	stream=SimulatedStream(rate=250, disconnect_after=1000),
	source_id="separate-simulation",
)
```

The coordinator registers immutable evidence, claims an acquisition, reconstructs its validated prefix, accepts source events, sends F01 outcomes to the bounded F03 writer, drains accepted work, and requests completion. It contains no SQL. The original full source is persisted even when a later replay or export selects a time range.

`HistoricalSource` separates immutable registration, acquisition context, and candidate parsing. `MarketDataStream` separates asynchronous delivery and connection/coverage metrics from parsing. A future adapter must supply a reproducible immutable snapshot and stable source positions before using this completion/resume protocol. An indefinite live stream needs a separately reviewed snapshot/acknowledgement protocol.

## Module ownership

- `historical.py`, `historical_json.py`, and `csv_adapter.py` interpret explicit CSV or JSON Lines profiles without changing original bytes.
- `collection.py` defines source/delivery protocols, settings, immutable delivery metrics, and the deterministic simulator.
- `storage/ingestion.py` coordinates both ingestion modes. Its joined blocking operations keep archive and database work outside the event loop and retain resource ownership through cancellation. Queue submission tries once without blocking and waits on the writer condition only under backpressure. Cooperative scheduling after each handled delivery keeps immediately ready sources responsive.
- `engine.AcquisitionValidator` validates prefix and suffix in one continuous ordering state.
- `storage/batching.py` owns the only queue, backpressure, deadline flush, acknowledged counts, and monotonic waiting/lag metrics.
- `storage/repository.py` and `collection_state.py` own claims, receipt reconciliation, session diagnostics, exact redelivery verification, and bounded quality queries.
- `continuity.py` classifies suspected time gaps. `dataset_export.py` owns deterministic Parquet writing and publication. `collection_cli.py` maps CLI requests to these components.

## Identity and acknowledgement

A dataset combines exact source bytes, interpretation/configuration, and explicit source ID. A capture identifies an acquisition attempt. Source position is scoped to that capture. Timestamp/price equality is never a unique identity. Separate recordings or source IDs remain separate even if their market fields overlap.

F03 commits raw evidence, accepted ticks, diagnostics, receipt digest, counts, last source position, and last accepted event time in one transaction. Queue submission transfers local ownership only. It does not advance a durable checkpoint. `commit_status` reads committed progress on an independent connection; session metrics never replace that authority.

A lost acknowledgement uses the existing bounded F03 retry and receipt comparison. It does not blindly append another batch. The batch digest preserves Decimal spelling, provenance, raw payload hashes, and quality events. Archive publication and database commit are separate resources; unreferenced complete archive objects can remain after a database failure.

## Verified prefix recovery

Migration `0003_collection` adds process/session diagnostics after the unchanged F03 migrations. Only a selected `IMPORTING` acquisition registered as a collection can resume. Terminal acquisitions never reopen. The existing advisory dataset lock protects ownership, including reacquisition after database reconnect.

Resume opens verified archived bytes, replays their prefix through F01, and compares every original receipt boundary. It checks digest, count, first/last positions, contiguous batch numbers, accepted/rejected/error totals, and last accepted event time against independent committed progress. Only then does it append the suffix. The validator that consumed the prefix continues over the suffix. Changing batch size does not change old receipt boundaries.

Recovery uses O(prefix) parsing and bounded memory. It does not serialize validator objects or seek to guessed CSV line offsets. A malformed prefix, changed interpretation, corrupted counters, or invalid coverage metadata stops recovery. Source evidence is preserved. A fresh attempt is an explicit alternative, not an automatic repair of conflicting evidence.

## Repeated deliveries and source failures

An exact immediately repeated candidate shares the original observation's existing queue ownership. A single immutable recent-candidate reference bounds this check; no second record is enqueued and no durable acknowledgement is invented. A changed delivery at that position fails explicitly.

For an older repeated position, the coordinator drains accepted work, replays source interpretation to that position, and compares exact raw/canonical/quality evidence in F03. This also reproduces ordering-dependent rejections correctly. The common path retains one candidate. Frequent arbitrary older redelivery can require repeated prefix scans, so its CPU cost can grow quadratically; use explicit source checkpoints rather than treating that pattern as an efficient bulk retransmission API.

`disconnect_after` injects a one-shot replayable failure. Within the configured budget, the coordinator drains, reopens the archive, verifies its committed prefix, and reconnects without renumbering observations. `interrupt_after` intentionally pauses for a later invocation. Unknown coverage and declared drops prevent completion. Confirmed drop counts, bounded position samples, and uncertain coverage survive later failed resumes in the same capture. A fresh capture has separate provenance and coverage history.

Delivery instances are capture-scoped. Create a new `SimulatedStream` for another capture. One instance can reconnect within its capture; its fault and coverage counters are cumulative. `DeliveryMetrics` rejects negative/noninteger counters, mutable or oversized samples, and a claim of guaranteed coverage alongside reported drops.

## Shutdown and bounded resources

A stop event races source acquisition, so an idle source can stop without waiting for another event. The pending acquisition task is cancelled and joined before the source generator closes. A record acquired concurrently with stop is counted as unsubmitted and remains recoverable from the archive. Task cancellation also joins in-flight blocking operations and accepted writer work before releasing the claim.

Backpressure blocks the replayable source at the F03 queue boundary. An explicit `BatchWriter.submit` timeout raises `queue.Full` before accepting the record. There is no silent overflow/drop policy and no second unbounded queue. Graceful drain can take as long as the accepted bounded backlog and database timeouts require.

Defaults remain provisional: batch 100, deadline 1000 ms, queue 10,000, and three retry attempts. Queue count plus pending batch count bounds retained records. At maximum 64 KiB raw payload size, 10,000 queued raw payloads alone can occupy 625 MiB; metadata and Python objects add more. Configure smaller queues on constrained machines. The [benchmark status](../../benchmarks/ingestion-results.md) explains why no local measurements are published yet.

## Quality and observability

`data quality` reads selected acquisition facts in one repeatable-read snapshot. It reconciles stored raw/tick counts, reports valid/rejected/committed totals and event bounds, groups structured error codes, and distinguishes `PARTIAL`, `INVALID_SOURCE`, `INCONSISTENT`, and `VERIFIED_SOURCE_COMPLETE`.

Complete file evidence means every observation in the archived recording passed the completion proof. Upstream market coverage remains `UNKNOWN`. Physical CSV line gaps can be multiline records; they are not provider sequence gaps. An explicit silence threshold counts suspected gaps only. Confirmed delivery losses are simulator/source declarations, not proof of missing provider market events. No interpolation occurs.

Structured batch logs expose current queue depth, acknowledgement latency, batch wait, and monotonic local persistence lag. Session snapshots add process throughput, rejected records, recovery attempts, database retries, interruptions, and drop evidence. Historical event age is not called network latency. CLI status combines authoritative committed counters with the last process snapshot. An `ACTIVE` session has unverified liveness; no worker heartbeat is claimed. Database outages can prevent a final diagnostic snapshot while previously committed receipts remain durable.

## Exact research exports

`research_ticks.v1` exports only selected completed datasets. F03's bounded evidence factory streams canonical ticks with their original envelopes. Parquet retains exact `price_text`, microsecond UTC event time, nullable reception time, instrument, provider, provider symbol, precision, original source type, capture/position, optional event ID, payload hash, and deterministic source metadata JSON. Supplied price basis is preserved in that metadata; absent metadata stays absent.

The manifest records source/configuration hashes, full interpretation, schema/validation versions, writer version/options, explicit time range, split boundaries, logical checksum, file checksums, counts, and continuity. Fixed 1000-record row groups bound writer memory. Exact price text supports the canonical Decimal range beyond Arrow fixed decimal types.

Train, validation, and test intervals are chronological, with inclusive starts and exclusive ends. Equal timestamps stay together. Empty selections/partitions remain explicit. No random shuffle, labels, or learned transformation is added. Purging/embargo is explicitly unapplied until feature lookback and label horizons are defined.

Files are closed and fsynced before exclusive publication. `manifest.json` publishes last and denotes success. Existing exports must match the request and every checksum; incomplete/conflicting outputs are rejected. Same pinned writer version is tested for byte equality; different Arrow versions are not promised identical bytes.

## Design decision and compatibility

Two independent sketches compared a new collection owner plus old import wrappers with extending the existing coordinator. The cross-judge scored the latter 23/25 against 22/25. Keeping `IngestionCoordinator` removes a second public coordinator and preserves current imports. The synthesis adopted the alternative's exact older-position reconciliation and retained the preserved branch's prefix, archive, session, and export components. Available passes used the inherited model; there is no model-family consensus claim.

The preserved `feat/04-data-collection-pipeline` branch was inspected and its reviewed components adapted selectively. It was not merged. Current F01 normalization, F03 COPY/receipt behavior, and F02 replay/chart options were retained.

The canonical market contract is unchanged. Existing synchronous import signatures and fresh-attempt interruption behavior remain. Modern `data import`/`collect` support verified same-capture continuation. Synchronous methods called inside a running event loop now give an explicit error; async embedders must await `collect`. The unused internal `ImportRepository` protocol was removed: integrations instantiate the existing F03 `Repository` rather than supplying a partial custom sink. Run `db init` before using the expanded coordinator to apply the additive collection migration.
