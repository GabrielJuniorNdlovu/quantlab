# Recover recorded-source collection

Preserve the original file, profile, source identifier, raw archive, and database. Use this procedure after an interrupted modern `data import` or `stream-simulate` command.

## Resume a verified prefix

1. Restore database availability and archive access.
2. Inspect `quantlab data status` and `quantlab ingest status DATASET_UUID --attempts`.
3. Repeat the original source/profile/source-ID command without intentional interruption flags.
4. Inspect `quantlab data quality DATASET_UUID`.
5. Validate the completed source with `quantlab data validate DATASET_UUID` before research use.

Only acknowledged F03 transactions define progress. The coordinator opens verified archive bytes, revalidates the entire committed prefix through F01, checks all receipt/counter/time state, then continues the same capture. Batch size and delivery rate may change. A queued record is not a checkpoint.

An already completed dataset returns its original capture. A killed process can leave `ACTIVE` session metadata; status labels its liveness unverified. A graceful stop drains accepted records and leaves a paused `IMPORTING` capture. An idle source wakes on the stop event. Shutdown waits for in-flight blocking operations and the accepted backlog before releasing ownership.

## Handle a conflicting or corrupted checkpoint

Stop using that attempt when recovery reports a digest, count, position, timestamp, or coverage-metadata conflict. Do not edit counters, receipts, source bytes, or archive objects to make the checks pass. Retain the attempt for diagnosis.

After confirming that the unchanged source and profile are correct, start a separate attempt:

```bash
quantlab data import SOURCE_FILE --config PROFILE_JSON \
	--source-id ORIGINAL_SOURCE_ID --no-resume
```

The new capture preserves distinct provenance. The prior importing attempt becomes interrupted and its evidence remains. An invalid source still fails under a fresh attempt; correcting bytes or interpretation creates a different dataset identity.

Invalid persisted coverage metadata is refused and preserved. Its session may remain active with unverified liveness because recovery does not overwrite the damaged diagnostic document. A fresh capture is the supported recovery path.

## Handle declared or uncertain source loss

The simulator's `--drop-after` is a deliberate delivery-loss test. Its count and bounded source-position samples remain visible even if a later resume fails. Appending after a skipped record may mean the stored data is no longer a valid source prefix. Resume then fails rather than insert an earlier record out of order. Start a fresh capture without the fault, and keep the old evidence.

Unknown source coverage remains uncertain; later EOF does not erase that fact. A fresh capture starts separate coverage history. Create a fresh delivery instance for a different capture. A simulator instance's fault/counter history belongs to its original capture.

Confirmed delivery-loss counts describe declared lost deliveries, not inferred missing provider ticks. Timestamp silence creates suspected gaps only. These adapters have no verified provider sequence contract, and market coverage remains unknown.

## Handle uncertain database acknowledgement

A connection failure near COMMIT does not prove that the batch was lost. F03 reconnects, reacquires ownership, and compares the existing batch receipt and digest. Matching committed work is acknowledged once; conflicting contents stop the operation. Exhausted retries propagate failure.

If a status update also fails during an outage, use database receipts after recovery. Last process metrics can be stale. Do not call partial data complete because a process once printed received/queued counts.

## Preserve archive and export evidence

Archive copying and publication retain F03's bounded checksum/fsync/no-overwrite behavior. Interrupted `.partial-` files are not registered completed sources. A complete orphan archive can remain after database registration fails. Preserve these artifacts for inspection; there is no automatic cleanup policy.

Back up both the database and raw archive. The existing isolated failure suite verifies process kill, backend termination, container restart, and Timescale-aware logical restore with archive restoration. Do not run destructive tests against valuable data.

A Parquet output is successful only when its final `manifest.json` and all checksums agree. A partial output directory is rejected on reuse. Choose another output path rather than replacing files in place.

See [the ingestion architecture](../architecture/market-data-ingestion.md) for transaction and ownership details, and [the implementation review](../reviews/feature-04-implementation-review.md) for executed evidence and limits.
