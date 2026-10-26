# Collect and export a recorded dataset

Use an authorized local recording and its explicit profile. The commands below use the repository's synthetic engineering fixtures.

## Prepare storage

Install the locked development environment or the storage/export extras, configure `QUANTLAB_DATABASE_URL` and `QUANTLAB_ARCHIVE_DIR`, then initialize migrations:

```bash
python -m pip install -r requirements-dev.lock
python -m pip install --no-build-isolation --no-deps -e '.[storage,export]'
quantlab db init
quantlab db status
```

Keep credentials in your local environment. The application does not load dotenv files automatically.

## Inspect and import

```bash
quantlab data inspect data/fixtures/historical/ticks.csv \
	--config data/fixtures/historical/profile-csv.json
quantlab data import data/fixtures/historical/ticks.csv \
	--config data/fixtures/historical/profile-csv.json \
	--source-id synthetic-import --batch-size 100 --queue-size 1000
quantlab data datasets --limit 20
```

Inspection scans the full file without a database. The fixture reports three valid observations. Import returns a manifest containing dataset/capture identity and committed counts. Repeating identical bytes, profile, and source ID selects the completed original capture. Historical input runs without event-time delays.

## Simulate delivery

Use a distinct source ID to keep the simulation's provenance separate:

```bash
quantlab data stream-simulate data/fixtures/historical/ticks.csv \
	--config data/fixtures/historical/profile-csv.json \
	--source-id synthetic-simulation --rate 250 \
	--disconnect-after 2 --redeliver-every 1 \
	--batch-size 100 --max-wait-ms 1000 --queue-size 1000
quantlab data status --limit 20
```

Rate zero runs at available throughput. A positive rate limits delivery before persistence backpressure; it is not a promised achieved rate. Replay speed never changes source timestamps, reception fields, or identifiers. The one-shot disconnect reconnects within the configured retry budget. Exact repeated deliveries are counted without another stored source position.

`--interrupt-after N` intentionally pauses before verified EOF and returns exit 3. Repeat the command without that fault flag to resume. `--drop-after N` intentionally skips one recorded observation for a failure exercise. It records a known delivery loss and cannot complete. Use a fresh capture as described in the [recovery guide](collection-recovery.md).

Use `--max-attempts` to bound both database retries and source connection attempts. `--no-recovery` disables automatic recovery and prefix continuation. `--no-resume` starts a fresh attempt while preserving the prior one. Default queue/batch/deadline values are engineering hypotheses; see the [benchmark status](../../benchmarks/ingestion-results.md) before selecting larger batches or queues.

## Inspect quality and replay

Substitute the import's dataset UUID:

```bash
quantlab data quality DATASET_UUID --gap-threshold-seconds 60
quantlab ingest status DATASET_UUID --attempts --limit 100
quantlab data validate DATASET_UUID
quantlab data replay DATASET_UUID --limit 100
quantlab data replay DATASET_UUID --show-chart --headless --limit 100
```

Quality includes failed/partial selected acquisitions. A long interval is a suspected gap only. A complete archived file does not establish complete upstream market history. Replay and export require completed datasets. The headless command checks the existing F02 chart geometry and does not generate signals.

## Export chronological partitions

Choose explicit UTC cutoffs for your study:

```bash
quantlab data export DATASET_UUID --format parquet --instrument BOOM_1000 \
	--output research-export \
	--train-end 2026-01-01T00:00:00.5Z \
	--validation-end 2026-01-01T00:00:02Z
```

Optional `--start` and `--end` select a range without changing the source dataset. The export writes train, validation, and test Parquet files plus a final manifest and checksum. Exact prices are strings; nullable reception timestamps and per-record source metadata are retained. Inspect `manifest.json` for lineage, writer options, split boundaries, and counts. Equal timestamps never cross split boundaries. No labels, shuffling, or model fitting occurs.

Reuse the same output only for the same request and intact checksums. Choose a new directory for another request or an interrupted output. The exporter never overwrites conflicting evidence.

## Interpret exits and logs

Exit 0 indicates success, 1 invalid source data, 2 configuration/I/O/database/integrity failure, 3 a recoverable incomplete collection, and 130 keyboard interruption. JSON results go to stdout; structured operational logs go to stderr. `data import` preserves its manifest output shape. `stream-simulate` returns a manifest and metrics.

Status shows committed database counters and a last process snapshot. An active status does not prove a process is still alive. Batch logs provide current queue/wait/commit metrics; historical event age is not network ingestion lag.
