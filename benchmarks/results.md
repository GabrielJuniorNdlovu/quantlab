# Feature 01 validation measurements

The incremental engine and CSV path processed every engineering observation with the expected accepted and rejected counts. Memory measurements are consistent with the constant-state design across a tenfold increase in input size. These results establish a local baseline, not a production service objective or a speedup over the old implementation.

## Reproduce the measurements

```bash
source .venv/bin/activate
python -m benchmarks.validate --sizes 10000 100000 --runs 5
```

The script writes `benchmarks/results.json` and a separate `benchmarks/profile.txt`. It creates and removes deterministic temporary CSV files. It does not create or download market history. The largest valid input contains 100,000 repeated observations and is 5,600,060 bytes including the header.

## Environment and method

- Python: 3.14.4 (main, Aug 20 2026, 10:41:58) [GCC 15.2.0].
- Platform: Linux-7.0.0-38-generic-x86_64-with-glibc2.43; 12 logical CPUs.
- Measurement timestamp: 2026-10-25T03:07:29.431282+00:00.
- Load average before: 1.75, 1.90, 1.46; after: 1.70, 1.90, 1.57.
- Five fresh worker processes per scenario and size, interleaved across cases. Other host activity was uncontrolled.
- Timed work includes input iteration, immutable raw envelopes, hashing, validation, result consumption, and correctness assertions. Core input generation is included in core timings. CSV file generation and Python imports are excluded.
- The CLI scenario calls the real argparse/file-I/O/summary function. Its timed interval excludes interpreter startup. Raw `process_seconds` includes benchmark-worker imports and process startup, so it is not the exact wall time of `python -m quantlab.validation`.
- Separate fresh-worker memory runs use tracemalloc after imports. Peak RSS includes interpreter and benchmark imports. Memory-run timings are deliberately excluded from throughput summaries.
- Files may be in the OS cache. No concurrency, cold-disk comparison, tuning comparison, or provider workload is claimed.

## Timing results

Rates below are medians of five untraced runs. Processed rate counts all candidates. Validated rate counts accepted ticks only. Seconds include successful consumption and result assertions.

- `core-valid-10000`: median 0.2841 s, range 0.2800 to 0.3031 s; 35,197 processed/s and 35,197 validated/s. Separate traced peak 10,293 bytes; process peak RSS 24,864 KiB.
- `core-10pct-reject-10000`: median 0.3072 s, range 0.3002 to 0.3191 s; 32,552 processed/s and 29,297 validated/s. Separate traced peak 14,080 bytes; process peak RSS 24,732 KiB.
- `csv-valid-10000`: median 0.6621 s, range 0.6577 to 0.6821 s; 15,104 processed/s and 15,104 validated/s. Separate traced peak 163,351 bytes; process peak RSS 24,896 KiB.
- `csv-10pct-reject-10000`: median 0.6690 s, range 0.6648 to 0.6907 s; 14,948 processed/s and 13,453 validated/s. Separate traced peak 170,954 bytes; process peak RSS 24,944 KiB.
- `cli-valid-10000`: median 0.6476 s, range 0.6391 to 0.6675 s; 15,443 processed/s and 15,443 validated/s. Separate traced peak 176,494 bytes; process peak RSS 25,056 KiB.
- `core-valid-100000`: median 2.8758 s, range 2.8163 to 2.9345 s; 34,773 processed/s and 34,773 validated/s. Separate traced peak 10,793 bytes; process peak RSS 24,872 KiB.
- `core-10pct-reject-100000`: median 3.0392 s, range 2.9878 to 3.2238 s; 32,903 processed/s and 29,613 validated/s. Separate traced peak 13,830 bytes; process peak RSS 24,880 KiB.
- `csv-valid-100000`: median 6.5287 s, range 6.4583 to 6.6575 s; 15,317 processed/s and 15,317 validated/s. Separate traced peak 162,008 bytes; process peak RSS 24,960 KiB.
- `csv-10pct-reject-100000`: median 6.8053 s, range 6.7262 to 6.9156 s; 14,694 processed/s and 13,225 validated/s. Separate traced peak 171,356 bytes; process peak RSS 24,936 KiB.
- `cli-valid-100000`: median 6.6341 s, range 6.5343 to 6.7083 s; 15,074 processed/s and 15,074 validated/s. Separate traced peak 174,348 bytes; process peak RSS 25,036 KiB.

## Rejection overhead and size scaling

Every rejection scenario rejects exactly every tenth observation: 1,000 of 10,000 or 10,000 of 100,000. All are expected `INVALID_PRICE` outcomes. There were no unexpected failures, skipped rows, or hidden retries.

At 100,000 observations, the core rejection workload took 5.7% longer than the all-valid workload. The ranges do not overlap. This measures the full changed workload, including constructing deterministic errors and consuming fewer ticks, rather than an isolated cost per rejected record.

The full CSV rejection workload took 4.2% longer by median at 100,000 rows.

Increasing size tenfold multiplied median core time by 10.12 and full CSV time by 9.86. The full CSV path had traced peaks of 163,351 and 162,008 bytes (and 10,293 vs. 10,793 bytes for `core-valid`). A single memory sample per case supports the O(1) constant-state design claim for these inputs; it is not a universal bound for all record shapes.

## Limiter evidence

`profile.txt` comes from a separate cProfile run of 100,000 valid CSV observations. Recursive metadata validation/freezing is the largest individual exclusive-time entry, followed by type checks and adapter work. Prices and timestamps are validated at the input and model-construction boundaries intentionally; no correctness checks were disabled for measurement.

A separate counter run in `cpu-check.json` validated 100,000 observations with 0 rejections in 6.1458 wall seconds and 6.1454 process CPU seconds. The CPU/wall ratio is 0.9999, consistent with saturation of one CPU by the sequential Python validation path. It does not demonstrate multi-core throughput. This diagnostic run is excluded from the five-run medians.

The counter run times `workload("csv", 100000, path, False)` using `time.process_time()` and `time.perf_counter()` after creating the same header and rows outside the interval. Result counts and values use the benchmark script's assertions.

## Interpretation limits

- These repeated small records exercise preservation of duplicate-looking ticks and the whole CSV validation path. Real source record sizes, metadata shapes, timestamps, storage costs, and hardware can change throughput.
- No production disk, durable archival, queueing, cross-file recovery, or network adapter is measured.
- The largest input is 100,000 rows. Adversarial record/metadata limits are tested separately by pytest, rather than represented as normal benchmark traffic.
- `results.json` records the exact runtime-module SHA-256 values. Re-run measurements after changing runtime code.
- Dependency/tool installation ran after throughput trials, during separate memory/diagnostic work. It does not change recorded source code or traced workload allocations.

Full raw trial values are in [results.json](results.json). The independent profile is [profile.txt](profile.txt), and process CPU evidence is [cpu-check.json](cpu-check.json).
