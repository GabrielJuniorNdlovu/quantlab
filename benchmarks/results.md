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
- Measurement timestamp: 2026-10-24T21:06:51.716399+00:00.
- Load average before: 1.55, 1.57, 1.56; after: 2.50, 1.87, 1.67.
- Five fresh worker processes per scenario and size, interleaved across cases. Other host activity was uncontrolled.
- Timed work includes input iteration, immutable raw envelopes, hashing, validation, result consumption, and correctness assertions. Core input generation is included in core timings. CSV file generation and Python imports are excluded.
- The CLI scenario calls the real argparse/file-I/O/summary function. Its timed interval excludes interpreter startup. Raw `process_seconds` includes benchmark-worker imports and process startup, so it is not the exact wall time of `python -m quantlab.validation`.
- Separate fresh-worker memory runs use tracemalloc after imports. Peak RSS includes interpreter and benchmark imports. Memory-run timings are deliberately excluded from throughput summaries.
- Files may be in the OS cache. No concurrency, cold-disk comparison, tuning comparison, or provider workload is claimed.

## Timing results

Rates below are medians of five untraced runs. Processed rate counts all candidates. Validated rate counts accepted ticks only. Seconds include successful consumption and result assertions.

- `core-valid-10000`: median 0.2605 s, range 0.2604 to 0.2639 s; 38,392 processed/s and 38,392 validated/s. Separate traced peak 14,845 bytes; process peak RSS 24,724 KiB.
- `core-10pct-reject-10000`: median 0.2770 s, range 0.2737 to 0.2875 s; 36,097 processed/s and 32,487 validated/s. Separate traced peak 14,290 bytes; process peak RSS 24,696 KiB.
- `csv-valid-10000`: median 0.6152 s, range 0.5997 to 0.6275 s; 16,254 processed/s and 16,254 validated/s. Separate traced peak 163,796 bytes; process peak RSS 24,744 KiB.
- `csv-10pct-reject-10000`: median 0.6229 s, range 0.6135 to 0.6440 s; 16,054 processed/s and 14,448 validated/s. Separate traced peak 170,094 bytes; process peak RSS 24,832 KiB.
- `cli-valid-10000`: median 0.6195 s, range 0.6078 to 0.6641 s; 16,141 processed/s and 16,141 validated/s. Separate traced peak 174,734 bytes; process peak RSS 25,064 KiB.
- `core-valid-100000`: median 2.5847 s, range 2.5277 to 2.6679 s; 38,689 processed/s and 38,689 validated/s. Separate traced peak 11,395 bytes; process peak RSS 24,568 KiB.
- `core-10pct-reject-100000`: median 2.8254 s, range 2.8069 to 2.8898 s; 35,393 processed/s and 31,854 validated/s. Separate traced peak 15,140 bytes; process peak RSS 24,664 KiB.
- `csv-valid-100000`: median 6.2539 s, range 6.0961 to 6.2972 s; 15,990 processed/s and 15,990 validated/s. Separate traced peak 168,098 bytes; process peak RSS 24,748 KiB.
- `csv-10pct-reject-100000`: median 6.2722 s, range 6.2320 to 6.5619 s; 15,943 processed/s and 14,349 validated/s. Separate traced peak 165,646 bytes; process peak RSS 24,820 KiB.
- `cli-valid-100000`: median 6.2035 s, range 6.0556 to 6.3859 s; 16,120 processed/s and 16,120 validated/s. Separate traced peak 172,038 bytes; process peak RSS 24,848 KiB.

## Rejection overhead and size scaling

Every rejection scenario rejects exactly every tenth observation: 1,000 of 10,000 or 10,000 of 100,000. All are expected `INVALID_PRICE` outcomes. There were no unexpected failures, skipped rows, or hidden retries.

At 100,000 observations, the core rejection workload took 9.3% longer than the all-valid workload. The ranges do not overlap. This measures the full changed workload, including constructing deterministic errors and consuming fewer ticks, rather than an isolated cost per rejected record.

The full CSV rejection workload took 0.29% longer by median, but its range overlaps the valid range. There is no measurable CSV rejection penalty distinguishable from run variation in these trials.

Increasing size tenfold multiplied median core time by 9.92 and full CSV time by 10.17. The full CSV path had traced peaks of 163,796 and 168,098 bytes. A single memory sample per case supports the design claim for these inputs; it is not a universal bound for all record shapes.

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
