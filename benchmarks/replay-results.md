# Feature 02 measured replay costs

These are absolute local engineering measurements, not a speedup comparison or
market performance claim. The full replay, validation, headless geometry, durable
audit, and independent verification path processed **30.16 observations/second**.
That is a median **1.127 seconds per complete 34-observation demonstration**,
with a range of **0.745 to 1.225 seconds** across five interleaved rounds. Durable
file synchronization dominates this machine's result.

## Reproduce the measurement

```bash
DISPLAY=:99 .venv-check/bin/python -m benchmarks.replay --gui
```

Use an available graphical display, or `xvfb-run -a` on Linux with Xvfb installed.
Omit `--gui` to measure the other paths without native rendering. The script
writes [raw results](replay-results.json) and an independently collected
[profile](replay-profile.txt). It checks that runtime source hashes do not change
during the run. The checked-in hashes match the delivered runtime source.

## Environment and workload

The run used CPython 3.14.4, Linux 7.0.0-38 x86_64, glibc 2.43, 12 logical CPUs,
Tk 8.6, and a temporary 1440×1000 Xvfb display. The audit directory was on the
workspace's ext4 filesystem, rather than `/tmp` tmpfs. Load averages changed from
1.29/1.93/1.44 to 2.54/2.16/1.54. Other host activity was not controlled.

Each case repeats the packaged 34-observation engineering fixture. No large
synthetic market dataset is generated. Five rounds interleave replay, candles,
alerts, geometry, native drawing, and the integrated run. Per round, cases repeat
1000, 500, 500, 100, 5, and 5 fixture runs respectively. Imports, setup, and initial
fixture validation are excluded from component timing. The integrated path
includes fresh fixture acquisition/validation, session construction, exclusive
file creation, 11 append/flush/fsync operations, rendering geometry, and independent
log verification. Setup and output serialization for the benchmark are excluded.

Every case fully consumes and checks its output. Assertions verify tick order,
OHLC, ten completed candles plus one forming candle, three issued/rendered/outcome
records, and terminal outcomes `[true, false, null]`. All timed trials recorded
zero errors. Components use already validated ticks or precomputed candle/state
inputs where stated; their rates do not include the other components.

## Results

All values are medians of five rounds; ranges are the minimum and maximum round
averages, not individual-operation latency percentiles.

- Replay over prevalidated ticks: **629,438 ticks/s**, 1.589 microseconds/tick;
  range 1.545–1.636 microseconds/tick. Includes engine creation, stepping,
  end detection, and output assertions.
- M1 aggregation: **56,411 ticks/s**, 17.727 microseconds/tick;
  range 17.617–18.334 microseconds/tick. Includes candle construction and checks.
- Alert lifecycle: **3.861 microseconds/tick**, range 3.648–3.910. Includes book
  construction, processing, issuance, outcomes, and expiry over the fixture.
  This is average processing cost per input tick, not p99 issuance latency.
- Pure chart geometry: **80.05 microseconds/update**, range 77.09–89.88.
- Actual Tk Canvas drawing: **7.76 ms/update**, range 7.33–7.84. Includes
  `draw_chart`, `update_idletasks`, and candle/arrow item assertions. It excludes
  the entire window's controls and physical monitor/compositor latency.
- Complete durable headless replay: **33.16 ms/observation**, range 21.91–36.03,
  or 30.16 observations/s. It includes headless geometry, not native Tk drawing.

Separate tracemalloc runs measured 1,240 bytes for core replay, 3,462 for candles,
3,128 for alerts, 19,088 for geometry, 13,644 for native drawing's Python
allocations, and **181,792 bytes** for the integrated run. These are incremental
Python allocations after imports/setup; they exclude native Tk allocations.
The benchmark process's Linux `/proc/self/status` high-water RSS reached
**40,112 KiB, about 39.2 MiB**, including Tk, fixture/state setup, and all cases.
Its sampled RSS ranged from 33,868 to 40,112 KiB.

`resource.ru_maxrss` instead returned 329,736 KiB throughout this tool environment,
inconsistent with the same process's Linux VmHWM and sampled RSS. Both are retained
in the raw artifact. The larger value is not presented as the replay engine's
memory footprint; peak RSS remains environment dependent.

## Explain the limiting costs

Separate profiling shows 11 `posix.fsync` calls taking 1.320 of 1.351 seconds in
the integrated diagnostic. Timed integrated CPU/wall time was about 0.016, which
supports a storage-wait explanation rather than CPU saturation. Per-record fsync
is the deliberate durability configuration used by the app and benchmark. Earlier
exploratory tmpfs measurements were discarded because they did not represent the
workspace filesystem.

Native rendering's profile is dominated by `_tkinter.call`; timed CPU/wall was
about 0.83. Core replay, aggregation, lifecycle, and geometry were each near one
CPU per wall second. This provides no evidence of multicore scaling or throughput
for live feeds. The UI timer releases at most one tick per 50 ms pulse, and durable
writes or slow rendering can stretch presentation further without skipping ticks.

No comparison, speedup, SLO, production capacity, large-history memory scaling,
real market throughput, cold-disk distribution, or native MT5 performance is
claimed. The fixture is intentionally small. Bounded buffers and maximum schedule
sizes are established by implementation and tests, rather than a large-data
benchmark in this feature.
