# Causal replay benchmark results

Absolute local measurements, not a speedup or production capacity claim. The final
100000-tick native-render run processed about 12702 ticks/s with 100 sampled chart
updates. All five trials of every case completed with zero assertion failures.

## Reproduce the run

```bash
DISPLAY=:99 env -u PYTHONPATH .venv-check/bin/python -m benchmarks.causal_replay \
	--ticks 100000 --gui --output benchmarks/causal-replay-results.json
```

The run used Python 3.14.4, 12 logical CPUs, and Xvfb on local Linux.
Load averages before: [2.18310546875, 1.6220703125, 1.302734375]; after: [1.61865234375, 1.62890625, 1.36474609375].
No database or test suite ran concurrently with these trials. Other host load was
not controlled. Cases alternate across five rounds; source construction is included.

## Measured work

- Engine replay: median **63851 ticks/s**, range 62271 to 65961; median 1.566 s, CPU/wall 1.00.
- Candle aggregation: median **34370 ticks/s**, range 33248 to 35017; median 2.910 s, CPU/wall 1.00.
- Session without signals: median **16284 ticks/s**, range 15700 to 16527; median 6.141 s, CPU/wall 1.00.
- Session with 100 synthetic signals and durable issuance: median **15044 ticks/s**, range 14715 to 15524; median 6.647 s, CPU/wall 1.00.
- Audit append: median **11661 records/s**, range 11309 to 11927; median 0.086 s, CPU/wall 1.00.
- Session, signals, audit, and sampled chart geometry: median **14704 ticks/s**, range 14319 to 14783; median 6.801 s, CPU/wall 1.00.
- Session, signals, audit, and sampled native Tk drawing: median **12702 ticks/s**, range 12528 to 12821; median 7.872 s, CPU/wall 0.98.

Each replay case consumes all 100000 generated canonical ticks. The source has a
500 ms interval, one fixed synthetic acquisition, exact Decimal prices, and strict
source order. Its identifier hashes the frozen generator configuration, not a
claim about archived provider bytes. The harness records configuration identity
and source module hashes and rejects source changes during measurement.

The session-only case isolates replay and candle coordination. The signal case
adds one fixture signal per 1000 observations, 100 issuance appends, and verifier
checks. These absolute side costs describe this sparse schedule, not dense-signal
capacity or an isolated function speedup. No tuning or implementation winner is
being selected from these measurements.

The audit case appends 1000 diagnostic records with flush and fsync per record,
plus lifecycle records, and verifies the complete file inside the timed region.
It measures the shipped per-record durability policy on this local filesystem.
Temporary files are removed after verification. This is not physical-disk or
power-loss durability evidence.

Geometry and native cases render every 1000 observations, exactly 100 frames.
Underlying replay never skips ticks. Native drawing checks actual canvas candle
objects and flushes Tk idle work. The viewport never exceeds 120 candles.
These rates include source and domain work and do not mean 12702 full frames/s.
They measure direct session/canvas driving, not the interactive controller's
50 ms timer. Interactive playback currently publishes one observation per timer
pulse, at most about 20 ticks/s before drawing cost. Large fast-forward workloads
use direct session driving with bounded sampled rendering, as measured here.

## Memory and limiting work

- Separate traced Python peak at 10,000 ticks: **203,805 bytes**.
- Separate traced Python peak at 100,000 ticks: **438,958 bytes**.
- Process lifetime RSS high-water mark, including native Tk: **40,196 KiB**.

Traced memory runs are separate and exclude native Tk allocations. RSS is a
process lifetime high-water mark, not an isolated per-case delta. More ticks add
signal identities and verifier state until the explicit 1000-signal limit; ticks
are never retained as a full dataset. A fixed 120-candle viewport bounds rendering.

The separate cProfile run identifies dataclass replacement, canonical price
validation, and session/candle processing as the main CPU work. Generated tick
construction is included and is a substantial part of the cost. Replay cases
are mostly one-core Python work; adding CPU cores cannot double this serial path.
Audit appends also wait for per-record filesystem durability. Profiling and
allocation tracing are excluded from reported throughput trials.

Output assertions check full tick count, source position, candle completion,
partial EOF policy, signal count, audit verification, and viewport bounds.
The harness raises on failure instead of counting rejected work as throughput.
Raw values and profile details are in [the JSON trials](causal-replay-results.json)
and [the independent profile](causal-replay-results.profile.txt).

Limits: synthetic constant-price source; no database fetch timing; sparse signals;
sampled display updates; local filesystem and Xvfb; no production SLO or
cross-machine comparison. No predictive performance is measured.
