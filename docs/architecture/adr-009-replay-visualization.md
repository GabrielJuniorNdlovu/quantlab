# ADR 009: Explicit replay and offline Tk visualization

Status: accepted for the Feature 02 implementation.

## Problem

Feature 01 produces exact, ordered market observations but has no user-visible
replay. Feature 02 must demonstrate an arrow issued before its future evaluation
window, preserve unsuccessful alerts, and retain independently inspectable evidence.
An entire preloaded chart would make accidental future exposure too easy.

## Decision and usage

Use one `ReplaySession` to coordinate `ReplayEngine`, `CandleBuilder`, `AlertBook`,
and `AuditLog`. Publish immutable `ChartState` snapshots to a Tk Canvas renderer.
The public source interface is a fresh `Iterator[MarketTick]` factory. Keep the
existing Feature 01 contract and standard-library runtime dependencies.

The executable entry point is:

```bash
.venv/bin/python -m quantlab.replay_app --fixture demo
```

The session's caller can step, play, pause, advance an explicit elapsed clock,
render, reset, and close. The [architecture reference](replay-engine.md) defines
the exact contracts. The [demonstration guide](../features/feature-02.md) describes
visual and headless use.

Tk supplies controls and a local drawing canvas without a browser service or
frontend build. Candlesticks use wick lines and body rectangles; fixture arrows
use separate fixed anchors. Pure `chart_geometry` supplies deterministic renderer
checks without a display. Actual native Tk drawing was also exercised through a
virtual display. Geometry checks alone are never labeled native rendering.

## Design synthesis

Two structurally different sketches were compared before implementation. Candidate
A placed mutable domain ownership in a session with dedicated candle and alert
components. Candidate B used a pull generator, event deltas, and a display reducer.
Both excluded future observations and preserved immutable anchors.

The rubric covered causality, API simplicity and depth, bounded streaming, audit
failure isolation, and offline UI testability. A scored `23/25`; B scored `22/25`.
A was selected because it avoided coordinating generator, reducer, and session
state for this focused application. B contributed pure chart geometry and strict
independent audit verification. The available runner used the inherited model;
these were separate design passes, not agreement between independent model families.

The implementation keeps one private lookahead and no tick-history buffer. It
retains at most 120 chart candles and a bounded fixture schedule. A session pulse
releases one tick so an issuance can be rendered before the next price appears.
The lower-level engine can release bounded batches for nonvisual consumers.

## Alternatives and consequences

A preloaded DataFrame and a slider offer convenient random access but retain the
whole dataset and make accidental future OHLC access easy. A background replay
thread requires queue overflow, synchronization, and shutdown policies. A local
web service adds a protocol and frontend lifecycle. None is necessary here.

Matplotlib offers reusable plotting tools but adds a substantial runtime dependency
for a small number of drawing operations. Tk keeps this slice smaller, with the
cost of requiring an installed Tk module and display for native use. Headless
execution imports no native Tk runtime.

We accept a bounded viewport, conservative EOF candle semantics, and fixture-only
outcome evaluation. The last candle remains forming. Insufficient windows expire
unevaluated. Logical anchors are immutable; chart pixels can rescale. Audit files
use exclusive creation, append, flush, `fsync`, and hash chains. The design detects
accidental corruption without claiming authentication or crash-safe database
recovery. Measured costs are recorded in the [benchmark report](../../benchmarks/replay-results.md).

## MT5 boundary

The long-term renderer remains a native MQL5 indicator. Python's immutable alert
identity, issuance time, source position, cutoff, horizon, and anchor need explicit
mapping into chart time and buffer values. Native seconds and `double` values
cannot preserve Python microseconds, exact Decimal values, or same-second source
order without additional fields. Server chart time must not be assumed UTC.

The standalone source prototype has no Python HTTP dependency. Indicators cannot
call `WebRequest()`; an unsupported indicator-to-Python REST design is excluded.
A later read-only file bridge requires sandbox, encoding, stale-data, atomic
publication, schema, and duplicate-ID policies before implementation.

Current official API references and timestamp, indexing, and file-access findings
are linked in the [MT5 feasibility document](../../integrations/mt5/README.md).
Native compilation and MT5 runtime behavior remain **UNVERIFIED**. The prototype
renders predetermined offline arrows; it does not certify integrated replay or
forecasting accuracy. Full MT5 presentation remains a later feature.

## 2026-10-25: Causal research signal extension

The F02 refactor retains the engine, candle builder, chart, storage factory, and
hash-chain audit format. `ResearchSignal` is the canonical new issued artifact;
legacy fixture types remain behind compatibility exports. Constructed outcome
calculation moves to `fixture_outcomes.py` and is absent from the research demo.
Externally supplied outcome facts append separately.

Two read-only design candidates were compared using the architect skill.
Candidate A extended session ownership and proposed a legacy projection adapter.
Candidate B made typed events the main API. Both identified reception-time
availability and cross-minute anchors as coordinated contract changes. We retain
A's session ownership and use B's immutable event envelopes, without adding an
event bus or another replay engine. The runners used the inherited available
model; this is independent design exploration, not distinct-model consensus.

The session accepts a producer and an event consumer. It validates only the
current released cutoff, deduplicates stable IDs, and limits signals to 1000.
The alternative of exposing historical tick arrays would enlarge the caller's
information boundary and require future-access controls. A generic event pipeline
would add another lifecycle owner. Neither is required for this feature.

Availability is the monotonic maximum of event time, supplied reception, and
previous availability. Candles retain event-time membership. For delayed inputs,
the arrow anchors to the observed event minute while preserving actual issuance.
Missing reception stays absent in canonical ticks. Legacy schedules reject delayed
inputs instead of claiming event-time issuance.

We accept current-cutoff-only producers, a bounded viewport, and finite per-run
signal identity storage. Later statistical research must evaluate hypothesis
quality separately. Exact Decimal strings and UTC timestamps define standalone
`research_signal.v1` interchange for a later read-only MT5 consumer. No new MQL5
implementation is part of this decision.

Audit hardening detects short writes before sequence advancement. Existing
`logical_events` already removes operational randomness, so a new audit format
would duplicate working code. Actual guarantees and limitations are in
[causal replay](causal-replay.md) and the
[implementation review](../reviews/feature-02-implementation-review.md).
