# Replay engine and alert semantics

This document describes the original fixture API retained for compatibility.
The canonical research path and reception-aware clock are documented in
[causal replay](causal-replay.md) and the
[research signal contract](research-signal-contract.md).

Feature 02 consumes Feature 01 `market_tick.v1` objects. `MarketTick` already owns
instrument, Decimal precision, UTC normalization, capture identity, and source
position. `validate_acquisition` owns raw-candidate validation. Replay checks the
ordered stream it receives and adds no parallel tick model.

## Caller's interface

A repeatable factory supplies a fresh iterator for each run:

```python
from pathlib import Path
from quantlab.replay import ReplayStatus
from quantlab.replay_fixture import EVENT_POSITIONS, demo_ticks, demo_alerts, dataset_hash
from quantlab.replay_session import ReplaySession

session = ReplaySession(
	demo_ticks, demo_alerts(), dataset_hash(), Path("replay-runs"),
	event_positions=EVENT_POSITIONS,
)
try:
	while session.status not in (ReplayStatus.ENDED, ReplayStatus.FAILED):
		session.step()
		frame = session.snapshot()
		# A real renderer consumes only this immutable frame.
finally:
	session.close()
```

`tick_factory` has type `Callable[[], Iterator[MarketTick]]`. A production adapter
must validate its input and provide the dataset hash. Constructed event annotations
are optional fixture inputs; the application passes its known `event_positions`
explicitly. An empty schedule is supported and produces no arrows.

## Responsibility and chronology

`ReplayEngine` owns source iteration, one private tick lookahead, an explicit
Decimal microsecond clock, and `PAUSED`, `PLAYING`, `ENDED`, or `FAILED` status.
`step()` releases one tick and pauses. `advance(elapsed_seconds)` releases due
observations only while playing. Speed multiplies presentation elapsed time.
The engine caps a call at `1000` ticks and retains clock debt. `ReplaySession`
uses `max_ticks=1`, so the application can render each issuance before revealing
the next observation. Slow rendering stretches presentation rather than skipping
observations or changing their order.

One run accepts one acquisition with stable capture ID, provider symbol, source
type, and pip precision. Source positions strictly increase; UTC event timestamps
never decrease. Equal timestamps keep source-position order. The engine rejects
unordered input instead of sorting or collecting the dataset. Clock lookahead
is private and never enters a public snapshot.

`CandleBuilder` owns one forming M1 candle and prior-tick ordering checks.
`AlertBook` owns the finite fixture schedule, active windows, issued alerts, and
outcomes. `ReplaySession` coordinates their updates, writes lifecycle evidence,
and publishes `ChartState`. The chart owns no source iterator or alert schedule.
It draws only a supplied snapshot. No subscriber queues, replay threads, or
shared global mutable state are used.

## Candles and immutable alerts

A `Candle` contains `instrument_id`, `opened_at_utc`, `open`, `high`, `low`, `close`,
`observation_count`, and `complete`. Opening time is a UTC minute boundary.
OHLC follows actual released tick order, including equal timestamps. A tick in a
later observed minute completes the prior candle. Gaps create no empty candles.
Dataset exhaustion leaves the final candle forming because no later observation
proves that its minute completed.

`ReplayAlert.schema_version` is `replay_alert.v1`. Its fields are `alert_id`,
`instrument_id`, `issued_at_utc`, `issued_at_source_position`, `direction`,
`alert_source`, `model_version`, `data_cutoff_position`, `evaluation_horizon`,
`evaluation_horizon_unit`, and `status`. Supported values include `BOOM_1000`,
`UP`, `DETERMINISTIC_FIXTURE`, and `SUBSEQUENT_COMPLETED_M1_CANDLES`.

A scheduled alert becomes an immutable `ISSUED` copy only at its exact matching
tick timestamp and source position. Cutoff must not exceed issuance position.
Missing or mismatched issuance ticks fail explicitly. Duplicate IDs are rejected.
`IssuedAlert` adds the original `candle_open_utc` and Decimal `price` from the
issuing tick. The session retains that anchor throughout the run.

`alert_outcome.v1` is a separate immutable `AlertOutcome` with `alert_id`, `status`,
`successful`, `evaluated_at_utc`, `evaluated_at_source_position`, `window_candles`,
and `reason`. Original issued objects remain unchanged. Terminal state is
`RESOLVED` with a Boolean result or `EXPIRED` with `successful=None`.

The default window contains three subsequently observed, completed M1 candles
whose opening times are strictly after the issuance candle. An alert at
`00:00:20` excludes the entire `00:00` candle. Without gaps, its window is `00:01`,
`00:02`, and `00:03`; a released tick at or after `00:04` completes evaluation.
If `00:02` is absent, the next observed completed candle replaces it in the count.
Known constructed-event positions affect the result only when those ticks are
consumed inside the future window. No inferred spike rule is introduced.

## Display and bounded state

Snapshots contain up to `120` observed candles plus issued alerts and outcomes.
The schedule and constructed event annotations each have a `1000`-entry limit.
No consumed tick history is retained. Old arrows can move outside the candle
viewport, but issued history and audit evidence retain them.

The chart converts Decimal prices to binary floats at the display boundary.
It supports prices from `1e-15` through `1e15` and rejects nonfinite values or
visible distinct prices that collapse to the same float. Pixel coordinates can
change during scaling. Stored issuance timestamps, positions, and prices cannot.
A future-data guarantee applies to the public snapshot and renderer boundary;
a developer with local file access can read the predetermined fixture itself.

## Audit and failure behavior

Each run creates an exclusive UUID-named JSONL file. `audit_event.v1` records
include sequence, run ID, dataset SHA-256, event time, separate processing time,
kind, payload, previous hash, and record hash. Sorted keys, compact JSON, UTC ISO
timestamps, and Decimal strings define serialization. Every append flushes and
calls `fsync`. The file is finalized explicitly after the last render. Reset
closes the prior run and creates another identity. A closed or failed session
rejects rendering instead of drawing unrecorded arrows.

The initial manifest includes schema versions, scheduled fixture alerts, and known
constructed event positions as reproducibility configuration. These future plans
are never published as visible chart alerts or prices. `alert_issued`,
`alert_rendered`, and `alert_outcome` remain separate evidence records.

The verifier checks bounded records, chaining, identities, lifecycle references,
cutoffs, immutable anchors, event-time order, and future-window consistency.
It cannot establish tick truth without the source dataset. Hash chaining detects
accidental modification; it does not authenticate a deliberately rewritten log.
Truncated or unfinished logs fail inspection without silent repair.

Empty streams and invalid records fail the run explicitly. Missing input files
raise a diagnosable file error. Duplicate or unmatched schedules fail validation.
No available alerts is a valid replay. A renderer exception pauses playback,
records `render_failed` when the sink remains usable, and preserves committed
state for retry. A source, domain, or audit failure stops publication; a failed
writer cannot resume. Each audit record is limited to 128 KiB, including the
initial configuration; oversized configurations fail before replay. Normal early
closure records interruption. Process death
can leave an incomplete file, which the verifier rejects. CLI input and file
errors use exit code `2`.

## Feature 03 integration

Storage must provide a repeatable validated tick factory with stable acquisition
positions and an immutable dataset hash. It must preserve equal-time observations,
reject cross-acquisition ambiguity, and close resources when iteration ends or
replay resets. Reset means replay from the start, not resume from a recovery
checkpoint. Durable raw archival, crash recovery, real dataset adapters, streaming
alert schedules, and native MT5 transport remain later work. The current
[visualization decision](adr-009-replay-visualization.md) describes that boundary.
