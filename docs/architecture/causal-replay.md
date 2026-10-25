# Causal replay and observation availability

F02 extends the existing `ReplayEngine`, `CandleBuilder`, `ReplaySession`, Tk chart,
and hash-chained audit writer. F01 owns canonical tick validation. F04 interprets
historical files and recorded streaming envelopes, and F03 supplies restartable,
paged factories for completed datasets. Replay creates no alternative ingestion
or storage framework and applies no forecasting model.

## Event time and availability answer different questions

Event time determines candle membership. Logical observation availability determines
when replay releases an observation and when a signal may be issued:

```text
availability(i) = max(event_time(i), supplied_reception(i), availability(i-1))
```

Absent reception is omitted from this maximum. Historical replay then assumes
availability at event time, subject to earlier delays. The original tick still has
`received_at_utc=None`; replay never fabricates source reception timestamps.
Reception before provider event time does not move the replay before that event.
Reception regressions do not reorder source observations or reverse availability.

The first available observation establishes the presentation-clock origin. `step`
releases exactly one observation. `advance` uses explicit elapsed time and speed,
retains clock debt, and releases at most its bounded pulse capacity. The engine
retains one private lookahead and the current tick. Callbacks receive no iterator,
lookahead, tick history, or full dataset.

## Candles preserve observed event order

Canonical event times cannot decrease, and source positions must strictly increase
within one acquisition and provider mapping. Equal event timestamps and repeated
prices remain separate observations. No sorting or deduplication takes place.

The forming M1 candle uses the first price as open, the observed maximum and
minimum as high and low, and the last released price as close. A later observed
minute closes the prior candle. Missing minutes create no candles. EOF leaves the
last candle partial, including a source truncated by an explicit replay limit.
Decimal prices retain their original coefficient and exponent.

A delayed event at `10:04:35`, received at `10:05:05`, updates the observed `10:04`
candle. Its signal issues at `10:05:05` and anchors to `10:04`. Creating an empty
`10:05` bar would invent data. The exact issuance label and availability gate
prevent the anchor from implying the arrow existed at `10:04:00`.

## One session owns publication

The session aggregates one released tick, invokes an optional producer with an
immutable `Observation`, validates the entire returned batch, and durably appends
issuance before publishing chart state. A failed append may leave an auditable
prefix, but publishes none of that batch. Event consumers then receive immutable envelopes
in this order: `tick_observed`, optional `candle_closed`, `candle_updated`, and any
`signal_issued` events. EOF emits `replay_completed` once. The final partial candle
is not emitted as closed.

`ReplayEvent.value` is an `Observation`, `Candle`, `ResearchSignal`, or `None`, as
specified by its kind. Each envelope carries logical availability. The callback
API prevents accidental access to future input; it is not a security sandbox for
arbitrary Python code. A producer must still be independently reviewed for hidden
external data or precomputed future labels.

Current-input signals require exact current position and event timestamp. Earlier
cutoffs and deferred issuance are deliberately unsupported by this producer API.
A future research component may maintain causal state from past observations, but
it cannot declare a false cutoff or backdate issuance. Reentrant `step`, `advance`,
`reset`, and `close` calls during callbacks fail explicitly. Public `issue` also
rejects callback calls; producers return signals instead. Callback exceptions
fail the run; consumer delivery has no exactly-once guarantee.

`close` closes the source iterator and records interruption unless market EOF was
observed. Reset opens a new run and source iterator. Engine status `ENDED` covers
both source exhaustion and explicit close; the audit completion reason distinguishes
them. An empty source fails explicitly. Source, producer, and audit failures stop
further publication. External outcome facts append separately and never change
original signals.

## Bounded state and rendering

The session retains 119 closed candles plus the forming candle and at most 1000
signals. It rejects capacity overflow without evicting signal identities. Audit
verification retains at most 10000 signal records per file and reads bounded lines.
Tests use temporary directories. Audit files remain append-only evidence on disk;
production retention and archiving belong outside this replay feature.

The interactive controller releases one observation per 50 ms timer pulse, at most
about 20 ticks/s before drawing cost. Large replay benchmarks drive the same session
directly with bounded sampled rendering; those rates are not interactive playback
rates. This retains the existing one-publication-per-observation behavior.

The renderer accepts canonical research signals and the legacy immutable fixture
projection. It checks visibility time and exact cutoff position, rejects duplicate
IDs, uses stable tags, and preserves logical anchors. It shows fixture arrows in
gold with their actual issue timestamp and an unevaluated synthetic label. Only
the visible 120-candle range is projected. Decimal-to-float conversion occurs at
the display boundary; unsupported ranges or collisions fail explicitly instead of
changing prices. Existing precision-limited Tk behavior remains documented.

## Reproducible evidence and actual durability

`audit_event.v1` is retained. New kinds are `signal_issued`, `signal_rendered`,
`signal_outcome_recorded`, and `signal_expired`. Issuance and render payloads use
`research_signal.v1`; outcome payloads retain identity, producer, dataset, source
position, and external metadata. `logical_events(path)` independently verifies the
file and excludes run UUID, wall-clock processing timestamp, and dependent chain
hashes. Market-event timestamps and payloads remain part of equality. Logical equality
assumes the same consumer, render-call schedule, and externally supplied outcomes;
these are auditable inputs, not random operational metadata.

Files use exclusive creation. Each complete record is canonical ASCII-escaped
JSON followed by newline, written, flushed, and `fsync`ed. A short write or I/O
error permanently stops the writer before advancing its sequence or publishing a
new signal. Duplicate canonical issuance is a no-op; conflicting identity reuse
fails. Verifiers detect truncated lines, invalid UTF-8, duplicate JSON keys,
changed hashes, inconsistent anchors, and incomplete lifecycle records.

ASCII escaping preserves diagnostic text without lossy replacements. New signal
identity and metadata reject NUL, invalid Unicode scalar text, nonfinite values,
and oversized metadata through F01's shared helpers. Prices serialize as Decimal
strings and aware times as UTC ISO strings with microsecond precision.

A restart opens another file. It never repairs, truncates, resumes, or overwrites
an interrupted run. Hash chains detect accidental corruption, not malicious
rewrites. File `fsync` does not prove directory-entry survival after power loss.
There is no authenticated, cross-process exactly-once, or crash-recovery claim.

See the [signal reference](research-signal-contract.md),
[ADR 009](adr-009-replay-visualization.md), and
[implementation review](../reviews/feature-02-implementation-review.md).
