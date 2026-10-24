# ADR-008: Canonical contracts with acquisition-scoped validation

Status: accepted for the Feature 01 refactoring branch.

## Context

The verified starting point is commit `2f86154` on `main`. The original pytest
suite passed all 63 tests before structural changes. The audit inspected
`models.py`, `validation.py`, packaging, fixtures, tests, CLI behavior, and the
README against the unchanged `goal.md` specification.

The existing implementation correctly preserved Decimal prices, normalized UTC
timestamps, rejected unsupported display names, retained duplicate-looking
observations, and reported physical CSV lines. Those rules were reusable.

The original `Tick` lacked acquisition identity, provider symbol, provider quote
precision, exact raw evidence, and structured diagnostics. A parsed CSV mapping
could not preserve source bytes. Frozen models did not enforce construction
invariants, and the CLI materialized the entire dataset through `load_ticks`.
These are concrete gaps against the revised contract, rather than reasons to
replace the application with a framework.

The baseline tracked files were clean. The user-supplied `goal.md` was untracked;
it remains unchanged and was not treated as generated refactor output.
All implementation belongs to `refactor/01-market-data-contract-v1`, with no
merge into `main` and no automatic push.

## Alternatives

Two independent design passes considered distinct ownership of validation state.
Requested external model families were unavailable in the current environment,
so the passes used the available inherited agent model. They are independent
design reviews, not evidence of agreement between different model families.

Candidate A proposed an explicit acquisition validation session with a generator
adapter. Candidate B proposed a pure single-record validator and one generator
call per acquisition. Both separated raw evidence, typed candidates, canonical
ticks, diagnostics, and legacy values.

The decision rubric assessed contract correctness, byte traceability, bounded
state, honest legacy compatibility, and API simplicity. The independent judge
scored A at 23 of 25 and B at 25 of 25. These are design-review scores, not
performance measurements. B was the selected base because the generator's
lifetime already defines the required lifetime of ordering state.

A's explicit expected provider-symbol and quote-precision configuration was
retained. It prevents a well-formed source value from being accepted merely
because it is present. The caller remains responsible for verifying the
configuration against an authoritative source.

Rejected alternatives included a global map of captures and seen positions,
a canonical model with nullable provider metadata, and inferred provider quote
precision from legacy price strings. Each adds ambiguity or state without a
Feature 01 requirement.

## Decision

Use `validate_tick(candidate, context)` for source-independent record checks and
`validate_acquisition(candidates, context)` for incremental acquisition checks.
The generator keeps the last consumed position and accepted event time. Strictly
increasing positions detect duplicates and decreasing coordinates with constant
state. Equal event timestamps retain source order.

A typed candidate separates parsing from validation. `MarketTick` constructors
also enforce invariants, so direct construction cannot bypass the rules.
`RawObservation` retains exact bytes and verifies SHA-256. Metadata is copied,
bounded, and deeply immutable. Provider-supplied fields and adapter-generated
provenance have separate namespaces.

Domain rejections produce `ValidationOutcome` with errors and no tick. Fatal
transport errors raise `QualityError` because malformed CSV boundaries cannot
always be recovered safely. Stable error identities and optional supplied
detection timestamps keep repeated validation deterministic.

Retain the existing four-column format through `LegacyTick` and its named legacy
adapter. Share the price and timestamp helpers. Legacy output cannot be accepted
as `market_tick.v1`, because it has no verified provider symbol or quote precision.
The default CLI remains compatible and labels that limitation explicitly.

Keep file ownership in the CLI and CSV adapter, outside the engine. The CLI
calculates summaries incrementally. Preserve the list-returning legacy loader
as an intentional convenience API, with its O(n) memory behavior documented.

## Consequences

Raw evidence, canonical values, and quality events have distinct trust levels.
This creates a few cohesive modules while keeping runtime dependencies in the
standard library. Future local JSON or WebSocket adapters can construct typed
candidates without duplicating price, time, or ordering rules.

A source record's position is unique only within its acquisition. A caller must
reuse the same capture identity and configuration to reproduce the same typed
results. The engine does not infer overlap or identity across separate files.

Exact bytes and hashes remain in-memory evidence. This decision does not claim
durable archival, atomic persistence, or canonical byte serialization. Those
contracts need a later storage decision. Provider mapping authenticity remains
an external review responsibility.

Inputs have explicit bounds, including microsecond timestamp precision and
bounded records, metadata, and numeric representations. Exceeding a bound
rejects data rather than normalizing it silently.

## Verification and remaining gates

Architecture grounding, independent sketches, comparison, and synthesis precede
implementation. The test suite checks source-independent behavior and CSV
transport independently, with deterministic property tests for decimal values,
timezone offsets, and repeated observations.

The [README](../../README.md) gives the repeatable test, lint, type-check, and
benchmark commands. [Benchmark results](../../benchmarks/results.md) record the
measured environment, throughput, peak memory, failure overhead, and scaling.
Final code review checks corruption, lost observations, ordering, traceability,
resource bounds, and unnecessary abstractions before delivery.

Feature 01 remains a contract and validation feature. It does not implement the
Feature 02 arrow-replay workflow or durable storage. It also does not claim
production readiness from passing local engineering checks.
