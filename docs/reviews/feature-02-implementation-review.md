# Feature 02 implementation review

Date: 2026-10-25. Scope: causal offline replay, immutable research signals,
non-repainting visualization, and append-only evidence. Verdict: ready for review
and merge within this scope, subject to the explicit verification limits below.
No forecasting performance or native MT5 compatibility is claimed.

## Baseline and dependency evidence

Work started on `refactor/01-unified-market-data-validation` at `5cdbefa`, with
only the user's untracked `goal.md`. Remote main already included that F01
remediation through merge `a0f3480`. The dedicated branch
`refactor/02-causal-replay-and-signals` starts from that merged baseline.

The initial local suite passed **410 tests, 56 skipped** in **3.22 seconds**.
F01 was checked through its actual implementation, regression tests, and later
real PostgreSQL runs, rather than inferred complete from Git history.

The actual `goal.md` remains a version 1.0 charter. It was read completely and
preserved, including its broader product direction. Its SHA-256 remains
`2f06f22ae695b370488490325f9e98cc9887240234734aa415958ea6cfa78c3c`.
It remains untracked and is excluded from this commit.

## Existing code reused and architecture changes

The canonical `MarketTick`, F01 parsers and immutable metadata helper, candle
builder, replay iterator, logical clock/debt machinery, bounded session viewport,
Tk canvas, audit serializer/hash chain/verifier, and F03 completed-dataset factory
are reused. Candle aggregation and database schemas need no changes.

`ResearchSignal` adds one canonical issued research contract, not another tick
model or storage framework. The session retains lifecycle ownership and adds an
immutable observation producer boundary plus typed event consumers. Availability
uses the monotonic maximum of event time and supplied reception; candles remain
in event time. Research signals use exact current cutoffs, stable explicit IDs,
actual availability-based issuance, observed-bar anchors, exact Decimal prices,
source identity, producer version, and deeply frozen metadata.

The existing fixture API is preserved through `alerts.py`. Its immutable types
move to `legacy_alerts.py`, and constructed-outcome calculations move to
`fixture_outcomes.py`. Research runs use neither future-event annotations nor
constructed prediction outcomes. `record_outcome` accepts external facts separately.
Legacy schedules reject delayed inputs instead of issuing backdated records.

The existing chart accepts the canonical contract and legacy display projection.
Both retain logical arrow identity and anchors. Research demo labels explicitly
say synthetic and unevaluated. The existing database replay commands gain
`--show-chart`, `--headless`, and `--audit-dir`; plain tick output remains compatible.
Persisted-source headless reports do not label market inputs as packaged fixtures.

Two independent read-only design candidates and a cross-judge applied the
architect/arena workflow. Candidate A retained session ownership; candidate B
centered event-stream ownership. The judge scored A **10/10** and B **8/10** on
causality, reuse/API depth, compatibility, bounded memory, and audit durability.
The synthesis keeps A's lifecycle and B's immutable event envelopes. Available
runners used the inherited model, so these are independent passes rather than
agreement between different model families.

## Confirmed defects and failing-before evidence

The first five new tests failed before implementation:

- `test_replay_waits_for_received_observation`: an observation received at second
  65 was released after advancing only five seconds because scheduling used event
  time. The clock now respects source reception without modifying canonical ticks.
- `test_short_audit_write_never_commits_chain`: a writer returning only seven
  bytes did not raise. Audit now checks the byte count and fail-stops before
  sequence advancement or publication.
- Contract, delayed-signal, and cutoff tests failed because the canonical signal
  module and producer/event APIs did not exist. They now exercise those APIs.

Review added focused failing-before reproductions and fixes:

- A valid first signal followed by an invalid second signal leaked partial public
  state from a failed pulse. `_issue_batch` validates the whole batch before any
  append and publishes identities only after every append succeeds.
- Event callbacks could call public `issue`, producing durable signals absent
  from chart state. Public issuance during replay callbacks is now rejected;
  producers return signals through the bounded producer boundary. External
  issuance callbacks also receive reentry protection.
- Decimal numeric equality accepted changed representations such as `1000.00`
  versus `1000.000` as identical duplicates or rendered anchors. Duplicate and
  render identity checks now compare canonical serialized content.
- Generic headless reports inherited the demo's fixture provenance label.
  Fixture labeling is now explicitly selected by the demo entry point.

New failure injection also verifies a second append failure leaves every signal
in that pulse unpublished, while preserving any already written append-only
prefix. This is not a transactional multi-record file guarantee.

## New regression coverage

`tests/test_causal_signals.py` contains **25 passing tests**, independently rerun
by the final reviewer. Coverage includes delayed reception across minute boundaries,
reception regression, equal timestamps, exact source cutoffs, immutable nested
metadata, precise Decimal interchange, schema/type/size validation, unsafe text,
duplicate delivery, conflicting identity, multiple signals per candle, separate
outcomes, source cancellation/reset, typed callbacks and reentry, batch failures,
capacity overflow, interrupted writes, invalid UTF-8, and restart preservation.

`tests/test_causal_replay_storage.py` adds three parameterized real-database cases
for CSV, JSONL, and recorded streaming envelopes. Synthetic observations preserve
18 decimal places, microsecond event/reception timestamps, repeated observations,
source order, immutable archive evidence, and F03 dataset identities. Both replay
CLI variants run their bounded headless chart path.

`test_replay_app.py` adds reproducible canonical demo and source-label regressions.
`test_replay_gui.py` adds a real Tk controller test: no arrow before the third
step, one at position 4, immutable first signal after subsequent ticks, three
final arrows, exact issuance labels, no research outcome evaluation, and verified
render evidence. Existing candle, replay, chart, legacy outcome, and audit tests
continue to pass.

## Executed checks and actual results

Python: 3.14.4. Commands ran with inherited ROS `PYTHONPATH` removed. Database
checks used a freshly created isolated TimescaleDB database,
`quantlab_f02_refactor_fa9748b1`, selected through protected local configuration.
Native tests used a real Tk display under Xvfb `:99`.

```bash
env -u PYTHONPATH .venv-check/bin/python -m pytest -q --tb=short -rs
env -u PYTHONPATH .venv-check/bin/python -m pytest -q \
	tests/test_alerts.py tests/test_audit.py tests/test_candles.py \
	tests/test_replay.py tests/test_replay_session.py tests/test_chart.py \
	tests/test_replay_app.py tests/test_replay_gui.py \
	tests/test_causal_signals.py tests/test_causal_replay_storage.py --tb=short
env -u PYTHONPATH .venv-check/bin/python -m ruff check .
env -u PYTHONPATH .venv-check/bin/python -m ruff format --check .
env -u PYTHONPATH .venv-check/bin/python -m mypy
```

For the pytest commands, `QUANTLAB_TEST_DATABASE_URL` and
`QUANTLAB_DATABASE_URL` selected the isolated database, and
`DISPLAY=:99 QUANTLAB_GUI_TEST=1` enabled native tests. The destructive-container
opt-in variables were absent.

- Full suite: **495 passed, 2 skipped**, **43.72 seconds**.
- Focused F02 suite, including actual database and native tests:
  **137 passed**, **7.54 seconds**.
- Ruff lint: passed.
- Ruff format: all **76 files** formatted.
- Strict Mypy: passed, **42 source files**.
- `git diff --check`: passed.
- Deterministic research demo: **34 observations, 3 signals, no evaluated outcomes**;
  two separately verified runs produced identical logical audit records.

An earlier integration run had three failures in the newly written test fixture:
its reception binding used `received_at` instead of F04's `received_at_utc`.
Correcting that test binding made all three actual import/replay cases pass.
The final review's callback and Decimal reproductions failed before their fixes
and passed afterward. No failing check is presented here as a successful run.

## Downstream compatibility

The full suite exercises F01 contract/property/remediation tests, F02 aggregation
and visualization, F03 real database ingestion/paging/archive/failure behavior,
and F04 configured historical and recorded-stream imports. F03 backend termination,
lost-acknowledgement reconciliation, and killed-import recovery tests ran.

Two existing opt-in F03 tests remain skipped:
`test_abrupt_database_restart_retains_completed_dataset` and
`test_logical_backup_restore_including_archive`. They operate on the shared
container/project rather than only the fresh database. No valuable shared
container or dataset was restarted or restored for this feature.

F04's collection, checkpoint, and Parquet extension exists only on
`feat/04-data-collection-pipeline` at `f4c4655`. It was not merged or represented as
verified here. Compatibility covers the F04 adapter interfaces actually present
on main. The canonical `market_tick.v1` and persistence schemas remain unchanged;
no database migration is needed. Legacy fixture imports and commands remain valid.

## Performance evidence

Final benchmark measurements are recorded in
[the benchmark report](../../benchmarks/causal-replay-results.md),
[raw trials](../../benchmarks/causal-replay-results.json), and
[separate profile](../../benchmarks/causal-replay-results.profile.txt).
Five interleaved trials processed 100000 deterministic generated ticks without
preloading them. Median engine replay was **63851 ticks/s**; candle aggregation
**34370 ticks/s**; signal/audit session **15044 ticks/s**; sampled native rendering
**12702 ticks/s**. Per-record durable audit append measured **11661 records/s**.
Separate traced Python peak grew from **203805 bytes** at 10000 ticks to **438958
bytes** at 100000 ticks. Process lifetime RSS, including Tk, peaked at **40196 KiB**.
All source module hashes match the final implementation. These are absolute local
costs with source construction included, not production capacity or speedup claims.

## Files changed

Source: `alerts.py`, `signals.py`, `legacy_alerts.py`, `fixture_outcomes.py`,
`replay.py`, `replay_session.py`, `chart.py`, `audit.py`, `replay_fixture.py`,
`replay_app.py`, and `storage_cli.py` under `src/quantlab/`.

Tests: new `test_causal_signals.py` and `test_causal_replay_storage.py`; extended
`test_replay_app.py` and `test_replay_gui.py`.

Documentation: README, F02 feature guide, causal replay architecture, research
signal reference, ADR 009 extension, legacy architecture pointer, this review,
and deterministic verification JSON examples.

Benchmarks: new `benchmarks/causal_replay.py`, measured JSON/profile artifacts,
and the associated benchmark report. No dependencies, database constraints,
raw source provenance, MQL5 code, or broader product roadmap changed.

## Final independent review and remaining limits

The strict maintainability review checked ownership, duplicate contracts, module
sizes, state publication, bounded resources, trust boundaries, and error paths.
No module crosses 1000 lines. The existing session remains the sole lifecycle
owner; immutable producer/event boundaries avoid another event bus or storage
implementation. The compatibility exports exist to preserve existing callers.

Final independent reviewer verdict after running all 25 causal tests:
**no confirmed Critical, Major, or Minor blockers in the reviewed scope**.
The reviewer did not independently rerun database tests or benchmarks; those
results come from the primary verification runs documented above.

No unresolved Critical or Major defect was confirmed. One Minor performance
limit remains: the existing interactive controller publishes one observation per
50 ms timer pulse, at most about 20 ticks/s before rendering cost. The benchmark
measures direct session/canvas driving with sampled rendering, not controller
playback. Improving interactive fast-forward would require preserving issuance
publication barriers while batching; it is not claimed in this change.

Other limits remain explicit: current-cutoff-only producers; 1000 signals and 120
visible candles per run; finite precision/range at the Tk display boundary;
per-run deduplication; no interrupted-file resume, power-loss durability proof,
authentication, or exactly-once consumer delivery. Callback exceptions fail the
run. Source truth requires the immutable dataset in addition to audit verification.
The two destructive storage checks and unmerged collection branch are outside
the executed compatibility claim. Native MT5 interchange is specified only;
no new EA, connection, model, event labeling, prediction evaluation, or trading
behavior was implemented.

Final verdict: **ready for merge review within the verified F02 scope**. Required
F02 tests, available F01–F04 compatibility checks, static analysis, deterministic
demo, measured benchmark, and independent review pass. The explicit destructive
storage and unmerged-branch limits prevent any claim that all possible downstream
recovery scenarios were rerun. Keep this branch separate from main; push and merge
remain manual user actions.
