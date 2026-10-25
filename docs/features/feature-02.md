# Run causal research-arrow replay

Replay the packaged synthetic tick fixture, build UTC M1 candles, and display
immutable research arrows. These arrows test visualization and establish no
predictive performance. No model, broker connection, or account is required.

## Open the research chart

Install the project using the [README](../../README.md), then run:

```bash
env -u PYTHONPATH .venv/bin/python -m quantlab.replay_app --research-signals
```

The chart starts paused. Python needs Tk and a graphical display.

1. Press **Step forward** twice. No arrow appears.
2. Press **Step forward** once more. The first gold `FIXTURE UP` arrow appears
   at `2026-01-01T00:00:20Z`, source position `4`, price `1000.12`.
3. Check its `00:00` candle anchor and `00:00:20` issuance label.
4. Press **Play**, then **Pause**. Previously issued arrow times and prices stay fixed.
5. Complete the replay. Three synthetic, unevaluated arrows remain in the history.
6. Close the window to finish its audit file.

**Reset run** opens a new audit file. The previous file remains unchanged. Closing
before EOF records an interrupted run. The viewport shows at most 120 observed
candles. Older arrows stay in immutable signal history and audit evidence, even
when their bars leave the viewport. Pixel positions change when the chart rescales.

## Verify without a display

Run the same replay and exact underlying candle calculations with geometry checks:

```bash
env -u PYTHONPATH .venv/bin/python -m quantlab.replay_app \
	--research-signals --headless --audit-dir replay-runs
```

Expect `34` observations, `3` issued signals, `outcomes: []`, `ENDED`, and
`audit_verified: true`. `signal_timeline` preserves actual issue times and input
positions `4`, `16`, and `31`. Headless geometry does not claim native rendering.

Inspect the returned audit path independently:

```bash
env -u PYTHONPATH .venv/bin/python -m quantlab.replay_app \
	--inspect-audit replay-runs/RUN_UUID.jsonl
```

The [automated demo test](../../tests/test_replay_app.py) repeats the command and
compares verified logical audit events. The [native controller tests](../../tests/test_replay_gui.py)
check arrow visibility before and after issuance using a real Tk canvas.

## Replay a completed persisted dataset

Set `QUANTLAB_DATABASE_URL` to an isolated or approved local database. Then run:

```bash
quantlab replay DATASET_UUID --show-chart --limit 100000 --audit-dir replay-runs
quantlab data replay DATASET_UUID --show-chart --headless --limit 1000
```

These commands use F03's existing completed-dataset factory. They do not invent
research signals for market datasets. `--start` and `--end` retain UTC inclusive
start and exclusive end filtering. Without `--show-chart`, both commands retain
the original JSON tick-stream output. The default limit remains 100 observations.

## Use the research producer interface

A producer receives one immutable `Observation`, containing only the released
canonical tick, logical availability, and immutable dataset identifier. Return a
tuple of `ResearchSignal` records. The session validates the exact current cutoff,
price representation, instrument, dataset, and issuance time before durable append.
An identical ID and record is ignored on redelivery; conflicting reuse fails.

Use the [signal contract reference](../architecture/research-signal-contract.md)
and [causal replay explanation](../architecture/causal-replay.md) for API semantics.
`record_outcome` appends externally supplied outcome or expiry facts. This feature
does not compute labels or evaluate a forecasting hypothesis.

## Preserve original fixture callers

`ReplayAlert`, `IssuedAlert`, `AlertOutcome`, and `AlertBook` remain importable from
`quantlab.alerts`. Their implementation resides in `legacy_alerts.py` and
`fixture_outcomes.py`. The original command without `--research-signals` retains
its constructed outcomes `[true, false, null]` and `--horizon` option.

Use the research producer for new callers. Legacy schedules require event-time
availability and reject delayed input instead of publishing backdated arrows.
Research and legacy schedules cannot be mixed in one session. Existing legacy
MT5 fixture experiments have not been extended to the new signal schema.

## Run engineering checks

```bash
env -u PYTHONPATH .venv/bin/python -m pytest -q
env -u PYTHONPATH .venv/bin/python -m ruff check .
env -u PYTHONPATH .venv/bin/python -m ruff format --check .
env -u PYTHONPATH .venv/bin/python -m mypy
env -u PYTHONPATH .venv/bin/python -m benchmarks.causal_replay --ticks 100000 --gui
```

Database tests need an explicit `QUANTLAB_TEST_DATABASE_URL`. Native tests need
`QUANTLAB_GUI_TEST=1` and a display. Read the [implementation review](../reviews/feature-02-implementation-review.md)
for executed checks, benchmark results, and remaining limits.
