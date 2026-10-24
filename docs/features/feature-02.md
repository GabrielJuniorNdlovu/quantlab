# Run the deterministic arrow replay

Use the packaged engineering fixture to replay Boom 1000 test observations and
inspect fixed research arrows. All prices, alerts, and the constructed upward
event are predetermined test data. Outcome correctness establishes no market
predictive performance. See the [replay architecture](../architecture/replay-engine.md)
for contract semantics and limits.

## Open the offline chart

From the repository root, install the project using the [README setup](../../README.md).
Then run:

```bash
.venv/bin/python -m quantlab.replay_app --fixture demo
```

The application starts paused before its first observation. It needs Python's Tk
module and a graphical display. On Debian or Ubuntu, install the matching
`python3-tk` system package if Tk is unavailable. No broker account, network
service, or market-data download is required.

1. Press **Step forward** three times. The first gold fixture arrow appears at
   `2026-01-01 00:00:20 UTC`, source position `4`, price `1000.12`.
2. Press **Play**. Observe the M1 candles as later ticks arrive.
3. Press **Pause**. Check that the time, source position, and observed prices stop.
4. Change **Replay speed**, then press **Play** to resume.
5. Follow the alert history below the chart. The first alert resolves with the
   constructed event present, the second resolves without one, and the third
   expires with insufficient completed future candles.
6. Check that every original arrow retains its issuance candle and price.
7. Close the window to finalize its audit file before inspection.

Green and rose candles contain observed test prices. A white body outline marks
the forming candle. Gold arrows identify fixture alerts; the history describes
subsequent outcomes. Rescaling the chart can change pixel coordinates without
changing an arrow's recorded time or price.

Use **Reset run** to start again with a new run identity and audit file. The old
run remains intact. A reset before exhaustion records an interrupted run.

## Change presentation and evaluation settings

For a faster display, a different completed-candle horizon, and an explicit audit
directory, run:

```bash
.venv/bin/python -m quantlab.replay_app --fixture demo \
	--speed 120 --horizon 3 --audit-dir replay-runs
```

`--speed` accepts finite values greater than zero and at most `1000`. It changes
presentation timing. `--horizon` accepts integers from `1` through `1000` and
changes the future window, so its outcomes can differ. The default horizon is
three observed completed M1 candles after the issuance candle. Missing minutes
never count as candles.

## Complete a run without a display

Run the same domain and deterministic chart geometry without opening Tk:

```bash
.venv/bin/python -m quantlab.replay_app --fixture demo --headless \
	--audit-dir replay-runs
```

The default fixture produces `34` observations, `3` issued alerts, outcomes
`[true, false, null]`, and status `ENDED`. Its JSON result includes `audit_path`
and `audit_verified: true`. The `null` outcome is unevaluated expiry.
`headless_geometry` records distinguish geometry checks from actual native
`tk_canvas` rendering.

To run the automated checks, use the project's development environment:

```bash
env -u PYTHONPATH .venv/bin/python -m pytest -q
env -u PYTHONPATH .venv/bin/python -m ruff check .
env -u PYTHONPATH .venv/bin/python -m ruff format --check .
env -u PYTHONPATH .venv/bin/python -m mypy
```

Actual Tk rendering and arrow behavior were exercised through a virtual display.
Native MT5 compilation and runtime behavior remain **UNVERIFIED**. See the
[MT5 prototype check](../../integrations/mt5/README.md) and
[performance measurements](../../benchmarks/replay-results.md).

## Inspect the audit independently

Substitute the emitted `audit_path` in this command:

```bash
.venv/bin/python -m quantlab.replay_app \
	--inspect-audit replay-runs/RUN_UUID.jsonl
```

The report contains record, issued-alert, rendered-alert, and outcome counts.
A malformed file, changed hash chain, illegal alert relationship, or missing
completion record produces an explicit error and exit code `2`. Close an open
visual demonstration before inspecting its completed log.

For logical comparisons across runs, call `quantlab.audit.logical_events(path)`.
Run IDs, processing timestamps, and hash bookkeeping are excluded; event-time
values, configuration, exact decimal strings, issuance, and outcomes remain.
Compare runs driven through the same rendering and control sequence. GUI timer
cadence and explicit pause actions need not produce identical rendering records.
