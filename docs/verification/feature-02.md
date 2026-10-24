# Feature 02 verification and delivery evidence

## Delivered behavior

Feature 02 provides an executable offline Tkinter replay over Feature 01
`MarketTick` values, exact Decimal M1 aggregation, scheduled fixture alerts with
fixed issuance anchors, separate outcomes, and exclusive JSONL research logs.
No broker access or order execution is included. Deterministic correctness is
not evidence of predictive performance.

The branch is `feat/02-arrow-replay`, based on accepted Feature 01 merge
`c06363e`. The implementation is delivered as a local Conventional Commit; use
`git log -1 --oneline` for its hash. No merge into main or push was performed.
`goal.md` remains untracked and byte-for-byte unchanged, SHA-256
`fa403438a403635005649bf32c01f5dc793e409eda753b12f19f8d1d2369a3fe`.

## Grounding and skills

Read the Feature 02 request and all of `goal.md`, inspected actual contracts,
validation, tooling, repository state, and workflow rules, and ran the original
202 tests before implementation. GitHub main already contained the accepted
Feature 01 merge; fetching and fast-forwarding local main established the baseline.
The unrelated untracked charter was preserved.

Applied the actual project skill instructions for architect, how, arena, TDD,
ponytail, thermo-nuclear-code-quality-review, benchmark-checklist,
principle-explain-the-number, technical-writing, unslop, and ui-ux-pro-max.
Two independent design passes compared a direct replay owner against a generator
plus reducer. The direct owner won 23/25 against 22/25, with pure display geometry
and an independent verifier retained from the alternative. Requested external
model families were unavailable; these were separate inherited-model passes,
not evidence of agreement between different model families.

## Actual checks

Executed on CPython 3.14.4:

```bash
env -u PYTHONPATH .venv-check/bin/python -m pytest -q
env -u PYTHONPATH .venv-check/bin/python -m ruff check .
env -u PYTHONPATH .venv-check/bin/python -m ruff format --check .
env -u PYTHONPATH .venv-check/bin/python -m mypy
env -u PYTHONPATH DISPLAY=:99 QUANTLAB_GUI_TEST=1 .venv-check/bin/python -m pytest -q --cov=quantlab --cov-branch --cov-report=term-missing
env -u PYTHONPATH .venv-check/bin/python -m quantlab.replay_app --fixture demo --headless --audit-dir /tmp/quantlab-feature02-headless
env -u PYTHONPATH .venv-check/bin/python -m quantlab.replay_app --inspect-audit docs/verification/feature-02-demo.jsonl
env -u PYTHONPATH DISPLAY=:99 .venv-check/bin/python -m benchmarks.replay --gui
env -u PYTHONPATH .venv-check/bin/python -m pip wheel --no-build-isolation --no-deps --no-index --wheel-dir /tmp/quantlab-feature02-wheels .
env -u PYTHONPATH .venv-check/bin/python -m pip check
git diff --check
```

The ordinary suite reports **306 passed, 2 skipped**. Both skips are explicit
native-display checks. With the temporary local Xvfb display enabled, the entire
suite reports **308 passed, no skips**. Ruff lint and formatting pass for all
37 Python files; strict Mypy passes all 19 runtime modules. Combined line/branch
coverage displays 89% with native tests; coverage is not a completeness claim.
CLI subprocess tests exercise behavior whose child-process lines are not all
included in that coverage run.

The native checks instantiate real Tk windows, invoke controls, verify candle
and arrow Canvas items, check immutable positions and historical count, pause,
reset, and independently verify the resulting audit. Tests compare deterministic
renderer state rather than pixels. They also confirm headless imports work when
Tkinter imports are deliberately blocked.

Built `quantlab-0.3.0-py3-none-any.whl`, installed it into an isolated temporary
target, and ran its headless application using Python isolated mode. Its packaged
fixture produced 34 observations, three issued arrows, and outcomes
`[true, false, null]`. No broken development-environment requirements were found.
The host injects unrelated ROS modules through PYTHONPATH, so checks explicitly
remove that environment variable.

## Failing-first and review findings

New module tests first failed because the modules did not exist. The CLI tests
first failed because `quantlab.replay_app` did not exist. Later reviews found additional defects. Several were reproduced with specific
failing behavior tests before their fixes:

- Failed replay batches previously published a partial current position. They
  now fail without publishing the failed batch.
- Invalid audit anchor prices leaked Decimal errors; the verifier now uses the
  shared price boundary and reports a contract error.
- Audit inspection previously accepted backdated records and expired outcomes
  containing uncompleted future minutes; both now reject.
- EOF closed the log before the final render, producing three displayed arrows
  but zero render records. The regression reproduced `0 != 3`. Terminal audit
  completion now occurs explicitly after the final rendering acknowledgement.
- Batching could hide issuance frames at high speeds. Sessions now publish at
  most one tick per pulse, with across-speed tests proving first visibility at
  the exact issuance position before any corresponding outcome.
- Anchor/outcome constructors accepted inconsistent or mutable contents. They
  now enforce immutable issued anchors and ordered immutable outcome windows.
- Closed/failed sessions and unsupported renderer identities could create
  unaudited or invalid rendering. Rendering rejects these before invoking it.
- The supposed lazy Tk import was still at module scope. A fresh-process test
  blocking Tk imports failed, then passed after moving imports into the window
  constructor.

Strict structural review found cohesive ownership, no circular imports, no
unnecessary services or general event bus, and no file approaching 1000 lines.
All confirmed Critical/Major/Minor code findings were fixed and rechecked.

## Inspect the recorded demonstration

Actual desktop captures show the initial alert transition and retained outcomes:

- [Before issuance](../features/replay-before-issuance.png): position 3, no arrow.
- [Issued](../features/replay-issued.png): position 4, one arrow, no future outcome.
- [Complete](../features/replay-complete.png): all three arrows retained, with
  constructed-event success, unsuccessful window, and insufficient-window expiry.

[The corresponding native-render audit](feature-02-demo.jsonl) verifies as valid,
with 11 records, three issued alerts, three rendered alerts, and three outcomes.
These screenshots were captured from the actual Tk window under Xvfb. Reviewed
captures prompted fixes to a clipped footer, overlapping arrow labels, and
indistinguishable abbreviated IDs. Stored chart anchors did not change.

## Performance

Five interleaved timing rounds measured core replay at 629,438 ticks/s, candle
aggregation at 56,411 ticks/s, alert processing at 3.861 microseconds/tick, and
native Canvas updates at 7.76 ms/update. The full durable headless path takes a
median 1.127 seconds per 34-observation fixture on this machine. Storage fsync
is its measured limiter. Separate integrated Python allocation peak is 181,792
bytes; whole-benchmark Linux high-water RSS is about 39.2 MiB.

See [all ranges, environment, raw measurements, and limits](../../benchmarks/replay-results.md).
No production throughput or forecasting accuracy is inferred from these numbers.

## Remaining verification and severity

No confirmed Critical or Major code issue remains from the local review.

Minor verification limitation: native MQL5 compilation and MT5 execution are
**UNVERIFIED**, because MetaEditor, an MT5 terminal, and Wine are unavailable.
Consequently the prototype cannot be treated as a working native integration.
Compile and follow [its manual verification instructions](../../integrations/mt5/README.md)
on a prepared offline terminal before relying on it. Current official APIs and
the indicator WebRequest prohibition were checked and linked there.

Python 3.11 and hosted GitHub Actions have not run locally. The existing matrix
covers 3.11 and 3.14, and a new virtual-display step exercises native chart behavior
on 3.14 after your manual push. Review those results before merging.

All mandatory local Feature 02 Definition of Done conditions are met. Native MT5
integration remains the explicitly optional, unverified feasibility prototype.
Hosted CI and human review remain release/merge gates. Durable dataset recovery,
real provider data, full native MT5 replay, learned models, and predictive
performance evaluation remain later features.

## Changed files

The following 36 files are included. Regenerate with `git diff --name-only main...HEAD` after committing.

- `.github/workflows/ci.yml`
- `.gitignore`
- `CHANGELOG.md`
- `README.md`
- `benchmarks/replay-profile.txt`
- `benchmarks/replay-results.json`
- `benchmarks/replay-results.md`
- `benchmarks/replay.py`
- `docs/architecture/adr-009-replay-visualization.md`
- `docs/architecture/replay-engine.md`
- `docs/features/feature-02.md`
- `docs/features/replay-before-issuance.png`
- `docs/features/replay-complete.png`
- `docs/features/replay-issued.png`
- `docs/verification/feature-02-demo.jsonl`
- `docs/verification/feature-02.md`
- `integrations/mt5/QuantLabFixtureArrows.mq5`
- `integrations/mt5/README.md`
- `pyproject.toml`
- `src/quantlab/alerts.py`
- `src/quantlab/audit.py`
- `src/quantlab/candles.py`
- `src/quantlab/chart.py`
- `src/quantlab/data/replay_demo.csv`
- `src/quantlab/replay.py`
- `src/quantlab/replay_app.py`
- `src/quantlab/replay_fixture.py`
- `src/quantlab/replay_session.py`
- `tests/test_alerts.py`
- `tests/test_audit.py`
- `tests/test_candles.py`
- `tests/test_chart.py`
- `tests/test_replay.py`
- `tests/test_replay_app.py`
- `tests/test_replay_gui.py`
- `tests/test_replay_session.py`
