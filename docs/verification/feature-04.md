# Feature 04 verification

## Baseline and scope

The feature branch starts at verified `origin/main` merge `34b86b6`, containing Feature 03
commit `383c628`. No merge or push was performed by the agent. The user's untracked
`goal.md` remains unchanged and excluded from the commit. Its SHA-256 is
`fa403438a403635005649bf32c01f5dc793e409eda753b12f19f8d1d2369a3fe`.

The initial default suite passed before implementation. A focused test first failed during
collection because `quantlab.historical` did not exist. The new tests then exercised mapped
CSV, exact JSON decimals, schema failures, explicit instrument mapping, missing timestamps,
precision, source evidence, and invalid records through the real canonical validator.

## Executed checks

The full run collected and passed **405 tests**, with no skips, including native GUI tests,
real TimescaleDB tests, CLI subprocesses, hard process termination, actual backend termination,
and explicitly isolated database container disruption. The final run took 53.44 seconds.

```bash
env -u PYTHONPATH .venv-check/bin/python -m ruff check .
env -u PYTHONPATH .venv-check/bin/python -m ruff format --check .
env -u PYTHONPATH .venv-check/bin/python -m mypy src/quantlab
```

Ruff and formatting passed. Strict Mypy passed for all 39 source modules. The host injects an
unrelated ROS `PYTHONPATH`; verification removes it explicitly.

The database run used an ignored local environment file and the existing isolated Compose
project `quantlab-feature03-agent`. It did not use a production database. To reproduce with
your own isolated configuration:

```bash
set -a
source .env.feature03
set +a
DISPLAY=:99 QUANTLAB_GUI_TEST=1 QUANTLAB_DESTRUCTIVE_TESTS=1 \
  QUANTLAB_TEST_COMPOSE_PROJECT=quantlab-feature03-agent \
  env -u PYTHONPATH .venv-check/bin/python -m pytest -q --tb=short
```

The display was a temporary Xvfb server. Destructive tests require the explicit project
allowlist and opt-in. Without database/display configuration, tests skip those environments
rather than simulating a successful integration run.

## Historical evidence

New database tests cover both formats through independent repository restarts, exact
repeated prices, timestamp equality with source-position tie breaking, full archive retention,
idempotent imports, configuration-sensitive identity, and Feature 02 engine reset/replay.

A direct prefix write fails completion. An interrupted attempt restarts in a new capture.
A JSON Lines child process is killed after its first durable batch; its partial dataset
cannot replay and the next import completes without duplicate publication. A terminated
PostgreSQL backend rolls back and retries through Feature 03. A modified archive fails
independent validation. Truncated JSON retains the valid prefix and fatal quality event
without publishing a complete dataset.

Fresh CLI processes exercise inspection without a DSN, import, repeated import, bounded list,
independent validation, and limited replay. Original fixtures include offset timestamps,
repeated-looking records, exact JSON numeric prices, and missing optional provider IDs.

## Review findings and fixes

The architect workflow produced two independent design passes and selected one configured
facade over duplicate format-specific orchestration. The configured alternate model families
were unavailable; available GPT agents supplied the design and architecture review.

The strict maintainability review found a per-record SQL query in dataset validation.
It was replaced by a second ordered server cursor, and complete quality events are now
compared rather than only event identifiers. Review also reproduced escaped surrogate and
NUL identifiers that passed domain validation but could not persist as PostgreSQL text.
The source boundary now rejects them with structured errors. Tests cover both defects.
A CLI fixture exposed blank CSV IDs being treated as empty provider identifiers; blank IDs
now remain absent, matching the canonical CSV adapter's behavior.

A final compatibility review found that arbitrary legacy provider notes named
`historical_adapter` could be mistaken for a new profile. Completion and validation now share
one version-tag discriminator; regression cases preserve string, object, and null legacy notes.

No confirmed Critical or Major issues remain in the reviewed scope. No trading, network
acquisition, canonical model rewrite, or arrow-rendering change was introduced.

## Packaging and measurements

A `quantlab-0.5.0-py3-none-any.whl` was built without fetching dependencies and installed under
a temporary target. Python isolated mode loaded the installed module from that target,
confirmed version 0.5.0, and inspected the JSON Lines fixture successfully. The final
installed wheel also applied its packaged migrations, imported the CSV fixture into the real
isolated database, independently validated it, and replayed all three exact prices.

The deterministic benchmark asserts accepted and replayed row counts, source positions,
exact price strings, batch transaction counts, completed status, and zero errors. It alternates
CSV and JSON Lines for five rounds, separates tracing/profiling from reported timing, and
records parsing and complete-import memory separately. See
[the benchmark report](../../benchmarks/historical-results.md) and its raw JSON results.

## Limits of the evidence

Fixtures establish engineering behavior, not real provider authenticity or complete market
coverage. JSON support is explicitly flat JSON Lines; arrays and provider envelopes are
unsupported. Database durability remains dependent on operational database/archive backups.
