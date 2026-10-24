# Feature 01 refactor verification

The refactor starts from verified `main` commit `2f86154`. The original 63 tests
passed before implementation. Work belongs to
`refactor/01-market-data-contract-v1`; `main` was not merged or rewritten.

## Audit and implementation plan

The existing Decimal conversion, UTC normalization, duplicate preservation,
CSV schema checks, and physical-line provenance were correct and reusable.
The original contracts lacked acquisition identity, provider mapping, confirmed
quote precision, byte-exact evidence, and structured failures. The CLI retained
all observations through its list loader.

The implementation followed these phases:

1. Audit source, callers, fixtures, packaging, documentation, and `goal.md`.
2. Compare independent architecture sketches and record the selected boundaries
   in [ADR-008](../architecture/adr-008-canonical-data-contract.md).
3. Write failing contract, adapter, and CLI tests before replacement modules.
4. Implement immutable contracts, shared rules, incremental validation, bounded
   CSV capture, explicit legacy compatibility, and streaming CLI summaries.
5. Run static checks, independent review, engineering benchmarks, and a fresh
   environment installation. Fix the confirmed transport defect before delivery.

The original `goal.md` remains unchanged and untracked. Its SHA-256 before and
after work is `fa403438a403635005649bf32c01f5dc793e409eda753b12f19f8d1d2369a3fe`.

## Tests and static analysis

The final isolated environment uses CPython 3.14.4 and the exact versions in
`requirements-dev.lock`. The host exports ROS packages through `PYTHONPATH`, so
local isolated checks unset that variable. This excludes unrelated host packages
without modifying them.

Executed commands include:

```bash
env -u PYTHONPATH .venv-check/bin/python -m pip check
env -u PYTHONPATH .venv-check/bin/python -m pytest -q --cov=quantlab --cov-branch --cov-report=term-missing
env -u PYTHONPATH .venv-check/bin/python -m ruff check .
env -u PYTHONPATH .venv-check/bin/python -m ruff format --check .
env -u PYTHONPATH .venv-check/bin/python -m mypy
```

Actual results:

- Dependency check: no broken requirements in the isolated environment.
- Pytest: **202 passed in 1.48 seconds**, including all original 63 tests.
- Combined statement and branch coverage: **95.59%**.
- Statement coverage: **563 of 583 lines, 96.57%**.
- Branch coverage: **195 of 210 branches, 92.86%**.
- Acquisition engine coverage: **100% of statements and branches**.
- Ruff 0.17.2: lint passes; all 20 Python files meet formatting checks.
- Mypy 2.4.0: strict checks pass for all 11 runtime source files, targeting Python 3.11.

Four deterministic Hypothesis properties use up to 60 examples each for Decimal
representation, UTC offsets, repeated-observation retention, and invalid quote
precision. Tests run offline. No market credentials or external service is used.

## Failing-before evidence and review

The new contract and CSV tests initially failed collection because the required
canonical modules did not exist. After implementation, they passed.

Independent strict review identified one **Major** transport defect:
`csv.reader(strict=True)` accepted a quote inside an unquoted field. The
regression test
`test_bare_quote_inside_unquoted_field_is_malformed` first failed with
`DID NOT RAISE QualityError`. Shared explicit CSV quote validation fixed the
root cause in both adapters. The test now passes. Positive tests also preserve
valid escaped quotes, commas, and multiline records.

No unresolved Critical, Major, or Minor defect was identified in the scoped
final review. Review covered precision, timestamp normalization, raw integrity,
metadata immutability, ordering, rejection context, duplicate preservation,
resource bounds, module ownership, and actual benchmark work/count checks.
This finding does not imply exhaustive production assurance.

## Packaging and command checks

Editable installation succeeded with version `0.2.0` in the isolated environment
and the existing project environment. A wheel built offline with:

```bash
.venv-check/bin/python -m pip wheel --no-build-isolation --no-deps --no-index --wheel-dir /tmp/quantlab-refactor-wheels .
```

Inspection of `quantlab-0.2.0-py3-none-any.whl` confirmed every runtime module and
the `py.typed` marker. Integration tests invoke the installed module command from
outside the project directory. Canonical and legacy success summaries pass;
invalid data exits nonzero without printing success. Fixture bytes remain
unchanged.

## Performance evidence

The [benchmark report](../../benchmarks/results.md) records the reproduction
command, environment, five-run medians and ranges, rejection overhead, two input
sizes, separate memory samples, CPU evidence, and interpretation limits.
All measured runtime-module hashes match the delivered source.

At 100,000 valid rows, full CSV validation had a median of 6.2539 seconds,
15,990 validated observations per second, and a separate tracemalloc peak of
168,098 bytes. This measures the full engineered CSV validation path, not a
production ingestion service or exact CLI process startup latency.

## Changed files

- `src/quantlab/models.py`, `parsing.py`, `metadata.py`, `quality.py`, and
  `engine.py` define the contracts and validation rules.
- `src/quantlab/csv_adapter.py`, `csv_syntax.py`, and `legacy.py` own transport
  and compatibility.
- `src/quantlab/cli.py` and `validation.py` provide streaming CLI behavior and
  preserve original imports. `py.typed` marks the package as typed.
- `tests/test_contracts.py`, `test_csv_adapter.py`, `test_canonical_cli.py`,
  `test_properties.py`, and `test_boundaries.py` add coverage. The original
  `test_validation.py` changes only formatting; its assertions are unchanged.
- `data/fixtures/canonical_ticks.csv` adds four deterministic canonical records.
- `pyproject.toml`, `requirements-dev.lock`, `.gitignore`, and
  `.github/workflows/ci.yml` configure packaging and quality checks.
- `README.md`, `CHANGELOG.md`, the architecture reference, ADR, and this document
  explain setup, compatibility, decisions, and verification.
- `benchmarks/__init__.py`, `validate.py`, `results.json`, `results.md`,
  `profile.txt`, and `cpu-check.json` contain executable measurements and evidence.

## Tool configuration references

Tool settings follow the primary [Ruff configuration reference](https://docs.astral.sh/ruff/configuration/),
[Mypy configuration reference](https://mypy.readthedocs.io/en/stable/config_file.html),
and [Hypothesis settings reference](https://hypothesis.readthedocs.io/en/latest/settings.html).
The CI workflow pins official [checkout](https://github.com/actions/checkout/tree/3d3c42e5aac5ba805825da76410c181273ba90b1)
and [setup-python](https://github.com/actions/setup-python/tree/6d4fd22b20465ccc0f7354476588e8df7c9ca0e0)
revisions, checked against their v7 tags.

## Remaining gates and limitations

The local Feature 01 implementation satisfies the refactor's Definition of
Done. Hosted CI is configured for Python 3.11 and 3.14 but has not run because
the branch has not been pushed. Only Python 3.14.4 was executed locally. Human
review, hosted CI, and merge into `main` remain delivery gates under `goal.md`.

Runtime limitations are explicit microsecond precision, bounded numeric and
metadata representations, caller-reviewed provider configuration, and no durable
raw archive or canonical storage serializer. Legacy output remains limited
fidelity. Incremental consumers must use one validation call per acquisition and
must handle all outcomes before finalization. No database, broker connection,
authentication, chart indicator, AI model, or trading feature was introduced.
