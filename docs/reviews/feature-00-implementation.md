# F00 implementation record

Status: `IMPLEMENTED — SOURCE QUALIFICATION INCOMPLETE`.

Branch: `feat/00-market-data-source-qualification`. Baseline: F04 commit `74a3288`.
The starting worktree was clean and no equivalent F00 branch existed. The current F04
interfaces were needed, so this is a dependent branch. F04 was not merged into main, and
its pending database/scale acceptance is not waived by F00. No push or merge was performed.

## Scope delivered

F00 provides a packaged JSON source registry, immutable reviewed definitions and unresolved
instrument mappings, local schema/provenance inspection, optional existing F01/F04 compatibility,
exclusive report publication, and `source list/describe/inspect/report` CLI commands.
The registry does not need a database or trading credentials. The source assessment and
adapter contract record technical and permission blockers with dated authoritative references.

No authorized genuine sample was supplied or inspected. Neither candidate is `QUALIFIED`.
The public API is `BLOCKED`; the MT5 recording route is `DOCUMENTED`. Documentation of general
API capability does not establish Boom 1000 availability on that API. No real feed was contacted.

## Architecture and review

The how/architect workflow grounded F01 canonical validation, F03 persistence and source
archival, F04 framing/profiles/collection, and CLI dispatch. Two design reviews converged on
an independent observational inspector with an optional unchanged canonical validation path.
An independent cross-judge selected the smaller explicit-parameter interface; the chosen design
incorporates immutable registry metadata and explicit caller-declared provenance.

Strict read-only code review identified and corrected these gaps:

- Selected scalar types alone did not expose source representation or timestamp precision.
  Reports now contain bounded decoded representations and valid ISO fractional/zone statistics.
- A canonical profile could contradict a resolved registry mapping while passing F01 syntax.
  Resolved symbol, precision, and external-to-internal binding contradictions now block compatibility.

The final source review found no further high-confidence Critical or Major issues. These
corrections were reviewed in source; they were not reproduced with tests during this phase.
The skill's preferred alternate model families were unavailable. Reviews used the inherited
model, without a cross-family consensus claim.

The existing CSV record framer and metadata JSON conversion are shared rather than duplicated.
F03's original serialization import name remains available. Existing historical inspection
adds accepted timestamp bounds. No canonical tick fields, database schema, acquisition service,
broker operation, prediction, or trading behavior was added or changed.

## Checks performed

The explicit F00 task prohibits creating or executing automated tests. No tests, test fixtures,
benchmarks, automated test scripts, or assertion-based sample checks were added or run.
No genuine-sample compatibility or production-readiness claim is made.

The following static checks passed:

```bash
env -u PYTHONPATH .venv-check/bin/ruff check .
env -u PYTHONPATH .venv-check/bin/ruff format --check .
env -u PYTHONPATH .venv-check/bin/mypy
git diff --check
```

Ruff reported all checks passed. Formatting reported 95 files already formatted. Strict Mypy
reported no issues in 53 source files. These results are static checks, not automated tests.

Manual CLI use listed the two packaged sources, displayed the MT5 definition, displayed
inspection help, and generated the public API qualification report in ignored
`source-reports/`. Listing/reporting succeeded without a DSN or account. No sample-inspection
command was executed because no authorized genuine sample was provided.

## Files created

- `src/quantlab/source_registry.py`
- `src/quantlab/source_inspection.py`
- `src/quantlab/source_cli.py`
- `src/quantlab/data/sources.json`
- `docs/data-sources/boom1000-source-assessment.md`
- `docs/architecture/market-data-source-contract.md`
- `docs/operations/source-qualification.md`
- `docs/reviews/feature-00-implementation.md`

## Files modified

- `.gitignore`, `pyproject.toml`: runtime-report exclusion and packaged JSON configuration.
- `README.md`, `CHANGELOG.md`, `goal.md`, `docs/architecture/market-data-ingestion.md`: current scope, status, and source-contract links.
- `src/quantlab/storage_cli.py`: pre-DSN source command registration and dispatch.
- `src/quantlab/historical.py`: additive accepted timestamp bounds in the reused canonical inspection report.
- `src/quantlab/csv_adapter.py`, `csv_syntax.py`: shared existing bounded CSV line reader.
- `src/quantlab/metadata.py`, `storage/contracts.py`: shared unchanged JSON conversion with the old storage export retained.

## Remaining gates

Independent testing must cover strict registry parsing and qualification gates, mapping
contradictions, source formats and fatal prefixes, numeric epochs, precision, source mutation,
report publication, packaging, and existing F01–F04 callers. The implementation has not passed
that acceptance phase. Existing F04 database and scale gates remain outstanding.

Source qualification additionally needs applicable storage permission, exact source identity
and precision, an authorized genuine sample with acquisition metadata, and reviewed coverage
and continuity behavior. These are evidence gaps, not implementation of a live collector.
The [source assessment](../data-sources/boom1000-source-assessment.md) records the findings;
the [source contract](../architecture/market-data-source-contract.md) defines the future F04 boundary.
