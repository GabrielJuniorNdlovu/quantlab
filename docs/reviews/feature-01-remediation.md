# Feature 01 audit remediation

## Result

The confirmed reception-time and persistence-text defects are repaired. Local F01 verification
passes, including the downstream F02, F03 and recorded-source F04 paths. The final full run
executed **464 passing tests** with **2 existing container-disruption checks skipped**.
Ruff, formatting and strict Mypy pass. The applicable independent audit assertions also pass.

The work stays on `refactor/01-unified-market-data-validation`, based on `a7b38b6`.
The untracked `goal.md` is preserved unchanged. The supplied Version 2.0 charter describes the
intended hybrid Python/MT5 direction; this repair adds no broker connection, spike scoring,
forecasting model, trading behavior, or other feature. No collection branch is merged.

## Confirmed original defects

The original evidence remains in `/tmp/quantlab-f01-audit/report.md` and its temporary tests.
Permanent regression tests independently reproduced the behavior before production changes.

1. A configured CSV `received_at_utc` field was accepted but ignored. A valid value disappeared
   from raw/canonical reception metadata; a malformed value could enter a COMPLETE dataset.
   JSON and recorded JSON envelopes parsed the same value and rejected malformed timestamps.
2. Metadata strings and keys could contain NUL. They constructed successfully but PostgreSQL
   JSONB rejected registration with `UntranslatableCharacter`. Diagnostic text also accepted
   NUL or lone Unicode surrogates that could not be sent/stored as PostgreSQL text.
3. JSON reception-time failures discarded the known affected field name.

The initial new unit run produced **25 failures and 8 passes**. A real pre-fix CSV import/replay
regression failed because its stored reception timestamp was `None` rather than the supplied
microsecond timestamp. Three independent persistence reproductions confirmed the actual NUL/
surrogate failures and atomic rollback before the corrections. These were engineering fixtures
in an isolated database, not production datasets.

## Root causes and corrections

### One reception normalizer

`engine.normalize_reception` now owns optional reception-time normalization for mapped CSV,
historical JSON Lines and recorded streaming envelopes. `normalize_mapped_record` calls it,
and the direct JSON reader retains its existing `received_at_column` keyword by delegating
to that same helper. Format-specific timestamp validation has been removed.

Only a supplied value is normalized with the existing timestamp parser. Aware values convert
to UTC without losing supported microseconds. Empty, null or absent optional values remain
absent, retaining the existing explicitly supplied raw/acquisition fallback if one exists.
No event time or machine/import time is substituted. A configured CSV reception column must
exist in its header; its values may be blank. JSON optional fields may be omitted.

Invalid values raise a structured `QualityError` containing `INVALID_TIMESTAMP`,
`field_name="received_at_utc"`, capture ID, source position and fatal severity. A valid reception
time before the event still yields the existing warning while preserving the tick.

Normalization retains the raw bytes, SHA-256, source position and metadata. When the normalized
reception attribute changes, a validated immutable raw envelope is created around the same
original bytes. The archive remains unchanged.

### PostgreSQL-safe text boundaries

`metadata.freeze_metadata` now rejects NUL recursively in metadata values and keys. Existing
UTF-8, type, depth and byte-budget rules remain. Rejection is a structured
`ContractError(INVALID_SOURCE_METADATA, field_name="source_metadata")`; provenance is never
silently sanitized or dropped.

`DataQualityEvent` now rejects NUL and invalid UTF-8/lone surrogates in description, field name
and observed value. Description must be text; optional field/observed values may remain null.
Existing length limits remain. Valid Unicode text persists unchanged. Database constraints
and transactional writes were not weakened or replaced.

No automatic diagnostic sanitization was introduced. A caller that intentionally attaches
an untrusted display preview can explicitly call the existing `redact_observed_value` helper.
That helper substitutes `?` for non-printable/surrogate code points and truncates the default
preview with a visible length marker when necessary. The original acquired bytes must remain
in raw evidence. New database tests persist such an explicit preview alongside the exact
NUL-containing or escaped-surrogate original payload and verify the unchanged archive.

## Compatibility and legacy-data handling

`market_tick.v1`, `raw_observation.v1`, canonical field types, source enums and error-code wire
values remain unchanged. The direct JSON reader keyword remains supported. Ordinary canonical
CSV, CSV profiles without a reception binding, historical JSON and recorded JSON retain their
existing `canonical.v1` configuration fingerprint. Existing valid idempotence and replay tests pass.

CSV profiles that explicitly bind `received_at_utc` now use
`validation_version="canonical.v2.csv_reception"` in the existing manifest configuration.
This deliberately changes dataset identity only for the corrected interpretation. Without it,
an import could return an old COMPLETE dataset created by the buggy normalizer.

Old datasets are immutable and stay available under their original IDs. Independent validation
of an affected legacy version now gives an explicit re-import instruction instead of falsely
certifying it under different semantics. No SQL schema migration or in-place evidence rewrite
is required.

To migrate an affected dataset:

1. Retain its old manifest, raw archive and dataset ID for audit history.
2. Locate the original profile and source identifier from the import configuration.
3. Reimport the original or archived file with the same profile and source identifier:

```bash
quantlab data import /path/to/original-or-archived-source \
  --config /path/to/original-profile.json --source-id ORIGINAL_SOURCE_ID
quantlab data validate NEW_DATASET_UUID
```

A new dataset/capture records the corrected reception times, or the import fails explicitly if
the previously ignored values are invalid. Select the corrected dataset explicitly downstream;
do not concatenate it with its legacy revision as though they were different market observations.
New imports remain idempotent under the corrected fingerprint.

## Files changed

- `src/quantlab/engine.py`: shared reception normalization and precise structured failures.
- `src/quantlab/historical.py`: mapped-reader dispatch uses the shared normalization path.
- `src/quantlab/historical_json.py`: delegate reception handling and preserve reader compatibility.
- `src/quantlab/metadata.py`: reject NUL metadata without mutating provenance.
- `src/quantlab/quality.py`: validate database-facing diagnostic text.
- `src/quantlab/storage/contracts.py`: distinguish corrected CSV validation identity.
- `src/quantlab/storage/dataset_validation.py`: reject incompatible legacy revalidation explicitly.
- `tests/test_validation_remediation.py`: permanent unit/contract regressions.
- `tests/test_validation_remediation_storage.py`: real persistence, archival, replay and legacy tests.
- `docs/reviews/feature-01-remediation.md`: evidence and migration instructions.

## Regression coverage

The new tests cover:

- CSV, flat JSON and nested recorded JSON reception parity; UTC offsets and microseconds.
- Absent/blank optional values, required configured CSV headers, native aware fallback,
  naive/invalid/over-precise timestamps and non-text JSON values.
- Pre-event reception warnings, stable field names/codes, capture IDs and source positions.
- Exact prices, supplied quote precision, explicit instrument mapping, independent acquisition
  identity, source ordering and valid duplicate-looking observations.
- Supplied `bid`, `ask` and `last` price-basis metadata retained in source evidence.
- Recursive NUL metadata rejection; direct diagnostic constructors; valid Unicode preservation.
- Real database import/replay/revalidation, idempotence, failed import quality rows and archive hashes.
- Explicit diagnostic previews persisted alongside untouched original bytes.
- A reconstructed pre-fix COMPLETE dataset preserved while reimport gets a corrected new identity.

The MT5-style observations are explicitly synthetic engineering recordings with documented ISO
values. They are not native `MqlTick` serialization, verified provider identifiers or live MT5
messages. No new live source format, epoch inference, or price-basis inference was introduced.

## Executed verification

Environment: CPython 3.14.4, project `.venv-check`, pinned project tooling and the isolated
PostgreSQL/TimescaleDB audit database. The host's unrelated ROS `PYTHONPATH` is unset.
Database credentials are loaded from a protected temporary file and are not included here.

```bash
env -u PYTHONPATH .venv-check/bin/python -m pytest tests/test_validation_remediation.py tests/test_contracts.py tests/test_historical.py -q --tb=short
```

Exit 0: **173 passed in 0.52 seconds**.

With `QUANTLAB_TEST_DATABASE_URL` and `QUANTLAB_DATABASE_URL` set to the isolated audit database:

```bash
env -u PYTHONPATH .venv-check/bin/python -m pytest tests/test_validation_remediation_storage.py tests/test_historical_storage.py tests/test_ingestion.py -q --tb=short
```

Exit 0: **30 passed in 13.66 seconds**.

With the same database, `DISPLAY=:99` and `QUANTLAB_GUI_TEST=1`:

```bash
env -u PYTHONPATH .venv-check/bin/python -m pytest -q --tb=short
```

Exit 0: **464 passed, 2 skipped in 38.00 seconds**. This executes the F02 native offline chart,
candles, deterministic replay and audit paths, F03 migration/mapping/rollback/recovery tests,
and F04 CSV/JSON/recorded-source ingestion and revalidation tests.

The temporary independent audit suite was also rerun without four checks whose explicit purpose
was to assert the old buggy behavior:

```bash
env -u PYTHONPATH .venv-check/bin/python -m pytest /tmp/quantlab-f01-audit/test_independent_verification.py -q --tb=short -k 'not bad_quality_text_rolls_back and not accepted_provenance_nul and not invalid_reception_csv_can_currently_publish_complete'
```

Exit 0: **30 passed, 4 deselected in 0.31 seconds**. This includes all nine previously failing
independent assertions. The permanent storage tests replace the obsolete bug-behavior checks
with constructor rejection and successful explicit diagnostic-preview persistence.

```bash
env -u PYTHONPATH .venv-check/bin/python -m ruff check .
env -u PYTHONPATH .venv-check/bin/python -m ruff format --check .
env -u PYTHONPATH .venv-check/bin/python -m mypy src/quantlab
git diff --check
```

All exit 0. Ruff checks **70 Python files**; strict Mypy checks **39 runtime modules**.

## Final review and remaining limitations

A separate final read-through checked changed functions, every production caller, diagnostic
serialization, source archive preservation, configuration identity and legacy revalidation.
The direct JSON-reader keyword was retained during review rather than introducing an accidental
API break. The original independent audit regressions were rerun after implementation.

- **Critical:** none confirmed in the remediation scope.
- **Major:** none unresolved in the confirmed audit defects.
- **Minor/operational limitation:** affected legacy CSV datasets require explicit reimport.
  This is documented and tested; immutable old evidence is preserved.
- Two existing checks that restart the shared database container and perform container backup/
  restore were skipped rather than disrupt other databases. No destructive recovery claim is
  made for this run. Real backend termination and process interruption tests did execute.
- The separate `feat/04-data-collection-pipeline` branch remains unmerged. Its asynchronous
  collection/checkpoint scheduler and Parquet exporter are absent here and were not certified.
- Runtime verification used Python 3.14.4. Python 3.11 remains covered by the configured type
  target and hosted CI, which was not run by this local session.
- Native MT5 compilation/live messages, broker connectivity, prediction accuracy, trading and
  fresh performance benchmarks are outside this repair. No performance result is inferred.

**Merge-readiness verdict: PASS for local F01 remediation verification, subject to ordinary PR
review and hosted CI.** The fixes are suitable for review on the existing refactor branch.
This verdict does not certify other branches, live MT5 functionality or trading behavior.
