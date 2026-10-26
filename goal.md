# QuantLab — Product Goal and Engineering Specification

> **Document status:** Living project charter and source of truth
> **Version:** 1.0 (initial specification)
> **Updated:** 2026-10-26
> **Initial instrument:** Boom 1000 Index only
> **Primary workspace:** `~/quantlab`
> **Languages:** Python for research/data services; MQL5 for MT5 chart visualization

## 1. Product vision

Develop **QuantLab**, a reliable, auditable quantitative research system that investigates whether historical observations contain information about future upward spike events in Boom 1000 Index, then displays *timestamped research alerts* as arrows on an MT5-style chart during controlled historical replay.

The intended user experience resembles the video reference: an arrow appears at decision time **before** the future candles have been revealed, and the system subsequently records whether a defined spike occurs within the alert horizon.

The long-term product aspiration is an MT5-native visual indicator experience, with the forecasting research performed by a separately testable Python engine. **A convincing visual arrow is not evidence of predictive ability.** The system must demonstrate genuine performance relative to rigorous null models before describing its outputs as forecasts rather than experimental scores.

### 1.1 Research questions

1. Can a reproducible algorithm detect and label Boom 1000 spike events using observations available *after* the event, without corrupting the forecasting dataset?
2. Is the conditional probability of a spike in a future horizon measurably different from an event-frequency or independent-event baseline given the observable past?
3. If a signal exists, does a calibrated model generalize to genuinely later, untouched observations?
4. Can an alert be displayed **before** its evaluation window and preserved without repainting, hindsight, timestamp drift, or selective deletion?
5. Can the entire experiment be reproduced from immutable source data, code revision, feature definitions, and model artifacts?

### 1.2 Non-negotiable boundaries

- **Research and simulation only:** No real-money trading, order execution, trade placement, account authentication, auto-trading EA, or broker account integration in this project scope.
- External data, when used, must be obtained through authorized, read-only means, with applicable licensing and access conditions respected. Do not assume an instrument API symbol, data field, entitlement, or history depth without verifying it.
- No promise of profits, return targets, or a specific spike hit rate. Randomly timed events may have **no learnable predictive signal**.
- A model which does not outperform a properly constructed null benchmark on unseen data **must fail the promotion gate**, even if its arrows look impressive.
- Features that use future values, shifted hindsight annotations, or retrospective signal modification are prohibited.
- The indicator is a visualization and research-alert product, not proof that a trade is suitable.

## 2. Product requirements and definition of success

### PR-01 — Single-instrument correctness

Support **Boom 1000 Index only** for this POC. Resolve and record the exact provider-specific symbol as a verified configuration value when an authorized dataset is introduced; never assume the display name is also the machine symbol.

**Acceptance:** An unrelated instrument is rejected with an explicit error and audit entry.

### PR-02 — Trusted, reproducible data

Import real historical observations through an approved read-only dataset interface. Maintain unchanged source evidence and a deterministic, validated representation with record provenance. Preserve all legitimate same-timestamp and same-price ticks.

**Acceptance:** Given the same input bytes and code revision, validation and replay produce bytewise-equivalent canonical outputs, within documented serialization rules.

### PR-03 — Defined spike events

Define *upward spike* with a versioned, deterministic labeling specification, including event start/end, direction, magnitude or return threshold, minimum separation, and handling of clustered events. The label may use future observations because it is the target, but **model inputs may not**. Prevent label definitions from being tuned on final test data.

**Acceptance:** Unit tests on carefully constructed fixtures cover normal movements, large upward changes, clustered events, timestamps and ambiguous boundaries; independent reviewer can reproduce labels.

### PR-04 — Forecasting hypothesis

Choose a specified **future tick horizon** or **completed-candle horizon** before training. A prediction at time `t` must use only data available by `t` and must refer to a strictly future interval. Tick and candle horizons are not interchangeable; a minute does not correspond to a fixed number of ticks.

**Acceptance:** Each prediction stores issue time, input-data cutoff, target horizon, calibrated score, model revision and eventual outcome.

### PR-05 — Auditable arrow experience

Display an arrow at its **original forecast issuance time** during a chronological, controlled chart replay. Record every arrow, including failed ones. No moving, backdating, removing or selectively hiding previous forecasts. Make 'insufficient data', 'stale input' and 'model unavailable' visible as **no prediction**, not an optimistic arrow.

**Acceptance:** Replay tests prove that every displayed arrow existed before the corresponding future interval was revealed and still appears when the sequence is reviewed later.

### PR-06 — Statistical honesty

Compare all learned models with defensible constant-event and independence baselines using chronological evaluation. Measure calibration, precision, recall, false alerts, lead time and relevant scoring rules. Report confidence intervals and unsuccessful experiments.

**Acceptance:** An independent test set remains untouched until model and thresholds are frozen; promotion occurs only with reproducible improvement and documented uncertainty.

### PR-07 — Software delivery quality

Use CI, tests, type checks, static analysis, code review, reproducible dependencies, migrations, metrics, recoverability and release artifacts. Do not describe a feature as 'production-ready' on unit tests alone.

**Acceptance:** All feature-specific tests and engineering gates pass in CI with review evidence before merging into `main`.

## 3. Current inventory — implemented vs. discussed

**Current branch:** `feat/00-market-data-source-qualification`, based on Feature 04 commit `74a3288`. F00 adds a documentation-backed source registry and authorized-local-sample inspector. Source qualification is incomplete: external Boom identity, genuine samples, applicable storage permission, and integration guarantees remain unresolved. No F00 automated tests were created or run by explicit task instruction. Feature 04's database integration and scale verification remain pending; its earlier 457-pass/96-skip run is historical evidence, not F00 acceptance. See `docs/data-sources/boom1000-source-assessment.md` and `docs/reviews/feature-00-implementation.md`.

| Item | Current evidence | Status | Required next action |
|---|---|---|---|
| Repository / VS Code workspace `~/quantlab` | F00 branch based on F04 commit `74a3288`; earlier merged F03 baseline is `3f355b1` | **Implementation branch; not merged** | Maintain branch hygiene and review gates |
| Installed project skills in `.agents/skills/` | Reviewed and applied `architect`, `tdd`, `thermo-nuclear-code-quality-review`, `benchmark-checklist`, `technical-writing`, `ponytail` | **Verified** | Apply skills on subsequent features |
| Feature 00: Source discovery, qualification, and sample inspection | `source_registry.py`, `source_inspection.py`, `source_cli.py`, packaged `data/sources.json` | **Implemented; source qualification incomplete; independent testing pending** | Resolve permission and exact source identity; obtain authorized genuine samples; run the separate verification task |
| Feature 01: Unified market-data validation & normalization | `src/quantlab/models.py`, `parsing.py`, `quality.py`, `engine.py`, `csv_adapter.py` (`9847cb4` + `refactor/01-unified-market-data-validation`) | **Verified & refactored** | Consumed as single source of validation truth by Features 02–04 |
| Feature 01: Canonical tick schema (`market_tick.v1`) | `MarketTick` (`instrument_id`, `provider_symbol`, `event_time_utc`, `price`, `pip_size`, `source_type`, `capture_id`, `source_position`, `received_at_utc`, `provider_tick_id`) + `RawObservation` (`raw_observation.v1`) | **Verified** | Preserve compatibility and exact Decimal/UTC invariants |
| Feature 02: Offline MT5 arrow-replay vertical slice | `src/quantlab/replay.py`, `candles.py`, `alerts.py`, `audit.py`, `chart.py` (`8fc920e`) | **Existing refactored implementation** | Preserve causal replay and immutable signal semantics |
| Feature 03: Durable TimescaleDB ingestion/storage | `src/quantlab/storage/` (`7af7052`) with raw source archival, batch receipts, and repeatable-read revalidation | **Existing refactored implementation** | Retain database constraints and whole-source evidence checks |
| Feature 04: Historical & recorded streaming data adapters, recovery, quality, and Parquet exports | `src/quantlab/storage/ingestion.py`, `src/quantlab/collection.py`, `src/quantlab/dataset_export.py`, migration `0003_collection` | **Implementation present; verification incomplete** | Run isolated TimescaleDB integration/recovery suite and required 100k/1m benchmarks; update review evidence before merge |
| ML models / feature pipeline | Logistic regression, LightGBM, TCN discussed | **Not built** | Define spike labeling (`PR-03`) and null baselines (`PR-06`) first |
| Native MT5 terminal compilation | Prototype in `integrations/mt5/`; Python offline chart verified | **Prototype only** | Verify in native MetaEditor/MT5 when terminal environment is available |
| Forecast performance | No audited real-data results supplied | **Unknown** | No accuracy or profitability claim permitted |

### 3.1 Changes required to earlier designs

- **Feature 01:** Keep a lean validated Tick value object; introduce separate source-envelope and ingestion-manifest objects, not dozens of indicator columns inside `Tick`. Add missing tests for timezone, format, precision, ordering and equal-timestamp observations. Record exact observed defects before refactoring.
- **Feature 02:** Stop treating CSV import into SQLite as the final product. This is a useful subsystem, but must be driven by verified data needs, storage/replay invariants and a planned end-to-end vertical slice.
- **Database implementation:** F03 already provides PostgreSQL/TimescaleDB persistence, and F04 adds research Parquet export. F00 reuses those boundaries. It introduces no storage replacement and does not resolve the pending F04 integration/scale checks.
- **ML feature lists:** Returns, volatility, RSI, momentum, intervals and tick rate are **research hypotheses**, not validated indicators of an imminent event.
- **Visualization requirement:** Move MT5-compatible arrows early in the plan. A backend-only research pipeline is incomplete relative to the intended user-facing product.
- **Forecast target:** Decide tick-based vs. candle-based future horizon explicitly; determine how a forecast at a tick is mapped to the appropriate chart bar.
- **Online ingestion frequency:** Retain every available observation when doing authorized data acquisition; ingestion batching and screen refresh cadence are separate engineering parameters. Measure actual source characteristics, not guessed tick rates.

## 4. End-to-end architecture (research/development)

```text
Immutable permitted historical data / deterministic test fixtures
                      |
                      v
             Source ingestion adapter
                      |
                      v
      Raw archival + acquisition manifest + hash
                      |
                      v
        Validation + canonical ordered tick stream
                      |
                      v
       Durable time-series store / replay interface
                      |
            +---------+-----------+
            |                     |
            v                     v
     Spike labeler        Causal feature builder
            |                     |
            +----------+----------+
                       |
                       v
        Baselines -> candidate ML models
                       |
                       v
      Offline chronological forecast-event log
                       |
           +-----------+------------+
           |                        |
           v                        v
   Evaluation/reporting    MT5-compatible replay
                            arrow visualization
```

**Deployment topology:** Single developer machine first. A component must be split into a service only when its security, isolation, availability or independent scaling requirements justify it.

### 4.1 Main contracts

- **RawObservation:** unmodified provider payload or import row plus source metadata, original offset/order, payload hash, acquisition run ID and any supplied provider identifiers.
- **ValidatedTick:** verified instrument, UTC event timestamp with documented source resolution, exact-precision price, source identity and deterministic intra-source order.
- **DataQualityEvent:** validation code, severity, affected record/range, timestamps, source run and remediation status.
- **SpikeEvent:** versioned label definition, start/end position, direction, magnitude and associated dataset revision.
- **FeatureVector:** as-of timestamp/sequence, feature version, history cutoff, numerical features and missingness indicators.
- **ForecastEvent:** unique issuance ID, issued-at time, data cutoff, target horizon, model version, score, status and expiration time.
- **ForecastOutcome:** immutable linkage to forecast ID, event occurrence and evaluation time, recorded only after horizon completion.
- **ReplayManifest:** input dataset hashes, code commit, model artifact hash, settings, deterministic ordering rule and environment version.

Use typed schemas (Python dataclasses/Pydantic where justified) and schema versioning; never treat a local CSV row number as a globally unique provider tick ID.

### 4.2 Data separation

1. **Raw:** Unaltered, access-controlled acquisition records; never overwritten by cleaning.
2. **Validated:** Canonical price/timestamp representation; anomalies quarantined with explanations.
3. **Analytical:** Versioned spikes, candles and causal features; recomputable.
4. **Forecasts:** Append-only scores and alert lifecycle events, including all failures and abstentions.
5. **Artifacts:** Frozen model weights/configurations and experiment reports.

## 5. Prioritized delivery roadmap

Every feature gets a separate `feat/<number>-<short-name>` Git branch, PR/review, CI, tests, documentation and acceptance evidence. The status labels are **not** claims of completion.

| ID | Feature | What must be delivered | Priority / status |
|---|---|---|---|
| F00 | Market data source discovery and qualification | Source evidence, unresolved/verified mappings, local schema/provenance inspection, future adapter contract | **Implemented; source qualification incomplete; testing deferred by task policy** |
| F01 | Market Data Integrity Engine | Schema validation, precision/UTC handling, rejected-record reporting, deterministic ordering | Existing refactored implementation; preserve contract |
| F02 | **End-to-end arrow replay feasibility slice** | Recorded/canned time series, predetermined alert timestamps, offline arrow visualization and audit log | Existing refactored implementation; native MT5 verification remains separate |
| F03 | Durable dataset provenance and storage | Immutable raw source, hashes, manifests, ordered ticks, idempotent imports, crash-safe persistence, deterministic replay | Existing implementation; preserve F03 ownership |
| F04 | Recorded data collection and export | Explicit adapters, bounded collection, recovery, quality reports, chronological Parquet | Implementation present; integration and scale verification pending |
| F05 | Spike-event labeling + market characterization | Versioned event definitions, distributions, intervals, independence tests | Planned |
| F06 | Research feature pipeline | Causal rolling statistics, training-only fitting, missingness, time alignment, reproducible feature snapshots | Planned |
| F07 | Statistical null models | Base rates, independent-event simulations for tests, hazard benchmarks, calibrated constant forecasts | Planned |
| F08 | Candidate ML experiments | Logistic regression, LightGBM; neural sequence models only if evidence supports complexity | Planned / conditional |
| F09 | Rigorous evaluation system | Walk-forward studies, leakage audits, locked holdout, calibration, recall, false-alert/lead-time analysis | Planned |
| F10 | Immutable alert engine | Issue/expire/resolve states, no-repainting invariants, no duplicate alert storms, visible abstention | Planned |
| F11 | MT5 indicator presentation hardening | MQL5 arrow overlay, clear event-time mapping, no stale alerts, deterministic local replay | Planned |
| F12 | Experiment dashboard and evidence reports | Coverage, data quality, study results, model cards, false alarms and replay inspection | Planned |
| F13 | Operational robustness / release | CI, backups/restores, migration tests, observability, packaging, documented release procedure | Cross-cutting; must mature at every phase |

### 5.1 Critical vertical slice: F02

Do **not** wait until after ML training to prove that an MT5 indicator-like display is feasible. First demonstrate a correctly timestamped arrow appearing during a paused/advancing historical replay, using a **canned** forecast event—not claimed predictive output. The backend should issue the alert event; the presentation component should draw it; an independent log should confirm it was not added after the corresponding replay timestamp.

**Gate:** A recorded demo, automated alignment checks and immutable arrow log showing that the visual behavior is achievable without repainting. No prediction-accuracy claim follows from this demonstration.

## 6. MT5 and MQL5 feasibility constraints

- Custom indicators can draw arrow marks using indicator buffers (`DRAW_ARROW`) and update from `OnCalculate`.
- **Confirmed architectural constraint:** MQL5 `WebRequest()` is synchronous and **cannot be called from an indicator**. Do not design a Python inference REST request directly inside an indicator. A non-indicator bridge or native/local test data approach requires a separate, reviewed ADR; a helper component must never have order-placement permissions in this project.
- `WebRequest()` also cannot run in the Strategy Tester; test plans must not assume it is available there.
- Define and verify how Python-time timestamps map to MT5 chart bar timestamps, timezone display, resolution, and partially formed candles.
- Offline visual replay must continue to work without a network service. The indicator must clearly show if the data is stale or unavailable, rather than drawing a fresh-looking signal.
- Separate **arrow display** from **model inference**: renderer correctness must not depend on whether forecasting works.

**Primary MQL5 references (verify against current docs before coding):**
- https://www.mql5.com/en/docs/customind/indicators_examples/draw_arrow
- https://www.mql5.com/en/docs/event_handlers/oncalculate
- https://www.mql5.com/en/docs/network/webrequest

## 7. Scientific and modeling standards

### 7.1 Honest benchmarks

Assume an independent-event/null model until contrary evidence survives strict testing. Average intervals (for example, a number in an index name) do not constitute a predictable countdown. Data volume, more engineered features, and neural network size do **not** manufacture predictive information.

### 7.2 Event definition before feature selection

- Freeze event direction and definition before running benchmark comparisons.
- Specify evaluation horizon in ticks or **completed** bars, not both interchangeably.
- Use point-in-time feature construction and demonstrate causality with property tests.
- Use train/validation/test periods in chronological order; purge/embargo overlapping targets where appropriate.
- Fit scalers, selectors, labels' adjustable thresholds and calibration only on eligible training/validation data.
- Avoid selecting the best-performing window after inspecting the holdout.

### 7.3 Candidate models and promotion

1. Constant-event probability / simple null process.
2. Logistic regression for interpretable baseline.
3. Gradient boosting for nonlinear tabular features.
4. TCN/LSTM or another temporal model only if simpler candidates show repeatable signal.

**Model must beat the benchmark on untouched future-period data** with meaningful calibration and a reproducible improvement; otherwise publish a negative result. Investigate drift and repeated significance testing. Forecast probability is not a guaranteed event prediction.

### 7.4 Required evaluation metrics

- Primary: log loss and Brier score for probabilistic forecasts.
- Calibration: reliability diagram, expected calibration error (with disclosed binning), predicted vs. observed rates.
- Alerting: precision, recall, false alerts per 1,000 observations, alert coverage, median/percentile lead time, missed-event frequency.
- Dataset quality: number of source records, validated records, rejects, duplicate-looking records preserved, known gaps.
- Engineering: end-to-end replay alignment, latency, memory, throughput, recovery and error rate.
- Uncertainty: time-aware confidence intervals/blocked bootstrap where appropriate; sensitivity to different time periods.

Do **not** market conventional classifier accuracy, screenshots, selective examples or hypothetical profit as proof of predictive success.

## 8. Software-engineering rules

### 8.1 Principles

- **Correctness first:** Explicit contracts, invariants and failure paths.
- **Clean architecture:** Domain logic separate from CSV parsing, DB clients, model implementation and MQL5 display.
- **SOLID pragmatically:** Small components with cohesive responsibilities; avoid speculative abstractions.
- **DRY without over-generalization:** Factor actual duplication, not hypothetical future needs.
- **KISS/YAGNI:** Single-machine components until measured requirements justify distributed infrastructure.
- **Testability:** Dependency injection at source/storage/clock boundaries; deterministic seeds and fixtures.
- **Traceability:** Feature IDs in PRs, ADRs, code review and tests.
- **Security:** No credentials committed, minimal privileges, bounded inputs, pinned dependencies, vulnerability review.
- **Reproducibility:** Lock environments, store data/model hashes, config and Git SHA.

### 8.2 Quality tooling (verify versions before adopting)

- `pytest` for unit, integration, property-based and recovery tests (consider `hypothesis`).
- `ruff` for linting and formatting; `mypy` or `pyright` for type checking.
- `pre-commit` hooks and CI checks on every pull request.
- Dedicated tests for SQL migration, database crash/restart, dataset determinism and replay synchronization.
- Benchmark scripts that capture actual machine specs and measured throughput/latency; no invented benchmark figures.
- Python dependencies tracked by `pyproject.toml` and a reproducible lock strategy.

### 8.3 Review gates

**Every feature PR must include:** objective, design/ADR if changed, exact file diff, tests, failure scenarios, security implications, performance implications, docs, screenshots/replay artifacts when applicable, rollback notes, and known limitations.

- **Critical:** Data corruption, look-ahead leakage, retroactive alerts, silent data loss or unauthorized side effects. Must block merge.
- **Major:** Incorrect idempotency, inconsistent time alignment, unbounded resources, missing recovery behavior. Must be fixed or explicitly waived with evidence and owner.
- **Minor:** Cosmetic/refactoring issues without correctness impact. Track as issues.

Suggested coverage gate: **at least 90% branch coverage for core validation, labeling and alert-state logic**, supplemented by property tests and failure injection. Coverage alone does not establish quality. CI must pass without skipped critical tests.

### 8.4 Branch strategy

- Protected branch: `main`.
- Feature branches: `feat/02-arrow-replay`, `feat/03-durable-storage`, etc.; fixes: `fix/<description>`.
- Never implement new features directly on `main`.
- Commit focused, descriptive changes; no generated environment files, secrets, caches or large raw datasets.
- Use Conventional Commits when reasonable (`feat:`, `fix:`, `test:`, `docs:`, `refactor:`).
- Open a pull request; require review and green CI; squash or no-fast-forward merge consistently, retaining traceability.
- Use SemVer for releases and keep a `CHANGELOG.md`.
- Tag reproducible research milestones and associate experiments with commit hashes.
- **Do not automatically merge or push** without explicit human authorization.

## 9. Codex agentic skills — how to use them

The screenshot showed folders under `.agents/skills/`; their instructions and quality have **not** yet been audited. Codex must first read each applicable `SKILL.md` and respect its documented entry points. Do not assume that folder names alone guarantee a working skill.

| Skill observed | Appropriate use | Guardrail |
|---|---|---|
| `architect` | Component boundaries, ADRs, data contracts, MT5 interface design | Challenge complexity; document alternatives |
| `interrogate` | Identify ambiguous requirements and incompatible assumptions | Resolve before changing APIs |
| `tdd` | Implement observable behavior using failing tests first | Include negative/failure paths |
| `thermo-nuclear-code-quality-review` | Critical review before requesting merge | Verify findings with tests; no manufactured issues |
| `benchmark-checklist` | Import throughput, replay latency and memory tests | Report measured numbers only |
| `technical-writing` | Keep README, `goal.md`, ADRs and runbooks current | Documentation must match code |
| `show-me-your-work` | Demand executed commands, test evidence and changed-file summary | No unsupported claims of success |
| `create-verification-skill` / `maintain-verification-skill` | Standardize recurring quality gates if needed | Only after reviewing existing checks |
| `frontend-design`, `ui-ux-pro-max`, `web-design-guidelines` | Optional research dashboard design | Not a substitute for MQL5 correctness |
| `pony*`, `arena`, `blast-radius`, etc. | Evaluate per-task only if their actual instructions match | Do not invoke all skills indiscriminately |

**Recommended Codex workflow for each feature:**

1. Inspect `goal.md`, current Git state, existing code and applicable `SKILL.md` files.
2. Summarize requirements, assumptions, dependency changes, risks and acceptance criteria.
3. Create/confirm the intended feature branch with clean working tree.
4. Define tests and invariants first; implement the minimum complete vertical feature.
5. Run tests, lint, type checks, integration/recovery checks and benchmarks as applicable.
6. Perform architecture/security/code-quality review; fix verified issues.
7. Update docs and ADRs, present diff and evidence, then stop before merge.

## 10. Nonfunctional reliability specifications

- **Ordering:** Stable per-source record order, including ties on timestamp; no uniqueness constraint on `(timestamp, price)`.
- **Atomicity:** Imports must never expose incomplete data as finalized data.
- **Idempotency:** Repeat import with identical source identity must not duplicate finalized observations; document how overlapping different files are treated.
- **Recovery:** Demonstrate restart after interruption, corrupt file, lock contention and partial-write failure with integration tests.
- **Observability:** Structured logs and counters for accepted/rejected events, processing lag, storage errors, replay state, model abstentions, stale-data flags and arrow outcomes.
- **Backpressure:** Bounded queues and deterministic overflow policy; no invisible drops.
- **Resource use:** Measure actual RAM, disk growth, replay throughput and end-to-end latency on the target computer before setting SLO values.
- **Availability:** Specify safe degradation: unavailable features/model means unavailable output, not silently reused scores.
- **Security/privacy:** No stored account passwords, keys, or personally identifying broker data.
- **Disaster recovery:** Test backup/restore and schema migration before describing storage as production-grade.
- **Testing environments:** Separate unit fixtures, archived research inputs, replay demos and release artifacts.

## 11. Decision log / open architecture questions

| ADR | Decision needed | Status |
|---|---|---|
| ADR-001 | SQLite vs TimescaleDB for operational tick storage; Parquet/DuckDB for offline queries | **Open — benchmark first** |
| ADR-002 | Verified authoritative source schema and instrument identifier for authorized datasets | **Open — verify** |
| ADR-003 | Spike-event label definition and minimum detectable event | **Open — research required** |
| ADR-004 | Target horizon: ticks, complete M1 candles, or separately modeled targets | **Open — must freeze** |
| ADR-005 | MT5 display data flow for offline replay (native MQL5 vs separate non-trading bridge) | **Open — prototype first** |
| ADR-006 | Timestamp resolution, missing observations and cross-environment time alignment | **Open — test** |
| ADR-007 | Null-model definition, test partitions and model promotion criteria | **Open — freeze before model search** |

No ADR may claim technical compatibility without citing a primary specification or an executed local experiment.

## 12. Release milestones / stop conditions

**M0 — Verified foundation:** Existing project audited; tests pass; discrepancies documented; Git and CI workflow established.

**M1 — Visual vertical slice:** MT5-compatible, offline replay shows immutable arrows with correct timestamps and a complete alert log.

**M2 — Reproducible research data:** Provenance-aware storage, quality checks and deterministic event labels work on authorized real historical observations.

**M3 — Evidence of forecastability:** Null hypothesis benchmark and chronology-safe ML studies completed, with uncertainty and negative outcomes fully documented.

**M4 — Research product:** Packaged offline evaluation/replay indicator experience with clear model availability, quality and audit diagnostics.

**Stop / no-go:** If unseen-period results do not reliably beat null models, preserve the system as a data-engineering and market-statistics research product. Do not relabel random alerts as successful predictions. Do not advance toward financial execution functionality.

## 13. Immediate next action

**Complete source qualification before adding external acquisition.** Obtain applicable storage permission, verify the actual Boom instrument identity and precision, and inspect an authorized genuine sample. Review timestamp, price, identifier, coverage, and continuity semantics against the F00 contract before separately scoping an F04 adapter.

Run F00's independent testing in the requested follow-up phase. Preserve the existing F04 database and scale-verification gates. The current development-only instruction overrides the usual test-first workflow for F00 only; it does not establish production readiness or waive acceptance.

**Rule for future development:** A proposed feature is not *done* unless it is demonstrably connected to this document's goals, tested for failure, reproducible, reviewed and merged with evidence.
