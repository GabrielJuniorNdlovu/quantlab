# Feature 01 refactor verification

The initial refactor started from verified `main` commit `2f86154` on
`refactor/01-market-data-contract-v1`, and the unified validation refactor
starts from verified `main` commit `c067994` on
`refactor/01-unified-market-data-validation`. `main` was not merged or rewritten.

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
5. Unify candidate normalization (`normalize_mapped_record`) and stateful
   single-acquisition validation (`AcquisitionValidator`) across historical CSV,
   historical JSON Lines, and recorded streaming JSON (`stream_jsonl` /
   `SourceType.WEBSOCKET`), and eliminate duplicated domain checks across
   Features 01–04.
6. Run static checks, independent review, engineering benchmarks, and isolated
   TimescaleDB integration checks.

## Unified validation and downstream deduplication (`refactor/01-unified-market-data-validation`)

1. **Machine-readable diagnostics (`src/quantlab/quality.py`):**
   - Added `ErrorCode.INVALID_INSTRUMENT` as a canonical enum alias for `ErrorCode.UNSUPPORTED_INSTRUMENT` (`"UNSUPPORTED_INSTRUMENT"`), allowing callers to match either `ErrorCode.INVALID_INSTRUMENT` or `ErrorCode.UNSUPPORTED_INSTRUMENT` without breaking persisted quality events or existing consumers.
   - Added `redact_observed_value(value, *, limit=64)` to sanitize control characters and UTF-16 surrogates and bound length (`<= 128` chars) when callers attach `observed_value` via `quality_event(..., observed_value=...)`.
2. **Centralized domain value rules (`src/quantlab/parsing.py`, `src/quantlab/models.py`):**
   - Hardened `required_text` to reject NUL (`\x00`) and lone UTF-16 surrogates (`UnicodeEncodeError` on `.encode("utf-8")`) centrally.
   - Added `require_instrument_id` and `require_decimal_price` in `quantlab.parsing` and reused them across `MarketTick`, `Candle`, `ReplayAlert`, `IssuedAlert`, and `HistoricalProfile`.
   - Added `SourceType.expected_format` (`SourceFormat.CSV` for `HISTORICAL_CSV`, `SourceFormat.JSON` for `HISTORICAL_JSON` and `WEBSOCKET`) and reused it in `engine.py` and `storage/repository.py`.
   - Added `MarketTick.market_fields()` and `MarketTick.equivalent_market_fields(other)` to compare canonical market fields (`instrument_id`, `provider_symbol`, `event_time_utc`, `price.as_tuple()`, `pip_size`) independently of transport provenance (`source_type`, `capture_id`, `source_position`, `received_at_utc`, `provider_tick_id`).
3. **Incremental acquisition validator & candidate normalization (`src/quantlab/engine.py`, `src/quantlab/historical.py`, `src/quantlab/historical_json.py`, `src/quantlab/storage/source_evidence.py`):**
   - Extracted `AcquisitionValidator` in `quantlab.engine` so streaming/incremental callers and batch `validate_acquisition` share identical O(1) ordering state.
   - Added `normalize_mapped_record` in `quantlab.engine` to unify candidate normalization across historical CSV, historical JSON Lines, and recorded streaming JSON messages.
   - Extended `HistoricalProfile`, `HistoricalAdapter`, `iter_json_records`, and `verify_complete_source` to support `"stream_jsonl"` (`SourceType.WEBSOCKET`) with dotted envelope field paths (e.g., `"tick.symbol"`, `"tick.time"`, `"tick.quote"`, `"tick.id"`) and optional per-message `"received_at_utc"`.

## Tests and static analysis

The final isolated environment uses CPython 3.14.4 and the exact versions in
`requirements-dev.lock`. The host exports ROS packages through `PYTHONPATH`, so
local isolated checks unset that variable. This excludes unrelated host packages
without modifying them.

Executed commands include:

```bash
env -u PYTHONPATH .venv/bin/python -m pytest -q
env -u PYTHONPATH .venv/bin/python -m ruff check .
env -u PYTHONPATH .venv/bin/python -m ruff format --check .
env -u PYTHONPATH .venv/bin/python -m mypy
```

Actual results:

- Offline Pytest suite: **366 passed, 45 skipped in 3.30 seconds**.
- Full Pytest suite with isolated TimescaleDB 2.31.0 / PostgreSQL 18 integration: **408 passed, 4 skipped in 34.50 seconds**.
- Ruff: lint passes; all 68 Python files meet formatting checks.
- Mypy: strict checks pass for all 39 runtime source files, targeting Python 3.11.

Four deterministic Hypothesis properties use up to 60 examples each for Decimal
representation, UTC offsets, repeated-observation retention, and invalid quote
precision. Contract tests verify canonical equivalence across historical CSV,
historical JSON Lines, and recorded streaming JSON messages.

## Failing-before evidence and review

The new contract and recorded-streaming tests initially failed collection with
`ImportError: cannot import name 'AcquisitionValidator' from 'quantlab.engine'`.
After implementation, all contract, historical, and database integration tests
passed.

Independent strict review identified one **Major** transport defect during the
initial contract pass (`csv.reader(strict=True)` accepted a quote inside an
unquoted field, fixed via `csv_syntax.py`) and duplicated NUL/UTF-8, instrument,
Decimal price, format-mapping, and candidate-normalization checks across
Features 01–04 during the unified validation pass (fixed by centralizing in
`parsing.py`, `models.py`, and `engine.py`).

No unresolved Critical, Major, or Minor defect was identified in the scoped
final review.

## Performance evidence

The [benchmark report](../../benchmarks/results.md) records the reproduction
command, environment, five-run medians and ranges, rejection overhead, two input
sizes, separate memory samples, CPU evidence, and interpretation limits.
All measured runtime-module hashes match the delivered source.

At 100,000 valid rows, core validation had a median of 2.8758 seconds (34,773
validated observations/s, traced peak 10,793 bytes) and full CSV validation had
a median of 6.5287 seconds (15,317 validated observations/s, traced peak 162,008
bytes). This measures the full engineered validation path, not a production
ingestion service or exact CLI process startup latency.

## Remaining gates and limitations

The local Feature 01 refactor satisfies the Definition of Done. Hosted CI is
configured for Python 3.11 and 3.14. Human review, hosted CI, and merge into
`main` remain delivery gates under `goal.md`.

Runtime limitations are explicit microsecond precision, bounded numeric and
metadata representations, caller-reviewed provider configuration, and explicit
retained datasets across separate acquisitions without automatic overlap merging.
No live broker connection, authentication, forecasting model, or trading
execution feature was introduced.
