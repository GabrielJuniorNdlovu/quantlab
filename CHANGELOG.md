# Changelog

## 0.4.0

- Add PostgreSQL/TimescaleDB storage with additive packaged Alembic migrations.
- Preserve full original sources and exact raw observations with SHA-256 evidence.
- Persist canonical ticks, quality events, batch receipts, and progress atomically.
- Publish completed datasets and retain interrupted attempts during restart recovery.
- Add bounded event batching, idle deadlines, backpressure, retries, and JSON diagnostics.
- Provide operational commands and bounded completed-dataset replay factories.
- Add pinned Docker Compose deployment, database/failure tests, benchmarks, and runbooks.

## 0.3.0

- Add explicit-clock historical replay over the existing canonical tick contract.
- Build exact Decimal M1 candles without fabricated gap candles or EOF completion.
- Issue predetermined alerts with fixed anchors and separate immutable outcomes.
- Add an offline Tkinter chart with playback controls and headless geometry checks.
- Record exclusive append-only JSONL runs with integrity and lifecycle verification.
- Package a small engineering fixture and preserve successful, unsuccessful, and
  insufficient-window alerts.
- Add native chart checks, replay benchmarks, architecture and usage documentation.
- Include an MQL5 arrow prototype with native compilation/runtime explicitly unverified.

## 0.2.0

- Add versioned raw observations, canonical market ticks, acquisition context,
  and structured quality events for Feature 01.
- Separate binary CSV transport from source-independent incremental validation.
- Preserve exact raw record bytes, hashes, physical lines, and byte offsets.
- Require explicit provider symbol and quote precision for canonical validation.
- Keep the original CSV adapter and imports as limited-fidelity compatibility.
- Stream CLI summaries without retaining the dataset.
- Label legacy CLI success output and reject oversized records, metadata, and
  numeric representations. Reject malformed unquoted CSV quotes explicitly.
- Add property tests, static checks, pinned development dependencies, CI,
  architecture documentation, and measured engineering benchmarks.

## 0.1.0

- Validate local four-column Boom 1000 Index tick CSV files with Decimal prices,
  UTC timestamps, physical line provenance, and duplicate preservation.
