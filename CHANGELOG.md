# Changelog

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
