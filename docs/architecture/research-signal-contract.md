# Research signal contract reference

`quantlab.signals.ResearchSignal` is a frozen, slotted dataclass with wire schema
`research_signal.v1`. It represents synthetic research arrows. `FIXTURE_UP` is the
only accepted signal type in this feature. No evaluated forecast or trading
signal is implied. `quantlab.alerts` re-exports the class for existing import paths.

## Fields

- `signal_id`: explicit UUID, stable across equivalent replays. The packaged
  fixture derives UUID5 from dataset identity, source position, and producer version.
- `schema_version`: literal `research_signal.v1`, explicitly included on the wire.
- `instrument_id`: F01 internal identifier `BOOM_1000`.
- `signal_type`: `FIXTURE_UP`.
- `issued_at_utc`: exact simulated instant when the signal becomes available.
- `input_cutoff_utc`: provider event timestamp of the most recent used observation.
- `input_cutoff_source_position`: exact positive position within the source dataset.
  This distinguishes observations with identical event timestamps.
- `bar_open_time_utc`: UTC minute containing the cutoff event. It is a display
  anchor and can precede the issuance minute for delayed observations.
- `observed_price_at_issue`: exact canonical Decimal price of the current input.
- `source_dataset_id`: immutable source identifier. Sessions default to the source
  SHA-256; persisted-dataset sessions use the completed F03 dataset UUID.
- `producer_version`: bounded nonblank producer identity and version.
- `metadata`: copied, recursively immutable JSON-compatible values, using F01's
  depth and 16 KiB size limits. NUL, invalid Unicode, and nonfinite numbers fail.

## Issuance checks

The constructor enforces domain types, UTC normalization, price validity, event
cutoff no later than issuance, and minute-aligned observed-bar anchoring. In the
session, `check_observation` additionally requires exact equality with the current
released event timestamp, source position, instrument, dataset, price coefficient
and exponent, and logical availability time. The session provides the observation;
standalone JSON verification alone cannot establish truth against source ticks.

Earlier input cutoffs and deferred issuance require a future explicitly evidenced
interface. Current producers receive a single released `Observation` and can keep
causal state from past calls. They do not receive future ticks or candles.

## Python use

```python
from uuid import UUID
from quantlab.alerts import ResearchSignal


def fixture(observation):
	return (ResearchSignal.issue(
		signal_id=UUID("00000000-0000-0000-0000-000000000001"),
		observation=observation,
		producer_version="example.fixture.v1",
		metadata={"purpose": "synthetic visualization test"},
	),)
```

Call `signal.to_dict()` or `signal.to_json()` to export. Call
`ResearchSignal.from_json(text)` to validate standalone interchange. The decoder
rejects unknown or missing fields, duplicate keys, unsupported versions, numeric
prices, invalid UTF-8 scalar text, nonfinite values, and records above 128 KiB.
Metadata float values retain JSON float semantics; all prices use Decimal strings.

The [machine-readable example](../verification/research-signal.v1.json) is generated
from the first packaged synthetic signal. UTC timestamps use ISO 8601 with
`+00:00` and up to six fractional digits. Decimal strings preserve trailing zeros
and never pass through binary floating point. JSON keys are sorted, and Unicode
uses ASCII escaping. No Python imports are needed to consume the JSON object.

## Audit events and identity

`audit_event.v1` remains the outer JSONL envelope. Issuance and render payloads
contain the full signal, including its explicit schema version. Outcome and expiry
payloads contain signal, instrument, dataset, and producer identity, current source
position, and separately frozen external metadata. Their event time comes from
the logical replay availability clock.

`signal_issued` is immutable. `signal_outcome_recorded` and `signal_expired` append
facts without rewriting it. Each signal has at most one terminal record in a
session. Identical issuance redelivery produces no duplicate record or arrow;
conflicting serialized content under the same ID fails, including altered Decimal
precision. Public issuance during event callbacks is rejected; producers return
signal tuples instead. Restart creates another run, preserving previous files.
Deduplication is per run, not a cross-process delivery guarantee.

## Future read-only MT5 consumer

A separate consumer can read the JSON object, deduplicate `signal_id`, map internal
instrument identity using its own verified mapping, and wait until `issued_at_utc`
and the exact observation cutoff before displaying the bar anchor and price.
It must retain the actual issuance instant even when the anchor is earlier.
F02 defines the contract only. No native MQL5 parser, EA, live bridge, push alerts,
or order execution is implemented or verified here.

## Legacy migration

`ReplayAlert` remains a finite fixture schedule, not the new issued-signal
contract. `IssuedAlert` remains a compatible immutable chart projection with
read-only aliases for the shared display fields. Existing imports and original
fixture commands remain valid. New producers and interchange consumers use
`ResearchSignal` directly. Old `alert_issued` audit events remain verifiable;
canonical research runs write `signal_issued` instead.
