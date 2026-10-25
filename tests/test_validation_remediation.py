"""Regressions for reception-time parity and PostgreSQL-safe validation contracts."""

import json
from copy import deepcopy
from dataclasses import replace
from datetime import datetime, timezone
from io import BytesIO
from uuid import uuid4

import pytest
from test_contracts import candidate, context, raw
from test_historical import profile

from quantlab.engine import normalize_mapped_record, validate_acquisition
from quantlab.historical import HistoricalAdapter
from quantlab.quality import ContractError, ErrorCode, QualityError, Severity, quality_event

FIELDS = {
    "instrument_id": "symbol",
    "event_time": "time",
    "price": "quote",
    "received_at_utc": "received",
}


def observation(format, received="2026-01-01T02:00:00.123456+02:00", *, omit=False):
    """Synthetic MT5-style quote recording, not a verified provider payload."""
    record = {"symbol": "Boom export", "time": "2026-01-01T00:00:00.123456Z", "quote": "1000.10"}
    if not omit:
        record["received"] = received
    fields = FIELDS
    if format == "csv":
        payload = (
            "symbol,time,quote,received\nBoom export,"
            + record["time"]
            + ",1000.10,"
            + str(received or "")
            + "\n"
        ).encode()
    elif format == "stream_jsonl":
        record = {"tick": record}
        fields = {k: "tick." + v for k, v in FIELDS.items()}
        payload = json.dumps(record).encode()
    else:
        payload = json.dumps(record).encode()
    adapter = HistoricalAdapter(profile(format, fields=fields))
    ctx = adapter.context()
    rows = list(validate_acquisition(adapter.candidates(BytesIO(payload), ctx), ctx))
    return payload, rows


@pytest.mark.parametrize("format", ["csv", "jsonl", "stream_jsonl"])
def test_reception_time_is_normalized_without_changing_event_or_payload(format):
    payload, (row,) = observation(format)
    assert row.accepted and not row.events
    assert row.tick.received_at_utc == datetime(2026, 1, 1, 0, 0, 0, 123456, tzinfo=timezone.utc)
    assert row.tick.event_time_utc == row.tick.received_at_utc
    assert row.raw.received_at_utc == row.tick.received_at_utc
    expected = payload.split(b"\n", 1)[1] if format == "csv" else payload
    assert row.raw.raw_payload == expected


@pytest.mark.parametrize("format", ["csv", "jsonl", "stream_jsonl"])
@pytest.mark.parametrize(
    "received",
    ["not-a-time", "2026-01-01T00:00:00", "2026-01-01T00:00:00.1234567Z", "2026-01-01T25:00:00Z"],
)
def test_invalid_reception_is_a_structured_field_error(format, received):
    with pytest.raises(QualityError) as error:
        observation(format, received)
    event = error.value.event
    assert event.error_code is ErrorCode.INVALID_TIMESTAMP
    assert event.field_name == "received_at_utc"
    assert event.source_position == (2 if format == "csv" else 1)
    assert event.capture_id and event.description and event.severity is Severity.FATAL


@pytest.mark.parametrize("format", ["csv", "jsonl", "stream_jsonl"])
def test_optional_reception_is_never_fabricated(format):
    _, (row,) = observation(format, "")
    assert row.tick.received_at_utc is None and row.raw.received_at_utc is None
    if format != "csv":
        _, (row,) = observation(format, omit=True)
        assert row.tick.received_at_utc is None


@pytest.mark.parametrize("format", ["csv", "jsonl", "stream_jsonl"])
def test_supplied_pre_event_reception_generates_same_warning(format):
    _, (row,) = observation(format, "2025-12-31T23:59:59Z")
    assert row.accepted
    assert row.events[0].error_code is ErrorCode.RECEPTION_BEFORE_EVENT
    assert row.events[0].field_name == "received_at_utc"


def test_missing_configured_csv_reception_header_is_schema_failure():
    adapter = HistoricalAdapter(profile(fields=FIELDS))
    with pytest.raises(QualityError) as error:
        list(
            adapter.candidates(
                BytesIO(b"symbol,time,quote\nBoom export,t,1.00\n"), adapter.context()
            )
        )
    assert error.value.event.error_code is ErrorCode.INVALID_CSV_SCHEMA


def test_normalization_preserves_explicit_context_fallback_and_aware_native_time():
    ctx = context(received_at_utc=datetime(2026, 1, 2, tzinfo=timezone.utc))
    item = candidate()
    mapped = {"instrument_id": "external", "event_time": item.event_time, "price": item.price}
    tick = normalize_mapped_record(item.raw, ctx, mapped, {"external": "BOOM_1000"})
    from quantlab.engine import validate_tick

    assert validate_tick(tick, ctx).tick.received_at_utc == ctx.received_at_utc
    mapped["received_at_utc"] = datetime(2026, 1, 3, tzinfo=timezone.utc)
    tick = normalize_mapped_record(item.raw, ctx, mapped, {"external": "BOOM_1000"})
    assert tick.raw.received_at_utc == mapped["received_at_utc"]


@pytest.mark.parametrize(
    "metadata",
    [{"note": "bad\x00value"}, {"bad\x00key": "value"}, {"nested": [{"note": "bad\x00value"}]}],
)
def test_metadata_rejects_nul_without_sanitizing_provenance(metadata):
    original = deepcopy(metadata)
    for construct in (
        lambda: context(source_metadata=metadata),
        lambda: raw(source_metadata=metadata),
    ):
        with pytest.raises(ContractError) as error:
            construct()
        assert error.value.code is ErrorCode.INVALID_SOURCE_METADATA
        assert error.value.field_name == "source_metadata"
    assert metadata == original


@pytest.mark.parametrize("field", ["description", "field_name", "observed_value"])
@pytest.mark.parametrize("bad", ["bad\x00value", "bad\ud800value"])
def test_direct_quality_records_reject_unpersistable_text(field, bad):
    event = quality_event(uuid4(), 1, ErrorCode.INVALID_PRICE, "invalid price", field_name="price")
    with pytest.raises(ValueError):
        replace(event, **{field: bad})


def test_quality_description_requires_text_but_optional_fields_allow_none():
    event = quality_event(uuid4(), 1, ErrorCode.INVALID_PRICE, "invalid price")
    assert event.field_name is event.observed_value is None
    with pytest.raises(ValueError):
        replace(event, description=None)


@pytest.mark.parametrize("basis", ["bid", "ask", "last"])
def test_synthetic_recording_keeps_price_basis_and_market_fields(basis):
    from decimal import Decimal
    from hashlib import sha256

    from quantlab.models import SourceFormat, SourceType

    ticks = []
    for source_type, source_format, event in (
        (SourceType.HISTORICAL_CSV, SourceFormat.CSV, "2026-01-01T02:00:00.123456+02:00"),
        (SourceType.WEBSOCKET, SourceFormat.JSON, "2026-01-01T00:00:00.123456Z"),
    ):
        metadata = {"price_basis": basis, "recording": "synthetic MT5-style engineering fixture"}
        ctx = context(capture_id=uuid4(), source_type=source_type, source_metadata=metadata)
        # These are explicit synthetic records, not native MqlTick/provider payload claims.
        payload = (
            f"synthetic-boom,{event},1000.10,{basis}\n".encode()
            if source_format is SourceFormat.CSV
            else json.dumps(
                {
                    "symbol": "synthetic-boom",
                    "event": event,
                    "quote": "1000.10",
                    "price_basis": basis,
                }
            ).encode()
        )
        envelope = raw(
            capture_id=ctx.capture_id,
            source_format=source_format,
            raw_payload=payload,
            payload_sha256=sha256(payload).hexdigest(),
            source_metadata=metadata,
        )
        mapped = {"instrument_id": "synthetic-boom", "event_time": event, "price": "1000.10"}
        candidates = [
            normalize_mapped_record(
                replace(envelope, source_position=p), ctx, mapped, {"synthetic-boom": "BOOM_1000"}
            )
            for p in (1, 2)
        ]
        rows = list(validate_acquisition(candidates, ctx))
        assert all(row.accepted for row in rows)
        assert all(row.raw.source_metadata["price_basis"] == basis for row in rows)
        assert [row.tick.source_position for row in rows] == [1, 2]
        assert rows[0].tick.price.as_tuple() == Decimal("1000.10").as_tuple()
        assert rows[0].raw.raw_payload == payload
        ticks.append(rows[0].tick)
    assert ticks[0].market_fields() == ticks[1].market_fields()
    assert ticks[0].capture_id != ticks[1].capture_id


@pytest.mark.parametrize("format", ["jsonl", "stream_jsonl"])
@pytest.mark.parametrize("received", [True, 42, 1.5])
def test_non_text_json_reception_rejects_with_canonical_field(format, received):
    with pytest.raises(QualityError) as error:
        observation(format, received)
    assert error.value.event.error_code is ErrorCode.INVALID_TIMESTAMP
    assert error.value.event.field_name == "received_at_utc"


def test_csv_reception_correction_has_distinct_validation_identity():
    from quantlab.storage.contracts import configuration

    adapter = HistoricalAdapter(profile("csv", fields=FIELDS))
    version = configuration(adapter.context(), "fixture")["validation_version"]
    assert version != "canonical.v1"
    for format in ("csv", "jsonl", "stream_jsonl"):
        assert (
            configuration(HistoricalAdapter(profile(format)).context(), "fixture")[
                "validation_version"
            ]
            == "canonical.v1"
        )


def test_direct_json_reader_reception_keyword_remains_compatible():
    from quantlab.historical_json import iter_json_records

    adapter = HistoricalAdapter(profile("jsonl"))
    payload = (
        b'{"symbol":"Boom export","time":"2026-01-01T00:00:00Z",'
        b'"quote":"1000.10","received":"2026-01-01T02:00:00.123456+02:00"}'
    )
    raw_record, _ = next(
        iter_json_records(
            BytesIO(payload),
            adapter.context(),
            ("symbol", "time", "quote"),
            optional_columns=("received",),
            received_at_column="received",
        )
    )
    assert raw_record.received_at_utc == datetime(2026, 1, 1, 0, 0, 0, 123456, tzinfo=timezone.utc)
    assert raw_record.raw_payload == payload
