"""Historical source interpretation uses canonical validation without losing bytes."""

import hashlib
from decimal import Decimal
from io import BytesIO

import pytest

from quantlab.engine import validate_acquisition
from quantlab.historical import HistoricalAdapter, HistoricalProfile
from quantlab.quality import QualityError


def profile(format="csv", **changes):
    value = dict(
        version="historical.v1",
        format=format,
        provider="engineering-fixture",
        provider_symbol="BOOM-export",
        pip_size=2,
        timestamp="iso8601",
        fields={"instrument_id": "symbol", "event_time": "time", "price": "quote"},
        instruments={"Boom export": "BOOM_1000"},
    )
    value.update(changes)
    return HistoricalProfile.from_mapping(value)


def scan(payload, config):
    adapter = HistoricalAdapter(config)
    ctx = adapter.context()
    return list(validate_acquisition(adapter.candidates(BytesIO(payload), ctx), ctx))


def test_mapped_csv_preserves_multiline_evidence_and_repeated_ticks():
    payload = (
        b"symbol,time,quote,note\r\n"
        b'Boom export,2026-01-01T02:00:00+02:00,1000.10,"a\r\nb"\r\n'
        b"Boom export,2026-01-01T00:00:00Z,1000.10,last\r\n"
    )
    rows = scan(payload, profile())
    assert all(row.accepted for row in rows)
    assert [row.raw.source_position for row in rows] == [2, 4]
    assert rows[0].tick.price.as_tuple() == Decimal("1000.10").as_tuple()
    assert rows[0].tick.event_time_utc == rows[1].tick.event_time_utc
    raw = rows[0].raw.source_metadata["adapter"]["header"].encode() + b"".join(
        r.raw.raw_payload for r in rows
    )
    assert raw == payload
    assert rows[0].raw.payload_sha256 == hashlib.sha256(rows[0].raw.raw_payload).hexdigest()


def test_jsonl_exact_numeric_price_optional_id_and_final_line():
    payload = b'{"symbol":"Boom export","time":"2026-01-01T00:00:00Z","quote":1000.10}'
    (row,) = scan(payload, profile("jsonl"))
    assert row.accepted and row.raw.raw_payload == payload
    assert row.tick.price.as_tuple() == Decimal("1000.10").as_tuple()
    assert row.tick.provider_tick_id is None


@pytest.mark.parametrize(
    "payload", [b"{", b"[]\n", b"\n", b'{"x":1,"x":2}\n', b'{"x":NaN}\n', b'{"x":{"y":1}}\n']
)
def test_invalid_json_is_structured(payload):
    with pytest.raises(QualityError):
        scan(payload, profile("jsonl"))


@pytest.mark.parametrize(
    "changes",
    [
        {"timestamp": "guess"},
        {"instruments": {"x": "OTHER"}},
        {"fields": {"event_time": "t", "price": "p"}},
        {"pip_size": True},
        {"unknown": 1},
    ],
)
def test_invalid_profiles_rejected(changes):
    with pytest.raises(ValueError):
        profile(**changes)


@pytest.mark.parametrize(
    "symbol,price,time,code",
    [
        ("BOOM_1000", "10.10", "2026-01-01T00:00:00Z", "UNSUPPORTED_INSTRUMENT"),
        ("Boom export", "-1", "2026-01-01T00:00:00Z", "INVALID_PRICE"),
        ("Boom export", "1.123", "2026-01-01T00:00:00Z", "INVALID_PRECISION"),
        ("Boom export", "1.10", "", "MISSING_REQUIRED_FIELD"),
        ("Boom export", "1.10", "2026-01-01T00:00:00", "INVALID_TIMESTAMP"),
    ],
)
def test_domain_errors_remain_f01_outcomes(symbol, price, time, code):
    (row,) = scan(f"symbol,time,quote\n{symbol},{time},{price}\n".encode(), profile())
    assert not row.accepted and row.events[0].error_code.value == code


def test_json_missing_optional_id_and_present_invalid_id():
    config = profile(
        "jsonl",
        fields={
            "instrument_id": "symbol",
            "event_time": "time",
            "price": "quote",
            "provider_tick_id": "id",
        },
    )
    base = b'{"symbol":"Boom export","time":"2026-01-01T00:00:00Z","quote":1000}'
    (row,) = scan(base, config)
    assert row.accepted and row.tick.price == Decimal("1000") and row.tick.provider_tick_id is None
    with pytest.raises(QualityError, match="provider_tick_id"):
        scan(base[:-1] + b',"id":42}', config)


@pytest.mark.parametrize(
    "payload",
    [
        b"symbol,time\nBoom export,2026-01-01T00:00:00Z\n",
        b'symbol,time,quote\n"unfinished',
        b"symbol,time,quote\nBoom export,2026-01-01T00:00:00Z,1,extra\n",
        b"symbol,symbol,time,quote\na,a,t,p\n",
        b"symbol,time,quote\n" + b"x" * 65537,
    ],
)
def test_csv_schema_and_transport_errors(payload):
    with pytest.raises(QualityError):
        scan(payload, profile())


def test_reversals_and_conflicting_metadata_rejected():
    config = profile(
        fields={
            "instrument_id": "symbol",
            "event_time": "time",
            "price": "quote",
            "provider_symbol": "provider",
            "pip_size": "pips",
        }
    )
    payload = (
        b"symbol,time,quote,provider,pips\n"
        b"Boom export,2026-01-01T00:00:01Z,1.00,BOOM-export,2\n"
        b"Boom export,2026-01-01T00:00:00Z,1.00,BOOM-export,2\n"
        b"Boom export,2026-01-01T00:00:02Z,1.00,wrong,2\n"
        b"Boom export,2026-01-01T00:00:03Z,1.00,BOOM-export,3\n"
    )
    rows = scan(payload, config)
    assert rows[0].accepted
    assert [r.events[0].error_code.value for r in rows[1:]] == [
        "OUT_OF_ORDER_EVENT",
        "INVALID_SOURCE_METADATA",
        "INVALID_PRECISION",
    ]


def test_inspect_scans_invalid_tail_and_missing_source(tmp_path):
    path = tmp_path / "source.jsonl"
    base = b'{"symbol":"Boom export","time":"2026-01-01T00:00:00Z","quote":"1.00"}\n'
    path.write_bytes(base * 50 + b"{")
    adapter = HistoricalAdapter(profile("jsonl"))
    report = adapter.inspect(path)
    assert report["valid"] is False and report["accepted"] == 50
    assert report["source_sha256"] == hashlib.sha256(path.read_bytes()).hexdigest()
    with pytest.raises(FileNotFoundError):
        adapter.inspect(tmp_path / "missing")


def test_profile_loader_rejects_duplicate_keys_and_excess_size(tmp_path):
    path = tmp_path / "profile.json"
    for payload in [b'{"version":1,"version":2}', b"x" * 16385, b"{"]:
        path.write_bytes(payload)
        with pytest.raises(ValueError):
            HistoricalProfile.load(path)


@pytest.mark.parametrize("identifier", [b'"\\ud800"', b'"\\u0000"', b"42"])
def test_unpersistable_json_identifiers_are_structured(identifier):
    config = profile(
        "jsonl",
        fields={
            "instrument_id": "symbol",
            "event_time": "time",
            "price": "quote",
            "provider_tick_id": "id",
        },
    )
    payload = (
        b'{"symbol":"Boom export","time":"2026-01-01T00:00:00Z",'
        b'"quote":"1.00","id":' + identifier + b"}"
    )
    with pytest.raises(QualityError):
        scan(payload, config)


def test_csv_nul_rejected_before_persistence():
    with pytest.raises(QualityError):
        scan(b"symbol,time,quote\nBoom export,2026-01-01T00:00:00Z,1.00\x00\n", profile())
