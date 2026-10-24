"""Deterministic integrity and CLI regression tests, independent of services."""

import csv
import io
import subprocess
import sys
from dataclasses import FrozenInstanceError
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from quantlab.models import INSTRUMENT
from quantlab.validation import (
    REQUIRED_COLUMNS,
    ValidationError,
    iter_csv_ticks,
    load_ticks,
    main,
    parse_tick,
    validate_records,
)


def record(**changes: str) -> dict[str, str]:
    return dict(
        symbol=INSTRUMENT, timestamp_utc="2026-01-01T00:00:00Z",
        price="1000.10", source="local-fixture",
    ) | changes


def csv_text(rows: list[list[str]], header: tuple[str, ...] = REQUIRED_COLUMNS) -> str:
    stream = io.StringIO(newline="")
    writer = csv.writer(stream)
    writer.writerow(header)
    writer.writerows(rows)
    return stream.getvalue()


def test_valid_fixture_preserves_duplicates_and_provenance(fixture_dir: Path) -> None:
    path = fixture_dir / "valid_ticks.csv"
    original = path.read_bytes()
    ticks = load_ticks(path)
    assert len(ticks) == 4
    assert ticks[0].timestamp_utc == ticks[1].timestamp_utc == ticks[2].timestamp_utc
    assert ticks[0].price == ticks[1].price == Decimal("1000.10")
    assert isinstance(ticks[0].price, Decimal)
    assert ticks[0].timestamp_utc == datetime(2026, 1, 1, tzinfo=timezone.utc)
    assert ticks[0].timestamp_utc.tzinfo is timezone.utc
    assert [tick.source_row for tick in ticks] == [2, 3, 4, 5]
    assert all(tick.source == "deterministic-fixture" for tick in ticks)
    assert path.read_bytes() == original
    with pytest.raises(FrozenInstanceError):
        ticks[0].price = Decimal("1")  # type: ignore[misc]


@pytest.mark.parametrize("header", [
    REQUIRED_COLUMNS[:-1], REQUIRED_COLUMNS + ("extra",),
    ("symbol", "timestamp_utc", "price", "price"),
    ("symbol", "timestamp_utc", "price", "Source"),
])
def test_incorrect_schema(header: tuple[str, ...]) -> None:
    with pytest.raises(ValidationError, match="Row 1: required columns"):
        list(iter_csv_ticks(io.StringIO(csv_text([], header))))


@pytest.mark.parametrize("text, row", [("", 1), ("\n", 1), (csv_text([]), 2)])
def test_empty_input(text: str, row: int) -> None:
    with pytest.raises(ValidationError) as error:
        list(iter_csv_ticks(io.StringIO(text)))
    assert error.value.row == row


@pytest.mark.parametrize("field", REQUIRED_COLUMNS)
@pytest.mark.parametrize("value", ["", "   "])
def test_missing_values(field: str, value: str) -> None:
    with pytest.raises(ValidationError, match=f"Row 7: missing value for {field}"):
        parse_tick(record(**{field: value}), 7)


@pytest.mark.parametrize("price", [
    "-1", "0", "-0", "NaN", "sNaN", "Infinity", "-Infinity", "inf",
    "null", "None", "abc", "1_000", " 1.0", "1.0 ",
])
def test_invalid_prices(price: str) -> None:
    with pytest.raises(ValidationError, match="Row 9: price must be a finite positive decimal"):
        parse_tick(record(price=price), 9)


def test_decimal_precision_and_exponents() -> None:
    price = "123456789.123456789123456789123456789"
    assert str(parse_tick(record(price=price), 2).price) == price
    assert parse_tick(record(price="1e-8"), 2).price == Decimal("0.00000001")


@pytest.mark.parametrize("timestamp", [
    "not-a-time", "2026-01-01T00:00:00", "2026-01-01", "2026-02-30T00:00:00Z",
    "2026-01-01T25:00:00Z", "2026-01-01T00:00:00+24:00",
    "2026-01-01T00:00:00+00:60", "2026-01-01T00:00:60Z",
    "2026-01-01T00:00:00.1234567Z", "0001-01-01T00:00:00+01:00",
    "9999-12-31T23:59:59-01:00",
])
def test_invalid_timestamps(timestamp: str) -> None:
    with pytest.raises(ValidationError, match="Row 4: .*timestamp_utc"):
        parse_tick(record(timestamp_utc=timestamp), 4)


@pytest.mark.parametrize("timestamp", [
    "2026-01-01T02:00:00+02:00", "2025-12-31T19:00:00-05:00",
    "2026-01-01T05:30:00+05:30", "2026-01-01T00:00:00Z",
])
def test_normalizes_offsets(timestamp: str) -> None:
    assert parse_tick(record(timestamp_utc=timestamp), 2).timestamp_utc == datetime(
        2026, 1, 1, tzinfo=timezone.utc
    )


def test_order_uses_utc_and_rejects_decrease() -> None:
    records = [(2, record()), (3, record(timestamp_utc="2026-01-01T01:59:59+02:00"))]
    with pytest.raises(ValidationError, match="Row 3: timestamp_utc is out of order"):
        list(validate_records(records))


@pytest.mark.parametrize("symbol", ["BOOM1000", "Boom 500 Index", "boom 1000 index"])
def test_unsupported_symbols(symbol: str) -> None:
    with pytest.raises(ValidationError, match="unsupported instrument"):
        parse_tick(record(symbol=symbol), 2)


@pytest.mark.parametrize("values", [[], ["one"], list(record().values()) + ["extra"]])
def test_row_width_and_blank_rows(values: list[str]) -> None:
    with pytest.raises(ValidationError, match="Row 2: expected 4 fields"):
        list(iter_csv_ticks(io.StringIO(csv_text([values]), newline="")))


def test_multiline_source_reports_physical_start_line() -> None:
    rows = [list(record(source="origin\npart-two").values()), list(record(price="bad").values())]
    stream = iter_csv_ticks(io.StringIO(csv_text(rows), newline=""))
    first = next(stream)
    assert first.source_row == 2
    assert first.source == "origin\npart-two"
    with pytest.raises(ValidationError, match="Row 4: price"):
        next(stream)


def test_reordered_columns() -> None:
    header = tuple(reversed(REQUIRED_COLUMNS))
    ticks = list(iter_csv_ticks(io.StringIO(csv_text([[record()[key] for key in header]], header))))
    assert ticks[0] == parse_tick(record(), 2)


def test_malformed_csv() -> None:
    with pytest.raises(ValidationError, match="Row 2: invalid CSV"):
        list(iter_csv_ticks(io.StringIO(csv_text([]) + '"unterminated')))


def test_invalid_encoding(tmp_path: Path) -> None:
    path = tmp_path / "bad.csv"
    path.write_bytes(b"\xff")
    with pytest.raises(ValidationError, match="invalid CSV or UTF-8"):
        load_ticks(path)


def test_record_schema_and_empty_iterator() -> None:
    with pytest.raises(ValidationError, match="Row 5: expected fields"):
        parse_tick({"price": "1"}, 5)
    with pytest.raises(ValidationError, match="empty dataset"):
        list(validate_records([]))


def test_cli_summary(fixture_dir: Path, capsys: pytest.CaptureFixture[str]) -> None:
    assert main([str(fixture_dir / "valid_ticks.csv")]) == 0
    output = capsys.readouterr()
    assert output.err == ""
    for expected in [
        "Validated records: 4", f"Instrument: {INSTRUMENT}",
        "Earliest timestamp: 2026-01-01T00:00:00+00:00",
        "Latest timestamp: 2026-01-01T00:00:01.123456+00:00",
        "Minimum price: 999.90", "Maximum price: 1000.20",
    ]:
        assert expected in output.out


@pytest.mark.parametrize("filename", ["invalid_ticks.csv", "nonexistent.csv"])
def test_cli_failure(fixture_dir: Path, filename: str, capsys: pytest.CaptureFixture[str]) -> None:
    assert main([str(fixture_dir / filename)]) == 1
    output = capsys.readouterr()
    assert output.out == ""
    assert "Error:" in output.err
    if filename == "invalid_ticks.csv":
        assert "Row 3:" in output.err


@pytest.mark.parametrize("filename, status", [("valid_ticks.csv", 0), ("invalid_ticks.csv", 1)])
def test_module_cli(fixture_dir: Path, tmp_path: Path, filename: str, status: int) -> None:
    result = subprocess.run(
        [sys.executable, "-m", "quantlab.validation", str(fixture_dir / filename)],
        cwd=tmp_path, capture_output=True, text=True, check=False,
    )
    assert result.returncode == status, result.stderr
