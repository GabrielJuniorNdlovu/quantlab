"""Explicit legacy CSV compatibility; results have no verified provider metadata."""

import csv
from collections.abc import Iterable, Iterator, Mapping
from datetime import datetime
from pathlib import Path
from typing import TextIO

from quantlab.csv_syntax import is_csv_record
from quantlab.models import INSTRUMENT, MAX_RAW_BYTES, LegacyTick
from quantlab.parsing import parse_price, parse_timestamp, require_position, required_text
from quantlab.quality import ContractError

REQUIRED_COLUMNS = ("symbol", "timestamp_utc", "price", "source")


class ValidationError(ValueError):
    """An invalid dataset, identified by its physical CSV line."""

    def __init__(self, row: int, reason: str) -> None:
        self.row = row
        super().__init__(f"Row {row}: {reason}")


def parse_tick(record: Mapping[str, str], source_row: int) -> LegacyTick:
    """Validate limited-fidelity values; never infer canonical precision/symbol."""
    if set(record) != set(REQUIRED_COLUMNS):
        raise ValidationError(source_row, "expected fields: " + ", ".join(REQUIRED_COLUMNS))
    try:
        require_position(source_row)
        for name in REQUIRED_COLUMNS:
            required_text(record[name], name)
        if record["symbol"] != INSTRUMENT:
            raise ValidationError(source_row, "unsupported instrument")
        price = parse_price(record["price"])
        timestamp = parse_timestamp(record["timestamp_utc"], "timestamp_utc")
    except ContractError as exc:
        raise ValidationError(source_row, exc.description) from exc
    return LegacyTick(INSTRUMENT, timestamp, price, record["source"], source_row)


def validate_records(records: Iterable[tuple[int, Mapping[str, str]]]) -> Iterator[LegacyTick]:
    """Yield ticks in non-decreasing UTC order using O(1) validation state.

    Earlier yields are provisional: exhaust the iterator before accepting the
    dataset or committing downstream results. Invalid records always raise.
    """
    previous: datetime | None = None
    for source_row, record in records:
        tick = parse_tick(record, source_row)
        if previous is not None and tick.timestamp_utc < previous:
            raise ValidationError(source_row, "timestamp_utc is out of order")
        previous = tick.timestamp_utc
        yield tick
    if previous is None:
        raise ValidationError(2, "empty dataset: no tick records")


class _LegacyLines:
    """Bound text records even when a quoted field spans many physical lines."""

    def __init__(self, stream: TextIO) -> None:
        self.stream = stream
        self.size = 0
        self.record: list[str] = []

    def __iter__(self) -> "_LegacyLines":
        return self

    def __next__(self) -> str:
        line = self.stream.readline(MAX_RAW_BYTES - self.size + 1)
        if not line:
            raise StopIteration
        self.size += len(line)
        if self.size > MAX_RAW_BYTES:
            raise csv.Error("legacy CSV record exceeds 65536 characters")
        self.record.append(line)
        return line


def _csv_records(stream: TextIO) -> Iterator[tuple[int, Mapping[str, str]]]:
    """Check CSV structure and retain each record's starting physical line."""
    lines = _LegacyLines(stream)
    reader = csv.reader(lines, strict=True)
    row_number = 1
    try:
        header = next(reader, None)
        if header is None:
            raise ValidationError(1, "empty input: required CSV header is missing")
        if not is_csv_record("".join(lines.record)):
            raise ValidationError(1, "invalid CSV header quote placement")
        if len(header) != len(REQUIRED_COLUMNS) or set(header) != set(REQUIRED_COLUMNS):
            raise ValidationError(1, "required columns exactly: " + ", ".join(REQUIRED_COLUMNS))
        while True:
            row_number = reader.line_num + 1
            lines.size = 0
            lines.record.clear()
            values = next(reader, None)
            if values is None:
                break
            if not is_csv_record("".join(lines.record)):
                raise ValidationError(row_number, "invalid CSV quote placement")
            if len(values) != len(header):
                raise ValidationError(
                    row_number, f"expected {len(header)} fields, got {len(values)}"
                )
            yield row_number, dict(zip(header, values, strict=True))
    except (csv.Error, UnicodeError) as exc:
        raise ValidationError(
            row_number, "invalid CSV or UTF-8, or record exceeds size limit"
        ) from exc


def iter_legacy_csv_ticks(stream: TextIO) -> Iterator[LegacyTick]:
    """Validate a text stream, explicitly returning limited-fidelity legacy ticks."""
    yield from validate_records(_csv_records(stream))


iter_csv_ticks = iter_legacy_csv_ticks


def load_ticks(path: str | Path) -> list[LegacyTick]:
    """Return a fully validated list using O(n) memory; open input read-only."""
    with Path(path).open("r", encoding="utf-8", newline="") as stream:
        return list(iter_legacy_csv_ticks(stream))
