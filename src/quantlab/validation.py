"""Fail-closed CSV validation, with no input writes or network access."""

import argparse
import csv
import re
import sys
from collections.abc import Iterable, Iterator, Mapping
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import TextIO

from quantlab.models import INSTRUMENT, Tick

REQUIRED_COLUMNS = ("symbol", "timestamp_utc", "price", "source")
_TIMESTAMP = re.compile(
    r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}"
    r"(?:\.[0-9]{1,6})?(?:Z|[+-][0-9]{2}:[0-9]{2})"
)
_PRICE = re.compile(r"[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?")


class ValidationError(ValueError):
    """An invalid dataset, identified by its physical CSV line."""

    def __init__(self, row: int, reason: str) -> None:
        self.row = row
        super().__init__(f"Row {row}: {reason}")


def parse_tick(record: Mapping[str, str], source_row: int) -> Tick:
    """Parse and validate one row, independently of dataset ordering."""
    if set(record) != set(REQUIRED_COLUMNS):
        raise ValidationError(source_row, "expected fields: " + ", ".join(REQUIRED_COLUMNS))
    for name in REQUIRED_COLUMNS:
        if not record[name] or not record[name].strip():
            raise ValidationError(source_row, f"missing value for {name}")
    if record["symbol"] != INSTRUMENT:
        raise ValidationError(source_row, f"unsupported instrument {record['symbol']!r}")

    raw_price = record["price"]
    try:
        price = Decimal(raw_price)
    except InvalidOperation as exc:
        raise ValidationError(source_row, "price must be a finite positive decimal") from exc
    if not price.is_finite() or price <= 0 or not _PRICE.fullmatch(raw_price):
        raise ValidationError(source_row, "price must be a finite positive decimal")

    raw_timestamp = record["timestamp_utc"]
    if not _TIMESTAMP.fullmatch(raw_timestamp):
        raise ValidationError(
            source_row,
            "timestamp_utc requires ISO 8601 YYYY-MM-DDTHH:MM:SS, "
            "optional 1–6 fractional digits, and explicit timezone (Z or ±HH:MM)",
        )
    try:
        # fromisoformat normalizes overflowing offset minutes, so check them first.
        if raw_timestamp[-1] != "Z" and (
            int(raw_timestamp[-5:-3]) > 23 or int(raw_timestamp[-2:]) > 59
        ):
            raise ValueError("invalid timezone offset")
        timestamp = datetime.fromisoformat(raw_timestamp).astimezone(timezone.utc)
    except (ValueError, OverflowError) as exc:
        raise ValidationError(source_row, "invalid timestamp_utc or timezone offset") from exc
    return Tick(INSTRUMENT, timestamp, price, record["source"], source_row)


def validate_records(records: Iterable[tuple[int, Mapping[str, str]]]) -> Iterator[Tick]:
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


def _csv_records(stream: TextIO) -> Iterator[tuple[int, Mapping[str, str]]]:
    """Check CSV structure and retain each record's starting physical line."""
    reader = csv.reader(stream, strict=True)
    row_number = 1
    try:
        header = next(reader, None)
        if header is None:
            raise ValidationError(1, "empty input: required CSV header is missing")
        if len(header) != len(REQUIRED_COLUMNS) or set(header) != set(REQUIRED_COLUMNS):
            raise ValidationError(1, "required columns exactly: " + ", ".join(REQUIRED_COLUMNS))
        while True:
            row_number = reader.line_num + 1
            values = next(reader, None)
            if values is None:
                break
            if len(values) != len(header):
                raise ValidationError(row_number, f"expected {len(header)} fields, got {len(values)}")
            yield row_number, dict(zip(header, values))
    except (csv.Error, UnicodeError) as exc:
        raise ValidationError(row_number, f"invalid CSV or UTF-8: {exc}") from exc


def iter_csv_ticks(stream: TextIO) -> Iterator[Tick]:
    """Validate a caller-owned text stream, preserving all legitimate duplicates."""
    yield from validate_records(_csv_records(stream))


def load_ticks(path: str | Path) -> list[Tick]:
    """Return a fully validated list using O(n) memory; open input read-only."""
    with Path(path).open("r", encoding="utf-8", newline="") as stream:
        return list(iter_csv_ticks(stream))


def main(argv: list[str] | None = None) -> int:
    """Print a summary only after the entire input passes validation."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("csv_path", type=Path)
    args = parser.parse_args(argv)
    try:
        ticks = load_ticks(args.csv_path)
    except (ValidationError, OSError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1
    print(f"Validated records: {len(ticks)}")
    print(f"Instrument: {INSTRUMENT}")
    print(f"Earliest timestamp: {ticks[0].timestamp_utc.isoformat()}")
    print(f"Latest timestamp: {ticks[-1].timestamp_utc.isoformat()}")
    print(f"Minimum price: {min(tick.price for tick in ticks)}")
    print(f"Maximum price: {max(tick.price for tick in ticks)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
