"""File I/O and streaming summaries for canonical and legacy CSV validation."""

import argparse
import sys
from collections.abc import Iterator
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from uuid import UUID

from quantlab.csv_adapter import iter_csv_candidates
from quantlab.engine import validate_acquisition
from quantlab.legacy import ValidationError, iter_legacy_csv_ticks
from quantlab.models import INSTRUMENT, INSTRUMENT_ID, AcquisitionContext, SourceType
from quantlab.quality import ContractError, QualityError


def _canonical_values(
    path: Path, context: AcquisitionContext
) -> Iterator[tuple[datetime, Decimal]]:
    with path.open("rb") as stream:
        for outcome in validate_acquisition(iter_csv_candidates(stream, context), context):
            if outcome.tick is None:
                raise QualityError(outcome.events[0])
            for event in outcome.events:
                print(f"Warning: {event.error_code.value}: {event.description}", file=sys.stderr)
            yield outcome.tick.event_time_utc, outcome.tick.price


def _legacy_values(path: Path) -> Iterator[tuple[datetime, Decimal]]:
    with path.open("r", encoding="utf-8", newline="") as stream:
        for tick in iter_legacy_csv_ticks(stream):
            yield tick.timestamp_utc, tick.price


def main(argv: list[str] | None = None) -> int:
    """Validate the whole dataset with O(1) state before printing success."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("csv_path", type=Path)
    parser.add_argument("--format", choices=("legacy", "canonical"), default="legacy")
    parser.add_argument("--capture-id", help="caller-assigned acquisition UUID (canonical mode)")
    parser.add_argument("--provider", help="source provider label (canonical mode)")
    parser.add_argument(
        "--provider-symbol", help="explicitly verified provider mapping (canonical mode)"
    )
    parser.add_argument(
        "--pip-size", type=int, help="explicitly verified quote decimal places (canonical mode)"
    )
    args = parser.parse_args(argv)
    try:
        if args.format == "canonical":
            if (
                not all((args.capture_id, args.provider, args.provider_symbol))
                or args.pip_size is None
            ):
                raise ValueError(
                    "canonical mode requires capture-id, provider, provider-symbol, and pip-size"
                )
            try:
                context = AcquisitionContext(
                    UUID(args.capture_id),
                    args.provider,
                    SourceType.HISTORICAL_CSV,
                    args.provider_symbol,
                    args.pip_size,
                )
            except (ContractError, ValueError) as exc:
                raise ValueError("invalid canonical acquisition configuration") from exc
            values = _canonical_values(args.csv_path, context)
        else:
            if any(
                value is not None
                for value in (args.capture_id, args.provider, args.provider_symbol, args.pip_size)
            ):
                raise ValueError("acquisition options require --format canonical")
            values = _legacy_values(args.csv_path)
        count = 0
        earliest: datetime | None = None
        latest: datetime | None = None
        minimum: Decimal | None = None
        maximum: Decimal | None = None
        for timestamp, price in values:
            count += 1
            if earliest is None:
                earliest = timestamp
            latest = timestamp
            minimum = price if minimum is None else min(minimum, price)
            maximum = price if maximum is None else max(maximum, price)
    except (ValidationError, QualityError, OSError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1
    except ValueError as exc:
        print(f"Error: INVALID_SOURCE_METADATA: {exc}", file=sys.stderr)
        return 1
    assert earliest is not None and latest is not None
    if args.format == "legacy":
        print(
            "Contract: legacy CSV / limited fidelity (provider mapping and precision unavailable)"
        )
    else:
        print("Contract: market_tick.v1")
    print(f"Validated records: {count}")
    print(f"Instrument: {INSTRUMENT if args.format == 'legacy' else INSTRUMENT_ID}")
    print(f"Earliest timestamp: {earliest.isoformat()}")
    print(f"Latest timestamp: {latest.isoformat()}")
    print(f"Minimum price: {minimum}")
    print(f"Maximum price: {maximum}")
    return 0
