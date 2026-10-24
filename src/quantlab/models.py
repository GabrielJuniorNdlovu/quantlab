"""Immutable tick records and the supported research instrument."""

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

INSTRUMENT = "Boom 1000 Index"


@dataclass(frozen=True, slots=True)
class Tick:
    """A validated tick; source_row is its starting physical CSV line."""

    symbol: str
    timestamp_utc: datetime
    price: Decimal
    source: str
    source_row: int
