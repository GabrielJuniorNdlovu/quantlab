"""Shared value rules for canonical data and the explicit legacy adapter."""

import re
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from uuid import UUID

from quantlab.quality import ContractError, ErrorCode

INSTRUMENT_ID = "BOOM_1000"
MAX_PRICE_CHARS = 256
MAX_PRICE_EXPONENT = 1000
MAX_PIP_SIZE = 18
_TIMESTAMP = re.compile(
    r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}"
    r"(?:\.[0-9]{1,6})?(?:Z|[+-][0-9]{2}:[0-9]{2})"
)
_PRICE = re.compile(r"[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?")


def required_text(value: object, field: str, *, limit: int = 256) -> str:
    """Require nonblank bounded UTF-8 text without NUL or trimming."""
    if value is None or value == "" or (isinstance(value, str) and not value.strip()):
        raise ContractError(ErrorCode.MISSING_REQUIRED_FIELD, field, f"missing value for {field}")
    if not isinstance(value, str) or len(value) > limit or "\x00" in value:
        raise ContractError(
            ErrorCode.INVALID_SOURCE_METADATA, field, f"{field} requires bounded text"
        )
    try:
        value.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise ContractError(
            ErrorCode.INVALID_SOURCE_METADATA, field, f"{field} requires valid UTF-8 text"
        ) from exc
    return value


def require_instrument_id(value: object, field: str = "instrument_id") -> str:
    """Require the canonical Boom 1000 internal instrument identifier."""
    instrument_id = required_text(value, field)
    if instrument_id != INSTRUMENT_ID:
        raise ContractError(ErrorCode.INVALID_INSTRUMENT, field, "unsupported instrument")
    return instrument_id


def require_decimal_price(value: object, field: str = "price") -> Decimal:
    """Require a native positive finite Decimal price within numeric bounds."""
    if not isinstance(value, Decimal):
        raise ContractError(ErrorCode.INVALID_PRICE, field, f"canonical {field} must be Decimal")
    return parse_price(value)


def parse_price(value: str | Decimal | None) -> Decimal:
    """Preserve Decimal coefficient/exponent exactly, never quantize or use floats."""
    if value is None or value == "" or (isinstance(value, str) and not value.strip()):
        raise ContractError(ErrorCode.MISSING_REQUIRED_FIELD, "price", "missing value for price")
    reason = "price must be a finite positive decimal"
    if isinstance(value, str):
        if len(value) > MAX_PRICE_CHARS or not _PRICE.fullmatch(value):
            raise ContractError(ErrorCode.INVALID_PRICE, "price", reason)
        try:
            price = Decimal(value)
        except InvalidOperation as exc:
            raise ContractError(ErrorCode.INVALID_PRICE, "price", reason) from exc
    elif isinstance(value, Decimal):
        price = value
    else:
        raise ContractError(ErrorCode.INVALID_PRICE, "price", reason)
    if not price.is_finite() or price <= 0:
        raise ContractError(ErrorCode.INVALID_PRICE, "price", reason)
    parts = price.as_tuple()
    if len(parts.digits) > MAX_PRICE_CHARS or abs(int(parts.exponent)) > MAX_PRICE_EXPONENT:
        raise ContractError(
            ErrorCode.INVALID_PRICE, "price", "price representation exceeds numeric limits"
        )
    return price


def parse_timestamp(value: str | datetime | None, field: str = "event_time_utc") -> datetime:
    """Require an aware ISO timestamp and normalize it without truncating precision."""
    if value is None or value == "":
        raise ContractError(ErrorCode.MISSING_REQUIRED_FIELD, field, f"missing value for {field}")
    reason = (
        f"{field} requires ISO 8601 YYYY-MM-DDTHH:MM:SS, "
        "optional 1–6 fractional digits, and explicit timezone (Z or ±HH:MM)"
    )
    try:
        if isinstance(value, str):
            if not _TIMESTAMP.fullmatch(value):
                raise ValueError(reason)
            if value[-1] != "Z" and (int(value[-5:-3]) > 23 or int(value[-2:]) > 59):
                raise ValueError("invalid timezone offset")
            timestamp = datetime.fromisoformat(value)
        elif isinstance(value, datetime):
            timestamp = value
        else:
            raise ValueError(reason)
        if timestamp.tzinfo is None or timestamp.utcoffset() is None:
            raise ValueError(reason)
        return timestamp.astimezone(timezone.utc)
    except (ValueError, OverflowError) as exc:
        raise ContractError(ErrorCode.INVALID_TIMESTAMP, field, reason) from exc


def require_uuid(value: UUID, field: str = "capture_id") -> None:
    if not isinstance(value, UUID):
        raise ContractError(ErrorCode.INVALID_SOURCE_METADATA, field, f"{field} must be a UUID")


def require_position(value: int) -> None:
    if type(value) is not int or value < 1:
        raise ContractError(
            ErrorCode.INVALID_SOURCE_POSITION,
            "source_position",
            "source_position must be a positive integer",
        )


def parse_pip_size(value: int | str | None) -> int:
    """Quote precision is decimal places, supplied explicitly by configuration."""
    if value is None or value == "":
        raise ContractError(
            ErrorCode.MISSING_REQUIRED_FIELD, "pip_size", "missing value for pip_size"
        )
    if isinstance(value, str) and len(value) <= 2 and value.isascii() and value.isdecimal():
        value = int(value)
    if type(value) is not int or not 0 <= value <= MAX_PIP_SIZE:
        raise ContractError(
            ErrorCode.INVALID_PRECISION, "pip_size", "pip_size must be an integer from 0 to 18"
        )
    return value


def require_price_precision(price: Decimal, pip_size: int) -> None:
    if int(price.as_tuple().exponent) < -pip_size:
        raise ContractError(
            ErrorCode.INVALID_PRECISION,
            "price",
            "price formatting exceeds configured quote precision",
        )
