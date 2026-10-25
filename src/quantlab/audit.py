"""Exclusive, append-only JSONL research evidence and independent verification."""

import hashlib
import json
import math
import os
import re
from collections.abc import Callable, Iterator, Mapping
from dataclasses import fields, is_dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, NoReturn

if TYPE_CHECKING:
    from quantlab.signals import ResearchSignal
from uuid import UUID, uuid4

from quantlab.parsing import parse_price, parse_timestamp

MAX_RECORD_BYTES = 131072
MAX_ALERTS = 10000
_ZERO_HASH = "0" * 64
_HASH = re.compile(r"[0-9a-f]{64}\Z")


def _now() -> datetime:
    return datetime.now(UTC)


def json_value(value: object, depth: int = 0) -> object:
    if depth > 32:
        raise ValueError("audit payload nesting exceeds 32")
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("audit values must be finite")
        return value
    if isinstance(value, datetime):
        return parse_timestamp(value).isoformat()
    if isinstance(value, (UUID, Decimal)):
        if isinstance(value, Decimal) and not value.is_finite():
            raise ValueError("audit Decimal must be finite")
        return str(value)
    if isinstance(value, Enum):
        return json_value(value.value, depth + 1)
    if is_dataclass(value) and not isinstance(value, type):
        return {
            item.name: json_value(getattr(value, item.name), depth + 1) for item in fields(value)
        }
    if isinstance(value, Mapping):
        result: dict[str, object] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise TypeError("audit mapping keys must be strings")
            result[key] = json_value(item, depth + 1)
        return result
    if isinstance(value, (tuple, list)):
        return [json_value(item, depth + 1) for item in value]
    raise TypeError(f"unsupported audit value: {type(value).__name__}")


def _encode(value: object) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False
    ).encode("ascii")


class AuditLog:
    """One new file per run. I/O failure permanently closes the writer."""

    def __init__(
        self,
        directory: Path,
        dataset_hash: str,
        *,
        run_id: UUID | None = None,
        clock: Callable[[], datetime] = _now,
        configuration: dict[str, object] | None = None,
    ) -> None:
        if not _HASH.fullmatch(dataset_hash):
            raise ValueError("dataset hash must be lowercase SHA-256")
        self.run_id = run_id if run_id is not None else uuid4()
        if not isinstance(self.run_id, UUID):
            raise ValueError("run_id must be UUID")
        self.path = directory / f"{self.run_id}.jsonl"
        self._dataset_hash = dataset_hash
        self._clock = clock
        self._signal_records: dict[str, bytes] = {}
        self._sequence = 0
        self._previous = _ZERO_HASH
        self._closed = False
        self._failed = False
        directory.mkdir(parents=True, exist_ok=True)
        self._file = self.path.open("xb")
        try:
            self._append(
                "run_started",
                None,
                {
                    "configuration": configuration or {},
                    "market_tick_schema": "market_tick.v1",
                    "alert_schema": "replay_alert.v1",
                    "outcome_schema": "alert_outcome.v1",
                },
            )
        except (OSError, ValueError, TypeError):
            self._file.close()
            self._closed = True
            raise

    def append(
        self,
        kind: str,
        event_time_utc: datetime | None,
        payload: Mapping[str, object],
    ) -> None:
        if kind in {"run_started", "run_finished"}:
            raise ValueError("run lifecycle records are reserved")
        self._append(kind, event_time_utc, payload)

    def _append(
        self,
        kind: str,
        event_time_utc: datetime | None,
        payload: Mapping[str, object],
    ) -> None:
        if self._failed:
            raise ValueError("audit writer failed; start a new run")
        if self._closed:
            raise ValueError("audit writer is closed")
        if not isinstance(kind, str) or not kind or len(kind) > 64:
            raise ValueError("audit kind must contain 1 to 64 characters")
        signal_key = None
        signal_bytes = None
        if kind == "signal_issued":
            signal = _research_signal(payload)
            signal_key = str(signal.signal_id)
            signal_bytes = _encode(signal.to_dict())
            if signal_key in self._signal_records:
                if self._signal_records[signal_key] != signal_bytes:
                    raise ValueError("conflicting signal ID")
                return
            if len(self._signal_records) >= MAX_ALERTS:
                raise ValueError("audit signal capacity exceeded")
            if signal.issued_at_utc != event_time_utc:
                raise ValueError("signal event time differs from issuance")
        record: dict[str, object] = {
            "schema_version": "audit_event.v1",
            "sequence": self._sequence + 1,
            "run_id": str(self.run_id),
            "dataset_sha256": self._dataset_hash,
            "event_time_utc": json_value(event_time_utc),
            "processed_at_utc": json_value(self._clock()),
            "kind": kind,
            "payload": json_value(payload),
            "previous_sha256": self._previous,
        }
        digest = hashlib.sha256(_encode(record)).hexdigest()
        record["record_sha256"] = digest
        encoded = _encode(record) + b"\n"
        if len(encoded) > MAX_RECORD_BYTES:
            raise ValueError("audit record exceeds 128 KiB")
        try:
            if self._file.write(encoded) != len(encoded):
                raise OSError("short audit write; start a new run")
            self._file.flush()
            os.fsync(self._file.fileno())
        except OSError:
            self._failed = True
            self._file.close()
            raise
        if signal_key is not None and signal_bytes is not None:
            self._signal_records[signal_key] = signal_bytes
        self._previous = digest
        self._sequence += 1

    def finish(self, reason: str, event_time_utc: datetime | None = None) -> None:
        if not isinstance(reason, str) or not reason:
            raise ValueError("run completion requires a reason")
        self._append("run_finished", event_time_utc, {"reason": reason})
        self._file.close()
        self._closed = True

    def close(self) -> None:
        if self._closed or self._failed:
            return
        self.finish("interrupted")


def _mapping(value: object, name: str) -> dict[str, object]:
    if not isinstance(value, dict) or any(not isinstance(key, str) for key in value):
        raise ValueError(f"{name} must be a JSON object")
    return {key: item for key, item in value.items()}


def _time(value: object, name: str) -> datetime:
    if not isinstance(value, str):
        raise ValueError(f"{name} must be an ISO UTC timestamp")
    parsed = parse_timestamp(value, name)
    if parsed.isoformat() != value:
        raise ValueError(f"{name} must use canonical UTC serialization")
    return parsed


def _position(value: object, name: str) -> int:
    if type(value) is not int or value <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return value


def _reject_constant(value: str) -> NoReturn:
    raise ValueError(f"nonfinite JSON constant {value}")


def _unique_pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


def decode_json(data: bytes) -> object:
    """Bounded callers supply canonical JSON; reject duplicate keys and nonfinite values."""
    try:
        return json.loads(data, object_pairs_hook=_unique_pairs, parse_constant=_reject_constant)
    except (UnicodeDecodeError, RecursionError) as exc:
        raise ValueError("invalid audit JSON") from exc


def _records(path: Path) -> Iterator[dict[str, object]]:
    previous = _ZERO_HASH
    identity: tuple[object, object] | None = None
    previous_event_time: datetime | None = None
    with path.open("rb") as stream:
        number = 0
        while line := stream.readline(MAX_RECORD_BYTES + 1):
            number += 1
            if len(line) > MAX_RECORD_BYTES or not line.endswith(b"\n"):
                raise ValueError(f"audit line {number} is oversized or truncated")
            try:
                value = decode_json(line)
            except (UnicodeDecodeError, RecursionError) as exc:
                raise ValueError(f"invalid audit JSON at line {number}") from exc
            record = _mapping(value, "audit record")
            if record.get("schema_version") != "audit_event.v1":
                raise ValueError("unsupported audit schema")
            kind = record.get("kind")
            if not isinstance(kind, str) or not kind or len(kind) > 64:
                raise ValueError("invalid audit event kind")
            sequence = record.get("sequence")
            if type(sequence) is not int or sequence != number:
                raise ValueError("audit sequence is not contiguous")
            digest = record.pop("record_sha256", None)
            if record.get("previous_sha256") != previous:
                raise ValueError("audit previous hash mismatch")
            if digest != hashlib.sha256(_encode(record)).hexdigest():
                raise ValueError("audit record hash mismatch")
            assert isinstance(digest, str)
            current_identity = (record.get("run_id"), record.get("dataset_sha256"))
            if identity is None:
                identity = current_identity
                try:
                    UUID(str(identity[0]))
                except ValueError as exc:
                    raise ValueError("invalid audit run identity") from exc
                if not isinstance(identity[1], str) or not _HASH.fullmatch(identity[1]):
                    raise ValueError("invalid audit dataset hash")
            elif current_identity != identity:
                raise ValueError("audit run identity changed")
            _time(record.get("processed_at_utc"), "processed_at_utc")
            if record.get("event_time_utc") is not None:
                event_time = _time(record.get("event_time_utc"), "event_time_utc")
                if previous_event_time is not None and event_time < previous_event_time:
                    raise ValueError("audit event time was backdated")
                previous_event_time = event_time
            previous = digest
            record["record_sha256"] = digest
            yield record


def _issued(payload: dict[str, object], event_time: object) -> dict[str, object]:
    alert = _mapping(payload.get("alert"), "issued alert")
    if alert.get("status") != "ISSUED" or alert.get("direction") != "UP":
        raise ValueError("invalid issued alert state/direction")
    if alert.get("instrument_id") != "BOOM_1000":
        raise ValueError("unsupported issued instrument")
    if alert.get("alert_source") != "DETERMINISTIC_FIXTURE":
        raise ValueError("unsupported issued alert source")
    if alert.get("evaluation_horizon_unit") != "SUBSEQUENT_COMPLETED_M1_CANDLES":
        raise ValueError("unsupported alert horizon unit")
    position = _position(alert.get("issued_at_source_position"), "issuance position")
    cutoff = _position(alert.get("data_cutoff_position"), "data cutoff")
    _position(alert.get("evaluation_horizon"), "evaluation horizon")
    issued = _time(alert.get("issued_at_utc"), "issuance time")
    if cutoff > position or event_time != alert.get("issued_at_utc"):
        raise ValueError("issued alert has future cutoff or inconsistent issuance time")
    candle = _time(payload.get("candle_open_utc"), "anchor candle")
    if candle != issued.replace(second=0, microsecond=0):
        raise ValueError("issued anchor does not match issuance candle")
    price = payload.get("price")
    if not isinstance(price, str):
        raise ValueError("anchor price must be an exact Decimal string")
    try:
        parse_price(price)
    except ValueError as exc:
        raise ValueError("invalid anchor price") from exc
    return alert


def _outcome(
    payload: dict[str, object],
    issue: dict[str, object],
    alert: dict[str, object],
    event_time: object,
) -> None:
    issued_time = _time(alert.get("issued_at_utc"), "issuance time")
    issued_position = _position(alert.get("issued_at_source_position"), "issuance position")
    evaluated = _time(payload.get("evaluated_at_utc"), "outcome time")
    position = _position(payload.get("evaluated_at_source_position"), "outcome position")
    if event_time != payload.get("evaluated_at_utc"):
        raise ValueError("outcome event timestamp is inconsistent")
    windows = payload.get("window_candles")
    if not isinstance(windows, list):
        raise ValueError("outcome window must be a list")
    opening = _time(issue.get("candle_open_utc"), "anchor candle")
    previous = opening
    for value in windows:
        current = _time(value, "outcome candle")
        if current <= previous or current.second != 0 or current.microsecond != 0:
            raise ValueError("outcome windows must be subsequent ordered M1 candles")
        previous = current
    horizon = _position(alert.get("evaluation_horizon"), "evaluation horizon")
    if evaluated < issued_time or position < issued_position:
        raise ValueError("outcome precedes issuance")
    if windows and evaluated < previous + timedelta(minutes=1):
        raise ValueError("outcome claims an unseen completed candle")
    if payload.get("status") == "RESOLVED":
        if (
            len(windows) != horizon
            or type(payload.get("successful")) is not bool
            or position <= issued_position
            or evaluated < previous + timedelta(minutes=1)
        ):
            raise ValueError("resolved outcome lacks a completed future horizon")
    elif payload.get("status") == "EXPIRED":
        if len(windows) >= horizon or payload.get("successful") is not None:
            raise ValueError("expired outcome cannot contain a resolved result")
    else:
        raise ValueError("invalid outcome status")


def _research_signal(payload: Mapping[str, object]) -> "ResearchSignal":
    from quantlab.signals import ResearchSignal

    return ResearchSignal.from_json(_encode(json_value(payload)).decode("ascii"))


def verify_log(path: Path) -> dict[str, object]:
    """Check chains and alert causality. Tick truth requires the dataset itself."""
    signals: dict[str, ResearchSignal] = {}
    signal_terminal: set[str] = set()
    signal_rendered: set[str] = set()
    issued: dict[str, dict[str, object]] = {}
    rendered: set[str] = set()
    outcomes: set[str] = set()
    started = False
    ended = False
    count = 0
    reason: object = None
    run_id: object = None
    for record in _records(path):
        count += 1
        if ended:
            raise ValueError("audit contains records after run completion")
        payload = _mapping(record.get("payload"), "payload")
        kind = record.get("kind")
        event_time = record.get("event_time_utc")
        if not started:
            if kind != "run_started":
                raise ValueError("audit must start with run_started")
            started = True
            run_id = record.get("run_id")
            continue
        if kind == "run_started":
            raise ValueError("duplicate audit run_started")
        if kind == "run_finished":
            reason = payload.get("reason")
            if not isinstance(reason, str) or not reason:
                raise ValueError("run completion requires a reason")
            ended = True
            continue
        if kind == "signal_issued":
            signal = _research_signal(payload)
            key = str(signal.signal_id)
            if key in signals or len(signals) >= MAX_ALERTS:
                raise ValueError("duplicate issued signal or signal capacity exceeded")
            if event_time != signal.issued_at_utc.isoformat():
                raise ValueError("signal event time differs from issuance")
            signals[key] = signal
        elif kind == "signal_rendered":
            signal = _research_signal(payload)
            key = str(signal.signal_id)
            if key not in signals or signal.to_json() != signals[key].to_json():
                raise ValueError("rendered signal differs from original issuance")
            if _time(event_time, "render time") < signal.issued_at_utc:
                raise ValueError("render precedes signal issuance")
            if key in signal_rendered:
                raise ValueError("duplicate signal render")
            signal_rendered.add(key)
        elif kind in {"signal_expired", "signal_outcome_recorded"}:
            outcome_key = payload.get("signal_id")
            if (
                not isinstance(outcome_key, str)
                or outcome_key not in signals
                or outcome_key in signal_terminal
            ):
                raise ValueError("outcome requires a unique issued signal")
            original_signal = signals[outcome_key]
            for name in ("instrument_id", "source_dataset_id", "producer_version"):
                if payload.get(name) != getattr(original_signal, name):
                    raise ValueError("outcome changed signal identity")
            if (
                _time(event_time, "outcome time") < original_signal.issued_at_utc
                or _position(payload.get("source_position"), "outcome source position")
                < original_signal.input_cutoff_source_position
            ):
                raise ValueError("outcome precedes signal issuance")
            signal_terminal.add(outcome_key)
        elif kind == "alert_issued":
            alert = _issued(payload, event_time)
            identity = alert.get("alert_id")
            if not isinstance(identity, str):
                raise ValueError("issued alert ID must be UUID string")
            UUID(identity)
            if identity in issued:
                raise ValueError("duplicate issued alert ID")
            if len(issued) >= MAX_ALERTS:
                raise ValueError("audit alert capacity exceeded")
            issued[identity] = payload
        elif kind in {"alert_rendered", "alert_outcome"}:
            identity = payload.get("alert_id")
            if not isinstance(identity, str) or identity not in issued:
                raise ValueError("render/outcome references an alert that was not issued")
            original = issued[identity]
            alert = _mapping(original.get("alert"), "issued alert")
            if kind == "alert_rendered":
                expected = {key: original.get(key) for key in ("candle_open_utc", "price")}
                expected.update(
                    {
                        key: alert.get(key)
                        for key in ("alert_id", "issued_at_utc", "issued_at_source_position")
                    }
                )
                renderer = payload.get("renderer")
                if renderer is not None and renderer not in ("tk_canvas", "headless_geometry"):
                    raise ValueError("unsupported alert renderer")
                actual = {key: value for key, value in payload.items() if key != "renderer"}
                if actual != expected:
                    raise ValueError("rendered arrow anchor changed")
                if _time(event_time, "render time") < _time(alert.get("issued_at_utc"), "issue"):
                    raise ValueError("render precedes issuance")
                rendered.add(identity)
            else:
                if identity in outcomes:
                    raise ValueError("duplicate alert outcome")
                _outcome(payload, original, alert, event_time)
                outcomes.add(identity)
    if not started or not ended:
        raise ValueError("audit run is incomplete; missing start or completion record")
    return {
        "valid": True,
        "run_id": run_id,
        "records": count,
        "issued_signals": len(signals),
        "rendered_signals": len(signal_rendered),
        "signal_outcomes": len(signal_terminal),
        "issued_alerts": len(issued),
        "rendered_alerts": len(rendered),
        "outcomes": len(outcomes),
        "reason": reason,
    }


def logical_events(path: Path) -> tuple[dict[str, object], ...]:
    """Return verified events with run/processing/hash bookkeeping removed."""
    verify_log(path)
    ignored = {"run_id", "processed_at_utc", "previous_sha256", "record_sha256"}
    return tuple(
        {key: value for key, value in record.items() if key not in ignored}
        for record in _records(path)
    )
