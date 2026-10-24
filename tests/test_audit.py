"""Audit evidence survives chart failures and can be checked independently."""

import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from uuid import UUID

import pytest

from quantlab.audit import AuditLog, logical_events, verify_log

TIME = datetime(2026, 1, 1, tzinfo=UTC)
ALERT_ID = UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
HASH = "a" * 64


@dataclass(frozen=True)
class Alert:
    alert_id: UUID = ALERT_ID
    instrument_id: str = "BOOM_1000"
    issued_at_utc: datetime = TIME
    issued_at_source_position: int = 2
    data_cutoff_position: int = 2
    evaluation_horizon: int = 3
    evaluation_horizon_unit: str = "SUBSEQUENT_COMPLETED_M1_CANDLES"
    direction: str = "UP"
    alert_source: str = "DETERMINISTIC_FIXTURE"
    model_version: str = "fixture.v1"
    status: str = "ISSUED"


def issue(log: AuditLog) -> None:
    log.append(
        "alert_issued",
        TIME,
        {"alert": Alert(), "candle_open_utc": TIME, "price": Decimal("1000.10")},
    )


def render(log: AuditLog) -> None:
    log.append(
        "alert_rendered",
        TIME,
        {
            "alert_id": ALERT_ID,
            "candle_open_utc": TIME,
            "price": Decimal("1000.10"),
            "issued_at_utc": TIME,
            "issued_at_source_position": 2,
        },
    )


def outcome(log: AuditLog) -> None:
    end = TIME + timedelta(minutes=4)
    log.append(
        "alert_outcome",
        end,
        {
            "alert_id": ALERT_ID,
            "status": "RESOLVED",
            "successful": False,
            "evaluated_at_utc": end,
            "evaluated_at_source_position": 10,
            "window_candles": tuple(TIME + timedelta(minutes=n) for n in (1, 2, 3)),
            "reason": "fixture window completed",
        },
    )


def test_log_serializes_exact_values_and_independently_verifies(tmp_path: Path) -> None:
    log = AuditLog(tmp_path, HASH, clock=lambda: TIME)
    issue(log)
    render(log)
    outcome(log)
    log.finish("ended", TIME + timedelta(minutes=4))
    report = verify_log(log.path)
    assert report["issued_alerts"] == 1
    assert report["rendered_alerts"] == 1
    assert report["outcomes"] == 1
    lines = log.path.read_text().splitlines()
    assert json.loads(lines[1])["payload"]["price"] == "1000.10"
    assert json.loads(lines[1])["processed_at_utc"] == TIME.isoformat()
    assert json.loads(lines[1])["event_time_utc"] == TIME.isoformat()


def test_new_run_never_overwrites_old_run(tmp_path: Path) -> None:
    identity = UUID("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb")
    log = AuditLog(tmp_path, HASH, run_id=identity)
    log.finish("ended")
    original = log.path.read_bytes()
    with pytest.raises(FileExistsError):
        AuditLog(tmp_path, HASH, run_id=identity)
    assert log.path.read_bytes() == original


def test_repeated_runs_have_equivalent_logical_events(tmp_path: Path) -> None:
    paths = []
    for _ in range(2):
        log = AuditLog(tmp_path, HASH)
        issue(log)
        render(log)
        outcome(log)
        log.finish("ended")
        paths.append(log.path)
    assert logical_events(paths[0]) == logical_events(paths[1])


def test_close_records_explicit_interruption_and_is_idempotent(tmp_path: Path) -> None:
    log = AuditLog(tmp_path, HASH)
    log.close()
    before = log.path.read_bytes()
    log.close()
    assert log.path.read_bytes() == before
    assert verify_log(log.path)["reason"] == "interrupted"
    with pytest.raises(ValueError, match="closed"):
        log.append("anything", None, {})


@pytest.mark.parametrize("mutation", ["edit", "truncate", "remove_end", "append_garbage"])
def test_corrupt_or_incomplete_files_fail_explicitly(tmp_path: Path, mutation: str) -> None:
    log = AuditLog(tmp_path, HASH)
    issue(log)
    log.finish("ended")
    data = log.path.read_bytes()
    if mutation == "edit":
        data = data.replace(b"1000.10", b"1000.20")
    elif mutation == "truncate":
        data = data[:-1]
    elif mutation == "remove_end":
        data = b"\n".join(data.splitlines()[:-1]) + b"\n"
    else:
        data += b"garbage\n"
    log.path.write_bytes(data)
    with pytest.raises(ValueError):
        verify_log(log.path)


def test_render_requires_prior_issued_alert(tmp_path: Path) -> None:
    log = AuditLog(tmp_path, HASH)
    render(log)
    log.finish("ended")
    with pytest.raises(ValueError, match="issued"):
        verify_log(log.path)


def test_changed_anchor_is_rejected(tmp_path: Path) -> None:
    log = AuditLog(tmp_path, HASH)
    issue(log)
    log.append(
        "alert_rendered",
        TIME,
        {
            "alert_id": ALERT_ID,
            "candle_open_utc": TIME,
            "price": "999.00",
            "issued_at_utc": TIME,
            "issued_at_source_position": 2,
        },
    )
    log.finish("ended")
    with pytest.raises(ValueError, match="anchor"):
        verify_log(log.path)


def test_outcome_cannot_precede_completed_horizon(tmp_path: Path) -> None:
    log = AuditLog(tmp_path, HASH)
    issue(log)
    log.append(
        "alert_outcome",
        TIME,
        {
            "alert_id": ALERT_ID,
            "status": "RESOLVED",
            "successful": True,
            "evaluated_at_utc": TIME,
            "evaluated_at_source_position": 2,
            "window_candles": (),
            "reason": "invalid early outcome",
        },
    )
    log.finish("ended")
    with pytest.raises(ValueError, match="outcome"):
        verify_log(log.path)


def test_duplicate_issue_is_rejected_by_verifier(tmp_path: Path) -> None:
    log = AuditLog(tmp_path, HASH)
    issue(log)
    issue(log)
    log.finish("ended")
    with pytest.raises(ValueError, match="duplicate"):
        verify_log(log.path)


def test_nonfinite_or_unsupported_payload_is_rejected_before_write(tmp_path: Path) -> None:
    log = AuditLog(tmp_path, HASH)
    before = log.path.read_bytes()
    for value in (float("nan"), object()):
        with pytest.raises((ValueError, TypeError)):
            log.append("bad", None, {"value": value})
    assert log.path.read_bytes() == before
    log.close()


def test_io_failure_poisoned_writer_cannot_continue(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    log = AuditLog(tmp_path, HASH)

    def broken_fsync(fd: int) -> None:
        raise OSError("simulated disk failure")

    monkeypatch.setattr("quantlab.audit.os.fsync", broken_fsync)
    with pytest.raises(OSError, match="simulated"):
        log.append("test", None, {})
    with pytest.raises(ValueError, match="failed"):
        log.append("test", None, {})
    log.close()
    with pytest.raises(ValueError, match="incomplete"):
        verify_log(log.path)


def test_invalid_anchor_price_is_a_diagnosable_validation_error(tmp_path: Path) -> None:
    log = AuditLog(tmp_path, HASH)
    log.append(
        "alert_issued", TIME, {"alert": Alert(), "candle_open_utc": TIME, "price": "not-a-price"}
    )
    log.finish("ended")
    with pytest.raises(ValueError, match="anchor price"):
        verify_log(log.path)


def test_future_cutoff_is_rejected(tmp_path: Path) -> None:
    from dataclasses import replace

    log = AuditLog(tmp_path, HASH)
    log.append(
        "alert_issued",
        TIME,
        {
            "alert": replace(Alert(), data_cutoff_position=3),
            "candle_open_utc": TIME,
            "price": Decimal("1000.10"),
        },
    )
    log.finish("ended")
    with pytest.raises(ValueError, match="future cutoff"):
        verify_log(log.path)


def test_expired_alert_remains_unevaluated(tmp_path: Path) -> None:
    log = AuditLog(tmp_path, HASH)
    issue(log)
    log.append(
        "alert_outcome",
        TIME,
        {
            "alert_id": ALERT_ID,
            "status": "EXPIRED",
            "successful": None,
            "evaluated_at_utc": TIME,
            "evaluated_at_source_position": 2,
            "window_candles": (),
            "reason": "insufficient completed future candles",
        },
    )
    log.finish("ended")
    assert verify_log(log.path)["outcomes"] == 1


def test_oversized_record_does_not_modify_file(tmp_path: Path) -> None:
    log = AuditLog(tmp_path, HASH)
    before = log.path.read_bytes()
    with pytest.raises(ValueError, match="128 KiB"):
        log.append("oversized", None, {"value": "x" * 131072})
    assert log.path.read_bytes() == before
    log.close()


@pytest.mark.parametrize("renderer", ["tk_canvas", "headless_geometry"])
def test_render_records_identify_the_rendering_boundary(tmp_path: Path, renderer: str) -> None:
    log = AuditLog(tmp_path, HASH)
    issue(log)
    log.append(
        "alert_rendered",
        TIME,
        {
            "alert_id": ALERT_ID,
            "candle_open_utc": TIME,
            "price": Decimal("1000.10"),
            "issued_at_utc": TIME,
            "issued_at_source_position": 2,
            "renderer": renderer,
        },
    )
    log.finish("ended")
    assert verify_log(log.path)["rendered_alerts"] == 1


def test_later_append_cannot_backdate_an_alert(tmp_path: Path) -> None:
    log = AuditLog(tmp_path, HASH)
    log.append("replay_progress", TIME + timedelta(minutes=2), {"position": 8})
    issue(log)
    log.finish("ended")
    with pytest.raises(ValueError, match="backdated"):
        verify_log(log.path)


def test_expiry_cannot_claim_a_future_completed_candle(tmp_path: Path) -> None:
    log = AuditLog(tmp_path, HASH)
    issue(log)
    log.append(
        "alert_outcome",
        TIME,
        {
            "alert_id": ALERT_ID,
            "status": "EXPIRED",
            "successful": None,
            "evaluated_at_utc": TIME,
            "evaluated_at_source_position": 2,
            "window_candles": (TIME + timedelta(minutes=1),),
            "reason": "invalid future completed candle",
        },
    )
    log.finish("ended")
    with pytest.raises(ValueError, match="outcome"):
        verify_log(log.path)
