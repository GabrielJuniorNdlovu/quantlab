"""Synthetic source observations; these are not verified MT5 provider messages."""

from dataclasses import FrozenInstanceError, replace
from datetime import timedelta
from decimal import Decimal
from uuid import UUID

import pytest
from test_alerts import BASE, tick

from quantlab.audit import AuditLog, logical_events, verify_log
from quantlab.chart import chart_geometry
from quantlab.replay import ReplayEngine, ReplayStatus
from quantlab.replay_fixture import dataset_hash
from quantlab.replay_session import ReplaySession


def test_replay_waits_for_received_observation():
    rows = [tick(1, 0), replace(tick(2, 5), received_at_utc=BASE + timedelta(seconds=65))]
    engine = ReplayEngine(lambda: iter(rows))
    engine.play()
    assert engine.advance(5) == (rows[0],)
    assert engine.advance(59) == ()
    assert engine.advance(1) == (rows[1],)
    assert engine.available_at_utc == BASE + timedelta(seconds=65)


def test_signal_contract_exact_roundtrip_and_frozen_metadata():
    from quantlab.signals import ResearchSignal

    metadata = {"purpose": ["synthetic"]}
    signal = ResearchSignal(
        UUID(int=123),
        "BOOM_1000",
        "FIXTURE_UP",
        BASE + timedelta(seconds=65),
        BASE,
        1,
        BASE,
        Decimal("1000.000000000000000001"),
        dataset_hash(),
        "fixture.v2",
        metadata,
    )
    metadata["purpose"].append("changed")
    assert signal.metadata["purpose"] == ("synthetic",)
    assert ResearchSignal.from_json(signal.to_json()) == signal
    assert "1000.000000000000000001" in signal.to_json()
    with pytest.raises(FrozenInstanceError):
        signal.issued_at_utc = BASE
    with pytest.raises(ValueError):
        replace(signal, issued_at_utc=BASE - timedelta(seconds=1))
    with pytest.raises(ValueError):
        ResearchSignal.from_json(signal.to_json().replace('"research_signal.v1"', '"bad.v1"'))


def synthetic(observation):
    from quantlab.signals import ResearchSignal

    return (
        ResearchSignal.issue(
            signal_id=UUID(int=observation.tick.source_position),
            observation=observation,
            producer_version="fixture.v2",
            metadata={"purpose": "synthetic"},
        ),
    )


def test_delayed_cross_minute_signal_events_no_lookahead_and_determinism(tmp_path):
    rows = [
        tick(1, 0),
        replace(tick(2, 5), received_at_utc=BASE + timedelta(seconds=65)),
        replace(tick(3, 5), received_at_utc=BASE + timedelta(seconds=64)),
        tick(4, 120),
    ]
    logs = []
    for _ in range(2):
        events = []
        run = ReplaySession(
            lambda: iter(rows),
            (),
            dataset_hash(),
            tmp_path,
            signal_producer=synthetic,
            on_event=events.append,
        )
        for row in rows:
            state = run.step()
            observed = [e for e in events if e.kind == "tick_observed"][-1].value
            assert observed.tick == row
            assert all(s.input_cutoff_source_position <= row.source_position for s in run.signals)
            assert len(chart_geometry(state, 1100, 600).arrows) == len(run.signals)
        second = run.signals[1]
        assert second.issued_at_utc == BASE + timedelta(seconds=65)
        assert second.bar_open_time_utc == BASE
        assert run.signals[2].issued_at_utc == second.issued_at_utc
        assert [e.kind for e in events[:4]] == [
            "tick_observed",
            "candle_updated",
            "signal_issued",
            "tick_observed",
        ]
        before = run.audit_path.read_bytes()
        run.issue(second)  # exact redelivery after subsequent ticks is idempotent
        assert run.audit_path.read_bytes() == before
        run.step()
        assert events[-1].kind == "replay_completed"
        assert run.snapshot().candles[-1].complete is False
        run.close()
        assert verify_log(run.audit_path)["issued_signals"] == 4
        logs.append(logical_events(run.audit_path))
    assert logs[0] == logs[1]


def test_future_cutoff_and_backdated_received_signal_fail_closed(tmp_path):
    rows = [replace(tick(1, 0), received_at_utc=BASE + timedelta(seconds=65))]
    for change in (
        {"issued_at_utc": BASE},
        {"input_cutoff_source_position": 2},
        {"input_cutoff_utc": BASE + timedelta(seconds=1)},
        {"observed_price_at_issue": Decimal("999")},
    ):

        def bad(observation, change=change):
            return (replace(synthetic(observation)[0], **change),)

        run = ReplaySession(lambda: iter(rows), (), dataset_hash(), tmp_path, signal_producer=bad)
        with pytest.raises(ValueError):
            run.step()
        assert run.status is ReplayStatus.FAILED
        assert run.snapshot().arrows == ()
        run.close()


def test_short_audit_write_never_commits_chain(tmp_path):
    log = AuditLog(tmp_path, dataset_hash())
    original = log._file

    class ShortWriter:
        def write(self, data):
            return original.write(data[:7])

        def __getattr__(self, name):
            return getattr(original, name)

    log._file = ShortWriter()
    with pytest.raises(OSError, match="short"):
        log.append("diagnostic", BASE, {"message": "not committed"})
    with pytest.raises(ValueError, match="failed"):
        log.append("diagnostic", BASE, {})
    with pytest.raises(ValueError, match="truncated"):
        verify_log(log.path)


def test_signal_repeated_delivery_conflicts_and_multiple_same_bar(tmp_path):
    def repeated(observation):
        one = synthetic(observation)[0]
        return (one, one, replace(one, signal_id=UUID(int=999)))

    run = ReplaySession(
        lambda: iter([tick(1, 0)]), (), dataset_hash(), tmp_path, signal_producer=repeated
    )
    state = run.step()
    assert len(run.signals) == len(chart_geometry(state, 900, 500).arrows) == 2
    with pytest.raises(ValueError, match="conflicting"):
        run.issue(replace(run.signals[0], producer_version="different"))
    run.render(lambda s: chart_geometry(s, 900, 500), renderer_name="headless_geometry")
    run.render(lambda s: chart_geometry(s, 900, 500), renderer_name="headless_geometry")
    run.step()
    run.close()
    result = verify_log(run.audit_path)
    assert result["issued_signals"] == result["rendered_signals"] == 2


def test_outcome_append_preserves_original_signal(tmp_path):
    run = ReplaySession(
        lambda: iter([tick(1, 0), tick(2, 60)]),
        (),
        dataset_hash(),
        tmp_path,
        signal_producer=synthetic,
    )
    run.step()
    original = run.signals[0]
    prefix = run.audit_path.read_bytes()
    run.step()
    run.record_outcome(original.signal_id, expired=True, metadata={"reason": "external fixture"})
    assert run.signals[0] == original
    assert run.audit_path.read_bytes().startswith(prefix)
    with pytest.raises(ValueError, match="terminal"):
        run.record_outcome(original.signal_id, expired=False, metadata={})
    run.step()
    run.close()
    assert verify_log(run.audit_path)["signal_outcomes"] == 1


def test_callbacks_are_immutable_and_cannot_reenter(tmp_path):
    def callback(event):
        with pytest.raises(FrozenInstanceError):
            event.kind = "bad"
        with pytest.raises(ValueError, match="reentrant"):
            run.step()

    run = ReplaySession(
        lambda: iter([tick(1, 0)]),
        (),
        dataset_hash(),
        tmp_path,
        signal_producer=synthetic,
        on_event=callback,
    )
    run.step()
    run.step()
    run.close()


@pytest.mark.parametrize("metadata", [{"x": "\x00"}, {"x": "\ud800"}, {"x": float("nan")}])
def test_unsafe_signal_metadata_rejected(metadata):

    with pytest.raises(ValueError):
        replace(synthetic_observation_signal(), metadata=metadata)


def synthetic_observation_signal():
    from quantlab.signals import Observation

    return synthetic(Observation(tick(1, 0), BASE, dataset_hash()))[0]


def test_signal_json_rejects_duplicate_keys_float_prices_and_unknown_fields():
    from quantlab.signals import ResearchSignal

    signal = replace(synthetic_observation_signal(), metadata={"weight": 0.5})
    assert ResearchSignal.from_json(signal.to_json()) == signal
    for text in (
        signal.to_json()[:-1] + ',"extra":1}',
        signal.to_json().replace('"FIXTURE_UP"', '"FIXTURE_UP","signal_type":"X"'),
        signal.to_json().replace('"1000.00"', "1000.0"),
        signal.to_json().replace('"fixture.v2"', '"\\ud800"'),
    ):
        with pytest.raises(ValueError):
            ResearchSignal.from_json(text)


def test_audit_writer_dedup_and_restart_never_rewrites(tmp_path):
    signal = synthetic_observation_signal()
    log = AuditLog(tmp_path, dataset_hash())
    log.append("signal_issued", signal.issued_at_utc, signal.to_dict())
    prefix = log.path.read_bytes()
    log.append("signal_issued", signal.issued_at_utc, signal.to_dict())
    assert log.path.read_bytes() == prefix
    with pytest.raises(ValueError, match="conflicting"):
        log.append(
            "signal_issued",
            signal.issued_at_utc,
            replace(signal, producer_version="other").to_dict(),
        )
    log.close()
    assert verify_log(log.path)["issued_signals"] == 1
    original = log.path.read_bytes()
    restarted = AuditLog(tmp_path, dataset_hash())
    restarted.close()
    assert log.path.read_bytes() == original
    assert restarted.path != log.path
    bad = tmp_path / "invalid-utf8.jsonl"
    bad.write_bytes(b"\xff\n")
    with pytest.raises(ValueError):
        verify_log(bad)


def test_cancel_closes_iterator_and_reset_repeats_signal_sequence(tmp_path):
    closed = []

    def source():
        try:
            yield tick(1, 0)
            yield tick(2, 60)
        finally:
            closed.append(True)

    run = ReplaySession(source, (), dataset_hash(), tmp_path, signal_producer=synthetic)
    first = run.step()
    run.close()
    assert len(closed) == 1
    run.reset()
    assert run.step().arrows == first.arrows
    run.close()
    assert len(closed) == 2


def test_exact_minute_equal_timestamp_cutoffs_and_candle_events(tmp_path):
    rows = [tick(1, 59, "1000.01"), tick(2, 60, "1001.02"), tick(3, 60, "999.03")]
    events = []
    run = ReplaySession(
        lambda: iter(rows),
        (),
        dataset_hash(),
        tmp_path,
        signal_producer=synthetic,
        on_event=events.append,
    )
    states = [run.step() for _ in rows]
    assert states[0].arrows[0].bar_open_time_utc == BASE
    assert states[1].arrows[-1].bar_open_time_utc == BASE + timedelta(minutes=1)
    assert states[1].candles[-1].observation_count == 1
    assert states[2].candles[-1].observation_count == 2
    assert [s.input_cutoff_source_position for s in run.signals] == [1, 2, 3]
    closed = [e.value for e in events if e.kind == "candle_closed"]
    assert len(closed) == 1 and closed[0].close == Decimal("1000.01")
    run.step()
    run.close()


def test_direct_issue_publishes_after_audit_and_sink_failure_failstops(tmp_path):
    from quantlab.signals import Observation

    row = tick(1, 0)
    run = ReplaySession(lambda: iter([row]), (), dataset_hash(), tmp_path)
    run.step()
    signal = synthetic(Observation(row, BASE, dataset_hash()))[0]
    assert run.issue(signal)
    assert run.snapshot().arrows == (signal,)
    run._audit.close()
    with pytest.raises(ValueError):
        run.issue(replace(signal, signal_id=UUID(int=999)))
    assert run.status is ReplayStatus.FAILED
    assert run.snapshot().arrows == (signal,)
    run.close()


def test_legacy_schedule_cannot_backdate_delayed_input(tmp_path):
    from quantlab.alerts import ReplayAlert

    row = replace(tick(1, 0), received_at_utc=BASE + timedelta(seconds=65))
    run = ReplaySession(
        lambda: iter([row]),
        (ReplayAlert(UUID(int=10), "BOOM_1000", BASE, 1, 1),),
        dataset_hash(),
        tmp_path,
    )
    with pytest.raises(ValueError, match="delayed"):
        run.step()
    assert run.snapshot().arrows == ()
    run.close()


def test_failed_multi_signal_pulse_cannot_expose_partial_public_signals(tmp_path):
    def bad_batch(observation):
        one = synthetic(observation)[0]
        return (one, replace(one, signal_id=UUID(int=999), input_cutoff_source_position=2))

    run = ReplaySession(
        lambda: iter([tick(1, 0)]), (), dataset_hash(), tmp_path, signal_producer=bad_batch
    )
    with pytest.raises(ValueError):
        run.step()
    assert run.snapshot().position == 0
    assert run.signals == ()
    run.close()


def test_batch_io_failure_keeps_all_signals_unpublished(tmp_path):
    def two(observation):
        one = synthetic(observation)[0]
        return (one, replace(one, signal_id=UUID(int=999)))

    run = ReplaySession(
        lambda: iter([tick(1, 0)]), (), dataset_hash(), tmp_path, signal_producer=two
    )
    original = run._audit.append

    def interrupted(kind, time, payload):
        if payload.get("signal_id") == str(UUID(int=999)):
            raise OSError("injected second append failure")
        original(kind, time, payload)

    run._audit.append = interrupted
    with pytest.raises(OSError, match="second append"):
        run.step()
    assert run.signals == () and run.snapshot().arrows == ()
    assert run.status is ReplayStatus.FAILED
    run.close()
    assert verify_log(run.audit_path)["reason"] == "interrupted"


def test_signal_capacity_rejects_before_partial_writes(tmp_path):
    def too_many(observation):
        one = synthetic(observation)[0]
        return tuple(replace(one, signal_id=UUID(int=i)) for i in range(1, 1002))

    run = ReplaySession(
        lambda: iter([tick(1, 0)]), (), dataset_hash(), tmp_path, signal_producer=too_many
    )
    prefix = run.audit_path.read_bytes()
    with pytest.raises(ValueError, match="capacity"):
        run.step()
    assert run.signals == () and run.audit_path.read_bytes() == prefix
    run.close()


def test_signal_decoder_rejects_bad_types_and_records_larger_than_limit():
    import json

    from quantlab.signals import ResearchSignal

    payload = synthetic_observation_signal().to_dict()
    for field, value in (
        ("metadata", []),
        ("input_cutoff_source_position", True),
        ("input_cutoff_source_position", "1"),
        ("schema_version", "research_signal.v2"),
    ):
        with pytest.raises(ValueError):
            ResearchSignal.from_json(json.dumps({**payload, field: value}))
    with pytest.raises(ValueError, match="128 KiB"):
        ResearchSignal.from_json(" " * 131073)


def test_consumers_cannot_issue_unpublished_signals_during_callbacks(tmp_path):
    def callback(event):
        if event.kind == "tick_observed":
            with pytest.raises(ValueError, match="producer"):
                run.issue(synthetic(event.value)[0])

    run = ReplaySession(lambda: iter([tick(1, 0)]), (), dataset_hash(), tmp_path, on_event=callback)
    run.step()
    assert run.signals == run.snapshot().arrows == ()
    run.step()
    run.close()
    assert verify_log(run.audit_path)["issued_signals"] == 0


def test_external_issue_event_is_also_protected_against_reentry(tmp_path):
    from quantlab.signals import Observation

    def callback(event):
        if event.kind == "signal_issued":
            with pytest.raises(ValueError, match="reentrant"):
                run.step()

    row = tick(1, 0)
    run = ReplaySession(
        lambda: iter([row, tick(2, 60)]), (), dataset_hash(), tmp_path, on_event=callback
    )
    run.step()
    run.issue(synthetic(Observation(row, BASE, dataset_hash()))[0])
    assert run.snapshot().position == 1
    run.close()


def test_duplicate_identity_preserves_decimal_representation(tmp_path):
    run = ReplaySession(
        lambda: iter([tick(1, 0)]), (), dataset_hash(), tmp_path, signal_producer=synthetic
    )
    run.step()
    original = run.signals[0]
    changed = replace(original, observed_price_at_issue=Decimal("1000.000"))
    assert original.to_json() != changed.to_json()
    with pytest.raises(ValueError, match="conflicting"):
        run.issue(changed)
    run.close()


def test_verifier_rejects_changed_decimal_render_anchor(tmp_path):
    signal = synthetic_observation_signal()
    changed = replace(signal, observed_price_at_issue=Decimal("1000.000"))
    log = AuditLog(tmp_path, dataset_hash())
    log.append("signal_issued", BASE, signal.to_dict())
    log.append("signal_rendered", BASE, changed.to_dict())
    log.close()
    with pytest.raises(ValueError, match="differs"):
        verify_log(log.path)
