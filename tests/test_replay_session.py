from dataclasses import replace
from datetime import UTC, datetime

import pytest

from quantlab.audit import verify_log
from quantlab.replay import ReplayStatus
from quantlab.replay_fixture import EVENT_POSITIONS, dataset_hash, demo_alerts, demo_ticks
from quantlab.replay_session import ReplaySession


def session(tmp_path, **kw):
    return ReplaySession(
        demo_ticks, demo_alerts(), dataset_hash(), tmp_path, event_positions=EVENT_POSITIONS, **kw
    )


def drain(run):
    states = []
    while run.status not in (ReplayStatus.ENDED, ReplayStatus.FAILED):
        states.append(run.step())
    return states


def test_demo_hidden_future_retains_success_failure_and_expiry(tmp_path):
    run = session(tmp_path)
    assert run.snapshot().candles == () and run.snapshot().arrows == ()
    first = run.step()
    assert first.position == 2 and first.arrows == ()
    assert first.candles[-1].high == first.candles[-1].open
    states = drain(run)
    final = states[-1]
    assert len(final.arrows) == 3
    assert [o.successful for o in final.outcomes] == [True, False, None]
    assert final.candles[-1].complete is False
    issued = next(s.arrows[0] for s in states if s.arrows)
    assert all(s.arrows[0] == issued for s in states if s.arrows)
    assert final.position == 35
    run.close()
    verify_log(run.audit_path)


def test_reset_run_identity_and_equivalent_arrows(tmp_path):
    run = session(tmp_path)
    final = drain(run)[-1]
    old = run.audit_path
    run.reset()
    assert run.audit_path != old and old.exists()
    assert run.snapshot().arrows == () and run.snapshot().position == 0
    assert drain(run)[-1].arrows == final.arrows
    run.close()
    verify_log(old)
    verify_log(run.audit_path)


def test_render_failure_pauses_without_losing_domain_and_can_retry(tmp_path):
    run = session(tmp_path)
    for _ in range(3):
        run.step()
    before = run.snapshot()

    def broken(state):
        raise RuntimeError("canvas unavailable")

    run.play()
    with pytest.raises(RuntimeError, match="canvas unavailable"):
        run.render(broken)
    assert run.status is ReplayStatus.PAUSED
    assert run.snapshot().candles == before.candles
    assert run.snapshot().arrows == before.arrows
    rendered = []
    run.render(rendered.append)
    run.render(rendered.append)
    run.close()
    verify_log(run.audit_path)


def test_pause_advance_and_speed_preserve_logical_results(tmp_path):
    run = session(tmp_path)
    assert run.advance(100).position == 0
    outputs = []
    for speed in (1, 60, 1000):
        run.reset()
        run.speed = speed
        run.play()
        while run.status is ReplayStatus.PLAYING:
            run.advance(60)
        outputs.append(run.snapshot())
    assert outputs[0] == outputs[1] == outputs[2]


def test_audit_failure_failstops_and_blocks_future_publication(tmp_path):
    run = session(tmp_path)
    run.step()
    original = run.snapshot()
    run._audit.close()
    # Issuance attempts a durable append; a closed sink cannot be ignored.
    run.step()
    with pytest.raises((ValueError, OSError)):
        run.step()
    assert run.status is ReplayStatus.FAILED
    assert run.snapshot().position == 3
    assert run.snapshot().arrows == ()
    assert run.step().position == 3
    assert original.position == 2


def test_invalid_source_fails_without_publishing_future(tmp_path):
    rows = tuple(demo_ticks())

    def invalid():
        yield rows[0]
        yield replace(rows[1], source_position=1)

    run = ReplaySession(invalid, (), dataset_hash(), tmp_path)
    run.step()
    with pytest.raises(ValueError):
        run.step()
    assert run.status is ReplayStatus.FAILED and run.snapshot().position == 2
    run.close()


def test_bad_schedule_and_empty_source_diagnosed(tmp_path):
    run = ReplaySession(lambda: iter(()), (), dataset_hash(), tmp_path)
    with pytest.raises(ValueError, match="empty"):
        run.step()
    assert run.status is ReplayStatus.FAILED
    run.close()
    late = replace(demo_alerts()[0], issued_at_utc=datetime(2026, 1, 1, tzinfo=UTC))
    run = ReplaySession(demo_ticks, (late,), dataset_hash(), tmp_path)
    with pytest.raises(ValueError, match="issuance"):
        drain(run)
    assert run.status is ReplayStatus.FAILED
    run.close()


def test_final_frame_render_is_audited_even_after_large_clock_advance(tmp_path):
    run = session(tmp_path, speed=1000)
    run.play()
    while run.status is ReplayStatus.PLAYING:
        state = run.advance(1000)
    assert state.status == "ENDED" and len(state.arrows) == 3
    run.render(lambda frame: None, renderer_name="headless_geometry")
    run.close()
    assert verify_log(run.audit_path)["rendered_alerts"] == 3


@pytest.mark.parametrize("speed", [1, 60, 1000])
def test_each_publication_shows_issuance_before_future_outcomes(tmp_path, speed):
    run = session(tmp_path, speed=speed)
    run.play()
    frames = []
    while run.status is ReplayStatus.PLAYING:
        frame = run.advance(1000)
        frames.append(frame)
        run.render(lambda state: None, renderer_name="headless_geometry")
    for anchor in frames[-1].arrows:
        first = next(frame for frame in frames if anchor in frame.arrows)
        assert first.position == anchor.alert.issued_at_source_position
        assert not any(o.alert_id == anchor.alert.alert_id for o in first.outcomes)
    run.close()
    assert verify_log(run.audit_path)["rendered_alerts"] == 3


def test_repeat_runs_have_equivalent_logical_evidence(tmp_path):
    from quantlab.audit import logical_events

    events = []
    run = session(tmp_path)
    for speed in (1, 60, 1000):
        run.speed = speed
        run.play()
        while run.status is ReplayStatus.PLAYING:
            run.advance(1000)
            run.render(lambda state: None, renderer_name="headless_geometry")
        run.close()
        events.append(logical_events(run.audit_path))
        if speed != 1000:
            run.reset()
    assert events[0] == events[1] == events[2]


def test_closed_and_invalid_renderer_cannot_draw_unlogged_arrows(tmp_path):
    run = session(tmp_path)
    run.step()
    drawn = []
    with pytest.raises(ValueError, match="renderer"):
        run.render(drawn.append, renderer_name="unknown")
    assert drawn == []
    run.close()
    with pytest.raises(ValueError, match="closed"):
        run.render(drawn.append)
    assert drawn == []
