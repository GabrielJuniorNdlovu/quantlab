import json
import os
import subprocess
import sys
from pathlib import Path


def invoke(*args):
    env = dict(os.environ)
    env.pop("PYTHONPATH", None)
    return subprocess.run(
        [sys.executable, "-m", "quantlab.replay_app", *map(str, args)],
        env=env,
        text=True,
        capture_output=True,
    )


def test_headless_entrypoint_and_independent_audit(tmp_path):
    result = invoke("--fixture", "demo", "--headless", "--audit-dir", tmp_path)
    assert result.returncode == 0, result.stderr
    report = json.loads(result.stdout)
    assert report["observations"] == 34
    assert report["issued_alerts"] == 3
    assert report["outcomes"] == [True, False, None]
    assert report["fixture"] == "DETERMINISTIC_FIXTURE"
    log = Path(report["audit_path"])
    inspected = invoke("--inspect-audit", log)
    assert inspected.returncode == 0, inspected.stderr
    assert json.loads(inspected.stdout)["rendered_alerts"] == 3


def test_missing_and_corrupt_audit_and_invalid_options(tmp_path):
    assert invoke("--inspect-audit", tmp_path / "missing").returncode != 0
    bad = tmp_path / "bad.jsonl"
    bad.write_text("{broken")
    assert invoke("--inspect-audit", bad).returncode != 0
    assert invoke("--fixture", "unknown", "--headless").returncode != 0
    assert invoke("--headless", "--horizon", "0", "--audit-dir", tmp_path).returncode != 0
    assert invoke("--headless", "--speed", "nan", "--audit-dir", tmp_path).returncode != 0


def test_native_missing_display_reports_error_and_preserves_log(tmp_path):
    env = dict(os.environ, DISPLAY=":65530")
    env.pop("PYTHONPATH", None)
    result = subprocess.run(
        [sys.executable, "-m", "quantlab.replay_app", "--audit-dir", str(tmp_path)],
        env=env,
        text=True,
        capture_output=True,
    )
    assert result.returncode == 2
    assert "display" in result.stderr.lower()
    assert "Traceback" not in result.stderr


def test_canonical_research_demo_reproducible_and_unevaluated(tmp_path):
    from quantlab.audit import logical_events

    logs = []
    for _ in range(2):
        result = invoke("--headless", "--research-signals", "--audit-dir", tmp_path)
        assert result.returncode == 0, result.stderr
        report = json.loads(result.stdout)
        assert report["observations"] == 34 and report["issued_signals"] == 3
        assert report["outcomes"] == [] and report["audit_verified"]
        first = report["signal_timeline"][0]
        assert first["issued_at_utc"] == "2026-01-01T00:00:20+00:00"
        assert first["bar_open_time_utc"] == "2026-01-01T00:00:00+00:00"
        assert first["cutoff_source_position"] == 4
        logs.append(logical_events(Path(report["audit_path"])))
    assert logs[0] == logs[1]


def test_headless_does_not_label_arbitrary_source_as_fixture(tmp_path):
    from quantlab.replay_app import run_headless
    from quantlab.replay_fixture import dataset_hash, demo_ticks
    from quantlab.replay_session import ReplaySession

    run = ReplaySession(demo_ticks, (), dataset_hash(), tmp_path)
    report = run_headless(run)
    assert report["fixture"] is None
