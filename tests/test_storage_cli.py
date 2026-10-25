import json
import os
import subprocess
import sys
from pathlib import Path
from uuid import uuid4

import pytest


def invoke(*args, env=None):
    current = dict(os.environ if env is None else env)
    current.pop("PYTHONPATH", None)
    return subprocess.run(
        [sys.executable, "-m", "quantlab", *map(str, args)],
        env=current,
        capture_output=True,
        text=True,
    )


def test_help_and_missing_configuration():
    result = invoke("--help")
    assert result.returncode == 0 and "ingest" in result.stdout
    env = dict(os.environ)
    env.pop("QUANTLAB_DATABASE_URL", None)
    result = invoke("ingest", "datasets", env=env)
    assert result.returncode == 2 and "QUANTLAB_DATABASE_URL" in result.stderr
    assert "Traceback" not in result.stderr


@pytest.mark.skipif(
    not os.environ.get("QUANTLAB_TEST_DATABASE_URL"), reason="isolated real database"
)
def test_import_status_replay_and_structured_diagnostics(tmp_path):
    env = dict(os.environ)
    env["QUANTLAB_DATABASE_URL"] = env["QUANTLAB_TEST_DATABASE_URL"]
    env["QUANTLAB_ARCHIVE_DIR"] = str(tmp_path / "archive")
    fixture = Path(__file__).parents[1] / "src/quantlab/data/replay_demo.csv"
    result = invoke(
        "ingest",
        "import",
        fixture,
        "--provider",
        "engineering-fixture",
        "--provider-symbol",
        "fixture-symbol",
        "--pip-size",
        "2",
        "--source-id",
        str(uuid4()),
        "--batch-size",
        "3",
        env=env,
    )
    assert result.returncode == 0, result.stderr
    manifest = json.loads(result.stdout)
    assert manifest["status"] == "COMPLETE" and manifest["accepted"] == 34
    messages = [json.loads(line) for line in result.stderr.splitlines()]
    assert any(m["event"] == "ingestion_batch_committed" for m in messages)
    assert all(env["QUANTLAB_DATABASE_URL"] not in line for line in result.stderr.splitlines())
    result = invoke("ingest", "status", manifest["dataset_id"], env=env)
    assert result.returncode == 0 and json.loads(result.stdout)["accepted"] == 34
    progress = invoke("ingest", "status", manifest["dataset_id"], "--attempts", env=env)
    assert progress.returncode == 0, progress.stderr
    attempts = json.loads(progress.stdout)
    assert len(attempts) == 1 and attempts[0]["observed"] == 34
    assert attempts[0]["source_position"] == 35 and attempts[0]["status"] == "COMPLETE"
    health = invoke("db", "status", env=env)
    assert health.returncode == 0, health.stderr
    assert json.loads(health.stdout)["database"] == "connected"
    assert json.loads(health.stdout)["storage"]["market_total_bytes"] > 0
    replay = invoke("replay", manifest["dataset_id"], "--limit", "3", env=env)
    rows = [json.loads(line) for line in replay.stdout.splitlines()]
    assert replay.returncode == 0 and len(rows) == 3
    assert rows[1]["event_time_utc"] == rows[2]["event_time_utc"]
    assert rows[2]["price"] == "1000.12"
    missing = invoke(
        "ingest",
        "import",
        tmp_path / "missing",
        "--provider",
        "fixture",
        "--provider-symbol",
        "fixture-symbol",
        "--pip-size",
        "2",
        env=env,
    )
    assert missing.returncode == 2 and "Traceback" not in missing.stderr
