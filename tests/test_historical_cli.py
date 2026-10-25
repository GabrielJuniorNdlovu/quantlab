"""The public historical workflow runs through fresh CLI processes."""

import json
import os
from pathlib import Path
from uuid import uuid4

import pytest
from test_storage_cli import invoke

FIXTURES = Path(__file__).parents[1] / "data/fixtures/historical"


@pytest.mark.parametrize("format", ["csv", "jsonl"])
def test_inspect_without_database(format):
    env = dict(os.environ)
    env.pop("QUANTLAB_DATABASE_URL", None)
    result = invoke(
        "data",
        "inspect",
        FIXTURES / f"ticks.{format}",
        "--config",
        FIXTURES / f"profile-{format}.json",
        env=env,
    )
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)["accepted"] == 3


@pytest.mark.skipif(not os.environ.get("QUANTLAB_TEST_DATABASE_URL"), reason="isolated DB")
@pytest.mark.parametrize("format", ["csv", "jsonl"])
def test_import_validate_list_and_restart_replay(tmp_path, format):
    env = dict(os.environ)
    env["QUANTLAB_DATABASE_URL"] = env["QUANTLAB_TEST_DATABASE_URL"]
    env["QUANTLAB_ARCHIVE_DIR"] = str(tmp_path / "archive")
    args = (
        "data",
        "import",
        FIXTURES / f"ticks.{format}",
        "--config",
        FIXTURES / f"profile-{format}.json",
        "--source-id",
        str(uuid4()),
        "--batch-size",
        "2",
    )
    imported = invoke(*args, env=env)
    assert imported.returncode == 0, imported.stderr
    manifest = json.loads(imported.stdout)
    assert manifest["accepted"] == 3 and manifest["status"] == "COMPLETE"
    repeated = invoke(*args, env=env)
    assert json.loads(repeated.stdout) == manifest
    checked = invoke("data", "validate", manifest["dataset_id"], env=env)
    assert checked.returncode == 0, checked.stderr
    assert json.loads(checked.stdout)["valid"]
    listing = invoke("data", "list", "--limit", "1", env=env)
    assert listing.returncode == 0 and len(json.loads(listing.stdout)) == 1
    replay = invoke("data", "replay", manifest["dataset_id"], "--limit", "2", env=env)
    assert replay.returncode == 0, replay.stderr
    rows = [json.loads(line) for line in replay.stdout.splitlines()]
    assert len(rows) == 2 and [r["price"] for r in rows] == ["1000.10", "1000.10"]
    assert rows[0]["event_time_utc"] == rows[1]["event_time_utc"]
    invalid = invoke("data", "replay", manifest["dataset_id"], "--limit", "0", env=env)
    assert invalid.returncode == 2 and "Traceback" not in invalid.stderr
