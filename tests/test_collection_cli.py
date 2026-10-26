"""Fresh CLI processes pause, resume, select and export recorded source data."""

import json
import os
from pathlib import Path
from uuid import uuid4

import pytest
from test_storage_cli import invoke

FIXTURE = Path(__file__).parents[1] / "data/fixtures/historical"
pytestmark = pytest.mark.skipif(
    not os.environ.get("QUANTLAB_TEST_DATABASE_URL"), reason="isolated DB"
)


def test_cli_stream_resume_status_quality_and_export(tmp_path):
    env = dict(os.environ)
    env["QUANTLAB_DATABASE_URL"] = env["QUANTLAB_TEST_DATABASE_URL"]
    env["QUANTLAB_ARCHIVE_DIR"] = str(tmp_path / "archive")
    source_id = str(uuid4())
    args = (
        "data",
        "stream-simulate",
        FIXTURE / "ticks.jsonl",
        "--config",
        FIXTURE / "profile-jsonl.json",
        "--source-id",
        source_id,
        "--queue-size",
        "1",
        "--batch-size",
        "1",
    )
    stopped = invoke(*args, "--interrupt-after", "2", env=env)
    assert stopped.returncode == 3, stopped.stderr
    first = json.loads(stopped.stdout)
    assert first["manifest"]["status"] == "IMPORTING"
    status = invoke("data", "status", "--limit", "1", env=env)
    assert status.returncode == 0, status.stderr
    health = json.loads(status.stdout)[0]
    assert health["collection_status"] == "PAUSED" and health["observed"] == 2
    resumed = invoke(*args, env=env)
    assert resumed.returncode == 0, resumed.stderr
    second = json.loads(resumed.stdout)
    assert second["manifest"]["capture_id"] == first["manifest"]["capture_id"]
    assert second["manifest"]["accepted"] == 3
    quality = invoke("data", "quality", second["manifest"]["dataset_id"], env=env)
    assert quality.returncode == 0, quality.stderr
    assert json.loads(quality.stdout)["completeness"] == "VERIFIED_SOURCE_COMPLETE"
    exported = invoke(
        "data",
        "export",
        second["manifest"]["dataset_id"],
        "--format",
        "parquet",
        "--output",
        tmp_path / "export",
        "--train-end",
        "2026-01-01T00:00:00.5Z",
        "--validation-end",
        "2026-01-01T00:00:02Z",
        env=env,
    )
    assert exported.returncode == 0, exported.stderr
    manifest = json.loads(exported.stdout)["manifest"]
    assert manifest["files"]["train.parquet"]["records"] == 2
    assert manifest["files"]["validation.parquet"]["records"] == 1
    assert manifest["files"]["test.parquet"]["records"] == 0
    datasets = invoke("data", "datasets", "--limit", "1", env=env)
    assert datasets.returncode == 0 and json.loads(datasets.stdout)[0]["status"] == "COMPLETE"
