"""F01/F03/F04 compatibility using isolated, synthetic historical imports."""

import json
import os
from uuid import UUID, uuid4

import pytest
from test_historical import profile
from test_storage_cli import invoke

from quantlab.audit import verify_log
from quantlab.historical import HistoricalAdapter
from quantlab.replay import ReplayStatus
from quantlab.replay_session import ReplaySession
from quantlab.signals import ResearchSignal
from quantlab.storage.ingestion import IngestionCoordinator
from quantlab.storage.repository import Repository
from quantlab.storage.source_archive import SourceArchive


@pytest.mark.parametrize("format", ["csv", "jsonl", "stream_jsonl"])
def test_persisted_delayed_observations_causal_replay_and_cli(tmp_path, format):
    dsn = os.environ.get("QUANTLAB_TEST_DATABASE_URL")
    if not dsn:
        pytest.skip("requires isolated PostgreSQL/TimescaleDB")
    record = {
        "symbol": "Boom export",
        "time": "2026-01-01T00:00:00.123456Z",
        "quote": "1000.123456789012345678",
        "received": "2026-01-01T00:01:05.234567Z",
    }
    payload = (
        (",".join(record) + "\n" + (",".join(record.values()) + "\n") * 3).encode()
        if format == "csv"
        else (
            (json.dumps({"tick": record} if format == "stream_jsonl" else record) + "\n") * 3
        ).encode()
    )
    fields = {
        "instrument_id": "symbol",
        "event_time": "time",
        "price": "quote",
        "received_at_utc": "received",
    }
    if format == "stream_jsonl":
        fields = {k: "tick." + v for k, v in fields.items()}
    adapter = HistoricalAdapter(profile(format, fields=fields, pip_size=18))
    path = tmp_path / "synthetic-recorded-source"
    path.write_bytes(payload)
    archive = SourceArchive(tmp_path / "archive")
    with Repository(dsn) as repo:
        manifest = IngestionCoordinator(repo, archive).import_historical(
            path, adapter=adapter, source_id=str(uuid4())
        )
        assert manifest.status == "COMPLETE" and manifest.accepted == 3
        factory = repo.tick_factory(manifest.dataset_id, page_size=1)
        source_hash = repo.manifest(manifest.dataset_id).source_sha256
    ticks = tuple(factory())
    assert len(ticks) == 3 and all(str(t.price) == record["quote"] for t in ticks)

    def producer(observation):
        return (
            ResearchSignal.issue(
                signal_id=UUID(int=observation.tick.source_position),
                observation=observation,
                producer_version="synthetic.v1",
            ),
        )

    run = ReplaySession(
        factory,
        (),
        source_hash,
        tmp_path / "research-audit",
        source_dataset_id=str(manifest.dataset_id),
        signal_producer=producer,
    )
    while run.status is not ReplayStatus.ENDED:
        run.step()
    assert len(run.signals) == 3
    assert all(
        s.input_cutoff_utc == t.event_time_utc
        and s.issued_at_utc == t.received_at_utc
        and s.input_cutoff_source_position == t.source_position
        and s.observed_price_at_issue.as_tuple() == t.price.as_tuple()
        for s, t in zip(run.signals, ticks, strict=True)
    )
    run.close()
    assert verify_log(run.audit_path)["issued_signals"] == 3
    env = dict(os.environ, QUANTLAB_DATABASE_URL=dsn)
    for command in (("replay",), ("data", "replay")):
        result = invoke(
            *command,
            manifest.dataset_id,
            "--show-chart",
            "--headless",
            "--limit",
            "2",
            "--audit-dir",
            tmp_path / "cli-audit",
            env=env,
        )
        assert result.returncode == 0, result.stderr
        summary = json.loads(result.stdout)
        assert summary["observations"] == 2 and summary["audit_verified"]
    assert archive.preserve(path).path.read_bytes() == payload
