"""Bounded, reproducible Parquet partitions of immutable canonical datasets."""

import hashlib
import json
import os
import tempfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any
from uuid import UUID

from quantlab.continuity import Continuity, TimeRange
from quantlab.models import MarketTick, RawObservation
from quantlab.parsing import parse_timestamp, require_instrument_id
from quantlab.storage.contracts import StorageError
from quantlab.storage.mapping import wire_value

if TYPE_CHECKING:
    from quantlab.storage.repository import Repository

_PARTITIONS = ("train", "validation", "test")
_ROW_GROUP = 1000


@dataclass(frozen=True, slots=True)
class SplitPlan:
    train_end: datetime
    validation_end: datetime

    def __post_init__(self) -> None:
        object.__setattr__(self, "train_end", parse_timestamp(self.train_end))
        object.__setattr__(self, "validation_end", parse_timestamp(self.validation_end))
        if self.train_end >= self.validation_end:
            raise ValueError("train_end must precede validation_end")

    def partition(self, event: datetime) -> str:
        if event < self.train_end:
            return "train"
        return "validation" if event < self.validation_end else "test"


def _json(value: object) -> bytes:
    return (
        json.dumps(wire_value(value), sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n"
    ).encode()


def _sha(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def _sync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _existing(output: Path, request: dict[str, object]) -> dict[str, Any]:
    try:
        with (output / "manifest.json").open("rb") as stream:
            payload = stream.read(65537)
        if len(payload) > 65536:
            raise StorageError("export manifest exceeds byte limit")
        manifest = json.loads(payload)
        if not isinstance(manifest, dict) or not isinstance(manifest.get("files"), dict):
            raise StorageError("invalid existing export manifest")
        if manifest["request"] != request or set(manifest["files"]) != {
            p + ".parquet" for p in _PARTITIONS
        }:
            raise StorageError("existing export does not match requested source/configuration")
        for name, info in manifest["files"].items():
            if not isinstance(info, dict):
                raise StorageError("invalid existing export file metadata")
            path = output / name
            if path.stat().st_size != info["bytes"] or _sha(path) != info["sha256"]:
                raise StorageError("existing export file checksum mismatch")
        digest = hashlib.sha256(payload).hexdigest()
        with (output / "manifest.sha256").open("rb") as checksum:
            checksum_bytes = checksum.read(66)
        if checksum_bytes != (digest + "\n").encode():
            raise StorageError("existing export manifest checksum mismatch")
        return {"manifest": manifest, "manifest_sha256": digest}
    except (OSError, KeyError, TypeError, json.JSONDecodeError) as exc:
        raise StorageError(
            "existing export is incomplete or invalid; choose a new output directory"
        ) from exc


def _row(tick: MarketTick, raw: RawObservation) -> dict[str, object]:
    return {
        "instrument_id": tick.instrument_id,
        "provider": raw.provider,
        "payload_sha256": raw.payload_sha256,
        "source_metadata_json": _json(raw.source_metadata).decode().rstrip(),
        "provider_symbol": tick.provider_symbol,
        "event_time_utc": tick.event_time_utc,
        "price_text": str(tick.price),
        "pip_size": tick.pip_size,
        "source_type": tick.source_type.value,
        "capture_id": str(tick.capture_id),
        "source_position": tick.source_position,
        "provider_tick_id": tick.provider_tick_id,
        "received_at_utc": tick.received_at_utc,
    }


def export_parquet(
    repository: "Repository",
    dataset_id: UUID,
    output: Path,
    *,
    splits: SplitPlan,
    selection: TimeRange | None = None,
    gap_threshold_seconds: float | None = None,
    instrument_id: str = "BOOM_1000",
) -> dict[str, Any]:
    import pyarrow as pa
    import pyarrow.parquet as pq

    selection = selection or TimeRange()
    require_instrument_id(instrument_id)
    continuity = Continuity(selection, gap_threshold_seconds)
    source = repository.dataset_info(dataset_id)
    request: dict[str, object] = {
        "export_schema": "research_ticks.v1",
        "exporter_version": "quantlab.export.v2",
        "dataset_id": str(dataset_id),
        "instrument_id": instrument_id,
        "source": {
            "provider": source["configuration"]["provider"],
            "source_id": source["source_id"],
            "configuration": source["configuration"],
            "observation_metadata": "source_metadata_json and original observation references",
        },
        "capture_id": str(source["selected_capture"]),
        "source_sha256": source["source_sha256"],
        "source_bytes": source["source_bytes"],
        "configuration_sha256": source["configuration_sha256"],
        "market_tick_schema": MarketTick.schema_version,
        "raw_schema": "raw_observation.v1",
        "validation_version": source["configuration"]["validation_version"],
        "start": selection.start.isoformat() if selection.start else None,
        "end": selection.end.isoformat() if selection.end else None,
        "train_end": splits.train_end.isoformat(),
        "validation_end": splits.validation_end.isoformat(),
        "gap_threshold_seconds": gap_threshold_seconds,
        "writer": {
            "pyarrow": pa.__version__,
            "parquet_version": "2.6",
            "compression": "NONE",
            "dictionary": False,
            "row_group_records": _ROW_GROUP,
        },
        "labels": "none",
        "purge_embargo": "not applied; no labels or prediction horizon defined",
    }
    if output.exists():
        return _existing(output, request)
    schema = pa.schema(
        [
            pa.field("instrument_id", pa.string(), nullable=False),
            pa.field("provider", pa.string(), nullable=False),
            pa.field("payload_sha256", pa.string(), nullable=False),
            pa.field("source_metadata_json", pa.string(), nullable=False),
            pa.field("provider_symbol", pa.string(), nullable=False),
            pa.field("event_time_utc", pa.timestamp("us", tz="UTC"), nullable=False),
            pa.field("price_text", pa.string(), nullable=False),
            pa.field("pip_size", pa.int8(), nullable=False),
            pa.field("source_type", pa.string(), nullable=False),
            pa.field("capture_id", pa.string(), nullable=False),
            pa.field("source_position", pa.int64(), nullable=False),
            pa.field("provider_tick_id", pa.string()),
            pa.field("received_at_utc", pa.timestamp("us", tz="UTC")),
        ],
        metadata={b"quantlab.request": _json(request).rstrip()},
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    counts = {p: 0 for p in _PARTITIONS}
    logical = hashlib.sha256()
    with tempfile.TemporaryDirectory(prefix=".quantlab-export-", dir=output.parent) as temporary:
        root = Path(temporary)
        from contextlib import ExitStack

        with ExitStack() as stack:
            writers = {
                p: stack.enter_context(
                    pq.ParquetWriter(
                        root / (p + ".parquet"),
                        schema,
                        version="2.6",
                        compression="NONE",
                        use_dictionary=False,
                    )
                )
                for p in _PARTITIONS
            }
            pending: list[dict[str, object]] = []
            partition = "train"

            def flush() -> None:
                if pending:
                    writers[partition].write_table(
                        pa.Table.from_pylist(pending, schema=schema), row_group_size=_ROW_GROUP
                    )
                    pending.clear()

            for raw, tick in repository.evidence_factory(
                dataset_id, start_utc=selection.start, end_utc=selection.end, page_size=_ROW_GROUP
            )():
                destination = splits.partition(tick.event_time_utc)
                if destination != partition:
                    flush()
                    partition = destination
                continuity.observe(tick)
                logical.update(_json(tick))
                pending.append(_row(tick, raw))
                counts[partition] += 1
                if len(pending) >= _ROW_GROUP:
                    flush()
            flush()
        files = {
            name + ".parquet": {
                "sha256": _sha(root / (name + ".parquet")),
                "bytes": (root / (name + ".parquet")).stat().st_size,
                "records": counts[name],
            }
            for name in _PARTITIONS
        }
        manifest = {
            "request": request,
            "files": files,
            "logical_sha256": logical.hexdigest(),
            "continuity": continuity.report(),
            "ordering": "event_time_utc,source_position",
            "source_file_complete": True,
            "source_market_coverage": "UNKNOWN",
        }
        payload = _json(manifest)
        if len(payload) > 65536:
            raise StorageError("export manifest exceeds byte limit")
        digest = hashlib.sha256(payload).hexdigest()
        (root / "manifest.json").write_bytes(payload)
        (root / "manifest.sha256").write_text(digest + "\n")
        for path in root.iterdir():
            with path.open("rb") as artifact:
                os.fsync(artifact.fileno())
            path.chmod(0o444)
        # A manifest is published last; a partial directory is never a successful export.
        output.mkdir(exist_ok=False)
        _sync_directory(output.parent)
        for name in (*files, "manifest.sha256"):
            os.link(root / name, output / name)
        _sync_directory(output)
        os.link(root / "manifest.json", output / "manifest.json")
        _sync_directory(output)
    return {"manifest": manifest, "manifest_sha256": digest}
