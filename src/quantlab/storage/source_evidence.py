"""Bounded proof that committed CSV or JSON Lines evidence covers the full archived source."""

import hashlib
from typing import Any
from uuid import UUID

import psycopg

from quantlab.storage.contracts import StorageError, historical_profile


def verify_complete_source(
    connection: psycopg.Connection[dict[str, Any]], capture_id: UUID, dataset_id: UUID
) -> None:
    source = connection.execute(
        "SELECT source_sha256,source_bytes,configuration FROM dataset_manifests "
        "WHERE dataset_id=%s",
        (dataset_id,),
    ).fetchone()
    assert source is not None
    config = source["configuration"]
    profile = historical_profile(config) or {}
    expected_format = {
        "historical_csv": "csv",
        "historical_json": "jsonl",
        "websocket": "stream_jsonl",
    }.get(config["source_type"])
    if profile and (
        profile.get("version") != "historical.v1" or profile.get("format") != expected_format
    ):
        raise StorageError("unsupported historical source profile version or format")
    json_lines = (
        config["source_type"] in ("historical_json", "websocket")
        and profile.get("version") == "historical.v1"
        and profile.get("format") in ("jsonl", "stream_jsonl")
    )
    if config["source_type"] != "historical_csv" and not json_lines:
        raise StorageError("source completion requires a supported whole-source framing contract")
    digest = hashlib.sha256()
    count = 0
    header: str | None = None
    with connection.cursor(name="quantlab_source_evidence") as cursor:
        cursor.execute(
            "SELECT raw_payload,source_metadata FROM raw_observations "
            "WHERE capture_id=%s ORDER BY source_position",
            (capture_id,),
        )
        while rows := cursor.fetchmany(100):
            for row in rows:
                adapter = row["source_metadata"].get("adapter", {})
                current = adapter.get("header") if isinstance(adapter, dict) else None
                if not json_lines and not isinstance(current, str):
                    raise StorageError("source completion requires preserved CSV header evidence")
                if json_lines:
                    if adapter.get("framing") != "json_lines.v1":
                        raise StorageError("unsupported JSON source framing")
                elif header is None:
                    assert isinstance(current, str)
                    header = current
                    encoded = header.encode("utf-8")
                    digest.update(encoded)
                    count = len(encoded)
                elif current != header:
                    raise StorageError("source header evidence changed between observations")
                if profile and adapter.get("byte_offset") != count:
                    raise StorageError("noncontiguous original source evidence")
                payload = bytes(row["raw_payload"])
                digest.update(payload)
                count += len(payload)
    if count != source["source_bytes"] or digest.hexdigest() != source["source_sha256"]:
        raise StorageError("committed observations do not cover the complete original source")
