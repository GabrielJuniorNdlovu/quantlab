"""Bounded proof that committed CSV row evidence covers the full archived source."""

import hashlib
from typing import Any
from uuid import UUID

import psycopg

from quantlab.storage.contracts import StorageError


def verify_complete_source(
    connection: psycopg.Connection[dict[str, Any]], capture_id: UUID, dataset_id: UUID
) -> None:
    source = connection.execute(
        "SELECT source_sha256,source_bytes,configuration FROM dataset_manifests "
        "WHERE dataset_id=%s",
        (dataset_id,),
    ).fetchone()
    assert source is not None
    if source["configuration"]["source_type"] != "historical_csv":
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
                if not isinstance(current, str):
                    raise StorageError("source completion requires preserved CSV header evidence")
                if header is None:
                    header = current
                    encoded = header.encode("utf-8")
                    digest.update(encoded)
                    count = len(encoded)
                elif current != header:
                    raise StorageError("source header evidence changed between observations")
                payload = bytes(row["raw_payload"])
                digest.update(payload)
                count += len(payload)
    if count != source["source_bytes"] or digest.hexdigest() != source["source_sha256"]:
        raise StorageError("committed observations do not cover the complete original source")
