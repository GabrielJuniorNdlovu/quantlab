"""Reviewed source evidence, separate from observed ticks and acquisition state."""

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date
from enum import Enum
from importlib.resources import files
from pathlib import Path
from urllib.parse import urlsplit

from quantlab.historical_json import strict_json
from quantlab.metadata import MetadataValue, freeze_metadata, json_value
from quantlab.parsing import parse_pip_size, require_instrument_id, required_text


class Qualification(str, Enum):
    UNASSESSED = "UNASSESSED"
    DOCUMENTED = "DOCUMENTED"
    SAMPLE_VERIFIED = "SAMPLE_VERIFIED"
    QUALIFIED = "QUALIFIED"
    BLOCKED = "BLOCKED"


STATE_MEANINGS = {
    "UNASSESSED": "No completed authoritative-source assessment.",
    "DOCUMENTED": "Capabilities documented; genuine sample compatibility remains unverified.",
    "SAMPLE_VERIFIED": "Reviewed authorized genuine sample compatibility; other gates may remain.",
    "QUALIFIED": "Reviewed identity, permission, sample compatibility and integration gates met.",
    "BLOCKED": "An identified prerequisite prevents the intended research integration.",
}


def _object(value: object, keys: set[str], name: str) -> dict[str, object]:
    if not isinstance(value, dict) or set(value) != keys:
        raise ValueError(f"{name} requires exactly: {', '.join(sorted(keys))}")
    return value


def _texts(value: object, name: str) -> tuple[str, ...]:
    if not isinstance(value, list) or len(value) > 64:
        raise ValueError(f"{name} requires at most 64 text entries")
    return tuple(required_text(v, name, limit=2048) for v in value)


@dataclass(frozen=True, slots=True)
class InstrumentMapping:
    internal_id: str
    display_name: str
    external_id: str | None
    pip_size: int | None
    price_basis: str | None
    evidence: tuple[str, ...]

    @classmethod
    def from_mapping(cls, value: object) -> "InstrumentMapping":
        row = _object(
            value,
            {"internal_id", "display_name", "external_id", "pip_size", "price_basis", "evidence"},
            "instrument mapping",
        )
        internal = require_instrument_id(row["internal_id"])
        display = required_text(row["display_name"], "display_name")
        external = row["external_id"]
        external = None if external is None else required_text(external, "external_id")
        pip = row["pip_size"]
        if pip is not None and type(pip) is not int:
            raise ValueError("pip_size must be decimal places as an integer, or null")
        precision = None if pip is None else parse_pip_size(pip)
        basis = row["price_basis"]
        basis = None if basis is None else required_text(basis, "price_basis")
        evidence = _texts(row["evidence"], "mapping evidence")
        if (external is not None or precision is not None or basis is not None) and not evidence:
            raise ValueError("resolved instrument metadata requires documentation references")
        return cls(internal, display, external, precision, basis, evidence)

    def describe(self) -> dict[str, object]:
        return {
            "internal_id": self.internal_id,
            "display_name": self.display_name,
            "external_id": self.external_id,
            "pip_size": self.pip_size,
            "price_basis": self.price_basis,
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class SourceDefinition:
    source_id: str
    status: Qualification
    instruments: tuple[InstrumentMapping, ...]
    metadata: Mapping[str, MetadataValue]

    @classmethod
    def from_mapping(cls, value: object) -> "SourceDefinition":
        row = _object(
            value,
            {
                "source_id",
                "name",
                "publisher",
                "source_type",
                "qualification",
                "instruments",
                "formats",
                "historical",
                "streaming",
                "timestamps",
                "prices",
                "access",
                "documentation",
                "limitations",
                "blockers",
                "sample_verification_reference",
                "authorization_reference",
                "integration_review_reference",
                "non_event_id_fields",
            },
            "source",
        )
        identity = required_text(row["source_id"], "source_id")
        if not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,63}", identity):
            raise ValueError("source_id requires 1 to 64 lowercase letters, digits or hyphens")
        for key in ("name", "publisher", "source_type"):
            required_text(row[key], key)
        status = Qualification(required_text(row["qualification"], "qualification"))
        mappings = row["instruments"]
        if not isinstance(mappings, list) or not 1 <= len(mappings) <= 32:
            raise ValueError("source requires 1 to 32 instrument mappings")
        instruments = tuple(InstrumentMapping.from_mapping(v) for v in mappings)
        for key in ("formats", "limitations", "blockers", "non_event_id_fields"):
            _texts(row[key], key)
        for key in ("historical", "streaming", "timestamps", "prices", "access"):
            if not isinstance(row[key], dict):
                raise ValueError(f"{key} requires structured capability metadata")
        docs = row["documentation"]
        if not isinstance(docs, list) or len(docs) > 32:
            raise ValueError("documentation requires at most 32 references")
        references: set[str] = set()
        for item in docs:
            ref = _object(
                item,
                {"id", "url", "publisher", "retrieved_on", "kind", "claim"},
                "documentation reference",
            )
            ref_id = required_text(ref["id"], "reference id")
            if ref_id in references:
                raise ValueError("duplicate documentation reference id")
            references.add(ref_id)
            url = urlsplit(required_text(ref["url"], "documentation URL", limit=2048))
            if url.scheme != "https" or not url.netloc or url.username or url.password:
                raise ValueError("documentation references require HTTPS URLs without credentials")
            date.fromisoformat(required_text(ref["retrieved_on"], "retrieved_on"))
            required_text(ref["publisher"], "publisher")
            required_text(ref["claim"], "claim", limit=2048)
            if ref["kind"] not in ("DOCUMENTATION_CLAIM", "VERIFIED_METADATA"):
                raise ValueError("reference kind requires DOCUMENTATION_CLAIM or VERIFIED_METADATA")
        if any(ref not in references for mapping in instruments for ref in mapping.evidence):
            raise ValueError("instrument evidence must reference this source's documentation")
        for key in (
            "sample_verification_reference",
            "authorization_reference",
            "integration_review_reference",
        ):
            if row[key] is not None:
                required_text(row[key], key, limit=2048)
        if status is not Qualification.UNASSESSED and not docs:
            raise ValueError("assessed sources require documentation evidence")
        if status is Qualification.BLOCKED and not row["blockers"]:
            raise ValueError("BLOCKED requires at least one explicit blocker")
        if status in (Qualification.SAMPLE_VERIFIED, Qualification.QUALIFIED):
            if not row["sample_verification_reference"] or any(
                m.external_id is None or m.pip_size is None for m in instruments
            ):
                raise ValueError(
                    "sample verification requires reviewed evidence and resolved mapping"
                )
        if status is Qualification.QUALIFIED and (
            row["blockers"]
            or not row["authorization_reference"]
            or not row["integration_review_reference"]
            or any(m.price_basis is None for m in instruments)
        ):
            raise ValueError(
                "QUALIFIED requires permission, integration review, semantics and no blockers"
            )
        metadata = freeze_metadata({k: v for k, v in row.items() if k != "instruments"})
        return cls(identity, status, instruments, metadata)

    def describe(self) -> dict[str, object]:
        return {
            **dict(self.metadata),
            "instruments": [mapping.describe() for mapping in self.instruments],
        }

    def report(self) -> dict[str, object]:
        return {
            "schema_version": "source_qualification.v1",
            "source": self.describe(),
            "state_meaning": STATE_MEANINGS[self.status.value],
            "external_connectivity_implemented": False,
            "qualification_is_reviewed_configuration": True,
            "inspection_automatically_promotes_source": False,
        }


@dataclass(frozen=True, slots=True)
class SourceRegistry:
    sources: tuple[SourceDefinition, ...]
    sha256: str

    @classmethod
    def load(cls, path: Path | None = None) -> "SourceRegistry":
        resource = path if path is not None else files("quantlab").joinpath("data/sources.json")
        with resource.open("rb") as stream:
            payload = stream.read(131073)
        if len(payload) > 131072:
            raise ValueError("source registry exceeds 128 KiB")
        row = _object(strict_json(payload), {"version", "sources"}, "registry")
        values = row["sources"]
        if row["version"] != "source_registry.v1" or not isinstance(values, list):
            raise ValueError("registry requires source_registry.v1 and a source array")
        if not 1 <= len(values) <= 32:
            raise ValueError("registry requires 1 to 32 sources")
        sources = tuple(SourceDefinition.from_mapping(value) for value in values)
        if len({source.source_id for source in sources}) != len(sources):
            raise ValueError("duplicate source identifiers")
        return cls(sources, hashlib.sha256(payload).hexdigest())

    def get(self, source_id: str) -> SourceDefinition:
        for source in self.sources:
            if source.source_id == source_id:
                return source
        raise ValueError(f"unknown source_id: {source_id}")


def encode_report(report: Mapping[str, object]) -> bytes:
    return (
        json.dumps(json_value(report), sort_keys=True, indent=2, allow_nan=False) + "\n"
    ).encode()
