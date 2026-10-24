"""Small packaged engineering fixture. Provider symbols are deliberately synthetic."""

import hashlib
from collections.abc import Iterator
from datetime import UTC, datetime
from importlib.resources import files
from io import BytesIO
from uuid import UUID

from quantlab.alerts import ReplayAlert
from quantlab.csv_adapter import iter_csv_candidates
from quantlab.engine import validate_acquisition
from quantlab.models import INSTRUMENT_ID, AcquisitionContext, MarketTick, SourceType
from quantlab.quality import QualityError

CONTEXT = AcquisitionContext(
    UUID("22222222-2222-4222-8222-222222222222"),
    "engineering-fixture",
    SourceType.HISTORICAL_CSV,
    "fixture-symbol",
    2,
)
EVENT_POSITIONS = frozenset({10})


def dataset_hash() -> str:
    return hashlib.sha256(
        files("quantlab").joinpath("data/replay_demo.csv").read_bytes()
    ).hexdigest()


def demo_ticks() -> Iterator[MarketTick]:
    with BytesIO(files("quantlab").joinpath("data/replay_demo.csv").read_bytes()) as source:
        for outcome in validate_acquisition(iter_csv_candidates(source, CONTEXT), CONTEXT):
            if outcome.tick is None:
                raise QualityError(outcome.events[0])
            yield outcome.tick


def demo_alerts(horizon: int = 3) -> tuple[ReplayAlert, ...]:
    return tuple(
        ReplayAlert(
            UUID(int=number),
            INSTRUMENT_ID,
            datetime(2026, 1, 1, 0, minute, 20, tzinfo=UTC),
            position,
            position,
            evaluation_horizon=horizon,
        )
        for number, minute, position in ((101, 0, 4), (102, 4, 16), (103, 10, 31))
    )
