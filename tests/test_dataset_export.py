"""Training exports preserve exact prices and chronological boundaries."""

from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

import pytest

from quantlab.continuity import Continuity, TimeRange
from quantlab.dataset_export import SplitPlan
from quantlab.models import MarketTick, SourceType


def test_splits_never_divide_equal_timestamps():
    plan = SplitPlan(datetime(2026, 1, 2, tzinfo=UTC), datetime(2026, 1, 3, tzinfo=UTC))
    assert plan.partition(datetime(2026, 1, 2, tzinfo=UTC)) == "validation"
    assert plan.partition(datetime(2026, 1, 3, tzinfo=UTC)) == "test"
    assert plan.partition(datetime(2026, 1, 1, tzinfo=UTC)) == "train"
    with pytest.raises(ValueError):
        SplitPlan(plan.validation_end, plan.train_end)


def test_continuity_reports_suspicions_without_proven_missing_ticks():
    continuity = Continuity(TimeRange(), 2)
    capture = uuid4()
    for position, second in enumerate([0, 0, 10], 1):
        continuity.observe(
            MarketTick(
                "BOOM_1000",
                "fixture",
                datetime(2026, 1, 1, 0, 0, second, tzinfo=UTC),
                Decimal("1000.10"),
                2,
                SourceType.HISTORICAL_CSV,
                capture,
                position,
            )
        )
    report = continuity.report()
    assert report["repeated_timestamps"] == 1 and report["suspected_gaps"] == 1
    assert report["proven_missing_periods"] == [] and report["market_coverage"] == "UNKNOWN"
