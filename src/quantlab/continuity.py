"""Chronological range selection and explicitly uncertain timestamp continuity."""

import math
from dataclasses import dataclass
from datetime import datetime

from quantlab.models import MarketTick
from quantlab.parsing import parse_timestamp


@dataclass(frozen=True, slots=True)
class TimeRange:
    start: datetime | None = None
    end: datetime | None = None

    def __post_init__(self) -> None:
        for name in ("start", "end"):
            value = getattr(self, name)
            if value is not None:
                object.__setattr__(self, name, parse_timestamp(value))
        if self.start is not None and self.end is not None and self.start >= self.end:
            raise ValueError("range requires start before end")


class Continuity:
    """Silence suggests a gap; these source formats cannot prove missing market events."""

    def __init__(self, selection: TimeRange, threshold_seconds: float | None = None) -> None:
        if threshold_seconds is not None and (
            isinstance(threshold_seconds, bool)
            or not math.isfinite(threshold_seconds)
            or threshold_seconds <= 0
        ):
            raise ValueError("gap threshold must be finite and positive")
        self.selection = selection
        self.threshold = threshold_seconds
        self.first: datetime | None = None
        self.last: datetime | None = None
        self.count = 0
        self.repeated = 0
        self.suspected = 0
        self.samples: list[dict[str, object]] = []

    def observe(self, tick: MarketTick) -> None:
        event = tick.event_time_utc
        if self.last is not None:
            delta = (event - self.last).total_seconds()
            if delta < 0:
                raise ValueError("continuity input must be chronological")
            self.repeated += delta == 0
            if self.threshold is not None and delta > self.threshold:
                self.suspected += 1
                if len(self.samples) < 20:
                    self.samples.append(
                        {
                            "start": self.last.isoformat(),
                            "end": event.isoformat(),
                            "seconds": delta,
                            "classification": "SUSPECTED",
                        }
                    )
        if self.first is None:
            self.first = event
        self.last = event
        self.count += 1

    def report(self) -> dict[str, object]:
        return {
            "observations": self.count,
            "first_event": self.first.isoformat() if self.first else None,
            "last_event": self.last.isoformat() if self.last else None,
            "repeated_timestamps": self.repeated,
            "suspected_gaps": self.suspected,
            "suspected_gap_samples": self.samples,
            "threshold_seconds": self.threshold,
            "proven_missing_periods": [],
            "market_coverage": "UNKNOWN",
            "leading_coverage_unknown": self.selection.start is not None
            and (self.first is None or self.first > self.selection.start),
            "trailing_coverage_unknown": self.selection.end is not None,
            "interpolation": "none",
        }
