from dataclasses import FrozenInstanceError, replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import UUID

import pytest

from quantlab.alerts import AlertBook, ReplayAlert
from quantlab.candles import CandleBuilder
from quantlab.models import INSTRUMENT_ID, MarketTick, SourceType

BASE = datetime(2026, 1, 1, tzinfo=UTC)


def tick(position, seconds, price="1000.00"):
    return MarketTick(
        INSTRUMENT_ID,
        "fixture-only",
        BASE + timedelta(seconds=seconds),
        Decimal(price),
        2,
        SourceType.HISTORICAL_CSV,
        UUID(int=1),
        position,
    )


def alert(position=2, seconds=20, **kw):
    return ReplayAlert(
        UUID(int=2),
        INSTRUMENT_ID,
        BASE + timedelta(seconds=seconds),
        position,
        data_cutoff_position=position,
        **kw,
    )


def process(book, rows):
    candles = CandleBuilder()
    issued, outcomes = [], []
    for row in rows:
        new, resolved = book.consume(row, candles.consume(row))
        issued.extend(new)
        outcomes.extend(resolved)
    return issued, outcomes


def test_issuance_hidden_until_exact_tick_and_immutable():
    book = AlertBook((alert(),))
    assert book.issued == ()
    candles = CandleBuilder()
    one = tick(1, 0)
    assert book.consume(one, candles.consume(one)) == ((), ())
    two = tick(2, 20)
    issued, outcomes = book.consume(two, candles.consume(two))
    assert not outcomes
    anchor = issued[0]
    assert anchor.price == two.price and anchor.candle_open_utc == BASE
    assert anchor.alert.status == "ISSUED"
    with pytest.raises(FrozenInstanceError):
        anchor.price = Decimal("1")
    three = tick(3, 30, "900.00")
    book.consume(three, candles.consume(three))
    assert book.issued == (anchor,)


def test_horizon_excludes_issuance_candle_gaps_and_requires_completion():
    book = AlertBook((alert(),), event_positions=frozenset({4}))
    rows = [
        tick(1, 0),
        tick(2, 20),
        tick(3, 60),
        tick(4, 65, "1050.00"),
        tick(5, 180),
        tick(6, 240),
    ]
    issued, outcomes = process(book, rows)
    assert len(issued) == 1 and outcomes == []
    candles = CandleBuilder()
    # Continue using actual completion from the observed minute, no EOF fabrication.
    for row in rows:
        candles.consume(row)
    end = tick(7, 300)
    _, results = book.consume(end, candles.consume(end))
    assert results[0].successful is True
    assert results[0].window_candles == tuple(BASE + timedelta(seconds=s) for s in (60, 180, 240))
    assert book.issued == tuple(issued)


def test_failed_alert_retained_and_insufficient_window_expires():
    book = AlertBook((alert(),))
    issued, outcomes = process(
        book, [tick(1, 0), tick(2, 20), tick(3, 60), tick(4, 120), tick(5, 180), tick(6, 240)]
    )
    assert outcomes[0].successful is False
    assert book.issued == tuple(issued)
    short = AlertBook((alert(),))
    process(short, [tick(1, 0), tick(2, 20)])
    result = short.finish(tick(2, 20))[0]
    assert result.status == "EXPIRED" and result.successful is None
    assert short.finish(tick(2, 20)) == ()


def test_future_cutoff_duplicates_backdating_and_missed_schedule_rejected():
    with pytest.raises(ValueError, match="cutoff"):
        replace(alert(), data_cutoff_position=3)
    with pytest.raises(ValueError, match="duplicate"):
        AlertBook((alert(), alert()))
    with pytest.raises(ValueError, match="issuance"):
        process(AlertBook((alert(seconds=19),)), [tick(1, 0), tick(2, 20)])
    with pytest.raises(ValueError, match="issuance"):
        process(AlertBook((alert(),)), [tick(1, 0), tick(3, 30)])


@pytest.mark.parametrize("value", [0, -1, True, 1.5, 1001])
def test_horizon_boundary(value):
    with pytest.raises(ValueError):
        alert(evaluation_horizon=value)


def test_no_alerts_and_schedule_is_not_public():
    book = AlertBook(())
    assert process(book, [tick(1, 0), tick(2, 60)]) == ([], [])
    assert book.finish(tick(2, 60)) == ()


def test_anchor_and_outcome_contracts_enforce_deep_immutability():
    from quantlab.alerts import AlertOutcome, IssuedAlert

    with pytest.raises(ValueError, match="ISSUED"):
        IssuedAlert(alert(), BASE, Decimal("1000.00"))
    with pytest.raises(ValueError, match="anchor"):
        IssuedAlert(replace(alert(), status="ISSUED"), BASE + timedelta(minutes=1), Decimal("1000"))
    source = [BASE + timedelta(minutes=1)]
    outcome = AlertOutcome(
        UUID(int=2), "EXPIRED", None, BASE + timedelta(minutes=2), 10, source, "short"
    )
    source.clear()
    assert outcome.window_candles == (BASE + timedelta(minutes=1),)
    with pytest.raises(ValueError):
        replace(outcome, status="RESOLVED", successful=None)
    with pytest.raises(ValueError):
        replace(outcome, status="EXPIRED", successful=True)
