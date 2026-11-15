"""Market event calendar (SCRUM-176): the tracked file is valid and the loader rejects bad rows."""

from datetime import date, time

import pytest

from data_layer import market_events as me


def row(**kw):
    base = dict(date="2026-02-06", time_ist="10:00", kind="rbi_mpc", description="RBI MPC decision", source="https://rbi.org.in/x", verified="true")
    base.update(kw)
    return base


def test_tracked_file_is_valid():
    events = me.load()
    assert len(events) >= 40
    assert all(e.source for e in events)


def test_tracked_file_has_every_mpc_and_budget_year():
    events = me.load()
    for year in range(2022, 2027):
        assert sum(1 for e in events if e.kind == "rbi_mpc" and e.day.year == year) >= 6 - (1 if year == 2026 else 0)
        assert any(e.kind == "budget" and e.day.year == year for e in events)
    assert date(2024, 7, 23) in me.days_of(["budget"], events)                 # the full 2024 budget after the election
    assert date(2024, 11, 20) in me.days_of(["expiry_rule"], events)
    assert date(2025, 9, 1) in me.days_of(["expiry_rule"], events)


def test_parse_ok():
    (e,) = me.parse_rows([row()])
    assert e.day == date(2026, 2, 6) and e.time_ist == time(10, 0) and e.verified is True


@pytest.mark.parametrize("bad,match", [
    (dict(kind="earnings"), "unknown kind"),
    (dict(source="rbi.org.in"), "source"),
    (dict(source=""), "source"),
    (dict(verified="yes"), "verified"),
    (dict(date="2026-02-30"), "bad date"),
    (dict(time_ist="25:00"), "bad time"),
    (dict(description=""), "description"),
])
def test_bad_rows_are_rejected(bad, match):
    with pytest.raises(me.EventFileError, match=match):
        me.parse_rows([row(**bad)])


def test_duplicates_are_rejected():
    with pytest.raises(me.EventFileError, match="duplicate"):
        me.parse_rows([row(), row()])


def test_internal_source_allowed_and_on_filters():
    events = me.parse_rows([row(), row(kind="budget", description="Budget", source="internal", time_ist="")])
    assert [e.kind for e in me.on(date(2026, 2, 6), ["budget"], events)] == ["budget"]
    assert len(me.on(date(2026, 2, 6), events=events)) == 2


def test_header_is_checked(tmp_path):
    p = tmp_path / "e.csv"
    p.write_text("date,kind\n2026-01-01,budget\n")
    with pytest.raises(me.EventFileError, match="header"):
        me.load(p)
