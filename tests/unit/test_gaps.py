"""Nightly gap healing: what is missing is found from what is stored, fetched again, and given up on after two empty nights."""
from datetime import date, datetime, timedelta, timezone

import pytest

from data_layer import gaps, market_calendar, tick_store
from data_layer.ticks import Bar

TODAY = date(2026, 11, 12)                                   # a Thursday


def put(lake_dir, market, day, sym="X"):
    ts = datetime(day.year, day.month, day.day, 8, 0)
    tick_store.write_bars([Bar(ts, sym, market, 1, 2, 0.5, 1.5, 10, None, None, None, None, None, None, "yahoo")], "1m", lake_dir)


@pytest.fixture
def lake_dir(tmp_path):
    return tmp_path / "lake"


def test_weekdays_skip_weekends():
    assert [d.isoformat() for d in gaps.weekdays(date(2026, 11, 6), date(2026, 11, 10))] == ["2026-11-06", "2026-11-09", "2026-11-10"]


def test_a_market_missing_a_weekday_is_planned_for_just_that_market(lake_dir):
    for d in gaps.weekdays(TODAY - timedelta(days=28), TODAY - timedelta(days=1)):
        put(lake_dir, "US", d)
        if d != date(2026, 11, 9):
            put(lake_dir, "UK", d)
    plan = gaps.yahoo_plan(lake_dir, "1m", ["US", "UK", "JP"], TODAY)
    assert plan["US"] == [] and plan["UK"] == [date(2026, 11, 9)]
    assert len(plan["JP"]) == len(gaps.weekdays(TODAY - timedelta(days=28), TODAY - timedelta(days=1)))          # no JP rows at all: every weekday
    assert gaps.days_back_for(plan["UK"], TODAY) == 4 and gaps.days_back_for([], TODAY, floor=5) == 5 and gaps.days_back_for(plan["UK"], TODAY, floor=9) == 9


def test_a_day_that_stays_empty_is_given_up_on_after_two_attempts_and_a_filled_day_is_forgotten(lake_dir):
    for d in gaps.weekdays(TODAY - timedelta(days=28), TODAY - timedelta(days=1)):
        put(lake_dir, "US", d)
    holiday, flaky = date(2026, 11, 5), date(2026, 11, 6)
    for d in (holiday, flaky):                                    # remove both days from the lake
        import shutil
        shutil.rmtree(lake_dir / "bars" / "interval=1m" / f"date={d}")
    assert gaps.yahoo_plan(lake_dir, "1m", ["US"], TODAY)["US"] == [holiday, flaky]
    assert gaps.record(lake_dir, "1m", "US", [holiday, flaky]) == []                       # first empty night: counted, still asked for
    assert gaps.yahoo_plan(lake_dir, "1m", ["US"], TODAY)["US"] == [holiday, flaky]
    put(lake_dir, "US", flaky)                                                              # the second night Yahoo has one of them
    assert gaps.record(lake_dir, "1m", "US", [holiday, flaky]) == [holiday]                # the other is now taken to be a holiday
    assert gaps.yahoo_plan(lake_dir, "1m", ["US"], TODAY)["US"] == []


def test_india_missing_uses_the_nse_calendar_and_waits_for_the_close_today(lake_dir):
    market_calendar.save([{"date": "2026-11-10", "holiday_type": "TRADING_HOLIDAY", "closed_exchanges": ["NSE"], "description": "Diwali"}],
                         lake_dir, now=datetime(2026, 11, 11, 3, 0))
    put(lake_dir, "IN", date(2026, 11, 9))
    put(lake_dir, "US", date(2026, 11, 11))                                                 # another market's rows do not count for India
    before_close = datetime(2026, 11, 12, 9, 0, tzinfo=timezone.utc)
    after_close = datetime(2026, 11, 12, 10, 30, tzinfo=timezone.utc)
    m = gaps.india_missing(lake_dir, TODAY, before_close, span=5)
    assert date(2026, 11, 10) not in m and date(2026, 11, 9) not in m and date(2026, 11, 7) not in m       # holiday, present, weekend
    assert m == [date(2026, 11, 11)]                                                         # yesterday only; today is not expected yet
    assert gaps.india_missing(lake_dir, TODAY, after_close, span=5) == [date(2026, 11, 11), date(2026, 11, 12)]
    gaps.record(lake_dir, "1m", "IN", [date(2026, 11, 11)]); gaps.record(lake_dir, "1m", "IN", [date(2026, 11, 11)])
    assert gaps.india_missing(lake_dir, TODAY, after_close, span=5) == [date(2026, 11, 12)]  # given up on


def test_the_bars_job_fetches_back_to_the_oldest_missing_day_and_loads_those_files(sched, lake_dir, monkeypatch):
    for d in gaps.weekdays(date.today() - timedelta(days=28), date.today() - timedelta(days=1)):
        for m in sched.lake.BARS_YF_DEFAULT:
            if not (m == "JP" and d == gaps.weekdays(date.today() - timedelta(days=28), date.today() - timedelta(days=1))[-3]):
                put(sched.lake.LAKE_DIR, m, d)
    seen = {}
    monkeypatch.setattr(sched, "run_bars", lambda **kw: seen.update(kw))
    monkeypatch.setattr(sched.lake, "load_bars", lambda iv, since, markets=(): seen.update(since=since, markets=markets))
    assert sched.job_bars("ALL", "1m") is True
    assert set(seen["days_back_by"]) == {"JP"} and 2 <= seen["days_back_by"]["JP"] <= 6 and seen["since"] <= date.today() - timedelta(days=seen["days_back_by"]["JP"])
    seen.clear()                                                                            # nothing missing: the usual window, the usual load
    for m in ("JP",):
        d = gaps.weekdays(date.today() - timedelta(days=28), date.today() - timedelta(days=1))[-3]
        put(sched.lake.LAKE_DIR, m, d)
    assert sched.job_bars("ALL", "1m") is True and seen["days_back_by"] is None and seen["since"] is None
    seen.clear()
    assert sched.job_bars("ALL", "5m") is True and seen["days_back_by"] is None             # only 1-minute bars heal


def test_the_daily_job_re_fetches_the_last_30_days_by_default(sched, monkeypatch):
    calls = []
    monkeypatch.setattr(sched, "run_ohlc_daily", lambda **kw: calls.append(kw))
    monkeypatch.setattr(sched.lake, "load_ohlc", lambda m, daily=False: None)
    sched.job_ohlc_daily("ALL")
    assert calls[0]["days_back"] == 30
