"""Market calendar (SCRUM-141): Upstox holidays, special sessions, weekends, unknown days, and a refresh that never damages a good file."""
from __future__ import annotations

import json
from datetime import date, datetime

import pytest

from data_layer import market_calendar as mc
from data_layer.upstox.rest import Resp

BODY = {"status": "success", "data": [
    {"date": "2026-11-08", "description": "Diwali Laxmi Pujan", "holiday_type": "SPECIAL_TIMING", "closed_exchanges": [],
     "open_exchanges": [{"exchange": "NSE", "start_time": 1, "end_time": 2}, {"exchange": "NFO", "start_time": 1, "end_time": 2}]},
    {"date": "2026-11-10", "description": "Diwali-Balipratipada", "holiday_type": "TRADING_HOLIDAY",
     "closed_exchanges": ["NSE", "NFO", "CDS", "BSE", "BFO", "BCD"], "open_exchanges": []},
    {"date": "2026-11-24", "description": "Guru Nanak Jayanti", "holiday_type": "SETTLEMENT_HOLIDAY", "closed_exchanges": [], "open_exchanges": []},
    {"holiday_date": "2026-12-25", "description": "Christmas", "holiday_type": "trading_holiday", "closed_exchanges": ["nse", "nfo"]},
    {"date": "not a date", "description": "junk"}, "junk"]}


@pytest.fixture()
def cal(tmp_path):
    mc.save(mc.parse_holidays(BODY), tmp_path, now=datetime(2026, 11, 5, 3, 0))
    return mc.load(tmp_path)


def test_parse_keeps_readable_entries_and_upper_cases_exchanges():
    hols = mc.parse_holidays(BODY)
    assert [h["date"] for h in hols] == ["2026-11-08", "2026-11-10", "2026-11-24", "2026-12-25"]
    xmas = hols[-1]
    assert xmas["holiday_type"] == "TRADING_HOLIDAY" and xmas["closed_exchanges"] == ["NSE", "NFO"]
    assert hols[0]["open_exchanges"] == ["NSE", "NFO"]


def test_the_diwali_holiday_is_closed_for_stocks_and_derivatives(cal):
    d = mc.status(date(2026, 11, 10), "NSE", cal, today=date(2026, 11, 10))
    assert (d.state, d.reason, d.expects_data) == ("closed", "Diwali-Balipratipada", False)
    assert mc.status(date(2026, 11, 10), "NFO", cal, today=date(2026, 11, 10)).state == "closed"
    assert mc.status(date(2026, 12, 25), "NSE", cal, today=date(2026, 11, 10)).state == "closed"


def test_an_ordinary_weekday_is_open_and_a_settlement_holiday_still_trades(cal):
    assert mc.status(date(2026, 11, 9), "NSE", cal, today=date(2026, 11, 10)).state == "open"
    assert mc.status(date(2026, 11, 24), "NSE", cal, today=date(2026, 11, 24)).state == "open"        # settlement holiday: trading goes on


def test_a_sunday_is_closed_but_the_muhurat_session_is_special(cal):
    assert mc.status(date(2026, 11, 15), "NSE", cal, today=date(2026, 11, 15)) == mc.Day(date(2026, 11, 15), "closed", "weekend")
    d = mc.status(date(2026, 11, 8), "NSE", cal, today=date(2026, 11, 10))
    assert d.state == "special" and d.expects_data and "Laxmi" in d.reason


def test_without_a_calendar_a_weekday_is_unknown_not_closed():
    d = mc.status(date(2026, 11, 10), "NSE", None, today=date(2026, 11, 10))
    assert d.state == "unknown" and d.expects_data and "no calendar file" in d.reason
    assert mc.status(date(2026, 11, 14), "NSE", None).state == "closed"                              # a Saturday needs no calendar


def test_an_old_calendar_no_longer_vouches_for_a_weekday(tmp_path):
    mc.save(mc.parse_holidays(BODY), tmp_path, now=datetime(2026, 9, 1, 3, 0))
    cal = mc.load(tmp_path)
    assert mc.status(date(2026, 11, 3), "NSE", cal, today=date(2026, 11, 3)).state == "unknown"
    assert mc.status(date(2026, 11, 10), "NSE", cal, today=date(2026, 11, 10)).state == "closed"      # a listed holiday is still known


def test_previous_open_day_and_the_baseline_skip_closed_days(cal):
    assert mc.previous_open_day(date(2026, 11, 11), "NSE", cal) == date(2026, 11, 9)                  # the 10th was a holiday
    assert mc.open_days_back(date(2026, 11, 11), 3, "NSE", cal) == [date(2026, 11, 9), date(2026, 11, 8), date(2026, 11, 6)]


class _Rest:
    def __init__(self, resp):
        self.resp = resp

    def get(self, path, params=None, tag=None):
        assert path == "/v2/market/holidays"
        return self.resp


def test_refresh_stores_the_list_and_a_failed_or_empty_answer_keeps_the_old_file(tmp_path):
    ok = mc.refresh(_Rest(Resp(path="p", ok=True, status=200, data=BODY)), tmp_path, now=datetime(2026, 11, 5, 3, 0))
    assert ok["ok"] and ok["holidays"] == 4 and "2026-11-08 to 2026-12-25" in ok["message"]
    before = (tmp_path / "reference" / "market_calendar.json").read_text()
    bad = mc.refresh(_Rest(Resp(path="p", ok=False, status=401, error_code="UDAPI100050", error_message="Invalid token")), tmp_path)
    empty = mc.refresh(_Rest(Resp(path="p", ok=True, status=200, data={"status": "success", "data": []})), tmp_path)
    assert not bad["ok"] and "401" in bad["message"] and not empty["ok"]
    assert (tmp_path / "reference" / "market_calendar.json").read_text() == before                   # a holiday list that came back empty must not erase the holidays


def test_command_line_status_and_show(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(mc, "calendar_path", lambda lake_dir=None: tmp_path / "reference" / "market_calendar.json")
    assert mc.main(["status", "2026-11-10"]) == 1                                                    # no file yet: unknown
    mc.save(mc.parse_holidays(BODY), tmp_path, now=datetime(2026, 11, 5, 3, 0))
    assert mc.main(["status", "2026-11-10"]) == 0 and "closed (Diwali-Balipratipada)" in capsys.readouterr().out
    assert mc.main(["show"]) == 0 and "Diwali-Balipratipada" in capsys.readouterr().out
