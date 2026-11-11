"""The Data tab's numbers: dataset dates, gap states, the historical run, backfill progress, snapshot, storage, per-market ranges, and the API."""
import json
import os
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from webapp import coverage as cv
from webapp.app import create_app

TODAY = date(2026, 11, 11)                                         # a Wednesday


def parquet(path: Path, **cols):
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"x": [1], **cols}).to_parquet(path, index=False)
    return path


def calendar(lake: Path, holidays):
    p = lake / "reference" / "market_calendar.json"
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps({"fetched_at": "2026-11-10T00:00:00+00:00", "source": "test", "holidays": holidays}))


@pytest.fixture
def lake(tmp_path):
    d = tmp_path / "lake"
    for day in ("2026-11-03", "2026-11-04", "2026-11-05", "2026-11-06", "2026-11-09"):            # 2026-11-10 is the Diwali holiday: no data, closed
        parquet(d / f"ticks/segment=eq/date={day}/ticks.parquet")
    parquet(d / "ticks/segment=fo/date=2026-11-03/ticks.parquet")
    parquet(d / "ticks/segment=fo/date=2026-11-04/ticks.parquet")                                  # fo: 5 and 6 November missing
    parquet(d / "ticks/segment=fo/date=2026-11-09/ticks.parquet")
    parquet(d / "ticks/segment=fo/date=2026-11-09/seg-000000000000001.parquet")                      # still holding segments: open
    parquet(d / "events/date=2026-11-03/ev.parquet")
    parquet(d / "bars/interval=1m/date=2026-11-06/bars.parquet", ts=[datetime(2026, 11, 6, 4)], symbol=["A.NS"], market=["IN"])
    parquet(d / "bars/interval=5m/date=2026-10-01/bars.parquet")
    calendar(d, [{"date": "2026-11-10", "holiday_type": "TRADING_HOLIDAY", "closed_exchanges": ["NSE", "NFO"], "description": "Diwali-Balipratipada"}])
    return d


def by_id(ds):
    return {x["id"]: x for x in ds}


def states(ds, d):
    return {r["d"]: r["s"] for r in ds["recent"]}


def test_dataset_dates_gaps_and_the_state_of_each_recent_day(lake):
    ds = by_id(cv.build_datasets(lake, TODAY))
    eq, fo = ds["ticks_eq"], ds["ticks_fo"]
    assert (eq["first"], eq["last"], eq["days"], eq["missing"]) == ("2026-11-03", "2026-11-09", 5, 0)            # the weekend and the holiday are not gaps
    st = states(eq, TODAY)
    assert st["2026-11-07"] == "closed" and st["2026-11-08"] == "closed" and st["2026-11-10"] == "closed" and st["2026-11-09"] == "ok"
    assert st["2026-11-02"] == "before" and st["2026-11-11"] == "pending" and len(eq["recent"]) == cv.RECENT_DAYS
    assert (fo["days"], fo["missing"], fo["recent_missing"]) == (3, 2, 2) and states(fo, TODAY)["2026-11-05"] == "missing"
    assert states(fo, TODAY)["2026-11-09"] == "open" and fo["open_days"] == 1                                       # segments not compacted
    assert ds["events"]["missing"] == 4 and ds["events"]["recent_missing"] == 4 and ds["bars_1m"]["days"] == 1
    assert "bars_5m" in ds and ds["bars_5m"]["label"].startswith("5m bars") and "ohlc" in ds and ds["ohlc"]["days"] == 0 and ds["ohlc"]["recent"] == []


def test_without_a_calendar_a_weekday_holiday_is_a_gap_and_unknown_days_expect_data(lake):
    (lake / "reference" / "market_calendar.json").unlink()
    eq = by_id(cv.build_datasets(lake, TODAY))["ticks_eq"]
    assert eq["missing"] == 1 and states(eq, TODAY)["2026-11-10"] == "missing"


def test_daily_series_from_duckdb_and_fundamentals_are_not_daily_gaps(lake):
    pd.DataFrame({"date": pd.to_datetime(["2026-11-04", "2026-11-05", "2026-11-09"]), "symbol": "A", "market": "US", "close": 1.0}).to_parquet(
        parquet(lake / "ohlc/year=2026/ohlc.parquet").parent / "ohlc.parquet", index=False)
    pd.DataFrame({"date": pd.to_datetime(["2026-11-09"]), "currency": ["EUR"], "usd_per_unit": [1.1]}).to_parquet(parquet(lake / "fx/rates.parquet").parent / "rates.parquet", index=False)
    pd.DataFrame({"fetched_at": [datetime(2026, 11, 9, 1, 0)], "symbol": ["A"]}).to_parquet(parquet(lake / "yf_fundamentals/fetch_log.parquet").parent / "fetch_log.parquet", index=False)
    ds = by_id(cv.build_datasets(lake, TODAY))
    assert (ds["ohlc"]["first"], ds["ohlc"]["days"], ds["ohlc"]["missing"]) == ("2026-11-04", 3, 2)          # 6 November (Friday) and 10 November have nothing
    assert ds["fx"]["last"] == "2026-11-09" and ds["fund_yahoo"]["last"] == "2026-11-09" and ds["fund_yahoo"]["missing"] == 0
    assert {r["s"] for r in ds["fund_yahoo"]["recent"]} == set() and ds["fund_yahoo"]["recent"] == []          # not a daily series: no strip


def test_historical_run_stages_and_backfill_progress(lake, tmp_path):
    logs = tmp_path / "logs"
    logs.mkdir()
    (logs / "historical-run.state").write_text("daily\tok\t2026-11-12\nspot\tpartial\t2026-11-12\nspot\tpartial\t2026-11-13\ntrial-expired\tfailed\t2026-11-12\n")
    hr = cv.historical_run(logs)
    st = {s["stage"]: s["status"] for s in hr["stages"]}
    assert hr["present"] and st["daily"] == "ok" and st["spot"] == "partial" and st["trial-expired"] == "failed" and st["fx"] == "pending" and len(hr["stages"]) == 9
    assert [s["date"] for s in hr["stages"] if s["stage"] == "spot"] == ["2026-11-13"]
    assert cv.historical_run(tmp_path / "nowhere")["present"] is False
    (lake / "backfill").mkdir()
    (lake / "backfill" / "state.json").write_text(json.dumps({
        "spot|A.NS|2024-01": {"status": "ok", "rows": 100, "at": "2026-11-12T10:00:00Z"}, "spot|A.NS|2024-02": {"status": "empty", "rows": 0, "at": "2026-11-12T10:01:00Z"},
        "spot|B.NS|2024-01": {"status": "error", "rows": 0, "at": "2026-11-12T10:02:00Z", "msg": "HTTP 500"}, "expired|NIFTY_X|2024-11-01": {"status": "ok", "rows": 5, "at": "2026-11-12T09:00:00Z"}}))
    bf = cv.backfill_state(lake)
    spot = next(k for k in bf["kinds"] if k["kind"] == "spot")
    assert bf["present"] and (spot["ok"], spot["empty"], spot["error"], spot["rows"]) == (1, 1, 1, 100) and bf["last"] == "2026-11-12T10:02:00Z"
    assert bf["errors"] == [{"task": "spot|B.NS|2024-01", "msg": "HTTP 500", "at": "2026-11-12T10:02:00Z"}]
    (lake / "backfill" / "state.json").write_text("not json")
    assert cv.backfill_state(lake)["present"] is False and cv.backfill_state(lake / "none")["present"] is False


def test_snapshot_info_absent_and_present(lake, tmp_path, monkeypatch):
    from research import snapshot as sn
    monkeypatch.setenv("JARVIS_RESEARCH_ROOT", str(tmp_path / "research"))
    assert cv.snapshot_info() == {"present": False, "root": str(tmp_path / "research")}
    os.utime(lake / "ticks/segment=eq/date=2026-11-03/ticks.parquet", (1, 1))
    sn.create(lake, tmp_path / "research", now=datetime.now(timezone.utc) + timedelta(days=1), quiet_seconds=0)
    info = cv.snapshot_info()
    assert info["present"] and info["files"] > 0 and info["newest_tick_day"] and info["count"] == 1 and info["id"]


def test_storage_sizes_and_runway(lake):
    (lake / "ticks/segment=eq/date=2026-11-03/ticks.parquet").write_bytes(b"x" * 1000)
    st = cv.storage(lake)
    names = {r["name"]: r["bytes"] for r in st["rows"]}
    assert names["ticks/segment=eq"] >= 1000 and "bars/interval=1m" in names and "events" in names and st["total"] == sum(names.values())
    assert st["free"] > 0 and st["tick_days_counted"] >= 1 and st["runway_days"] > 0
    assert cv.storage(lake / "nolake")["rows"] == []


def test_market_ranges_read_the_bars_and_report_errors_instead_of_failing(lake):
    pd.DataFrame({"date": pd.to_datetime(["2026-11-04", "2026-11-05"]), "symbol": ["A", "B"], "market": ["US", "UK"], "close": 1.0}).to_parquet(
        parquet(lake / "ohlc/year=2026/ohlc.parquet").parent / "ohlc.parquet", index=False)
    r = cv.market_ranges(lake)
    assert r["error"] is None and [(m["market"], m["symbols"]) for m in r["ohlc"]] == [("UK", 1), ("US", 1)]
    assert r["bars_1m"] == [{"market": "IN", "first": "2026-11-06", "last": "2026-11-06", "bars": 1, "symbols_latest": 1, "latest_day": "2026-11-06"}]
    us = lake / "bars/interval=1m/date=2026-11-09/bars.parquet"
    us.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"ts": [datetime(2026, 11, 9, 4)] * 3, "symbol": ["U1", "U2", "U2"], "market": ["US"] * 3}).to_parquet(us, index=False)
    r2 = cv.market_ranges(lake)["bars_1m"]
    assert [(m["market"], m["symbols_latest"], m["latest_day"]) for m in r2] == [("IN", 1, "2026-11-06"), ("US", 2, "2026-11-09")]      # each on its own last day
    us.unlink()
    bad = lake / "bars/interval=1m/date=2026-11-09/bars.parquet"
    bad.parent.mkdir(exist_ok=True); bad.write_bytes(b"not parquet")
    assert cv.market_ranges(lake)["error"]


def test_api_returns_the_payload_is_cached_and_needs_the_viewer_login(lake, monkeypatch):
    client = TestClient(create_app(lake))
    j = client.get("/api/coverage").json()
    assert {d["id"] for d in j["datasets"]} >= {"ticks_eq", "ticks_fo", "events", "bars_1m", "ohlc"} and j["live_sessions"] == 5 and j["recent_days"] == 60
    assert j["history_run"]["present"] is False and j["snapshot"]["present"] is False and j["storage"]["rows"]
    assert client.get("/api/coverage/markets").json()["bars_1m"][0]["market"] == "IN"
    (lake / "ticks/segment=eq/date=2026-11-12").mkdir()
    parquet(lake / "ticks/segment=eq/date=2026-11-12/ticks.parquet")
    assert client.get("/api/coverage").json()["live_sessions"] == 5                              # served from the one-minute cache
    monkeypatch.setenv("WEBAPP_USER", "u"); monkeypatch.setenv("WEBAPP_PASSWORD", "p")
    locked = TestClient(create_app(lake))
    assert locked.get("/api/coverage").status_code == 401 and locked.get("/api/coverage", auth=("u", "p")).status_code == 200
