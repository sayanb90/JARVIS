"""Minute candles and the volume profile (analytics/profile.py), and the viewer's Candles API."""
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import pytest
from fastapi.testclient import TestClient

from analytics.profile import candle_summary, candles_payload, volume_profile
from data_layer import tick_store
from data_layer.query import Lake
from data_layer.ticks import Bar
from webapp.app import create_app

DAY = "2026-10-26"
REST_ONLY_DAY = "2026-10-27"
OPEN = datetime(2026, 10, 26, 3, 45)                         # 09:15 IST


def frame(rows):
    """rows: (minute, open, high, low, close, volume)"""
    return pd.DataFrame([{"ts": OPEN + timedelta(minutes=m), "open": o, "high": h, "low": l, "close": c, "volume": v}
                         for m, o, h, l, c, v in rows])


# -- the profile -------------------------------------------------------------------------------------------------
def test_a_candles_volume_is_spread_over_its_range_and_split_by_direction():
    df = frame([(0, 100.0, 102.0, 100.0, 102.0, 100),          # up candle across two levels of height 1: 50 each
                (1, 101.5, 101.9, 101.1, 101.2, 60)])          # down candle inside the upper level: 60
    p = volume_profile(df, bins=2)
    assert p["step"] == 1.0 and p["prices"] == [100.5, 101.5]
    assert p["up"] == [50.0, 50.0] and p["down"] == [0.0, 60.0] and p["volume"] == [50.0, 110.0]
    assert p["poc"] == 101.5 and p["total"] == 160.0
    assert (p["val"], p["vah"]) == (100.0, 102.0) and p["value_area_pct"] == 100.0       # the POC alone (68.75%) is short of 70%


def test_a_candle_without_range_puts_all_its_volume_on_one_level():
    p = volume_profile(frame([(0, 50.0, 50.0, 50.0, 50.0, 30), (1, 50.0, 52.0, 50.0, 51.0, 20)]), bins=4)
    assert sum(p["volume"]) == pytest.approx(50.0) and max(p["volume"]) >= 30
    assert p["poc"] == p["prices"][int(np.argmax(p["volume"]))]


def test_value_area_grows_from_the_poc_until_it_holds_70_percent():
    # three levels of volume 10, 80, 10: the POC alone is 80%
    p = volume_profile(frame([(0, 100.5, 100.5, 100.5, 100.5, 10), (1, 101.5, 101.5, 101.5, 101.5, 80), (2, 102.5, 102.5, 102.5, 102.5, 10)]), bins=3)
    assert p["volume"] == [10.0, 80.0, 10.0] and p["poc"] == 101.5
    assert (p["val"], p["vah"]) == (101.0, 102.0) and p["value_area_pct"] == 80.0


def test_profile_is_empty_without_volume_and_levels_are_capped():
    assert volume_profile(frame([(0, 10.0, 11.0, 9.0, 10.5, 0)])) == {}
    assert volume_profile(pd.DataFrame(columns=["ts", "open", "high", "low", "close", "volume"])) == {}
    wide = volume_profile(frame([(0, 1.0, 100000.0, 1.0, 5.0, 10)]), bins=100)                    # a mis-priced symbol
    assert len(wide["prices"]) <= 400 and sum(wide["volume"]) == pytest.approx(10.0, rel=1e-3)


def test_profile_volume_always_adds_up_to_the_traded_volume():
    rng = np.random.default_rng(3)
    px = 500 + np.cumsum(rng.normal(0, 0.4, 200))
    rows = [(i, px[i], px[i] + abs(rng.normal(0, 0.3)), px[i] - abs(rng.normal(0, 0.3)), px[i] + rng.normal(0, 0.1), int(rng.integers(1, 5000))) for i in range(200)]
    df = frame(rows)
    for bins in (20, 40, 100):
        assert sum(volume_profile(df, bins)["volume"]) == pytest.approx(df["volume"].sum(), rel=1e-4)


# -- the candle payload and summary --------------------------------------------------------------------------------
def test_payload_has_ist_times_direction_flags_and_a_running_vwap():
    df = frame([(0, 100.0, 101.0, 99.0, 100.5, 100), (1, 100.5, 100.8, 99.5, 99.6, 300)])
    s = candles_payload(df)
    assert s["ts"] == ["2026-10-26 09:15:00", "2026-10-26 09:16:00"] and s["up"] == [True, False]
    first = (101 + 99 + 100.5) / 3
    assert s["vwap"][0] == pytest.approx(first)
    assert s["vwap"][1] == pytest.approx((first * 100 + (100.8 + 99.5 + 99.6) / 3 * 300) / 400)
    assert candles_payload(df.iloc[0:0]) == {"ts": []}


def test_summary_reports_how_much_of_the_session_was_captured():
    df = frame([(m, 100.0, 101.0, 99.0, 100.0 + (m % 2), 10 + m) for m in range(75)])
    s = candle_summary(df, 1)
    assert s["candles"] == 75 and s["expected"] == 375 and s["coverage_pct"] == pytest.approx(20.0)
    assert s["volume"] == sum(10 + m for m in range(75)) and s["first_time"] == "09:15" and s["last_time"] == "10:29"
    assert s["peak_volume"] == 84 and s["peak_time"] == "10:29" and s["up_candles"] + s["down_candles"] == 75
    assert candle_summary(df, 5)["expected"] == 75
    assert candle_summary(df.iloc[0:0]) == {}


# -- the API -----------------------------------------------------------------------------------------------------
def bar(sym, ts, px, vol, src="live"):
    return Bar(ts, sym, "IN", px, px + 0.5, px - 0.5, px + 0.2, vol, px if src == "live" else None, 2 if src == "live" else None, None, None, None, None, src)


@pytest.fixture()
def lake(tmp_path):
    d = tmp_path / "lake"
    live = [bar("A.NS", OPEN + timedelta(seconds=i), 100 + (i // 60) * 0.5, 10) for i in range(300)]              # five minutes of 1-second bars
    rest = [bar("B.NS", OPEN + timedelta(minutes=i), 500 + i, 1000, "rest") for i in range(10)]
    rest += [bar("A.NS", OPEN + timedelta(minutes=i), 100 + i, 700, "rest") for i in range(3)]                    # A.NS also has REST candles that day
    tick_store.write_bars(live, "1s", d)
    tick_store.write_bars(rest, "1m", d)
    tick_store.write_bars([bar("C.NS", OPEN + timedelta(days=1, minutes=i), 50 + i, 100, "rest") for i in range(4)], "1m", d)    # a day with REST candles only
    return d


@pytest.fixture()
def client(lake):
    return TestClient(create_app(lake))


def test_meta_lists_the_days_that_have_candles_from_either_source(client):
    m = client.get("/api/meta").json()
    assert m["days"] == [DAY] and m["candle_days"] == [REST_ONLY_DAY, DAY]
    assert 1 in m["candle_minutes"] and 40 in m["profile_bins"]


def test_symbols_say_which_source_each_instrument_has(client):
    rows = client.get("/api/candles/symbols", params={"date": DAY}).json()
    extra = {"market": "IN", "name": "", "group": "Other"}                                   # no universe file in the test lake: the market comes from the suffix
    assert rows == [{"symbol": "A.NS", "live": True, "rest": True, **extra}, {"symbol": "B.NS", "live": False, "rest": True, **extra}]
    assert client.get("/api/candles/symbols", params={"date": REST_ONLY_DAY}).json() == [{"symbol": "C.NS", "live": False, "rest": True, **extra}]
    assert client.get("/api/candles/symbols", params={"date": "2020-01-01"}).status_code == 404


def test_live_candles_are_rolled_up_from_the_feed_and_win_in_auto(client):
    d = client.get("/api/candles", params={"date": DAY, "symbol": "A.NS"}).json()
    assert d["source"] == "live" and d["minutes"] == 1 and d["summary"]["candles"] == 5 and d["summary"]["volume"] == 3000
    assert d["series"]["ts"][0] == "2026-10-26 09:15:00" and all(len(d["series"][k]) == 5 for k in ("open", "close", "volume", "vwap", "up"))
    assert sum(d["profile"]["volume"]) == pytest.approx(3000, rel=1e-4) and d["profile"]["poc"] > 0
    five = client.get("/api/candles", params={"date": DAY, "symbol": "A.NS", "minutes": 5}).json()
    assert five["summary"]["candles"] == 1 and five["summary"]["volume"] == 3000 and five["summary"]["expected"] == 75


def test_rest_candles_are_used_when_there_is_no_live_data_or_when_asked_for(client):
    b = client.get("/api/candles", params={"date": DAY, "symbol": "B.NS"}).json()
    assert b["source"] == "rest" and b["summary"]["candles"] == 10 and b["summary"]["volume"] == 10000
    a = client.get("/api/candles", params={"date": DAY, "symbol": "A.NS", "source": "rest"}).json()
    assert a["source"] == "rest" and a["summary"]["candles"] == 3 and a["summary"]["volume"] == 2100
    c = client.get("/api/candles", params={"date": REST_ONLY_DAY, "symbol": "C.NS"}).json()
    assert c["source"] == "rest" and c["series"]["ts"][0] == "2026-10-27 09:15:00"


@pytest.mark.parametrize("params,code", [
    ({"date": DAY, "symbol": "B.NS", "source": "live"}, 404),        # B.NS has no live data
    ({"date": DAY, "symbol": "NOPE.NS"}, 404),
    ({"date": "2020-01-01", "symbol": "A.NS"}, 404),
    ({"date": DAY, "symbol": "A.NS", "minutes": 7}, 422),
    ({"date": DAY, "symbol": "A.NS", "bins": 3}, 422),
    ({"date": DAY, "symbol": "A.NS", "source": "other"}, 422),
    ({"date": "26-10-2026", "symbol": "A.NS"}, 422),
    ({"date": DAY, "symbol": "A/../B"}, 422),
])
def test_candles_reject_bad_input_and_report_missing_data(client, params, code):
    assert client.get("/api/candles", params=params).status_code == code


def test_candles_never_modify_the_lake(lake, client):
    before = sorted((p.name, p.stat().st_mtime_ns, p.stat().st_size) for p in lake.rglob("*") if p.is_file())
    client.get("/api/candles/symbols", params={"date": DAY})
    client.get("/api/candles", params={"date": DAY, "symbol": "A.NS"})
    assert sorted((p.name, p.stat().st_mtime_ns, p.stat().st_size) for p in lake.rglob("*") if p.is_file()) == before


def test_minute_candles_prefers_live_and_reports_its_source(lake):
    q = Lake(lake)
    df, src = q.minute_candles(DAY, "A.NS", 1)
    assert src == "live" and len(df) == 5
    df, src = q.minute_candles(DAY, "A.NS", 1, "rest")
    assert src == "rest" and len(df) == 3
    df, src = q.minute_candles(DAY, "ZZZ.NS", 1)
    assert df.empty and src is None
