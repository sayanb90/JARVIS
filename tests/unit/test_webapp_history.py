"""The History tab's numbers: daily candles, index days rolled up from 1-minute bars, intraday roll-ups on the day's own open, volume ranking, validation."""
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from webapp import history as hx
from webapp.app import create_app

NIFTY = "NSE_INDEX:Nifty 50"


def write_ohlc(lake: Path, rows):
    df = pd.DataFrame(rows, columns=["date", "symbol", "market", "open", "high", "low", "close", "volume"])
    df["date"] = pd.to_datetime(df["date"])
    for y, g in df.groupby(df["date"].dt.year):
        p = lake / f"ohlc/year={y}"
        p.mkdir(parents=True, exist_ok=True)
        g.to_parquet(p / "ohlc.parquet", index=False)


def write_bars(lake: Path, day: date, rows):
    p = lake / "bars/interval=1m" / f"date={day}"
    p.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows, columns=["ts", "symbol", "market", "open", "high", "low", "close", "volume"]).to_parquet(p / "bars.parquet", index=False)


def minutes(day: date, symbol, market, n, start_h=3, start_m=45, px=100.0, vol=10):
    t0 = datetime(day.year, day.month, day.day, start_h, start_m)
    return [(t0 + timedelta(minutes=k), symbol, market, px + k, px + k + 2, px + k - 1, px + k + 1, vol) for k in range(n)]


@pytest.fixture
def lake(tmp_path):
    d = tmp_path / "lake"
    rows = []
    for k in range(10):                                           # 10 weekdays from Mon 2026-11-02 minus holidays: none
        day = date(2026, 11, 2) + timedelta(days=k + (2 if k >= 5 else 0))
        rows.append((day, "AAA", "US", 10 + k, 12 + k, 9 + k, 11 + k, 1000 * (k + 1)))
        rows.append((day, "BBB.NS", "IN", 100 + k, 101 + k, 99 + k, 100.5 + k, 500 * (10 - k)))
        rows.append((day, "CCC", "US", 5.0, 6.0, 4.0, 5.0 + (k % 2), 100))
    write_ohlc(d, rows)
    for day in (date(2026, 11, 3), date(2026, 11, 4), date(2026, 11, 6)):                          # 5 November is a holiday: no bars
        write_bars(d, day, minutes(day, NIFTY, "IN", 120, vol=0) + minutes(day, "BBB.NS", "IN", 120, px=1000.0, vol=7))
    write_bars(d, date(2026, 11, 9), minutes(date(2026, 11, 9), "AAA", "US", 60, start_h=14, start_m=30, px=50.0, vol=3))
    return d


def test_instrument_list_names_kinds_and_where_each_has_data(lake):
    out = {r["symbol"]: r for r in hx.instruments(lake)}
    assert out["AAA"]["market"] == "US" and out["AAA"]["daily_days"] == 10 and out["AAA"]["minute"] is True and out["AAA"]["kind"] == "stock"
    assert out["BBB.NS"]["minute"] is True and out["CCC"]["minute"] is False
    assert out[NIFTY]["kind"] == "index" and out[NIFTY]["daily_days"] == 0 and out[NIFTY]["name"] == "Nifty 50" and out[NIFTY]["minute"] is True
    assert out[NIFTY]["group"] == "Indices" and all(r["group"] for r in out.values())     # every instrument sits in a group for the Market > Group > Instrument pickers
    assert hx.instruments(lake)[0]["symbol"] == NIFTY                                              # indices first, then India


def test_daily_candles_come_from_ohlc_with_a_default_range_and_holiday_gaps(lake):
    r = hx.get_bars(lake, "AAA", "1d")
    assert r["source"] == "ohlc" and r["adjusted"] is True and r["bars"] == 10 and r["tf"] == "1d" and r["tz"] == "UTC"
    assert r["open"][0] == 10 and r["high"][-1] == 21 and r["volume"][:2] == [1000, 2000] and r["from"] == "2026-11-02" and r["to"] == "2026-11-13"
    assert r["has_volume"] and r["market"] == "US"
    assert hx.get_bars(lake, "AAA", "1d", span=3)["t"] == ["2026-11-10", "2026-11-11", "2026-11-12", "2026-11-13"]               # 3 days back from the last day
    assert hx.get_bars(lake, "BBB.NS", "1m", span=1)["to"].startswith("2026-11-06") and hx.get_bars(lake, "BBB.NS", "1m", span=1)["from"].startswith("2026-11-06")
    cut = hx.get_bars(lake, "AAA", "1d", "2026-11-05", "2026-11-09")
    assert cut["t"] == ["2026-11-05", "2026-11-06", "2026-11-09"]


def test_an_index_day_is_rolled_up_from_its_minute_bars_and_has_no_volume(lake):
    r = hx.get_bars(lake, NIFTY, "1d", "2026-11-01", "2026-11-30")
    assert r["source"] == "bars_1m" and r["is_index"] and r["has_volume"] is False and r["t"] == ["2026-11-03", "2026-11-04", "2026-11-06"]
    assert (r["open"][0], r["high"][0], r["low"][0], r["close"][0]) == (100.0, 100.0 + 119 + 2, 99.0, 100.0 + 119 + 1)          # first open, max high, min low, last close
    assert r["missing_days"] == ["2026-11-05"]                                                                              # the holiday inside the range


def test_intraday_rolls_up_on_the_days_own_open_and_shows_india_in_ist(lake):
    one = hx.get_bars(lake, "BBB.NS", "1m", "2026-11-03", "2026-11-03")
    assert one["tz"] == "IST" and one["bars"] == 120 and one["t"][0] == "2026-11-03 09:15" and one["session"] == [9 * 60 + 15, 9 * 60 + 15 + 119 + 1]
    five = hx.get_bars(lake, "BBB.NS", "5m", "2026-11-03", "2026-11-03")
    assert five["bars"] == 24 and five["t"][:2] == ["2026-11-03 09:15", "2026-11-03 09:20"]
    assert (five["open"][0], five["close"][0], five["high"][0], five["low"][0], five["volume"][0]) == (1000.0, 1000.0 + 4 + 1, 1000.0 + 4 + 2, 999.0, 35.0)
    hour = hx.get_bars(lake, "BBB.NS", "60m", "2026-11-03", "2026-11-04")
    assert hour["bars"] == 4 and hour["t"][:2] == ["2026-11-03 09:15", "2026-11-03 10:15"]                                 # the first bar starts at the open, not on the hour
    us = hx.get_bars(lake, "AAA", "1m", "2026-11-09", "2026-11-09")
    assert us["tz"] == "UTC" and us["t"][0] == "2026-11-09 14:30"


def test_default_range_ends_on_the_latest_day_the_symbol_has_and_long_ranges_are_clipped(lake):
    r = hx.get_bars(lake, "BBB.NS", "1m")
    assert r["to"].startswith("2026-11-06") and r["clipped"] is False
    big = hx.get_bars(lake, "BBB.NS", "1m", "2020-01-01", "2026-11-06")
    assert big["clipped"] is True and big["from"].startswith("2026-11-03")                                                  # clipped to 30 days: Nov 3 is inside


def test_validation_and_missing_data_give_clear_errors(lake, tmp_path):
    for bad in ("", "a;drop table", "x" * 60):
        with pytest.raises(hx.HistoryError, match="invalid"):
            hx.get_bars(lake, bad)
    with pytest.raises(hx.HistoryError, match="timeframe"):
        hx.get_bars(lake, "AAA", "2h")
    with pytest.raises(hx.HistoryError, match="YYYY-MM-DD"):
        hx.get_bars(lake, "AAA", "1d", "yesterday")
    with pytest.raises(hx.HistoryError, match="after end"):
        hx.get_bars(lake, "AAA", "1d", "2026-12-01", "2026-11-01")
    with pytest.raises(hx.HistoryError, match="no daily candles for ZZZ"):
        hx.get_bars(lake, "ZZZ", "1d")
    with pytest.raises(hx.HistoryError, match="no 1-minute bars for ZZZ"):
        hx.get_bars(lake, "ZZZ", "1m")
    with pytest.raises(hx.HistoryError, match="no 1-minute bars for NSE_INDEX:Nope"):
        hx.get_bars(lake, "NSE_INDEX:Nope", "1m")
    with pytest.raises(hx.HistoryError, match="no data|no 1-minute bars between"):
        hx.get_bars(lake, "AAA", "1m", "2026-01-01", "2026-01-02")
    empty = tmp_path / "empty"
    empty.mkdir()
    with pytest.raises(hx.HistoryError, match="no daily candles in the lake"):
        hx.get_bars(empty, "AAA", "1d")
    with pytest.raises(hx.HistoryError, match="no 1-minute bars in the lake"):
        hx.get_bars(empty, "AAA", "1m")
    with pytest.raises(hx.HistoryError, match="no daily candles in the lake"):
        hx.top_volume(empty)
    assert hx.instruments(empty) == []


def test_symbols_never_reach_the_sql_text(lake):
    with pytest.raises(hx.HistoryError):
        hx.get_bars(lake, "AAA' OR '1'='1", "1d")
    with pytest.raises(hx.HistoryError, match="no daily candles for"):
        hx.get_bars(lake, "AAA OR 1", "1d")                                                      # allowed characters, but just a name nobody has


def test_top_volume_ranks_by_volume_relative_volume_change_and_value(lake):
    r = hx.top_volume(lake, None, "2026-11-13", 10, "volume")
    assert r["date"] == "2026-11-13" and r["latest"] == "2026-11-13" and [x["symbol"] for x in r["rows"]] == ["AAA", "BBB.NS", "CCC"] or r["rows"][0]["symbol"] == "AAA"
    assert r["rows"][0]["volume"] == 10000 and r["rows"][0]["rvol"] and r["rows"][0]["change_pct"] > 0
    us = hx.top_volume(lake, "US", None, 5, "value")
    assert us["market"] == "US" and [x["symbol"] for x in us["rows"]] == ["AAA", "CCC"] and us["rows"][0]["value"] == us["rows"][0]["close"] * us["rows"][0]["volume"]
    assert hx.top_volume(lake, "IN", None, 5, "rvol")["rows"][0]["symbol"] == "BBB.NS"
    ch = hx.top_volume(lake, "US", "2026-11-13", 5, "change")["rows"]
    assert [x["symbol"] for x in ch] == ["AAA", "CCC"] or ch[0]["change_pct"] >= ch[-1]["change_pct"]
    assert len(hx.top_volume(lake, None, None, 1)["rows"]) == 1 and len(hx.top_volume(lake, None, None, 0)["rows"]) == 1
    for args, msg in (((None, None, 5, "bogus"), "sort must be"), ((None, None, 5, "value"), "pick one market"), (("us", None, 5, "volume"), "invalid market"),
                      (("US", "2020-01-01", 5, "volume"), "no volume for 2020-01-01"), (("XX", None, 5, "volume"), "no daily candles for that market")):
        with pytest.raises(hx.HistoryError, match=msg):
            hx.top_volume(lake, *args)


def test_api_routes_status_codes_and_payloads(lake):
    c = TestClient(create_app(lake))
    assert {i["symbol"] for i in c.get("/api/history/instruments").json()} >= {"AAA", NIFTY}
    j = c.get("/api/history/bars", params={"symbol": "AAA", "tf": "1d"}).json()
    assert j["bars"] == 10 and len(j["t"]) == len(j["close"]) == len(j["volume"])
    assert c.get("/api/history/bars", params={"symbol": NIFTY, "tf": "5m", "start": "2026-11-03", "end": "2026-11-03"}).json()["bars"] == 24
    assert c.get("/api/history/bars", params={"symbol": "AAA", "span": 3}).json()["bars"] == 4 and c.get("/api/history/bars", params={"symbol": "AAA", "span": 0}).status_code == 422
    assert c.get("/api/history/bars", params={"symbol": "ZZZ"}).status_code == 404
    assert c.get("/api/history/bars", params={"symbol": "AAA", "tf": "2h"}).status_code == 422
    assert c.get("/api/history/bars", params={"symbol": "a;b"}).status_code == 422
    v = c.get("/api/history/volume", params={"market": "US", "sort": "volume", "n": 5}).json()
    assert v["rows"][0]["symbol"] == "AAA" and c.get("/api/history/volume", params={"sort": "value"}).status_code == 422
    assert c.get("/api/history/volume", params={"date": "2020-01-01"}).status_code == 404
