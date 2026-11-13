"""Exchange facts for the viewer: time zone, session hours, currency, and the Session tab showing a non-India stock in its own time."""
from datetime import datetime, timedelta

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from data_layer import tick_store
from data_layer.ticks import Bar
from webapp import markets
from webapp.app import create_app


@pytest.mark.parametrize("symbol,market,exchange,currency,minutes", [
    ("RELIANCE.NS", "IN", "NSE", "INR", 375), ("NSE_INDEX:Nifty 50", "IN", "NSE", "INR", 375), ("AAPL", "US", "NYSE / Nasdaq", "USD", 390),
    ("ABDN.L", "UK", "London", "GBp", 510), ("SAP.DE", "EU", "Xetra", "EUR", 510), ("NOVN.SW", "EU", "Zurich", "CHF", 510),
    ("VOLV-B.ST", "EU", "Stockholm", "SEK", 510), ("7203.T", "JP", "Tokyo", "JPY", 330), ("2330.TW", "TW", "Taipei", "TWD", 270),
    ("5274.TWO", "TW", "Taipei", "TWD", 270), ("0700.HK", "HK", "Hong Kong", "HKD", 330), ("600519.SS", "CN", "Shanghai / Shenzhen", "CNY", 240),
    ("D05.SI", "SG", "Singapore", "SGD", 480), ("005930.KS", "KR", "Seoul", "KRW", 390),
    ("ES=F", "GLOBAL", "CME · CBOE · FX", "USD", 1439), ("INR=X", "GLOBAL", "CME · CBOE · FX", "USD", 1439),
    ("^VIX", "GLOBAL", "CME · CBOE · FX", "USD", 1439),                     # a global cue listed in data/global_cues.csv
])
def test_each_symbol_gets_its_exchange_currency_and_session_length(symbol, market, exchange, currency, minutes):
    info = markets.exchange_for(symbol)
    assert (info["market"], info["exchange"], info["currency"], markets.session_minutes(info)) == (market, exchange, currency, minutes)


def test_times_are_shown_in_the_exchange_zone_with_daylight_saving():
    t = pd.Series(pd.to_datetime(["2026-11-09 08:00:00", "2026-07-09 07:00:00"]))              # the London open in winter and in summer
    assert markets.to_local(t, "Europe/London") == ["2026-11-09 08:00:00", "2026-07-09 08:00:00"]
    assert markets.tz_label("Europe/London", "2026-11-09") == "GMT" and markets.tz_label("Europe/London", "2026-07-09") == "BST"
    assert markets.tz_label("Asia/Singapore", "2026-11-09") == "SGT" and markets.tz_label("Asia/Kolkata", "2026-11-09") == "IST"


def bar(sym, mk, ts, px, vol):
    return Bar(ts, sym, mk, px, px + 0.5, px - 0.5, px + 0.2, vol, None, None, None, None, None, None, "rest")


def test_a_london_stock_is_shown_in_london_time_with_its_session_and_currency(tmp_path):
    d, day = tmp_path / "lake", datetime(2026, 11, 9, 8, 0)                                          # 08:00 UTC = 08:00 GMT
    tick_store.write_bars([bar("ABDN.L", "UK", day + timedelta(minutes=i), 228 + i * 0.01, 1000) for i in range(30)], "1m", d)
    tick_store.write_bars([bar("RELIANCE.NS", "IN", datetime(2026, 11, 9, 3, 45) + timedelta(minutes=i), 2500 + i, 500) for i in range(30)], "1m", d)
    c = TestClient(create_app(d))
    rows = {r["symbol"]: r for r in c.get("/api/candles/symbols", params={"date": "2026-11-09"}).json()}
    assert rows["ABDN.L"]["market"] == "UK" and rows["RELIANCE.NS"]["market"] == "IN"
    uk = c.get("/api/candles", params={"date": "2026-11-09", "symbol": "ABDN.L"}).json()
    assert uk["series"]["ts"][0] == "2026-11-09 08:00:00" and uk["summary"]["first_time"] == "08:00" and uk["summary"]["expected"] == 510
    assert uk["exchange"]["currency"] == "GBp" and uk["exchange"]["tz_label"] == "GMT" and uk["exchange"]["open"] == "08:00" and uk["exchange"]["close"] == "16:30"
    ind = c.get("/api/candles", params={"date": "2026-11-09", "symbol": "RELIANCE.NS"}).json()
    assert ind["series"]["ts"][0] == "2026-11-09 09:15:00" and ind["exchange"]["tz_label"] == "IST" and ind["summary"]["expected"] == 375
