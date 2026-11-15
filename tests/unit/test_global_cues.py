"""Global cues (SCRUM-188): ES=F, NQ=F, ^VIX, INR=X, CL=F and ^GSPC collected as market GLOBAL by the Yahoo daily and 1-minute jobs."""

from datetime import date, datetime, timedelta

import pandas as pd

from data_layer import gaps, lake, universe_files, yf_client, yf_fundamentals
from webapp import markets

CUES = ["ES=F", "NQ=F", "^VIX", "INR=X", "CL=F", "^GSPC"]


def test_the_tracked_file_lists_the_six_cues_under_global():
    rows = universe_files.read_universe("GLOBAL")
    assert [r["symbol"] for r in rows] == CUES and {r["market"] for r in rows} == {"GLOBAL"}
    everything = universe_files.read_universe(None)
    assert sum(r["symbol"] in CUES for r in everything) == len(CUES)              # once, not twice
    assert not any(r["symbol"] in CUES for r in universe_files.read_universe("US"))


def test_global_is_a_market_of_the_daily_and_1m_jobs_but_not_of_fundamentals():
    assert "GLOBAL" in lake.MARKETS and "GLOBAL" in lake.BARS_MARKETS and "GLOBAL" in lake.BARS_YF_DEFAULT
    assert not any(s in yf_fundamentals.universe_symbols() for s in CUES)


def test_daily_ohlc_and_1m_bars_ask_yahoo_for_the_cues(monkeypatch, tmp_path):
    asked = {}
    monkeypatch.setattr(yf_client, "BARS_DIR", tmp_path / "bars")          # run_bars writes its (empty) file here, not in the repo
    monkeypatch.setattr(yf_client, "fetch_ohlc_daily", lambda tickers, market, days: asked.setdefault(("daily", market), tickers) and [])
    monkeypatch.setattr(yf_client, "_require_rows", lambda *a: None)
    monkeypatch.setattr(yf_client, "write_ohlc_daily", lambda rows, market: None)
    yf_client.run_ohlc_daily("GLOBAL")
    monkeypatch.setattr(yf_client, "fetch_bars", lambda tickers, market, interval, days_back, window=None: asked.setdefault(("1m", market), tickers) and [])
    yf_client.run_bars("GLOBAL", interval="1m", days_back=3)
    assert asked[("daily", "GLOBAL")] == CUES and asked[("1m", "GLOBAL")] == CUES


def test_gap_healing_plans_missing_days_for_the_cues(tmp_path):
    day = date(2026, 11, 10)
    folder = tmp_path / "bars" / "interval=1m" / f"date={day}"
    folder.mkdir(parents=True)
    pd.DataFrame({"ts": [datetime(2026, 11, 10, 14, 30)], "symbol": ["AAPL"], "market": ["US"], "open": [1.0], "high": [1.0], "low": [1.0],
                  "close": [1.0], "volume": [1]}).to_parquet(folder / "bars.parquet")
    plan = gaps.yahoo_plan(tmp_path, "1m", ["US", "GLOBAL"], date(2026, 11, 13), span=5)
    assert day not in plan["US"] and day in plan["GLOBAL"]


def test_the_viewer_shows_cues_over_the_whole_utc_day():
    for s in CUES:
        assert markets.market_of(s) == "GLOBAL", s
    info = markets.exchange_for("CL=F", "GLOBAL")
    assert info["tz"] == "UTC" and markets.session_minutes(info) == 23 * 60 + 59
    assert markets.market_of("AAPL") == "US"
