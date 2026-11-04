"""Lake discovery methods, the viewer's JSON API, its security behaviour and the demo generator."""
import base64
import json
from datetime import date, datetime, timedelta

import numpy as np
import pandas as pd
import pytest
from fastapi.testclient import TestClient

from data_layer import tick_store
from data_layer.query import Lake
from data_layer.ticks import Bar, BarAggregator, Tick
from webapp import demo
from webapp.app import create_app

DAY = "2026-10-26"
OPEN = datetime(2026, 10, 26, 3, 45)


def bars(sym, n=120, px=100.0, book=True, vol=10, start=OPEN, step=1):
    out = []
    for i in range(n):
        p = px + (i % 7) * 0.05
        out.append(Bar(start + timedelta(seconds=i * step), sym, "IN", p, p + 0.05, p - 0.05, p, vol, p, 2,
                       p - 0.05 if book else None, p + 0.05 if book else None, 100 if book else None, 50 if book else None))
    return out


@pytest.fixture()
def lake(tmp_path):
    d = tmp_path / "lake"
    tick_store.write_bars(bars("A.NS") + bars("B.NS", px=500.0), "1s", d)
    tick_store.compact_day("1s", date(2026, 10, 26), d)
    return d


@pytest.fixture()
def client(lake):
    return TestClient(create_app(lake))


# -- lake discovery -------------------------------------------------------------------------------------------
def test_bar_days_newest_first_and_ignores_empty_folders(tmp_path):
    d = tmp_path / "lake"
    for day in ("2026-10-22", "2026-10-26", "2026-10-23"):
        (d / f"bars/interval=1s/date={day}").mkdir(parents=True)
        pd.DataFrame({"x": [1]}).to_parquet(d / f"bars/interval=1s/date={day}/bars.parquet")
    (d / "bars/interval=1s/date=2026-10-24").mkdir()                        # an empty folder must not be listed
    assert Lake(d).bar_days() == ["2026-10-26", "2026-10-23", "2026-10-22"]
    assert Lake(tmp_path / "nowhere").bar_days() == []


def test_bar_symbols_counts_distinct_timestamps_only(lake):
    tick_store.write_bars(bars("A.NS", n=10), "1s", lake)                  # duplicate segment for A.NS
    df = Lake(lake).bar_symbols(DAY)
    assert dict(zip(df.symbol, df.bars)) == {"A.NS": 120, "B.NS": 120}
    assert Lake(lake).bar_symbols("2020-01-01").empty


def test_day_bars_width_one_dedupes_and_coarser_rolls_up(lake):
    tick_store.write_bars(bars("A.NS", n=10, px=999.0), "1s", lake)        # later duplicate segment wins
    q = Lake(lake)
    d1 = q.day_bars(DAY, "A.NS")
    assert len(d1) == 120 and d1.ts.is_unique and d1.close.iloc[0] == 999.0
    d60 = q.day_bars(DAY, "A.NS", width_s=60)
    assert len(d60) == 2 and d60.volume.sum() == d1.volume.sum()
    assert q.day_bars("2020-01-01", "A.NS").empty


def test_day_overview_matches_hand_computation(lake):
    q = Lake(lake)
    o = q.day_overview(DAY).set_index("symbol")
    a = q.day_bars(DAY, "A.NS")
    r = np.log(a.close / a.close.shift(1)).dropna()
    assert o.loc["A.NS", "realised_vol_pct"] == pytest.approx(np.sqrt((r ** 2).sum() * 250) * 100)
    assert o.loc["A.NS", "volume"] == a.volume.sum() and o.loc["A.NS", "bars"] == 120
    sp = ((a.ask - a.bid) / ((a.ask + a.bid) / 2) * 1e4)
    assert o.loc["A.NS", "avg_spread_bps"] == pytest.approx(sp.mean()) and o.loc["A.NS", "median_spread_bps"] == pytest.approx(sp.median())
    assert list(o.index) == ["A.NS", "B.NS"] and q.day_overview("2020-01-01").empty


def test_day_overview_dedupes_uncompacted_duplicates(lake):
    tick_store.write_bars(bars("A.NS", n=120), "1s", lake)                  # whole day written twice
    assert Lake(lake).day_overview(DAY).set_index("symbol").loc["A.NS", "bars"] == 120


# -- API ------------------------------------------------------------------------------------------------------------
def test_meta_symbols_day_and_overview_happy_path(client):
    m = client.get("/api/meta").json()
    assert m["days"] == [DAY] and m["demo"] is False and 1 in m["widths"] and 300 in m["vol_windows"]
    syms = client.get("/api/symbols", params={"date": DAY}).json()
    assert syms == [{"symbol": "A.NS", "bars": 120}, {"symbol": "B.NS", "bars": 120}]
    d = client.get("/api/day", params={"date": DAY, "symbol": "A.NS"}).json()
    n = len(d["series"]["ts"])
    assert n == 120 and all(len(v) == n for v in d["series"].values()) and d["has_book"] is True
    assert d["summary"]["volume"] == 1200 and d["width"] == 1 and d["series"]["ts"][0] == "2026-10-26 09:15:00"
    o = client.get("/api/overview", params={"date": DAY}).json()
    assert [r["symbol"] for r in o] == ["A.NS", "B.NS"] and o[0]["bars"] == 120


@pytest.mark.parametrize("width,expected", [(5, 24), (60, 2), (300, 1)])
def test_coarser_widths(client, width, expected):
    d = client.get("/api/day", params={"date": DAY, "symbol": "A.NS", "width": width}).json()
    assert len(d["series"]["ts"]) == expected and d["summary"]["volume"] == 1200


def test_day_without_book_data_still_returns_valid_json(tmp_path):
    d = tmp_path / "lake"
    tick_store.write_bars(bars("R.NS", book=False), "1s", d)
    r = TestClient(create_app(d)).get("/api/day", params={"date": DAY, "symbol": "R.NS"})
    assert r.status_code == 200
    body = r.text
    assert "NaN" not in body and "Infinity" not in body
    j = json.loads(body)
    assert j["has_book"] is False and j["summary"]["avg_spread_bps"] is None and set(j["series"]["spread_bps"]) == {None}


@pytest.mark.parametrize("params,code", [
    ({"date": "2026-13-45", "symbol": "A.NS"}, 422), ({"date": "26-10-2026", "symbol": "A.NS"}, 422),
    ({"date": DAY, "symbol": "A.NS'; DROP TABLE x;--"}, 422), ({"date": DAY, "symbol": "../../etc/passwd"}, 422),
    ({"date": DAY, "symbol": "A.NS", "width": 7}, 422), ({"date": DAY, "symbol": "A.NS", "vol_window": 61}, 422),
    ({"date": DAY, "symbol": "NOPE.NS"}, 404), ({"date": "2020-01-01", "symbol": "A.NS"}, 404)])
def test_day_rejects_bad_input_and_reports_missing(client, params, code):
    assert client.get("/api/day", params=params).status_code == code


def test_symbols_and_overview_404_and_422(client):
    assert client.get("/api/symbols", params={"date": "2020-01-01"}).status_code == 404
    assert client.get("/api/overview", params={"date": "2020-01-01"}).status_code == 404
    assert client.get("/api/overview", params={"date": "tomorrow"}).status_code == 422
    assert client.get("/api/day").status_code == 422


def test_overview_cache_refreshes_when_the_day_changes(lake, client):
    first = client.get("/api/overview", params={"date": DAY}).json()
    assert client.get("/api/overview", params={"date": DAY}).json() == first
    tick_store.write_bars(bars("C.NS", px=50.0), "1s", lake)                # collector flushes a new segment
    assert [r["symbol"] for r in client.get("/api/overview", params={"date": DAY}).json()] == ["A.NS", "B.NS", "C.NS"]


def test_app_never_modifies_the_lake(lake, client):
    before = sorted((p.name, p.stat().st_mtime_ns, p.stat().st_size) for p in lake.rglob("*") if p.is_file())
    for path, params in (("/api/meta", {}), ("/api/symbols", {"date": DAY}), ("/api/day", {"date": DAY, "symbol": "A.NS"}),
                         ("/api/overview", {"date": DAY})):
        client.get(path, params=params)
    assert sorted((p.name, p.stat().st_mtime_ns, p.stat().st_size) for p in lake.rglob("*") if p.is_file()) == before


def test_empty_lake_serves_page_and_empty_meta(tmp_path):
    c = TestClient(create_app(tmp_path / "nothing"))
    assert c.get("/api/meta").json()["days"] == [] and c.get("/").status_code == 200


def test_static_assets_and_plotly_are_served_locally(client):
    idx = client.get("/")
    assert idx.status_code == 200 and "JARVIS" in idx.text and "https://" not in idx.text.replace("https://www.w3.org", "")
    assert client.get("/static/app.js").status_code == 200 and client.get("/static/style.css").status_code == 200
    js = client.get("/vendor/plotly.min.js")
    assert js.status_code == 200 and "javascript" in js.headers["content-type"] and len(js.content) > 1_000_000
    assert client.get("/static/../app.py").status_code == 404


def test_responses_are_gzip_compressed(client):
    r = client.get("/api/day", params={"date": DAY, "symbol": "A.NS"}, headers={"Accept-Encoding": "gzip"})
    assert r.headers.get("content-encoding") == "gzip"


# -- optional basic auth ---------------------------------------------------------------------------------------------------
def auth(u, p):
    return {"Authorization": "Basic " + base64.b64encode(f"{u}:{p}".encode()).decode()}


def test_basic_auth_when_configured(lake, monkeypatch):
    monkeypatch.setenv("WEBAPP_USER", "me"); monkeypatch.setenv("WEBAPP_PASSWORD", "s3cret")
    c = TestClient(create_app(lake))
    for path in ("/", "/api/meta", "/api/overview?date=" + DAY, "/vendor/plotly.min.js", "/static/app.js"):
        assert c.get(path).status_code == 401, path
    r = c.get("/api/meta")
    assert r.headers["www-authenticate"].startswith("Basic")
    assert c.get("/api/meta", headers=auth("me", "wrong")).status_code == 401
    assert c.get("/api/meta", headers=auth("you", "s3cret")).status_code == 401
    assert c.get("/api/meta", headers={"Authorization": "Basic !!!notbase64"}).status_code == 401
    assert c.get("/api/meta", headers=auth("me", "s3cret")).status_code == 200
    assert c.get("/", headers=auth("me", "s3cret")).status_code == 200


def test_no_auth_unless_both_user_and_password_are_set(lake, monkeypatch):
    monkeypatch.setenv("WEBAPP_USER", "me"); monkeypatch.delenv("WEBAPP_PASSWORD", raising=False)
    assert TestClient(create_app(lake)).get("/api/meta").status_code == 200


def test_lake_location_comes_from_env_when_not_given(lake, monkeypatch):
    monkeypatch.setenv("WEBAPP_LAKE", str(lake))
    assert TestClient(create_app()).get("/api/meta").json()["days"] == [DAY]


# -- demo generator -----------------------------------------------------------------------------------------------------------
def test_demo_lake_is_flagged_and_has_the_collector_schema(demo_lake):
    c = TestClient(create_app(demo_lake))
    m = c.get("/api/meta").json()
    assert m["demo"] is True and m["days"] == ["2026-10-23", "2026-10-22", "2026-10-21"]
    df = Lake(demo_lake).day_bars("2026-10-23", "RELIANCE.NS")
    assert {"ts", "symbol", "market", "open", "high", "low", "close", "volume", "vwap", "n_ticks", "bid", "ask",
            "bid_qty", "ask_qty"} <= set(df.columns)
    assert set(df.src) == {"demo"}


def test_demo_data_is_internally_consistent_and_deterministic(demo_lake, tmp_path):
    df = Lake(demo_lake).day_bars("2026-10-23", "INFY.NS")
    assert (df.low <= df[["open", "close"]].min(axis=1) + 1e-9).all() and (df.high >= df[["open", "close"]].max(axis=1) - 1e-9).all()
    assert (df.bid < df.ask).all() and (df.volume > 0).all() and df.ts.is_unique and df.ts.is_monotonic_increasing
    assert df.ts.min() >= datetime(2026, 10, 23, 3, 45) and df.ts.max() < datetime(2026, 10, 23, 10, 0)
    again = tmp_path / "again"
    demo.write_demo(again, days=1, end=date(2026, 10, 23), seed=11)
    other = Lake(again).day_bars("2026-10-23", "INFY.NS")
    pd.testing.assert_frame_equal(other.reset_index(drop=True), Lake(demo_lake).day_bars("2026-10-23", "INFY.NS").reset_index(drop=True), check_like=True)


def test_demo_volume_is_u_shaped(demo_lake):
    df = Lake(demo_lake).day_bars("2026-10-23", "RELIANCE.NS")
    t = (df.ts - datetime(2026, 10, 23, 3, 45)).dt.total_seconds()
    open_, mid, close_ = (df.volume[t < 1800].sum(), df.volume[(t > 10_000) & (t < 11_800)].sum(), df.volume[t > 20_700].sum())
    assert open_ > 1.5 * mid and close_ > 1.5 * mid


def test_demo_weekday_helper_and_cli(tmp_path, capsys, monkeypatch):
    import sys
    assert [d.isoformat() for d in demo.weekdays_ending(date(2026, 10, 26), 3)] == ["2026-10-22", "2026-10-23", "2026-10-26"]
    monkeypatch.setattr(sys, "argv", ["demo", "--out", str(tmp_path / "x"), "--days", "1", "--end", "2026-10-23"])
    demo.main()
    assert "wrote 1 synthetic days" in capsys.readouterr().out and (tmp_path / "x/DEMO").exists()


def test_demo_ar1_matches_the_step_by_step_recursion():
    rng = np.random.default_rng(3)
    for a in (0.9995, 0.97):
        eps = rng.normal(0, 1, 5000)
        want = np.zeros(5000)
        for i in range(1, 5000):
            want[i] = a * want[i - 1] + eps[i]
        np.testing.assert_allclose(demo._ar1(eps, a), want, atol=1e-9)


# -- a lake that holds raw ticks only, as production does since 2026-11-03 (SCRUM-129) -----------------------------
@pytest.fixture()
def tick_lake(tmp_path):
    d = tmp_path / "lake"
    ticks = []
    for i in range(60):
        t = OPEN + timedelta(seconds=i)
        ticks.append(Tick(t, "A.NS", 100.0 + (i % 5) * 0.1, 10 * (i + 1), 99.9, 100, 100.1, 50, None, fts=t, rts=t))
        ticks.append(Tick(t, "B.NS", 500.0 + (i % 3), 5 * (i + 1), 499.9, 100, 500.1, 50, None, fts=t, rts=t))
        ticks.append(Tick(t, "NIFTY_25000_CE_09_NOV_26", 120.0 + i % 4, 7 * (i + 1), 119.9, 75, 120.1, 75, 1000 + i, iv=0.15, delta=0.5, gamma=0.0005, fts=t, rts=t))
    tick_store.write_ticks(ticks, d)
    tick_store.compact_ticks_day(date(2026, 10, 26), d)
    (d / "instruments").mkdir()
    pd.DataFrame({"symbol": ["NIFTY_25000_CE_09_NOV_26"], "underlying": ["NIFTY"], "instrument_type": ["CE"], "expiry": [date(2026, 11, 9)],
                  "strike": [25000.0], "lot_size": [75]}).to_parquet(d / "instruments" / "fo.parquet")
    return d


def test_the_market_tab_works_on_a_lake_with_ticks_only(tick_lake):
    """It returned 404 'no bars' because it looked for lake/bars/interval=1s, which the collector no longer writes."""
    client = TestClient(create_app(tick_lake))
    r = client.get("/api/overview", params={"date": DAY})
    assert r.status_code == 200
    rows = r.json()
    assert sorted(x["symbol"] for x in rows) == ["A.NS", "B.NS"]                          # equities only: the option is not in the Market tab
    assert all(x["bars"] == 60 for x in rows)
    assert client.get("/api/overview", params={"date": "2020-01-01"}).status_code == 404
    assert [x["symbol"] for x in client.get("/api/symbols", params={"date": DAY}).json()] == ["A.NS", "B.NS"]


def test_equities_only_overview_equals_the_overview_without_the_options(tick_lake):
    q = Lake(tick_lake)
    full = q.day_overview(DAY).set_index("symbol").drop("NIFTY_25000_CE_09_NOV_26")
    only = q.day_overview(DAY, equities_only=True).set_index("symbol")
    pd.testing.assert_frame_equal(full, only)


def test_fo_summary_from_ticks_matches_the_bars_and_is_cached(tick_lake):
    q = Lake(tick_lake)
    st = q._fo_day_stats(DAY, ["NIFTY_25000_CE_09_NOV_26"]).set_index("symbol").loc["NIFTY_25000_CE_09_NOV_26"]
    b = q.day_bars(DAY, "NIFTY_25000_CE_09_NOV_26", "1s", 1)
    assert st["bars"] == len(b) == 60 and st["volume"] == b["volume"].sum() and st["close"] == b["close"].iloc[-1]
    assert st["oi_first"] == b["oi"].dropna().iloc[0] and st["oi_last"] == b["oi"].dropna().iloc[-1]
    assert q.day_symbol_set(DAY) == {"A.NS", "B.NS", "NIFTY_25000_CE_09_NOV_26"}
    client = TestClient(create_app(tick_lake))
    assert client.get("/api/fo/meta", params={"date": DAY}).json() == {"underlyings": ["NIFTY"]}
    c = client.get("/api/fo/chain", params={"date": DAY, "underlying": "NIFTY"}).json()
    assert c["chain"][0]["ce_oi"] == 1059 and c["chain"][0]["ce_iv"] is not None


# -- Precision: Ticks (SCRUM-131) ------------------------------------------------------------------------------------
def tick_lake_with_quotes(tmp_path):
    """Trades at 04:00:00, :01, :02; between them quote-only rows (no price) and a row before the first trade."""
    d = tmp_path / "lake"
    t0 = datetime(2026, 10, 26, 4, 0, 0)
    rows = [Tick(t0 - timedelta(seconds=5), "A.NS", None, 0, 99.0, 10, 101.0, 10, None, fts=t0 - timedelta(seconds=5), rts=t0 - timedelta(seconds=5))]
    px, vol = [100.0, 101.0, 102.0], [10, 25, 40]
    for i in range(3):
        f = t0 + timedelta(seconds=i)
        rows.append(Tick(f - timedelta(milliseconds=20), "A.NS", px[i], vol[i], px[i] - 0.1, 100, px[i] + 0.1, 50, None, fts=f, rts=f + timedelta(milliseconds=5)))
        rows.append(Tick(f - timedelta(milliseconds=20), "A.NS", px[i], vol[i], px[i] - 0.2, 90, px[i] + 0.2, 60, None, fts=f + timedelta(milliseconds=500), rts=f + timedelta(milliseconds=505)))
    tick_store.write_ticks(rows, d)
    tick_store.compact_ticks_day(date(2026, 10, 26), d)
    return d


def test_ticks_precision_is_offered_first_and_returns_every_row(tmp_path):
    client = TestClient(create_app(tick_lake_with_quotes(tmp_path)))
    assert client.get("/api/meta").json()["widths"][0] == 0
    r = client.get("/api/day", params={"date": DAY, "symbol": "A.NS", "width": 0})
    assert r.status_code == 200
    d = r.json()
    s = d["series"]
    assert d["width"] == 0 and len(s["ts"]) == 6                                       # six rows after the first trade; the row before it is left out
    assert s["ts"][0] == "2026-10-26 09:30:00.000"                                     # IST, milliseconds kept (event time = fts)
    assert s["close"] == [100.0, 100.0, 101.0, 101.0, 102.0, 102.0] and s["open"] == s["close"] == s["high"] == s["low"]
    assert s["volume"] == [0, 0, 15, 0, 15, 0]                                         # the rise of the cumulative volume, a row at a time
    assert s["bid"][0] == pytest.approx(99.9) and s["bid"][1] == pytest.approx(99.8)   # each row's own book
    assert d["summary"]["bars"] == 6 and d["summary"]["volume"] == 30


def test_ticks_precision_404_without_raw_ticks_and_other_widths_unchanged(client, tmp_path):
    assert client.get("/api/day", params={"date": DAY, "symbol": "A.NS", "width": 0}).status_code == 404     # a lake of legacy bars has no ticks
    assert client.get("/api/day", params={"date": DAY, "symbol": "A.NS", "width": 60}).status_code == 200
    assert client.get("/api/day", params={"date": DAY, "symbol": "A.NS", "width": 7}).status_code == 422


def test_ticks_precision_for_option_greeks_returns_each_row(tick_lake):
    client = TestClient(create_app(tick_lake))
    r = client.get("/api/fo/greeks", params={"date": DAY, "symbol": "NIFTY_25000_CE_09_NOV_26", "width": 0})
    assert r.status_code == 200
    s = r.json()["series"]
    assert len(s["ts"]) == 60 and s["ts"][0].endswith(".000") and s["delta"][0] == pytest.approx(0.5)
    assert s["iv"][0] == pytest.approx(15.0)                                           # shown as a percent
    bars = client.get("/api/fo/greeks", params={"date": DAY, "symbol": "NIFTY_25000_CE_09_NOV_26", "width": 60}).json()["series"]
    assert len(bars["ts"]) == 1                                                        # the same minute as one bar


def test_pages_use_a_date_picker_and_the_word_precision(client):
    """SCRUM-132, SCRUM-131: no date drop-down remains in the viewer or the admin page, and the selector is called Precision."""
    from pathlib import Path
    import webapp
    html = client.get("/").text
    assert 'type="date" id="date"' in html and '<select id="date"' not in html
    assert html.count("<span>Precision</span>") == 3 and "Bar width" not in html
    admin = (Path(webapp.__file__).parent / "static" / "admin.html").read_text()
    assert 'type="date" id="day"' in admin and '<select id="day"' not in admin
    js = (Path(webapp.__file__).parent / "static" / "app.js").read_text()
    assert "0: 'Ticks (raw)'" in js and "nearestDay" in js                           # the option exists and a day without data snaps to the nearest
