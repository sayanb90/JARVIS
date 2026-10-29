import base64
import json

import pytest
from fastapi.testclient import TestClient
from pytest_bdd import given, parsers, scenarios, then, when

from data_layer import tick_store
from tests.unit.test_webapp import DAY as _DAY, bars
from webapp.app import create_app

scenarios("../features/webapp_viewer.feature")
pytestmark = pytest.mark.bdd


@pytest.fixture()
def ctx(demo_lake):
    from types import SimpleNamespace
    return SimpleNamespace(lake=demo_lake, client=None, resp=None, auth=None)


@given(parsers.parse("synthetic 1-second data for {n:d} trading days and {k:d} instruments"))
def _data(ctx, n, k):
    assert len(list((ctx.lake / "bars/interval=1s").glob("date=*"))) == n


@given("the viewer is running on that data")
def _running(ctx):
    ctx.client = TestClient(create_app(ctx.lake))


@given("a day with bars that carry no bid or ask")
def _rest_day(ctx, tmp_path):
    d = tmp_path / "lake"
    tick_store.write_bars(bars("R.NS", book=False), "1s", d)
    ctx.lake = d
    ctx.client = TestClient(create_app(d))
    ctx.day = _DAY


@given("the viewer requires a username and password")
def _secure(ctx, monkeypatch):
    monkeypatch.setenv("WEBAPP_USER", "owner"); monkeypatch.setenv("WEBAPP_PASSWORD", "pw")
    ctx.client = TestClient(create_app(ctx.lake))
    ctx.basic = {"Authorization": "Basic " + base64.b64encode(b"owner:pw").decode()}


@when("I open the viewer")
def _open(ctx):
    ctx.meta = ctx.client.get("/api/meta").json()
    ctx.page = ctx.client.get("/")


@when(parsers.parse("I choose {sym} on {day}"))
def _choose(ctx, sym, day):
    ctx.resp = ctx.client.get("/api/day", params={"date": day, "symbol": sym})
    ctx.day = ctx.resp.json()


@when(parsers.parse("I choose {sym} on {day} with bars of {w:d} seconds"))
def _choose_w(ctx, sym, day, w):
    ctx.width = w
    ctx.base = ctx.client.get("/api/day", params={"date": day, "symbol": sym}).json()
    ctx.day = ctx.client.get("/api/day", params={"date": day, "symbol": sym, "width": w}).json()


@when("I view R.NS on the day that has no book data")
def _choose_rest(ctx):
    ctx.resp = ctx.client.get("/api/day", params={"date": ctx.day, "symbol": "R.NS"})


@when(parsers.parse("I open the market overview for {day}"))
def _overview(ctx, day):
    ctx.rows = ctx.client.get("/api/overview", params={"date": day}).json()


REQUESTS = {
    "a date that is not a calendar date": {"date": "2026-02-31", "symbol": "RELIANCE.NS"},
    "an instrument name containing SQL": {"date": "2026-10-23", "symbol": "X'; DROP TABLE bars;--"},
    "an instrument name that climbs folders": {"date": "2026-10-23", "symbol": "../../secrets"},
    "a bar width that is not offered": {"date": "2026-10-23", "symbol": "RELIANCE.NS", "width": 7},
    "an instrument with no data that day": {"date": "2026-10-23", "symbol": "NOSUCH.NS"},
    "a day with no data": {"date": "2020-01-02", "symbol": "RELIANCE.NS"},
}


@when(parsers.parse("I ask for {what}"))
def _ask(ctx, what):
    ctx.resp = ctx.client.get("/api/day", params=REQUESTS[what])


@when("someone opens it without credentials")
def _noauth(ctx):
    ctx.resp = ctx.client.get("/api/overview", params={"date": "2026-10-23"})


@when("the owner opens it with the right credentials")
def _owner(ctx):
    ctx.resp2 = ctx.client.get("/api/overview", params={"date": "2026-10-23"}, headers=ctx.basic)


@when("I browse every date, instrument and the overview")
def _browse(ctx):
    ctx.before = sorted((str(p), p.stat().st_mtime_ns, p.stat().st_size) for p in ctx.lake.rglob("*") if p.is_file())
    for day in ctx.client.get("/api/meta").json()["days"]:
        for s in ctx.client.get("/api/symbols", params={"date": day}).json():
            ctx.client.get("/api/day", params={"date": day, "symbol": s["symbol"], "width": 60})
        ctx.client.get("/api/overview", params={"date": day})


@then(parsers.parse("the dates offered are {a}, {b} and {c}"))
def _dates(ctx, a, b, c):
    assert ctx.meta["days"] == [a, b, c] and ctx.page.status_code == 200


@then("I am warned that the data is synthetic")
def _warned(ctx):
    assert ctx.meta["demo"] is True


@then(parsers.parse("the instrument list for {day} has {n:d} instruments with their bar counts"))
def _instruments(ctx, day, n):
    syms = ctx.client.get("/api/symbols", params={"date": day}).json()
    assert len(syms) == n and all(s["bars"] > 1000 for s in syms)


@then("I get price, volume, cumulative volume, best bid and ask, book sizes, imbalance, spread and volatility series of the same length")
def _series(ctx):
    s = ctx.day["series"]
    for k in ("close", "volume", "cum_volume", "bid", "ask", "bid_qty", "ask_qty", "imbalance", "spread_bps", "vol_ann"):
        assert k in s, k
    assert len({len(v) for v in s.values()}) == 1 and ctx.day["has_book"] is True


@then("the times are shown in Indian Standard Time starting at 09:15")
def _ist(ctx):
    first = ctx.day["series"]["ts"][0]
    assert first.startswith("2026-10-23 09:1") or first.startswith("2026-10-23 09:2")
    assert min(t[11:16] for t in ctx.day["series"]["ts"]) >= "09:15" and max(t[11:16] for t in ctx.day["series"]["ts"]) <= "15:29"


@then("the summary reports volume, VWAP, average spread in basis points and realised volatility")
def _summary(ctx):
    s = ctx.day["summary"]
    assert s["volume"] > 0 and s["vwap"] > 0 and s["avg_spread_bps"] > 0 and 5 < s["realised_vol_pct"] < 150


@then("the high is at least the open and the close and the low is at most both")
def _hl(ctx):
    s = ctx.day["summary"]
    assert s["high"] >= max(s["open"], s["close"]) and s["low"] <= min(s["open"], s["close"])


@then("the total volume equals the 1-second total")
def _tot(ctx):
    assert ctx.day["summary"]["volume"] == ctx.base["summary"]["volume"]


@then(parsers.parse("there are at most {n:d} bars"))
def _maxbars(ctx, n):
    assert len(ctx.day["series"]["ts"]) <= n and len(ctx.day["series"]["ts"]) > 1


@then("there is one row per instrument")
def _rows(ctx):
    assert len(ctx.rows) == 10 and len({r["symbol"] for r in ctx.rows}) == 10


@then("every row has volume, average spread, realised volatility and return")
def _rowfields(ctx):
    assert all(r["volume"] > 0 and r["avg_spread_bps"] > 0 and r["realised_vol_pct"] > 0 and r["return_pct"] is not None for r in ctx.rows)


@then("the instrument with the most volume is ITC.NS")
def _top(ctx):
    assert max(ctx.rows, key=lambda r: r["volume"])["symbol"] == "ITC.NS"


@then("a wider-spread instrument is shown to have a higher average spread than a tighter one")
def _wider(ctx):
    by = {r["symbol"]: r["avg_spread_bps"] for r in ctx.rows}
    assert by["WIPRO.NS"] > 3 * by["RELIANCE.NS"]


@then("the chart data is valid JSON with no missing-number markers")
def _json(ctx):
    assert ctx.resp.status_code == 200 and "NaN" not in ctx.resp.text
    json.loads(ctx.resp.text)


@then("the viewer reports that there is no book data")
def _nobook(ctx):
    j = ctx.resp.json()
    assert j["has_book"] is False and j["summary"]["avg_spread_bps"] is None


@then(parsers.parse("the viewer answers with status {code:d}"))
def _status(ctx, code):
    assert ctx.resp.status_code == code


@then("they are asked to log in and see no data")
def _login(ctx):
    assert ctx.resp.status_code == 401 and "www-authenticate" in ctx.resp.headers and "symbol" not in ctx.resp.text


@then("they see the data")
def _data_ok(ctx):
    assert ctx.resp2.status_code == 200 and len(ctx.resp2.json()) == 10


@then("no file in the lake has changed")
def _unchanged(ctx):
    after = sorted((str(p), p.stat().st_mtime_ns, p.stat().st_size) for p in ctx.lake.rglob("*") if p.is_file())
    assert after == ctx.before
