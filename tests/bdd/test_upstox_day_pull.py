from datetime import date, datetime, timedelta

import pytest
from pytest_bdd import given, parsers, scenarios, then, when

from data_layer import verify
from data_layer.upstox import candles
from data_layer.upstox.candles import CandleClient, pull
from data_layer.upstox.instruments import build_key_map
from data_layer.ticks import BarAggregator, Tick
from tests.unit.test_upstox_candles import FakeHttp, Resp, ok, session_candles

scenarios("../features/upstox_day_pull.feature")
pytestmark = pytest.mark.bdd
DAY = date(2026, 10, 26)


@given(parsers.parse("the India universe is {a} and {b}"))
def _universe(ctx, a, b):
    ctx.w.set_universe({"IN": [a, b]})
    ctx.keymap, _ = build_key_map()
    ctx.sym_key = {s: k for k, s in ctx.keymap.items()}
    ctx.urls = []


@given("a valid Upstox token")
def _token(ctx):
    ctx.token = "VALID"


def serve(ctx, missing=(), reject=False):
    def fn(url):
        ctx.urls.append(url)
        if reject:
            return Resp(401)
        for key, sym in ctx.keymap.items():
            if key.replace("|", "%7C") in url:
                return Resp(400, text="invalid instrument") if sym in missing else ok(session_candles(px=100.0 + len(sym)))
        return Resp(400, text="unknown")
    ctx.http = FakeHttp(fn=fn)


@given(parsers.parse("the Upstox candle service holds a full session for {day}"))
def _full(ctx, day):
    serve(ctx)


@given(parsers.parse("the Upstox candle service holds a full session for {day} but not for {sym}"))
def _missing(ctx, day, sym):
    serve(ctx, missing={sym})


@given("the Upstox candle service rejects the token")
def _reject(ctx):
    serve(ctx, reject=True)


@given(parsers.parse("live bars exist for the first {n:d} minutes of {sym} with a close of {px:g}"))
def _live(ctx, n, sym, px):
    ag, bars = BarAggregator(60), []
    for k in range(n):
        bars += ag.add(Tick(datetime(2026, 10, 26, 3, 45) + timedelta(minutes=k), sym, px, 1000 + k))
    ctx.w.tick_store.write_bars(bars + ag.flush_all(), "1m")


def do_pull(ctx, overwrite=False, source="auto"):
    ctx.error = None
    try:
        ctx.stats = pull(CandleClient(ctx.token, session=ctx.http, delay=0), ctx.keymap, DAY, DAY,
                         source=source, overwrite=overwrite, lake_dir=ctx.w.lakedir)
    except RuntimeError as exc:
        ctx.error = str(exc)


@when("I pull 2026-10-26 from the candle service")
def _pull(ctx, monkeypatch):
    monkeypatch.setattr(candles, "ist_today", lambda: date(2026, 10, 27))
    do_pull(ctx)


@when("I pull 2026-10-26 from the candle service in overwrite mode")
def _pull_over(ctx, monkeypatch):
    monkeypatch.setattr(candles, "ist_today", lambda: date(2026, 10, 27))
    do_pull(ctx, overwrite=True)


@when("I pull 2026-10-26 from the candle service while it is still 2026-10-26 in India")
def _pull_today(ctx, monkeypatch):
    monkeypatch.setattr(candles, "ist_today", lambda: DAY)
    ctx.urls.clear()
    do_pull(ctx)
    ctx.first_run_urls = list(ctx.urls)


@when("I pull 2026-10-26 from the candle service while it is 2026-10-27 in India")
def _pull_past(ctx, monkeypatch):
    monkeypatch.setattr(candles, "ist_today", lambda: date(2026, 10, 27))
    ctx.urls.clear()
    do_pull(ctx, overwrite=True)


def bars(ctx):
    q = ctx.w.query.Lake()
    return q.sql("SELECT * FROM bars_1m ORDER BY symbol, ts") if "bars_1m" in q.tables() else None


@then(parsers.parse("each instrument has {n:d} one-minute bars from {first} to {last} UTC"))
def _each(ctx, n, first, last):
    df = bars(ctx)
    for sym, g in df.groupby("symbol"):
        assert len(g) == n and g.ts.min().strftime("%H:%M") == first and g.ts.max().strftime("%H:%M") == last, sym


@then("the bars are marked as coming from the candle service")
def _src(ctx):
    assert set(bars(ctx).src) == {"rest"}


@then("the stored day is one compacted file")
def _compact(ctx):
    assert [p.name for p in (ctx.w.lakedir / "bars/interval=1m/date=2026-10-26").iterdir()] == ["bars.parquet"]


@then("the day's candle bars pass verification")
def _verify(ctx, capsys):
    rep = verify.verify_bars(ctx.w.query.Lake(), DAY, "1m")
    assert rep.fails == 0, capsys.readouterr().out


@then(parsers.parse("{sym} has {n:d} bars with no duplicates"))
def _nodup(ctx, sym, n):
    df = bars(ctx)
    g = df[df.symbol == sym]
    assert len(g) == n and g.ts.is_unique


@then(parsers.parse("the {n:d} live bars still have a close of {px:g}"))
def _live_kept(ctx, n, px):
    df = bars(ctx)
    live = df[df.src == "live"]
    assert len(live) == n and live.close.eq(px).all()


@then(parsers.parse("{n:d} bars for {sym} came from the candle service"))
def _rest_count(ctx, n, sym):
    df = bars(ctx)
    assert ((df.symbol == sym) & (df.src == "rest")).sum() == n


@then(parsers.parse("no stored bar has a close of {px:g}"))
def _no_px(ctx, px):
    assert not bars(ctx).close.eq(px).any()


@then(parsers.parse("the pull reports no data for {sym}"))
def _nodata(ctx, sym):
    assert ctx.stats["no_data"] == [sym]


@then("the pull stops and tells the operator the token expires daily")
def _expired(ctx):
    assert ctx.error and "expires daily" in ctx.error


@then("no bars are stored")
def _none(ctx):
    df = bars(ctx)
    assert df is None or df.empty


@then("the intraday endpoint was asked first")
def _intraday_first(ctx):
    assert "/intraday/" in ctx.first_run_urls[0]


@then("only the historical endpoint was used")
def _hist_only(ctx):
    assert ctx.urls and all("/intraday/" not in u for u in ctx.urls)
