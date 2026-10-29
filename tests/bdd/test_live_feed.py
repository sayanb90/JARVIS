import asyncio
import os
import signal
from datetime import date, datetime, timedelta

import pytest
from pytest_bdd import given, parsers, scenarios, then, when

from data_layer import verify
from data_layer.upstox.feed import UpstoxFeed, run_day
from data_layer.upstox import candles
from tests.bdd.fake_upstox import FakeUpstox, session_frames, session_price, tick_frame
from tests.unit.test_upstox_candles import FakeHttp, ok
from tests.helpers import OHLC_H, ms, write_csv

scenarios("../features/live_feed.feature")
pytestmark = pytest.mark.bdd

DAY = date(2026, 10, 23)
KEYS = {"NSE_EQ|INE000000001": "RELIANCE.NS", "NSE_EQ|INE000000002": "INFY.NS"}


@given(parsers.parse("the India universe is {a} and {b}"))
def _universe(ctx, a, b):
    ctx.w.set_universe({"IN": [a, b]})
    ctx.keymap = {k: s for k, s in KEYS.items() if s in (a, b)}


@given(parsers.parse("the exchange clock reads {hhmm} UTC on {day}"))
def _clock(ctx, hhmm, day):
    h, m = map(int, hhmm.split(":"))
    y, mo, d = map(int, day.split("-"))
    ctx.now = datetime(y, mo, d, h, m)
    ctx.stop_after_quiet = 0.4


@given("an Upstox server streaming a full session with one update every 5 seconds")
def _server(ctx):
    ctx.server = FakeUpstox(session_frames(list(ctx.keymap), step_s=5))


def sec(hms: str):
    h, m, s_ = map(int, hms.split(":"))
    return ms(h, m, s_)


@given(parsers.parse("an Upstox server that sends 3 updates for {sym} within the second starting {hms}"))
def _burst(ctx, sym, hms):
    key = next(k for k, v in ctx.keymap.items() if v == sym)
    base = sec(hms)
    ctx.server = FakeUpstox([tick_frame(key, base + off, px, vtt, bid=px - 0.05)
                             for off, px, vtt in ((100, 100.0, 1000), (450, 103.0, 1010), (900, 101.0, 1030))])
    ctx.burst = (sym, hms)


@given(parsers.parse("an Upstox server that sends updates for {sym} at {a}, {b} and {c} only"))
def _sparse(ctx, sym, a, b, c):
    key = next(k for k, v in ctx.keymap.items() if v == sym)
    ctx.server = FakeUpstox([tick_frame(key, sec(t), 100.0 + i, 1000 + 10 * i) for i, t in enumerate((a, b, c))])
    ctx.sparse = (sym, [a, b, c])


@given("Upstox's own 1-minute candles for that day match the session")
def _rest_candles(ctx):
    # independent of the collector: derive each minute's candle from the same price formula the server uses
    n_ticks = list(range(0, 22_500, 5))
    cs = {}
    for i, (key, sym) in enumerate(ctx.keymap.items()):
        rows = []
        for minute in range(375):
            idx = [n for n, off in enumerate(n_ticks) if off // 60 == minute]
            px = [session_price(i, n) for n in idx]
            t = datetime(2026, 10, 23, 9, 15) + timedelta(minutes=minute)
            rows.append([f"{t:%Y-%m-%dT%H:%M:%S}+05:30", px[0], max(px), min(px), px[-1], 10 * len(idx), 0])
        cs[key.replace("|", "%7C")] = rows[::-1]
    ctx.rest_http = FakeHttp(fn=lambda url: next(ok(v) for k, v in cs.items() if k in url))


@given(parsers.parse("the server drops the first connection after {n:d} frames"))
def _drop(ctx, n):
    ctx.server.drop_after = n


@given("the server then stays quiet")
def _quiet(ctx):
    ctx.server.hold_s = 60


@given(parsers.parse("an Upstox server that answers the handshake with HTTP {status:d}"))
def _deny(ctx, status):
    ctx.server = FakeUpstox([], deny_status=status)


@given("an Upstox server whose only updates were last traded on the previous day")
def _stale(ctx):
    ctx.server = FakeUpstox([tick_frame(k, ms(9, 59, 0, (2026, 10, 22)), 100.0, 5000) for k in ctx.keymap])


@given("an Upstox server whose updates are stamped hours ahead of the exchange clock")
def _future(ctx):
    ctx.server = FakeUpstox([tick_frame(k, ms(13, 0, 0), 100.0, 5000) for k in ctx.keymap])


@given("Yahoo's daily bar for that day agrees with the session")
def _yahoo_daily(ctx):
    rows = []
    for i, sym in enumerate(ctx.keymap.values()):
        rows.append(["2026.10.23", sym, "IN", 1, 2, 0, session_price(i, 4499), 4499 * 10])
    write_csv(ctx.w.data / "ohlc/ohlc_2026_in.csv", OHLC_H, rows)
    ctx.w.lake.load_ohlc(("IN",))


def collect(ctx, token, through_run_day=False, sigint_when_sent=False):
    async def main():
        async with ctx.server.start() as srv:
            feed = UpstoxFeed(token, ctx.keymap, url=ctx.server.url(srv), lake_dir=ctx.w.lakedir,
                              clock=lambda: ctx.now, flush_s=0.05, write_s=0.05)
            ctx.feed = feed
            stop = asyncio.Event()

            async def operator():
                await ctx.server.all_sent.wait()
                await asyncio.sleep(0.4)
                if sigint_when_sent:
                    os.kill(os.getpid(), signal.SIGINT)
                else:
                    stop.set()

            helper = None if ctx.server.deny_status or not ctx.server.frames else asyncio.create_task(operator())
            try:
                if through_run_day:
                    await asyncio.wait_for(run_day(feed, ctx.now + timedelta(seconds=120)), timeout=20)
                else:
                    await asyncio.wait_for(feed.run_session(stop), timeout=20)
                    from data_layer import tick_store
                    tick_store.compact_day(feed.label, DAY, ctx.w.lakedir)
            except RuntimeError as exc:
                ctx.error = str(exc)
            finally:
                if helper:
                    helper.cancel()
            if not ctx.server.frames and not ctx.server.deny_status:
                pass
    ctx.error = None
    asyncio.run(main())


@when("the collector runs the session with a valid token")
def _run_ok(ctx):
    collect(ctx, "VALID")


@when("the collector runs the session with an expired token")
def _run_expired(ctx):
    collect(ctx, "EXPIRED")


@when("I pull that day's candles from the candle service")
def _pull_candles(ctx, monkeypatch):
    monkeypatch.setattr(candles, "ist_today", lambda: date(2026, 10, 24))
    candles.pull(candles.CandleClient("T", session=ctx.rest_http, delay=0), ctx.keymap, DAY, DAY, source="historical",
                 lake_dir=ctx.w.lakedir)


@when("the operator presses Ctrl+C while the session is running")
def _ctrl_c(ctx):
    collect(ctx, "VALID", through_run_day=True, sigint_when_sent=True)


def bars(ctx):
    q = ctx.w.query.Lake()
    return q.sql("SELECT * FROM bars_1s ORDER BY symbol, ts") if "bars_1s" in q.tables() else None


@then("the server saw the bearer token and a full-mode subscription for both instruments")
def _server_saw(ctx):
    assert ctx.server.auth_headers[0] == "Bearer VALID"
    sub = ctx.server.subscriptions[0]
    assert sub["method"] == "sub" and sub["data"]["mode"] == "full"
    assert set(sub["data"]["instrumentKeys"]) == set(ctx.keymap)


@then(parsers.parse("{sym} has {n:d} one-second bars from {first} to {last} UTC"))
def _bar_count(ctx, sym, n, first, last):
    df = bars(ctx)
    df = df[df.symbol == sym]
    assert len(df) == n and df.ts.is_unique
    assert df.ts.min().strftime("%H:%M:%S") == first and df.ts.max().strftime("%H:%M:%S") == last


@then("the bars were stored at one-second resolution")
def _one_second(ctx):
    assert ctx.feed.label == "1s" and (ctx.w.lakedir / "bars/interval=1s").exists()
    assert not (ctx.w.lakedir / "bars/interval=1m").exists()


@then(parsers.parse("{sym} has exactly 1 bar at {hms} built from 3 updates with the first as open and the last as close"))
def _burst_bar(ctx, sym, hms):
    df = bars(ctx)
    assert len(df) == 1
    b = df.iloc[0]
    assert b.ts.strftime("%H:%M:%S") == hms and b.n_ticks == 3
    assert (b.open, b.high, b.low, b.close) == (100.0, 103.0, 100.0, 101.0)
    assert b.volume == 30                                      # (1010-1000) + (1030-1010); the first update is the baseline


@then(parsers.parse("{sym} has bars at exactly those 3 seconds"))
def _sparse_bars(ctx, sym):
    df = bars(ctx)
    assert [t.strftime("%H:%M:%S") for t in df.ts] == ctx.sparse[1]


@then("the live bars rolled up to one minute agree with the candles")
def _rollup_agrees(ctx, capsys):
    rep = verify.verify_against_rest(ctx.w.query.Lake(), DAY, "1s")
    out = capsys.readouterr().out
    assert rep.fails == 0 and "[WARN]" not in out, out
    assert "[PASS] REST minutes that also exist live: 100.0%" in out


@then("the bar volumes add up to the volume traded after the first update")
def _volumes(ctx):
    df = bars(ctx)
    assert df.groupby("symbol").volume.sum().eq(4499 * 10).all()


@then("every bar carries the last bid and ask")
def _l1(ctx):
    df = bars(ctx)
    assert df.bid.notna().all() and df.ask.notna().all() and (df.ask > df.bid).all()


@then("the day's bars pass verification")
def _verify(ctx, capsys):
    rep = verify.verify_bars(ctx.w.query.Lake(), DAY, "1s")
    assert rep.fails == 0, capsys.readouterr().out


@then("the bars agree with Yahoo's daily close within 0.5 percent")
def _agree(ctx, capsys):
    verify.verify_bars(ctx.w.query.Lake(), DAY, "1s")
    out = capsys.readouterr().out
    assert "[PASS] last-bar close vs Yahoo" in out and "median 1.00" in out


@then("the collector connected more than once and subscribed again")
def _reconnected(ctx):
    assert ctx.server.connections >= 2
    assert ctx.server.subscriptions[0] == ctx.server.subscriptions[1] or (
        set(ctx.server.subscriptions[0]["data"]["instrumentKeys"]) == set(ctx.server.subscriptions[1]["data"]["instrumentKeys"]))


@then("the collector stops and tells the operator the token expires daily")
def _expired(ctx):
    assert ctx.error and "expires daily" in ctx.error


@then("no bars are stored")
def _none(ctx):
    df = bars(ctx)
    assert df is None or df.empty


@then("the stored bars are compacted into one file for the day")
def _compacted(ctx):
    folder = ctx.w.lakedir / "bars/interval=1s/date=2026-10-23"
    assert [p.name for p in folder.iterdir()] == ["bars.parquet"]
