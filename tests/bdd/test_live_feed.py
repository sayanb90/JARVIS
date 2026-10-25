import asyncio
import os
import signal
from datetime import date, datetime, timedelta

import pytest
from pytest_bdd import given, parsers, scenarios, then, when

from data_layer import verify
from data_layer.upstox.feed import UpstoxFeed, run_day
from tests.bdd.fake_upstox import FakeUpstox, session_frames, tick_frame
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


@given("an Upstox server streaming a full session for the universe")
def _server(ctx):
    ctx.server = FakeUpstox(session_frames(list(ctx.keymap)))


@given(parsers.parse("the server drops the first connection after {n:d} frames"))
def _drop(ctx, n):
    ctx.server.drop_after = n


@given("the server then stays quiet")
def _quiet(ctx):
    ctx.server.hold_s = 60


@given(parsers.parse("an Upstox server that answers the handshake with HTTP {status:d}"))
def _deny(ctx, status):
    ctx.server = FakeUpstox([], deny_status=status)


@given("an Upstox server whose only ticks were last traded on the previous day")
def _stale(ctx):
    ctx.server = FakeUpstox([tick_frame(k, ms(9, 59, 0, (2026, 10, 22)), 100.0, 5000) for k in ctx.keymap])


@given("an Upstox server whose ticks are stamped hours ahead of the exchange clock")
def _future(ctx):
    ctx.server = FakeUpstox([tick_frame(k, ms(13, 0, 0), 100.0, 5000) for k in ctx.keymap])


@given("Yahoo's daily bar for that day agrees with the session")
def _yahoo_daily(ctx):
    rows = []
    for i, sym in enumerate(ctx.keymap.values()):
        last_px = 100.0 * (i + 1) + 374 * 0.01 + 35 * 0.0001
        rows.append(["2026.10.23", sym, "IN", 1, 2, 0, last_px, 749 * 10])
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


@when("the operator presses Ctrl+C while the session is running")
def _ctrl_c(ctx):
    collect(ctx, "VALID", through_run_day=True, sigint_when_sent=True)


def bars(ctx):
    q = ctx.w.query.Lake()
    return q.sql("SELECT * FROM bars_1m ORDER BY symbol, ts") if "bars_1m" in q.tables() else None


@then("the server saw the bearer token and a full-mode subscription for both instruments")
def _server_saw(ctx):
    assert ctx.server.auth_headers[0] == "Bearer VALID"
    sub = ctx.server.subscriptions[0]
    assert sub["method"] == "sub" and sub["data"]["mode"] == "full"
    assert set(sub["data"]["instrumentKeys"]) == set(ctx.keymap)


@then(parsers.parse("{sym} has {n:d} one-minute bars from {first} to {last} UTC"))
def _bar_count(ctx, sym, n, first, last):
    df = bars(ctx)
    df = df[df.symbol == sym]
    assert len(df) == n and df.ts.is_unique
    assert df.ts.min().strftime("%H:%M") == first and df.ts.max().strftime("%H:%M") == last


@then("the bar volumes add up to the volume traded after the first tick")
def _volumes(ctx):
    df = bars(ctx)
    assert df.groupby("symbol").volume.sum().eq(749 * 10).all()


@then("every bar carries the last bid and ask")
def _l1(ctx):
    df = bars(ctx)
    assert df.bid.notna().all() and df.ask.notna().all() and (df.ask > df.bid).all()


@then("the day's bars pass verification")
def _verify(ctx, capsys):
    rep = verify.verify_bars(ctx.w.query.Lake(), DAY, "1m")
    assert rep.fails == 0, capsys.readouterr().out


@then("the bars agree with Yahoo's daily close within 0.5 percent")
def _agree(ctx, capsys):
    verify.verify_bars(ctx.w.query.Lake(), DAY, "1m")
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
    folder = ctx.w.lakedir / "bars/interval=1m/date=2026-10-23"
    assert [p.name for p in folder.iterdir()] == ["bars.parquet"]
