import asyncio
import gzip
import json
from datetime import date, datetime, timedelta

import pandas as pd
import pytest
from fastapi.testclient import TestClient
from pytest_bdd import given, parsers, scenarios, then, when

from data_layer import tick_store, verify
from data_layer.ticks import Tick
from data_layer.upstox import candles
from data_layer.upstox import fo_instruments as fo
from data_layer.upstox.feed import UpstoxFeed
from tests.bdd.fake_upstox import FakeUpstox, tick_frame
from tests.helpers import ms
from tests.unit.test_fo_instruments import master_records
from tests.unit.test_upstox_candles import FakeHttp, ok
from webapp.app import create_app

scenarios("../features/futures_ingestion.feature")
pytestmark = pytest.mark.bdd

DAY = date(2026, 10, 23)
FUT = "NIFTY_FUT_27_OCT_26"


@pytest.fixture(autouse=True)
def _fixed_today(monkeypatch):
    monkeypatch.setattr(fo, "ist_today", lambda: DAY)


@given("an NSE instrument file with NIFTY and RELIANCE futures and options")
def _file(ctx, tmp_path):
    ctx.master = tmp_path / "NSE.json.gz"
    ctx.master.write_bytes(gzip.compress(json.dumps(master_records()).encode()))


def do_import(ctx, asof):
    fo.save_table(fo.parse_master(fo.read_records(ctx.master)), asof)


@when(parsers.parse("I import the instrument file as of {d}"))
def _import(ctx, d):
    do_import(ctx, date.fromisoformat(d))


@given("the instrument file has been imported")
def _imported(ctx):
    do_import(ctx, DAY)


@then("the contract table lists futures, calls and puts for both underlyings")
def _table(ctx):
    t = fo.load_table()
    assert set(t.instrument_type) == {"FUT", "CE", "PE"} and set(t.underlying) == {"NIFTY", "RELIANCE"}


@then("the expired NIFTY future is still resolvable by its symbol")
def _expired(ctx):
    assert fo.resolve("NIFTY_FUT_20_OCT_26")["underlying"] == "NIFTY"


@when(parsers.parse("I select NIFTY with {f:d} futures expiries, {o:d} option expiry and {k:d} strikes either side of {spot:d}"))
def _select(ctx, f, o, k, spot):
    sel, _ = fo.select_contracts(fo.load_table(), ["NIFTY"], DAY, futures=f, option_expiries=o, strikes=k, spot={"NIFTY": spot})
    fo.write_universe(sel)
    ctx.sel = sel


@then(parsers.parse("the collector list holds {f:d} futures and {o:d} options for NIFTY"))
def _list(ctx, f, o):
    u = fo.read_universe(day=DAY)
    assert (u.instrument_type == "FUT").sum() == f and (u.instrument_type != "FUT").sum() == o
    assert set(u.underlying) == {"NIFTY"}


@then("the collector list excludes the expired future")
def _no_expired(ctx):
    assert FUT.replace("27_OCT", "20_OCT") not in set(fo.read_universe(day=DAY).symbol)


@given("the collector list holds the nearest NIFTY future")
def _nearest(ctx):
    sel, _ = fo.select_contracts(fo.load_table(), ["NIFTY"], DAY, futures=1, option_expiries=0)
    fo.write_universe(sel)
    ctx.keymap = fo.fo_key_map(day=DAY)
    assert list(ctx.keymap.values()) == [FUT]
    ctx.key = next(iter(ctx.keymap))
    ctx.now = datetime(2026, 10, 23, 10, 6)


@given(parsers.parse("an Upstox server streaming that future for {n:d} seconds with open interest rising from {oi0:d}"))
def _stream(ctx, n, oi0):
    ctx.server = FakeUpstox([tick_frame(ctx.key, ms(3, 45, i), 24500.0 + i * 0.05, 1000 + 10 * i, bid=24499.95 + i * 0.05, oi=oi0 + i)
                             for i in range(n)])


@given(parsers.parse("equity bars for {sym} exist for the same day"))
def _equity(ctx, sym):
    # Raw ticks, not legacy bars: the live feed persists ticks only (SCRUM-77), and equities and F&O
    # are collected by separate feed processes into the SAME day's ticks folder, so this is the
    # realistic shape (no OI, since equities never carry open interest).
    t0 = datetime(2026, 10, 23, 3, 45)
    tick_store.write_ticks([Tick(t0 + timedelta(seconds=i), sym, 100.0 + (i % 5) * 0.1, 1000 + i * 5,
                                 bid=100.4, bid_qty=10, ask=100.6, ask_qty=12)
                            for i in range(30)], ctx.w.lakedir)


@when("the collector runs the session with a valid token")
def _run(ctx):
    async def main():
        async with ctx.server.start() as srv:
            feed = UpstoxFeed("VALID", ctx.keymap, url=ctx.server.url(srv), lake_dir=ctx.w.lakedir,
                              clock=lambda: ctx.now, flush_s=0.05, write_s=0.05)
            stop = asyncio.Event()

            async def operator():
                await ctx.server.all_sent.wait()
                await asyncio.sleep(0.4)
                stop.set()
            helper = asyncio.create_task(operator())
            try:
                await asyncio.wait_for(feed.run_session(stop), timeout=20)
            finally:
                helper.cancel()
            tick_store.compact_ticks_day(DAY, ctx.w.lakedir)
    asyncio.run(main())


@given("Upstox's candles for that future carry open interest")
def _rest(ctx):
    rows = [[f"2026-10-23T09:{15 + i}:00+05:30", 24500, 24510, 24490, 24505, 100, oi] for i, oi in enumerate((12000, 12010, 12025))]
    ctx.rest_http = FakeHttp(fn=lambda url: ok(rows[::-1]))


@when("I pull that day's candles for the collector list")
def _pull(ctx, monkeypatch):
    monkeypatch.setattr(candles, "ist_today", lambda: date(2026, 10, 24))
    candles.pull(candles.CandleClient("T", session=ctx.rest_http, delay=0), fo.fo_key_map(day=DAY), DAY, DAY,
                 source="historical", lake_dir=ctx.w.lakedir)


def stored(ctx, view="bars_1s"):
    return ctx.w.query.Lake().sql(f"SELECT * FROM {view} ORDER BY symbol, ts")


@then("the server saw a full-mode subscription for the future's key only")
def _sub(ctx):
    sub = ctx.server.subscriptions[0]["data"]
    assert sub["mode"] == "full" and sub["instrumentKeys"] == [ctx.key]


@then(parsers.parse("the future has {n:d} one-second bars stored under the contract's symbol"))
def _count(ctx, n):
    df = stored(ctx)
    df = df[df.symbol == FUT]
    assert len(df) == n and df.ts.is_unique and set(df.market) == {"IN"}


@then(parsers.parse("every bar carries open interest and the last one shows {oi:d}"))
def _oi(ctx):
    df = stored(ctx)
    df = df[df.symbol == FUT]
    assert df.oi.notna().all() and df.oi.iloc[-1] == 9800059 and df.oi.is_monotonic_increasing


@then("the futures bars pass verification")
def _verify(ctx, capsys):
    rep = verify.verify_fo(ctx.w.query.Lake(), DAY, fo.read_universe(day=DAY))
    out = capsys.readouterr().out
    assert rep.fails == 0, out


@then("the viewer reports open interest for that contract")
def _viewer(ctx):
    r = TestClient(create_app(ctx.w.lakedir)).get("/api/day", params={"date": "2026-10-23", "symbol": FUT}).json()
    assert r["has_oi"] and r["summary"]["oi_change"] == 59


@then(parsers.parse("the future has {n:d} one-minute bars with open interest {a:d}, {b:d} and {c:d}"))
def _rest_bars(ctx, n, a, b, c):
    df = stored(ctx, "bars_1m")
    assert len(df) == n and df.oi.tolist() == [a, b, c] and set(df.src) == {"rest"}


@then("both instruments are listed for that day")
def _both(ctx):
    syms = ctx.w.query.Lake().bar_symbols("2026-10-23")
    assert set(syms.symbol) == {FUT, "RELIANCE.NS"}


@then("the equity bars have no open interest")
def _eq_oi(ctx):
    df = stored(ctx)
    assert df[df.symbol == "RELIANCE.NS"].oi.isna().all() and df[df.symbol == FUT].oi.notna().all()
