"""Upstox v3 adapter tests: decode, processor, and a fake WebSocket server speaking the SDK's protocol."""
import asyncio
import json
from datetime import datetime
from http import HTTPStatus

import pytest
import websockets
from websockets.asyncio.server import serve

from data_layer.upstox import decode
from data_layer.upstox.feed import FeedProcessor, UpstoxFeed, next_window
from data_layer.upstox.instruments import build_key_map
from data_layer.upstox.proto import MarketDataFeedV3_pb2 as pb

KEY, IDX = "NSE_EQ|INE002A01018", "NSE_INDEX|Nifty 50"
KMAP = {KEY: "RELIANCE.NS", IDX: "NIFTY50"}
D = datetime(2026, 10, 23)


def ms(h, m, s):
    return int((datetime(2026, 10, 23, h, m, s) - datetime(1970, 1, 1)).total_seconds() * 1000)


def stock_frame(ltt, ltp, vtt, bid=None, key=KEY, typ=pb.live_feed):
    r = pb.FeedResponse(type=typ)
    m = r.feeds[key].fullFeed.marketFF
    m.ltpc.ltp, m.ltpc.ltt, m.ltpc.ltq, m.vtt = ltp, ltt, 1, vtt
    if bid:
        q = m.marketLevel.bidAskQuote.add()
        q.bidP, q.bidQ, q.askP, q.askQ = bid, 11, bid + 0.1, 13
    return r.SerializeToString()


def test_decode_variants_and_unknown_keys():
    r = pb.FeedResponse()
    f = r.feeds[KEY].fullFeed.marketFF
    f.ltpc.ltp, f.ltpc.ltt, f.vtt, f.oi = 100.5, ms(3, 50, 0), 1000, 7
    q = f.marketLevel.bidAskQuote.add(); q.bidP, q.bidQ, q.askP, q.askQ = 100.4, 5, 100.6, 9
    r.feeds[IDX].fullFeed.indexFF.ltpc.ltp = 25000.0
    r.feeds[IDX].fullFeed.indexFF.ltpc.ltt = ms(3, 50, 1)
    r.feeds["NSE_EQ|INE000000000"].ltpc.ltp = 1.0           # unknown instrument
    n = r.feeds["NSE_EQ|INE111111111"].ltpc                  # known, but never traded (ltt = 0)
    n.ltp = 5.0
    km = {**KMAP, "NSE_EQ|INE111111111": "NOTRADE.NS"}
    ticks = {t.symbol: t for t in decode.ticks_from_response(decode.parse_frame(r.SerializeToString()), km)}
    assert set(ticks) == {"RELIANCE.NS", "NIFTY50"}
    t = ticks["RELIANCE.NS"]
    assert (t.ts, t.ltp, t.vol_today, t.bid, t.bid_qty, t.ask, t.ask_qty, t.oi) == \
        (datetime(2026, 10, 23, 3, 50), 100.5, 1000, 100.4, 5, 100.6, 9, 7)
    assert ticks["NIFTY50"].vol_today is None


def test_decode_ltpc_mode_and_greeks_mode_and_market_status():
    r = pb.FeedResponse(type=pb.market_info)
    r.marketInfo.segmentStatus["NSE_EQ"] = pb.NORMAL_OPEN
    assert decode.market_status(decode.parse_frame(r.SerializeToString())) == {"NSE_EQ": "NORMAL_OPEN"}
    r = pb.FeedResponse()
    r.feeds[KEY].ltpc.ltp, r.feeds[KEY].ltpc.ltt = 10.0, ms(4, 0, 0)
    g = r.feeds["OPT"].firstLevelWithGreeks
    g.ltpc.ltp, g.ltpc.ltt, g.vtt, g.firstDepth.bidP = 2.0, ms(4, 0, 0), 50, 1.9
    out = list(decode.ticks_from_response(decode.parse_frame(r.SerializeToString()), {**KMAP, "OPT": "OPT"}))
    assert {t.symbol: t.vol_today for t in out} == {"RELIANCE.NS": None, "OPT": 50}


def test_processor_filters_and_builds_bars():
    p = FeedProcessor(KMAP, interval_s=60, raw_ticks=True)
    now = datetime(2026, 10, 23, 3, 47, 30)
    p.handle(stock_frame(ms(3, 45, 5), 100.0, 1000, bid=99.9), now)
    p.handle(stock_frame(ms(3, 45, 40), 101.0, 1200, bid=100.9), now)
    p.handle(stock_frame(ms(3, 46, 10), 102.0, 1500), now)
    p.handle(stock_frame(ms(3, 20, 0) - 86_400_000, 90.0, 5), now)      # yesterday's last trade
    p.handle(stock_frame(ms(3, 55, 0), 120.0, 9000), now)               # ahead of the clock
    assert (p.ticks, p.dropped_stale, p.dropped_future) == (3, 1, 1)
    bars, ticks = p.drain(now)
    assert len(ticks) == 3 and len(bars) == 2
    b0 = bars[0]
    assert (b0.ts, b0.open, b0.close, b0.volume, b0.bid) == (datetime(2026, 10, 23, 3, 45), 100.0, 101.0, 200, 100.9)


def test_key_map_from_isin():
    rows = [{"symbol": "RELIANCE.NS", "isin": "INE002A01018"}, {"symbol": "NOISIN.NS", "isin": ""},
            {"symbol": "BAD.NS", "isin": "US0378331005"}]
    km, skipped = build_key_map(rows)
    assert km == {KEY: "RELIANCE.NS"} and skipped == ["NOISIN.NS", "BAD.NS"]


def test_next_window_skips_weekend():
    start, end = next_window(datetime(2026, 10, 24, 12, 0))             # Saturday
    assert start == datetime(2026, 10, 26, 3, 40) and end == datetime(2026, 10, 26, 10, 5)
    assert next_window(datetime(2026, 10, 23, 5, 0))[0] == datetime(2026, 10, 23, 3, 40)


def run(coro):
    return asyncio.run(coro)


def test_end_to_end_against_fake_server(tmp_path):
    seen = {}

    async def handler(ws):
        seen["auth"] = ws.request.headers.get("Authorization")
        sub = await ws.recv()
        seen["sub_is_binary"] = isinstance(sub, bytes)
        seen["sub"] = json.loads(sub)
        await ws.send(stock_frame(ms(3, 45, 5), 100.0, 1000, bid=99.9, typ=pb.initial_feed))
        await ws.send(stock_frame(ms(3, 45, 40), 101.0, 1200, bid=100.9))
        await ws.send(stock_frame(ms(3, 46, 10), 102.0, 1500))
        await asyncio.sleep(0.4)

    async def main():
        async with serve(handler, "127.0.0.1", 0) as srv:
            port = srv.sockets[0].getsockname()[1]
            clock = lambda: datetime(2026, 10, 23, 3, 47, 0)  # noqa: E731
            feed = UpstoxFeed("TKN", KMAP, url=f"ws://127.0.0.1:{port}", mode="full", interval_s=60,
                              lake_dir=tmp_path / "lake", flush_s=0.05, write_s=0.05, clock=clock)
            stop = asyncio.Event()
            asyncio.get_running_loop().call_later(0.25, stop.set)
            await feed.run_session(stop)
            return feed

    feed = run(main())
    assert seen["auth"] == "Bearer TKN" and seen["sub_is_binary"]
    assert seen["sub"]["method"] == "sub" and seen["sub"]["data"]["mode"] == "full"
    assert set(seen["sub"]["data"]["instrumentKeys"]) == {KEY, IDX}
    from data_layer import tick_store
    from data_layer.query import Lake
    tick_store.compact_day("1m", D.date(), tmp_path / "lake")
    df = Lake(tmp_path / "lake").sql("SELECT * FROM bars_1m ORDER BY ts")
    assert df.symbol.tolist() == ["RELIANCE.NS", "RELIANCE.NS"]
    assert df.close.tolist() == [101.0, 102.0] and df.volume.tolist()[0] == 200


def test_rejected_token_is_fatal(tmp_path):
    async def deny(connection, request):
        return connection.respond(HTTPStatus.UNAUTHORIZED, "expired\n")

    async def handler(ws):
        pass

    async def main():
        async with serve(handler, "127.0.0.1", 0, process_request=deny) as srv:
            port = srv.sockets[0].getsockname()[1]
            feed = UpstoxFeed("OLD", KMAP, url=f"ws://127.0.0.1:{port}", lake_dir=tmp_path / "lake")
            await feed.run_session(asyncio.Event())

    with pytest.raises(RuntimeError, match="expires daily"):
        run(main())


# ---------------------------------------------------------------------------
# Auth helper
# ---------------------------------------------------------------------------
from data_layer.upstox import auth  # noqa: E402


def test_extract_code_and_login_url():
    assert auth.extract_code("http://localhost:8000/?code=AbC123&state=x") == "AbC123"
    assert auth.extract_code("  AbC123 \n") == "AbC123"
    u = auth.login_url("KEY", "http://localhost:8000")
    assert u.startswith("https://api.upstox.com/v2/login/authorization/dialog?client_id=KEY")
    assert "response_type=code" in u and "redirect_uri=http%3A%2F%2Flocalhost%3A8000" in u


def test_exchange_posts_form_and_update_env_preserves_lines(tmp_path):
    seen = {}

    class R:
        status_code = 200
        text = ""
        def json(self): return {"access_token": "ACC-1234567890", "extended_token": "EXT-0987654321"}

    def fake_post(url, headers, data, timeout):
        seen.update(url=url, headers=headers, data=data)
        return R()

    tok = auth.exchange_code("CODE", "KEY", "SECRET", "http://localhost:8000", post=fake_post)
    assert seen["url"].endswith("/v2/login/authorization/token")
    assert seen["data"] == {"code": "CODE", "client_id": "KEY", "client_secret": "SECRET",
                            "redirect_uri": "http://localhost:8000", "grant_type": "authorization_code"}
    env = tmp_path / ".env"
    env.write_text("FMP_API_KEY=abc\n# note\nUPSTOX_ACCESS_TOKEN=old\n")
    auth.update_env(env, {"UPSTOX_ACCESS_TOKEN": tok["access_token"], "UPSTOX_EXTENDED_TOKEN": tok["extended_token"]})
    lines = env.read_text().splitlines()
    assert lines == ["FMP_API_KEY=abc", "# note", "UPSTOX_ACCESS_TOKEN=ACC-1234567890",
                     "UPSTOX_EXTENDED_TOKEN=EXT-0987654321"]
    assert oct(env.stat().st_mode & 0o777) == "0o600"
    assert "ACC-1234567890" not in auth.mask("ACC-1234567890")


def test_probe_distinguishes_accepted_and_rejected_tokens():
    async def deny_old(connection, request):
        if request.headers.get("Authorization") == "Bearer OLD":
            return connection.respond(HTTPStatus.UNAUTHORIZED, "no\n")

    async def handler(ws):
        await ws.recv()
        await ws.send(stock_frame(ms(3, 45, 5), 100.0, 1))
        await asyncio.sleep(0.2)

    async def main():
        async with serve(handler, "127.0.0.1", 0, process_request=deny_old) as srv:
            url = f"ws://127.0.0.1:{srv.sockets[0].getsockname()[1]}"
            return (await auth.probe_feed("OLD", url, KEY, 1.0), await auth.probe_feed("NEW", url, KEY, 1.0))

    old, new = run(main())
    assert old == "rejected HTTP 401" and new == "accepted, frame received"


def test_sigint_saves_and_compacts(tmp_path):
    import os
    import signal
    from data_layer.upstox.feed import run_day

    async def handler(ws):
        await ws.recv()
        await ws.send(stock_frame(ms(3, 45, 5), 100.0, 1000))
        await ws.send(stock_frame(ms(3, 46, 10), 101.0, 1200))
        await asyncio.sleep(5)

    async def main():
        async with serve(handler, "127.0.0.1", 0) as srv:
            clock = lambda: datetime(2026, 10, 23, 3, 50, 0)  # noqa: E731
            feed = UpstoxFeed("T", KMAP, url=f"ws://127.0.0.1:{srv.sockets[0].getsockname()[1]}",
                              lake_dir=tmp_path / "lake", flush_s=30, write_s=30, clock=clock)
            asyncio.get_running_loop().call_later(0.3, os.kill, os.getpid(), signal.SIGINT)
            await asyncio.wait_for(run_day(feed, datetime(2026, 10, 23, 10, 5)), timeout=5)

    run(main())
    from data_layer.query import Lake
    df = Lake(tmp_path / "lake").sql("SELECT * FROM bars_1s ORDER BY ts")
    assert df.close.tolist() == [100.0, 101.0]
    assert not list((tmp_path / "lake/bars/interval=1s/date=2026-10-23").glob("seg-*"))   # compacted


def test_dump_frames_saves_raw_bytes(tmp_path):
    frame = stock_frame(ms(3, 45, 5), 100.0, 1000)

    async def handler(ws):
        await ws.recv()
        for _ in range(3):
            await ws.send(frame)
        await asyncio.sleep(0.3)

    async def main():
        async with serve(handler, "127.0.0.1", 0) as srv:
            feed = UpstoxFeed("T", KMAP, url=f"ws://127.0.0.1:{srv.sockets[0].getsockname()[1]}",
                              lake_dir=tmp_path / "lake", clock=lambda: datetime(2026, 10, 23, 3, 50, 0),
                              dump_frames=2, dump_dir=tmp_path / "frames")
            stop = asyncio.Event()
            asyncio.get_running_loop().call_later(0.2, stop.set)
            await feed.run_session(stop)

    run(main())
    files = sorted((tmp_path / "frames").glob("*.bin"))
    assert len(files) == 2 and all(f.read_bytes() == frame for f in files)
    assert next(decode.ticks_from_response(decode.parse_frame(files[0].read_bytes()), KMAP)).ltp == 100.0
