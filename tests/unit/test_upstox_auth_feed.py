"""Upstox auth CLI paths, feed resilience (batching, watchdog, reconnect, text frames) and instrument keys."""
import asyncio
import json
import sys
from datetime import datetime
from http import HTTPStatus

import pytest
from websockets.asyncio.server import serve

from data_layer.upstox import auth, feed
from data_layer.upstox.feed import UpstoxFeed, interval_label
from data_layer.upstox.instruments import build_key_map, equity_key
from tests.helpers import ms
from tests.unit.test_upstox_adapter import KEY, KMAP, stock_frame

CLOCK = lambda: datetime(2026, 10, 23, 3, 50, 0)  # noqa: E731


def run(coro):
    return asyncio.run(coro)


# -- auth CLI -----------------------------------------------------------------------------------------
@pytest.fixture()
def authenv(tmp_path, monkeypatch):
    env = tmp_path / ".env"
    env.write_text("UPSTOX_API_KEY=KEY\nUPSTOX_API_SECRET=SEC\nUPSTOX_REDIRECT_URI=http://localhost:8000\n")
    monkeypatch.setattr(auth, "ENV_PATH", env)
    for v in ("UPSTOX_API_KEY", "UPSTOX_API_SECRET", "UPSTOX_REDIRECT_URI", "UPSTOX_ACCESS_TOKEN",
              "UPSTOX_EXTENDED_TOKEN", "UPSTOX_FEED_TOKEN"):
        monkeypatch.delenv(v, raising=False)
    return env


def test_cli_login_url_prints_link(authenv, capsys):
    assert auth.main(["login-url"]) == 0
    assert "client_id=KEY" in capsys.readouterr().out


def test_cli_missing_credentials_exit_with_message(authenv):
    authenv.write_text("")
    with pytest.raises(SystemExit, match="UPSTOX_API_KEY"):
        auth.main(["login-url"])


def test_cli_exchange_stores_tokens_and_never_prints_them(authenv, monkeypatch, capsys):
    class R:
        status_code = 200
        def json(self): return {"access_token": "ACCESS-SECRET-VALUE", "extended_token": "EXT-SECRET-VALUE"}
    monkeypatch.setattr(auth.requests, "post", lambda *a, **k: R())
    assert auth.main(["exchange", "http://localhost:8000/?code=ABC"]) == 0
    out = capsys.readouterr().out
    assert "SECRET-VALUE" not in out
    txt = authenv.read_text()
    assert "UPSTOX_ACCESS_TOKEN=ACCESS-SECRET-VALUE" in txt and "UPSTOX_EXTENDED_TOKEN=EXT-SECRET-VALUE" in txt


def test_cli_exchange_without_extended_token_warns(authenv, monkeypatch, capsys):
    class R:
        status_code = 200
        def json(self): return {"access_token": "ONLYACCESSTOKEN1"}
    monkeypatch.setattr(auth.requests, "post", lambda *a, **k: R())
    auth.main(["exchange", "CODE"])
    assert "no extended_token" in capsys.readouterr().out and "UPSTOX_EXTENDED_TOKEN" not in authenv.read_text()


def test_exchange_http_error_and_missing_token_exit(authenv, monkeypatch):
    class Bad:
        status_code, text = 400, "invalid code"
    monkeypatch.setattr(auth.requests, "post", lambda *a, **k: Bad())
    with pytest.raises(SystemExit, match="HTTP 400"):
        auth.main(["exchange", "CODE"])

    class Empty:
        status_code = 200
        def json(self): return {}
    monkeypatch.setattr(auth.requests, "post", lambda *a, **k: Empty())
    with pytest.raises(SystemExit, match="no access_token"):
        auth.main(["exchange", "CODE"])


def test_check_adopts_extended_token_only_when_feed_accepts_it(authenv, monkeypatch, capsys):
    async def fake_probe(token, url=None, key=None, wait_s=6.0):
        return "accepted, frame received" if token == "EXT" else "rejected HTTP 401"
    monkeypatch.setattr(auth, "probe_feed", fake_probe)
    monkeypatch.setattr("data_layer.upstox.instruments.build_key_map", lambda: ({KEY: "X"}, []))
    monkeypatch.setenv("UPSTOX_ACCESS_TOKEN", "OLD"); monkeypatch.setenv("UPSTOX_EXTENDED_TOKEN", "EXT")
    assert auth.cmd_check(adopt=False) == 0
    assert "UPSTOX_FEED_TOKEN" not in authenv.read_text()
    assert auth.cmd_check(adopt=True) == 0
    assert "UPSTOX_FEED_TOKEN=EXT" in authenv.read_text()
    monkeypatch.setenv("UPSTOX_EXTENDED_TOKEN", "BAD")
    assert auth.cmd_check(adopt=True) == 1                         # rejected: exit 1, nothing adopted


def test_update_env_creates_file_when_absent(tmp_path):
    p = tmp_path / "new.env"
    auth.update_env(p, {"A": "1"})
    assert p.read_text() == "A=1\n"


# -- instruments ------------------------------------------------------------------------------------------
def test_equity_key_and_master_validation(tmp_path):
    assert equity_key(" INE002A01018 ") == "NSE_EQ|INE002A01018"
    master = tmp_path / "master.json"
    master.write_text(json.dumps([{"instrument_key": "NSE_EQ|INE002A01018"}]))
    rows = [{"symbol": "A.NS", "isin": "INE002A01018"}, {"symbol": "B.NS", "isin": "INE999A01010"}]
    keys, skipped = build_key_map(rows, master=master)
    assert keys == {"NSE_EQ|INE002A01018": "A.NS"} and skipped == ["B.NS"]


# -- feed resilience ----------------------------------------------------------------------------------------
def test_interval_labels():
    assert [interval_label(s) for s in (60, 300, 3600, 45)] == ["1m", "5m", "60m", "45s"]


def test_invalid_mode_rejected_at_construction(tmp_path):
    with pytest.raises(ValueError):
        UpstoxFeed("T", KMAP, mode="depth30")


def test_subscribe_messages_are_batched_binary_json(tmp_path):
    keys = {f"NSE_EQ|INE{i:09d}": f"S{i}.NS" for i in range(1200)}
    f = UpstoxFeed("T", keys, lake_dir=tmp_path, mode="ltpc")
    msgs = f._subscribe_msgs()
    parsed = [json.loads(m) for m in msgs]
    assert len(msgs) == 3 and [len(p["data"]["instrumentKeys"]) for p in parsed] == [500, 500, 200]
    assert all(p["method"] == "sub" and p["data"]["mode"] == "ltpc" and p["guid"] for p in parsed)
    assert len({p["guid"] for p in parsed}) == 3
    assert {k for p in parsed for k in p["data"]["instrumentKeys"]} == set(keys)


def test_reconnects_resubscribes_and_keeps_data_after_server_drop(tmp_path):
    connections = []

    async def handler(ws):
        sub = json.loads(await ws.recv())
        connections.append(sub["data"]["instrumentKeys"])
        n = len(connections)
        await ws.send(stock_frame(ms(3, 45 + n, 5), 100.0 + n, 1000 * n))
        if n == 1:
            await ws.close()                                     # first connection dies
        else:
            await asyncio.sleep(0.5)

    async def main():
        async with serve(handler, "127.0.0.1", 0) as srv:
            f = UpstoxFeed("T", KMAP, url=f"ws://127.0.0.1:{srv.sockets[0].getsockname()[1]}",
                           lake_dir=tmp_path / "lake", flush_s=0.05, write_s=0.05, clock=lambda: datetime(2026, 10, 23, 4, 10))
            stop = asyncio.Event()
            asyncio.get_running_loop().call_later(2.5, stop.set)
            await f.run_session(stop)
    run(main())
    assert len(connections) >= 2 and connections[0] == connections[1]       # resubscribed the same set
    from data_layer.query import Lake
    closes = Lake(tmp_path / "lake").sql("SELECT close FROM bars_1s ORDER BY ts").close.tolist()
    assert closes == [101.0, 102.0]                                         # data from both connections kept


def test_watchdog_reconnects_when_feed_goes_silent(tmp_path):
    connects = []

    async def handler(ws):
        await ws.recv()
        connects.append(1)
        await asyncio.sleep(3)                                    # never sends anything

    async def main():
        async with serve(handler, "127.0.0.1", 0) as srv:
            t = {"v": 0.0}
            def clock():
                t["v"] += 40.0                                    # fast-forward: each tick of the clock is 40 s
                return datetime(2026, 10, 23, 4, 0, 0) + __import__("datetime").timedelta(seconds=t["v"])
            f = UpstoxFeed("T", KMAP, url=f"ws://127.0.0.1:{srv.sockets[0].getsockname()[1]}",
                           lake_dir=tmp_path / "lake", flush_s=0.05, write_s=100, stale_s=100, clock=clock)
            stop = asyncio.Event()
            asyncio.get_running_loop().call_later(2.0, stop.set)
            await f.run_session(stop)
    run(main())
    assert len(connects) >= 2


def test_text_frames_and_unknown_binary_instruments_are_harmless(tmp_path):
    async def handler(ws):
        await ws.recv()
        await ws.send("hello from server")
        await ws.send(stock_frame(ms(3, 45, 5), 1.0, 1, key="NSE_EQ|INE000000000"))
        await ws.send(stock_frame(ms(3, 45, 6), 100.0, 1))
        await asyncio.sleep(0.3)

    async def main():
        async with serve(handler, "127.0.0.1", 0) as srv:
            f = UpstoxFeed("T", KMAP, url=f"ws://127.0.0.1:{srv.sockets[0].getsockname()[1]}",
                           lake_dir=tmp_path / "lake", clock=CLOCK, raw_ticks=True)
            stop = asyncio.Event()
            asyncio.get_running_loop().call_later(0.2, stop.set)
            await f.run_session(stop)
            return f
    f = run(main())
    assert f.proc.frames == 2 and f.proc.ticks == 1


def test_server_forbidden_is_fatal_too(tmp_path):
    async def deny(connection, request):
        return connection.respond(HTTPStatus.FORBIDDEN, "no\n")

    async def main():
        async with serve(lambda ws: None, "127.0.0.1", 0, process_request=deny) as srv:
            f = UpstoxFeed("T", KMAP, url=f"ws://127.0.0.1:{srv.sockets[0].getsockname()[1]}", lake_dir=tmp_path)
            await f.run_session(asyncio.Event())
    with pytest.raises(RuntimeError, match="403"):
        run(main())


def test_connection_refused_retries_until_stopped(tmp_path):
    async def main():
        f = UpstoxFeed("T", KMAP, url="ws://127.0.0.1:1", lake_dir=tmp_path)       # nothing listens on port 1
        stop = asyncio.Event()
        asyncio.get_running_loop().call_later(0.6, stop.set)
        await asyncio.wait_for(f.run_session(stop), timeout=5)
    run(main())                                                                   # returns cleanly, no exception


def test_main_requires_a_token_and_prefers_feed_token(monkeypatch, tmp_path):
    for v in ("UPSTOX_ACCESS_TOKEN", "UPSTOX_FEED_TOKEN"):
        monkeypatch.delenv(v, raising=False)
    monkeypatch.setattr(feed, "__name__", feed.__name__)
    monkeypatch.setattr(sys, "argv", ["feed", "--once"])
    monkeypatch.chdir(tmp_path)                                  # no .env here
    with pytest.raises(SystemExit, match="no token"):
        feed.main()
    seen = {}
    monkeypatch.setenv("UPSTOX_ACCESS_TOKEN", "DAILY"); monkeypatch.setenv("UPSTOX_FEED_TOKEN", "LONG")
    monkeypatch.setattr(feed, "build_key_map", lambda: ({KEY: "X.NS"}, []))
    monkeypatch.setattr(feed, "run_forever", lambda make, once: seen.update(token=make().token) or asyncio.sleep(0))
    feed.main()
    assert seen["token"] == "LONG"
