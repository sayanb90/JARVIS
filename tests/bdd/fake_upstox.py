"""A local WebSocket server that speaks the Upstox v3 protocol well enough to drive the real collector."""
from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta
from http import HTTPStatus

from websockets.asyncio.server import serve

from data_layer.upstox.proto import MarketDataFeedV3_pb2 as pb
from tests.helpers import ms


def tick_frame(key: str, ltt_ms: int, ltp: float, vtt: int, bid: float | None = None, typ=pb.live_feed) -> bytes:
    r = pb.FeedResponse(type=typ)
    m = r.feeds[key].fullFeed.marketFF
    m.ltpc.ltp, m.ltpc.ltt, m.ltpc.ltq, m.vtt = ltp, ltt_ms, 1, vtt
    if bid:
        q = m.marketLevel.bidAskQuote.add()
        q.bidP, q.bidQ, q.askP, q.askQ = bid, 10, bid + 0.05, 12
    return r.SerializeToString()


def session_frames(keys: list[str], minutes: int = 375, day=(2026, 10, 23)) -> list[bytes]:
    """Two ticks per instrument per minute from 09:15 IST, cumulative volume +10 per tick."""
    out, vtt = [], {k: 1000 for k in keys}
    for k in range(minutes):
        base = datetime(*day, 3, 45) + timedelta(minutes=k)
        for sec in (5, 35):
            for i, key in enumerate(keys):
                vtt[key] += 10
                px = 100.0 * (i + 1) + k * 0.01 + sec * 0.0001
                out.append(tick_frame(key, ms(base.hour, base.minute, sec, day), px, vtt[key], bid=px - 0.05))
    return out


class FakeUpstox:
    """
    Serves `frames` to each connection.  Behaviours:
      deny_status      reject the handshake with this HTTP status (token problems)
      drop_after       close the first connection after this many frames; the next one continues
      hold_s           keep the connection open this long after the last frame
    Records what the client sent (headers, subscribe messages).
    """

    def __init__(self, frames: list[bytes], deny_status: int | None = None, drop_after: int | None = None,
                 hold_s: float = 0.6, reject_tokens: set[str] | None = None):
        self.reject_tokens = reject_tokens or set()
        self.frames, self.deny_status, self.drop_after, self.hold_s = frames, deny_status, drop_after, hold_s
        self.auth_headers: list[str] = []
        self.subscriptions: list[dict] = []
        self.connections = 0
        self._cursor = 0
        self.all_sent = asyncio.Event()

    async def _deny(self, connection, request):
        if self.deny_status:
            return connection.respond(HTTPStatus(self.deny_status), "denied\n")
        if request.headers.get("Authorization", "").removeprefix("Bearer ") in self.reject_tokens:
            return connection.respond(HTTPStatus.UNAUTHORIZED, "bad token\n")

    async def _handler(self, ws):
        self.connections += 1
        self.auth_headers.append(ws.request.headers.get("Authorization", ""))
        self.subscriptions.append(json.loads(await ws.recv()))
        limit = self.drop_after if (self.drop_after and self.connections == 1) else None
        sent = 0
        while self._cursor < len(self.frames):
            await ws.send(self.frames[self._cursor])
            self._cursor += 1
            sent += 1
            if limit and sent >= limit:
                await ws.close()
                return
        self.all_sent.set()
        try:
            await asyncio.wait_for(ws.wait_closed(), self.hold_s)    # end as soon as the client leaves
        except asyncio.TimeoutError:
            pass

    def start(self):
        return serve(self._handler, "127.0.0.1", 0, process_request=self._deny)

    @staticmethod
    def url(server) -> str:
        return f"ws://127.0.0.1:{server.sockets[0].getsockname()[1]}"


class ThreadedFake:
    """Runs a FakeUpstox in its own thread/event loop so synchronous code (asyncio.run inside) can talk to it."""

    def __init__(self, fake: FakeUpstox):
        import threading
        self.fake, self.url = fake, None
        self._ready, self._stop, self._loop = threading.Event(), None, None
        self._thread = threading.Thread(target=lambda: asyncio.run(self._main()), daemon=True)

    async def _main(self):
        self._loop, self._stop = asyncio.get_running_loop(), asyncio.Event()
        async with self.fake.start() as srv:
            self.url = FakeUpstox.url(srv)
            self._ready.set()
            await self._stop.wait()

    def __enter__(self):
        self._thread.start()
        assert self._ready.wait(5)
        return self

    def __exit__(self, *_):
        self._loop.call_soon_threadsafe(self._stop.set)
        self._thread.join(5)
