"""
Upstox v3 live market-data collector: WebSocket -> raw ticks -> Parquet.

  python -m data_layer.upstox.auth ...      # obtain a token (writes .env); expires daily (runbook O10)
  python -m data_layer.upstox.feed                 # daemon: waits for each NSE session
  python -m data_layer.upstox.feed --once          # run one session now (until close)
  python -m data_layer.upstox.feed --limit 20              # subscribe only the first N universe symbols

Every tick/L1 update is persisted as-is (full raw-tick precision; see SCRUM-77 and docs/RUNBOOK.md
section 2.2/16). There is no 1-second bar aggregation in this path any more: that discarded real
sub-second detail that Upstox actually sends (ltt is epoch milliseconds, pushed per update, not per
second). 1-minute+ bars built from the REST candle API (data_layer/upstox/candles.py) are a separate,
unaffected pipeline. The query layer (data_layer/query.py) derives bars of any width from these raw
ticks on read.

Protocol (from the official SDK, upstox-python-sdk 2.30.0):
  wss://api.upstox.com/v3/feed/market-data-feed   header  Authorization: Bearer <token>
  subscribe  : binary frame, JSON {"guid", "method": "sub", "data": {"instrumentKeys": [...],
               "mode": "ltpc" | "full" | "option_greeks" | "full_d30"}}
  responses  : binary Protobuf FeedResponse (see decode.py)

Unverified against production (runbook Gate U1): per-connection subscription limits,
that `ltt` is epoch milliseconds, and behaviour on NSE holidays.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import signal
import uuid
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import websockets

from .. import tick_store
from ..lake import ENV_FILE
from ..ticks import SESSION_CLOSE_UTC, SESSION_OPEN_UTC, Tick
from . import decode
from .instruments import segment_key_map

log = logging.getLogger(__name__)

FEED_URL = "wss://api.upstox.com/v3/feed/market-data-feed"
MODES = ("ltpc", "full", "option_greeks", "full_d30")
SUBSCRIBE_BATCH = 500            # keys per subscribe message (conservative; limit unverified)
START_EARLY = timedelta(minutes=5)
STOP_LATE = timedelta(minutes=5)


def utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


class FeedProcessor:
    """Synchronous core: frame bytes in, raw ticks out.  The async runner only moves data."""

    def __init__(self, key_map: dict[str, str]) -> None:
        self.key_map = key_map
        self._ticks: list[Tick] = []
        self.status: dict[str, str] = {}
        self.frames = self.ticks = self.dropped_stale = self.dropped_future = 0

    def handle(self, data: bytes, now: datetime) -> None:
        resp = decode.parse_frame(data)
        self.frames += 1
        if resp.HasField("marketInfo") and resp.marketInfo.segmentStatus:
            st = decode.market_status(resp)
            if st != self.status:
                log.info("market status: %s", st)
            self.status = st
        for t in decode.ticks_from_response(resp, self.key_map):
            if t.ts.date() != now.date():                # last trade from an earlier day
                self.dropped_stale += 1
                continue
            if t.ts > now + timedelta(seconds=60):       # clock skew or wrong time unit
                self.dropped_future += 1
                if self.dropped_future in (1, 100, 10_000):
                    log.warning("tick timestamp %s is ahead of local clock %s "
                                "(is ltt really epoch ms?)", t.ts, now)
                continue
            self.ticks += 1
            self._ticks.append(t)

    def drain(self) -> list[Tick]:
        ticks, self._ticks = self._ticks, []
        return ticks


class UpstoxFeed:
    def __init__(self, token: str, key_map: dict[str, str], *, url: str = FEED_URL,
                 mode: str = "full",
                 lake_dir: Path | None = None, flush_s: float = 2.0, write_s: float = 15.0,
                 stale_s: float = 120.0, batch: int = SUBSCRIBE_BATCH, clock=utcnow,
                 dump_frames: int = 0, dump_dir: Path | None = None,
                 token_provider=None, auth_retry_s: float | None = None) -> None:
        if mode not in MODES:
            raise ValueError(f"mode must be one of {MODES}")
        self.token, self.url, self.mode, self.batch = token, url, mode, batch
        # daemon mode: when Upstox rejects the token, keep waiting and re-read it instead of exiting (see runbook 0.9)
        self.token_provider, self.auth_retry_s = token_provider, auth_retry_s
        self.keys = list(key_map)
        self.proc = FeedProcessor(key_map)
        self.lake_dir, self.flush_s, self.write_s, self.stale_s = lake_dir, flush_s, write_s, stale_s
        self.clock = clock
        self.dump_left, self.dump_dir = dump_frames, dump_dir or Path("logs/frames")
        self._last_frame = self.clock()

    # -- persistence -------------------------------------------------------
    def persist(self, final: bool = False) -> int:
        ticks = self.proc.drain()
        return tick_store.write_ticks(ticks, self.lake_dir)

    # -- one connection ----------------------------------------------------
    def _subscribe_msgs(self) -> list[bytes]:
        return [json.dumps({"guid": str(uuid.uuid4()), "method": "sub",
                            "data": {"instrumentKeys": self.keys[i:i + self.batch], "mode": self.mode}}
                           ).encode() for i in range(0, len(self.keys), self.batch)]

    async def _connect_once(self, stop: asyncio.Event) -> None:
        async with websockets.connect(self.url, additional_headers={"Authorization": f"Bearer {self.token}"},
                                      max_size=None, ping_interval=20) as ws:
            for m in self._subscribe_msgs():
                await ws.send(m)                          # bytes -> binary frame, as the SDK does
            log.info("subscribed %d instruments in mode %s", len(self.keys), self.mode)
            self._last_frame = self.clock()
            aux = asyncio.create_task(self._housekeeping(ws, stop))
            closer = asyncio.create_task(self._close_on_stop(ws, stop))
            try:
                async for msg in ws:
                    if stop.is_set():
                        break
                    if isinstance(msg, bytes):
                        self._last_frame = self.clock()
                        if self.dump_left > 0:                    # keep raw frames as real test fixtures
                            self.dump_dir.mkdir(parents=True, exist_ok=True)
                            (self.dump_dir / f"frame-{self._last_frame:%H%M%S}-{self.dump_left}.bin").write_bytes(msg)
                            self.dump_left -= 1
                        self.proc.handle(msg, self._last_frame)
                    else:
                        log.info("text frame from server: %s", msg[:200])
            finally:
                aux.cancel()
                closer.cancel()

    @staticmethod
    async def _close_on_stop(ws, stop: asyncio.Event) -> None:
        await stop.wait()                                 # wake a receive loop that is waiting on a quiet feed
        await ws.close()

    async def _housekeeping(self, ws, stop: asyncio.Event) -> None:
        last_write = self.clock()
        while not stop.is_set():
            await asyncio.sleep(self.flush_s)
            now = self.clock()
            if (now - last_write).total_seconds() >= self.write_s:
                n = self.persist()
                last_write = now
                log.info("frames=%d ticks=%d ticks_written=%d stale=%d future=%d",
                         self.proc.frames, self.proc.ticks, n,
                         self.proc.dropped_stale, self.proc.dropped_future)
            if (now - self._last_frame).total_seconds() > self.stale_s:
                log.warning("no frames for %.0fs, reconnecting", (now - self._last_frame).total_seconds())
                await ws.close()
                return

    # -- reconnect loop ----------------------------------------------------
    async def run_session(self, stop: asyncio.Event) -> None:
        delay = 1.0
        while not stop.is_set():
            try:
                await self._connect_once(stop)
                delay = 1.0
            except websockets.exceptions.InvalidStatus as exc:
                code = exc.response.status_code
                if code in (401, 403):
                    if not self.auth_retry_s:
                        raise RuntimeError(f"Upstox rejected the token (HTTP {code}); it expires daily, "
                                           "refresh UPSTOX_ACCESS_TOKEN") from exc
                    log.error("Upstox rejected the token (HTTP %s). Refresh it with python -m data_layer.upstox.auth; "
                              "trying again in %.0fs", code, self.auth_retry_s)
                    try:
                        await asyncio.wait_for(stop.wait(), timeout=self.auth_retry_s)
                    except asyncio.TimeoutError:
                        pass
                    new = self.token_provider() if self.token_provider else self.token
                    if new and new != self.token:
                        log.info("picked up a new token")
                        self.token = new
                    continue
                log.error("handshake failed: HTTP %s", code)
            except (OSError, websockets.exceptions.WebSocketException) as exc:
                log.warning("connection lost: %r", exc)
            if stop.is_set():
                break
            log.info("reconnecting in %.0fs", delay)
            try:
                await asyncio.wait_for(stop.wait(), timeout=delay)
            except asyncio.TimeoutError:
                pass
            delay = min(delay * 2, 60.0)
        self.persist(final=True)


# ---------------------------------------------------------------------------
# Daily scheduling
# ---------------------------------------------------------------------------
def session_window(day: date) -> tuple[datetime, datetime]:
    return (datetime.combine(day, SESSION_OPEN_UTC) - START_EARLY,
            datetime.combine(day, SESSION_CLOSE_UTC) + STOP_LATE)


def next_window(now: datetime) -> tuple[datetime, datetime]:
    """Current or next weekday session window."""
    d = now.date()
    while True:
        if d.weekday() < 5:
            start, end = session_window(d)
            if now < end:
                return start, end
        d += timedelta(days=1)


async def run_day(feed: UpstoxFeed, end: datetime) -> None:
    stop = asyncio.Event()

    async def stopper():
        await asyncio.sleep(max(0.0, (end - feed.clock()).total_seconds()))
        stop.set()
    t = asyncio.create_task(stopper())
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):          # Ctrl+C / systemctl stop: save and compact
        try:
            loop.add_signal_handler(sig, stop.set)
        except (NotImplementedError, RuntimeError):      # non-main thread / Windows
            pass
    try:
        await feed.run_session(stop)
    finally:
        t.cancel()
    day = end.date()
    rows = tick_store.compact_ticks_day(day, feed.lake_dir)
    log.info("session %s done: compacted %d ticks", day, rows)


async def run_forever(make_feed, once: bool = False) -> None:
    while True:
        now = utcnow()
        start, end = next_window(now) if not once else (now, session_window(now.date())[1])
        if start > now:
            log.info("waiting for %s UTC", start)
            await asyncio.sleep((start - now).total_seconds())
        await run_day(make_feed(), end)
        if once:
            return


def current_token() -> str:
    """
    The token to use right now.  Re-reads .env every call (overriding what the process started with), so refreshing
    the token needs no restart.  UPSTOX_FEED_TOKEN (for example a long-lived extended token adopted by
    `auth check --adopt`) wins over the daily access token.
    """
    from dotenv import load_dotenv
    load_dotenv(ENV_FILE, override=True)
    return os.getenv("UPSTOX_FEED_TOKEN") or os.getenv("UPSTOX_ACCESS_TOKEN", "")


def main() -> None:
    ap = argparse.ArgumentParser(description="Upstox v3 live feed collector (India)")
    ap.add_argument("--once", action="store_true", help="run one session now instead of daemon mode")
    ap.add_argument("--mode", choices=MODES, default="full")
    ap.add_argument("--segment", choices=["eq", "fo"], default="eq",
                    help="eq: the NSE equity universe; fo: the futures and options chosen by fo_instruments select")
    ap.add_argument("--fo-file", default=None, help="contract list for --segment fo (default data/fo_universe.csv)")
    ap.add_argument("--limit", type=int, default=None, help="subscribe only the first N universe symbols")
    ap.add_argument("--dump-frames", type=int, default=0, metavar="N",
                    help="save the first N raw binary frames to logs/frames/ (test fixtures, market data only)")
    ap.add_argument("--url", default=FEED_URL)
    a = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    token = current_token()
    if not token and a.once:
        raise SystemExit("no token: set UPSTOX_ACCESS_TOKEN (see python -m data_layer.upstox.auth)")
    if not token:
        log.warning("no Upstox token yet: the daemon will keep retrying; run python -m data_layer.upstox.auth")
    key_map = segment_key_map(a.segment, a.fo_file)
    if a.limit:
        key_map = dict(list(key_map.items())[: a.limit])
    if not key_map:
        raise SystemExit("no instruments to subscribe: " + ("run fo_instruments import and select" if a.segment == "fo"
                                                           else "no universe symbols with a usable ISIN"))
    make = lambda: UpstoxFeed(current_token(), key_map, url=a.url, mode=a.mode,  # noqa: E731
                              dump_frames=a.dump_frames, token_provider=current_token,
                              auth_retry_s=None if a.once else 60.0)
    asyncio.run(run_forever(make, once=a.once))


if __name__ == "__main__":
    main()
