"""
In-memory event bus between the live feed and its consumers (SCRUM-159; architecture decision of 14 Nov 2026).

The feed publishes every decoded tick as it arrives; the signal engine (and later the paper trader) subscribe.  Nothing on the bus
touches disk; Parquet writing stays in the feed as before, so the bus is purely additive and can be switched off.

  JARVIS_TICK_BUS=1          the feed publishes (off by default)
  JARVIS_BUS_DIR             folder for the local sockets (default /tmp/jarvis-bus); endpoints are ipc://<dir>/<name>.sock

Transport: ZeroMQ PUB/SUB over a local socket.  A PUB socket never blocks the publisher: when a subscriber is slow and its queue
(SNDHWM) is full, messages for it are dropped, and every message carries a per-publisher sequence number so a subscriber counts what
it missed.  Tick capture can therefore never be slowed or stopped by a consumer.

Wire format (msgpack, language-neutral): multipart [topic, payload]; payload = {"pub": name, "seq": n, "sent_ns": int,
"cols": [...], "rows": [[...], ...]}.  Datetimes travel as integer nanoseconds since the epoch (naive UTC).

  pub = Publisher("ticks-fo-1");  pub.publish("tick", cols, rows)
  sub = Subscriber(["ticks-fo-1", "ticks-fo-2", "ticks-eq-1"], topics=["tick"]);  for topic, msg in sub.poll(timeout_ms=100): ...
"""

from __future__ import annotations

import logging
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Iterable, Iterator, Optional

import msgpack
import zmq

log = logging.getLogger("bus")

SNDHWM = 200_000                       # messages queued per subscriber before the publisher starts dropping for it
RCVHWM = 200_000
_EPOCH = datetime(1970, 1, 1)


def bus_enabled(env=None) -> bool:
    return (os.environ if env is None else env).get("JARVIS_TICK_BUS", "").strip().lower() in ("1", "true", "yes", "on")


def bus_dir(env=None) -> Path:
    return Path((os.environ if env is None else env).get("JARVIS_BUS_DIR") or "/tmp/jarvis-bus")


def endpoint(name: str, env=None) -> str:
    return f"ipc://{bus_dir(env) / (name + '.sock')}"


def to_ns(v) -> Optional[int]:
    """Datetime-like to integer nanoseconds (naive UTC); None stays None."""
    if v is None:
        return None
    if isinstance(v, datetime):
        return ((v.replace(tzinfo=None) - _EPOCH).days * 86_400 + (v.replace(tzinfo=None) - _EPOCH).seconds) * 10**9 + v.microsecond * 1000
    if hasattr(v, "value"):                                    # pandas Timestamp
        return int(v.value)
    return int(v)


class Publisher:
    def __init__(self, name: str, env=None, ctx: Optional[zmq.Context] = None):
        self.name = name
        bus_dir(env).mkdir(parents=True, exist_ok=True)
        self._ctx = ctx or zmq.Context.instance()
        self._sock = self._ctx.socket(zmq.PUB)
        self._sock.setsockopt(zmq.SNDHWM, SNDHWM)
        self._sock.setsockopt(zmq.LINGER, 0)
        self._sock.bind(endpoint(name, env))
        self.seq = 0
        self.sent = 0
        self.dropped = 0

    def publish(self, topic: str, cols: list[str], rows: list[list]) -> bool:
        """Send one message; never blocks.  Returns False (and counts it) when the message could not be queued."""
        if not rows:
            return True
        self.seq += 1
        payload = msgpack.packb({"pub": self.name, "seq": self.seq, "sent_ns": time.time_ns(), "cols": cols, "rows": rows},
                                use_bin_type=True)
        try:
            self._sock.send_multipart([topic.encode(), payload], flags=zmq.NOBLOCK)
        except zmq.Again:
            self.dropped += 1
            return False
        self.sent += 1
        return True

    def close(self) -> None:
        self._sock.close(0)


class Subscriber:
    def __init__(self, names: Iterable[str], topics: Iterable[str] = ("tick",), env=None, ctx: Optional[zmq.Context] = None):
        self._ctx = ctx or zmq.Context.instance()
        self._sock = self._ctx.socket(zmq.SUB)
        self._sock.setsockopt(zmq.RCVHWM, RCVHWM)
        self._sock.setsockopt(zmq.LINGER, 0)
        for t in topics:
            self._sock.setsockopt(zmq.SUBSCRIBE, t.encode())
        self.names = list(names)
        for n in self.names:
            self._sock.connect(endpoint(n, env))         # connecting before the publisher binds is fine: ZeroMQ reconnects
        self._last_seq: dict[str, int] = {}
        self.received = 0
        self.missed = 0                                   # messages lost between a publisher and us (sequence gaps)
        self._poller = zmq.Poller()
        self._poller.register(self._sock, zmq.POLLIN)

    def poll(self, timeout_ms: int = 100, max_messages: int = 10_000) -> Iterator[tuple[str, dict]]:
        """Yield (topic, message) for every message waiting, waiting up to `timeout_ms` for the first."""
        if not self._poller.poll(timeout_ms):
            return
        n = 0
        while n < max_messages:
            try:
                topic, payload = self._sock.recv_multipart(flags=zmq.NOBLOCK)
            except zmq.Again:
                return
            msg = msgpack.unpackb(payload, raw=False)
            pub, seq = msg.get("pub"), msg.get("seq", 0)
            last = self._last_seq.get(pub)
            if last is not None and seq > last + 1:
                self.missed += seq - last - 1
            self._last_seq[pub] = seq
            self.received += 1
            n += 1
            yield topic.decode(), msg

    def close(self) -> None:
        self._sock.close(0)


# ---------------------------------------------------------------------------------------------- ticks on the bus
TICK_FIELDS = ("ts", "fts", "rts", "symbol", "ltp", "vol_today", "ltq", "bid", "ask", "bid_qty", "ask_qty", "oi", "iv", "delta", "gamma",
               *(f"{s}{i}{q}" for s in ("bid", "ask") for i in range(2, 6) for q in ("", "_qty")))
TIME_FIELDS = {"ts", "fts", "rts"}


def tick_rows(ticks) -> list[list]:
    """Feed Tick objects (or mappings) as bus rows in TICK_FIELDS order, times as nanoseconds."""
    rows = []
    for t in ticks:
        get = t.get if isinstance(t, dict) else (lambda k, _t=t: getattr(_t, k, None))
        rows.append([to_ns(get(f)) if f in TIME_FIELDS else get(f) for f in TICK_FIELDS])
    return rows


def rows_frame(msgs: Iterable[dict]):
    """Bus messages (any publishers) as one DataFrame with datetime columns, in arrival order."""
    import pandas as pd
    frames = [pd.DataFrame(m["rows"], columns=m["cols"]) for m in msgs if m.get("rows")]
    if not frames:
        return pd.DataFrame(columns=list(TICK_FIELDS))
    df = pd.concat(frames, ignore_index=True)
    for c in TIME_FIELDS & set(df.columns):
        df[c] = pd.to_datetime(df[c], unit="ns")
    return df


def feed_names(env=None) -> list[str]:
    """Publishers of the running feeds: the equity feed and the F&O shards (JARVIS_FO_SHARDS, default 2)."""
    n = int((os.environ if env is None else env).get("JARVIS_FO_SHARDS", "2"))
    return ["ticks-eq-1"] + [f"ticks-fo-{i}" for i in range(1, n + 1)]
