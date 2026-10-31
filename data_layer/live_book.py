"""
The live order book, kept in memory only (nothing here is ever written to the lake).

The feed collector already receives up to five bid and ask levels per instrument in `full` mode but stores only the
best level.  `BookStore` keeps the latest five-level book per symbol; `serve` exposes it on a local-only HTTP port so
the viewer can show a live ladder; `fetch` is what the viewer calls.  One collector process per segment, so equities
and F&O each have their own port (JARVIS_BOOK_PORT_EQ, default 8771; JARVIS_BOOK_PORT_FO, default 8772; 0 turns the
endpoint off).  The listener binds 127.0.0.1 only: the viewer, behind its own login, is the only way in from outside.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import urllib.error
import urllib.request
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import parse_qs, urlsplit

log = logging.getLogger(__name__)

DEFAULT_PORTS = {"eq": 8771, "fo": 8772}
SYMBOL_RE = re.compile(r"^[A-Za-z0-9&_.\-]{1,64}$")


def port_for(segment: str, env=None) -> int:
    env = os.environ if env is None else env
    raw = (env.get(f"JARVIS_BOOK_PORT_{segment.upper()}") or "").strip()
    return int(raw) if raw else DEFAULT_PORTS[segment]


def _now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


class BookStore:
    """Latest book per symbol: {"ts": exchange time, "recv": local receive time, "ltp", "bids": [[px, qty]], "asks": [...]}."""

    def __init__(self) -> None:
        self._books: dict[str, dict] = {}

    def update(self, symbol: str, ts: datetime, ltp: Optional[float], bids: list, asks: list) -> None:
        self._books[symbol] = {"ts": ts, "recv": _now(), "ltp": ltp, "bids": bids, "asks": asks}

    def get(self, symbol: str) -> Optional[dict]:
        b = self._books.get(symbol)
        if b is None:
            return None
        return {"symbol": symbol, "ts": b["ts"].isoformat(timespec="milliseconds") + "Z",
                "age_s": round((_now() - b["recv"]).total_seconds(), 2), "ltp": b["ltp"],
                "bids": b["bids"], "asks": b["asks"]}

    def __len__(self) -> int:
        return len(self._books)


def _response(status: str, body: dict) -> bytes:
    data = json.dumps(body).encode()
    return (f"HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {len(data)}\r\n"
            "Cache-Control: no-store\r\nConnection: close\r\n\r\n").encode() + data


async def _handle(store: BookStore, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    try:
        line = (await asyncio.wait_for(reader.readline(), 2.0)).decode("latin-1")
        parts = line.split()
        if len(parts) < 2 or parts[0] != "GET":
            writer.write(_response("405 Method Not Allowed", {"detail": "GET only"}))
        else:
            url = urlsplit(parts[1])
            if url.path == "/health":
                writer.write(_response("200 OK", {"symbols": len(store)}))
            elif url.path == "/book":
                sym = (parse_qs(url.query).get("symbol") or [""])[0]
                book = store.get(sym) if SYMBOL_RE.match(sym) else None
                writer.write(_response("200 OK", book) if book else _response("404 Not Found", {"detail": "no book"}))
            else:
                writer.write(_response("404 Not Found", {"detail": "not found"}))
        await writer.drain()
    except (asyncio.TimeoutError, ConnectionError, UnicodeDecodeError):
        pass
    finally:
        writer.close()


async def serve(store: BookStore, port: int, host: str = "127.0.0.1"):
    """Start the local listener; returns the server, or None if the port is off (0) or cannot be bound."""
    if not port:
        return None
    try:
        server = await asyncio.start_server(lambda r, w: _handle(store, r, w), host, port)
    except OSError as exc:
        log.warning("live book endpoint not started on %s:%d (%s); the viewer will show no live ladder", host, port, exc)
        return None
    log.info("live book endpoint on %s:%d", host, port)
    return server


def fetch(symbol: str, ports: Optional[list[int]] = None, timeout: float = 1.0) -> Optional[dict]:
    """The live book from whichever collector holds the symbol, or None (feed not running, or symbol not subscribed)."""
    if not SYMBOL_RE.match(symbol):
        return None
    for port in ports if ports is not None else [port_for("eq"), port_for("fo")]:
        if not port:
            continue
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{port}/book?symbol={symbol}", timeout=timeout) as r:
                return json.loads(r.read())
        except (urllib.error.URLError, OSError, ValueError):
            continue
    return None
