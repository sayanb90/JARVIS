"""
Upstox candle REST API -> 1-minute bars, for any past day or the present day (after-close pulls, gap repair).

  python -m data_layer.upstox.candles --date 2026-10-26                 # full session, whole universe
  python -m data_layer.upstox.candles --date 2026-10-26 --limit 5 --dump-raw 3
  python -m data_layer.upstox.candles --from 2026-09-01 --to 2026-10-23 --chunk-days 5

Endpoints (from the official SDK, upstox-python-sdk 2.30.0), header `Authorization: Bearer <token>`:
  GET /v3/historical-candle/intraday/{instrumentKey}/{unit}/{interval}                      present trading day
  GET /v3/historical-candle/{instrumentKey}/{unit}/{interval}/{to_date}/{from_date}         past days (YYYY-MM-DD)
Response: {"status": "success", "data": {"candles": [[timestamp, o, h, l, c, volume, oi], ...]}}

Unverified until the first live run (runbook section 14): the unit spelling ("minutes"), how deep 1-minute history
goes (community posts say about six months), per-call range limits, and the timestamp format (we accept ISO strings
with an offset, naive strings as IST, and epoch milliseconds).  `--dump-raw N` saves the first N raw responses.

Existing live bars are NOT overwritten unless --overwrite is given: REST only fills what is missing.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import requests

from .. import tick_store
from .. import lake as lake_mod
from ..query import Lake
from ..ticks import SESSION_CLOSE_UTC, SESSION_OPEN_UTC, Bar
from .instruments import build_key_map

log = logging.getLogger(__name__)

BASE = "https://api.upstox.com"
IST = timezone(timedelta(hours=5, minutes=30))
UNIT = "minutes"


def ist_today() -> date:
    return datetime.now(IST).date()


def candle_ts(raw) -> datetime:
    """Candle timestamp -> naive UTC.  ISO with offset, naive ISO (taken as IST) or epoch milliseconds."""
    if isinstance(raw, (int, float)):
        return datetime.fromtimestamp(raw / 1000, tz=timezone.utc).replace(tzinfo=None)
    ts = pd.Timestamp(raw)
    ts = ts.tz_localize(IST) if ts.tzinfo is None else ts
    return ts.tz_convert("UTC").tz_localize(None).to_pydatetime()


def candles_to_bars(candles: list, symbol: str, interval: int = 1) -> list[Bar]:
    """Keep regular-session candles only; drop malformed rows."""
    out: list[Bar] = []
    for c in candles:
        try:
            ts = candle_ts(c[0])
            o, h, lo, cl = (float(x) for x in c[1:5])
            v = int(float(c[5] or 0))
        except (TypeError, ValueError, IndexError):
            log.warning("skipping malformed candle for %s: %r", symbol, c)
            continue
        if not (SESSION_OPEN_UTC <= ts.time() < SESSION_CLOSE_UTC):
            continue
        out.append(Bar(ts, symbol, "IN", o, h, lo, cl, v, None, None, None, None, None, None, src="rest"))
    return out


class CandleClient:
    def __init__(self, token: str, session: requests.Session | None = None, delay: float = 0.15,
                 retries: int = 3, base: str = BASE):
        self.token, self.delay, self.retries, self.base = token, delay, retries, base
        self.http = session or requests.Session()
        self.raw_dumped = 0

    def _get(self, path: str) -> dict | None:
        url = f"{self.base}{path}"
        for attempt in range(1, self.retries + 1):
            time.sleep(self.delay)
            r = self.http.get(url, headers={"Authorization": f"Bearer {self.token}", "Accept": "application/json"},
                              timeout=30)
            if r.status_code in (401, 403):
                raise RuntimeError(f"Upstox rejected the token (HTTP {r.status_code}); it expires daily, refresh it")
            if r.status_code == 429 or r.status_code >= 500:
                time.sleep(2.0 ** attempt)
                continue
            if r.status_code >= 400:
                log.warning("HTTP %s for %s: %s", r.status_code, path, r.text[:150])
                return None
            return r.json()
        log.error("gave up on %s after %d attempts", path, self.retries)
        return None

    def candles(self, key: str, day_from: date, day_to: date, interval: int = 1, source: str = "auto") -> list:
        enc = key.replace("|", "%7C")
        use_intraday = source == "intraday" or (source == "auto" and day_from == day_to == ist_today())
        body = None
        if use_intraday:
            body = self._get(f"/v3/historical-candle/intraday/{enc}/{UNIT}/{interval}")
        if body is None or not (body.get("data") or {}).get("candles"):
            if source != "intraday":
                body = self._get(f"/v3/historical-candle/{enc}/{UNIT}/{interval}/{day_to:%Y-%m-%d}/{day_from:%Y-%m-%d}")
        return (body or {}).get("data", {}).get("candles", []) if body else []


def existing_keys(lake_dir: Path, label: str, day_from: date, day_to: date) -> set[tuple[str, datetime]]:
    q = Lake(lake_dir)
    if f"bars_{label}" not in q.tables():
        return set()
    df = q.sql(f"SELECT symbol, ts FROM bars_{label} WHERE CAST(ts AS DATE) BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)",
               [str(day_from), str(day_to)])
    return {(s, pd.Timestamp(t).to_pydatetime()) for s, t in zip(df.symbol, df.ts)}


def pull(client: CandleClient, key_map: dict[str, str], day_from: date, day_to: date, interval: int = 1,
         source: str = "auto", overwrite: bool = False, lake_dir: Path | None = None,
         dump_raw: int = 0, dump_dir: Path | None = None) -> dict:
    """Fetch, de-duplicate against existing bars, store and compact.  Returns a summary dict."""
    lake_dir = lake_dir or lake_mod.LAKE_DIR           # resolved at call time so tests/deployments can redirect it
    label = f"{interval}m"
    have = set() if overwrite else existing_keys(lake_dir, label, day_from, day_to)
    stats = {"instruments": len(key_map), "no_data": [], "bars_fetched": 0, "bars_written": 0, "skipped_existing": 0}
    new: list[Bar] = []
    for key, sym in key_map.items():
        raw = client.candles(key, day_from, day_to, interval, source)
        if dump_raw and client.raw_dumped < dump_raw:
            d = dump_dir or Path("logs/candles")
            d.mkdir(parents=True, exist_ok=True)
            (d / f"{sym}-{day_from}-{day_to}.json").write_text(json.dumps(raw[:400]))
            client.raw_dumped += 1
        bars = [b for b in candles_to_bars(raw, sym, interval) if day_from <= b.ts.date() <= day_to]
        if not bars:
            stats["no_data"].append(sym)
            continue
        stats["bars_fetched"] += len(bars)
        for b in bars:
            if (b.symbol, b.ts) in have:
                stats["skipped_existing"] += 1
            else:
                new.append(b)
    stats["bars_written"] = tick_store.write_bars(new, label, lake_dir)
    d = day_from
    while d <= day_to:
        tick_store.compact_day(label, d, lake_dir)
        d += timedelta(days=1)
    return stats


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--date", type=date.fromisoformat, help="single day, YYYY-MM-DD")
    ap.add_argument("--from", dest="d_from", type=date.fromisoformat)
    ap.add_argument("--to", dest="d_to", type=date.fromisoformat)
    ap.add_argument("--chunk-days", type=int, default=1, help="days per API call when using --from/--to")
    ap.add_argument("--interval", type=int, default=1, help="candle width in minutes")
    ap.add_argument("--source", choices=["auto", "intraday", "historical"], default="auto")
    ap.add_argument("--limit", type=int, default=None, help="first N universe symbols only")
    ap.add_argument("--overwrite", action="store_true", help="replace existing bars instead of only filling gaps")
    ap.add_argument("--dump-raw", type=int, default=0, metavar="N", help="save the first N raw responses to logs/candles/")
    a = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    from dotenv import load_dotenv
    load_dotenv()
    token = os.getenv("UPSTOX_FEED_TOKEN") or os.getenv("UPSTOX_ACCESS_TOKEN", "")
    if not token:
        raise SystemExit("no token: run python -m data_layer.upstox.auth first")
    if a.date:
        d0 = d1 = a.date
    elif a.d_from and a.d_to:
        d0, d1 = a.d_from, a.d_to
    else:
        raise SystemExit("give --date, or both --from and --to")
    key_map, _ = build_key_map()
    if a.limit:
        key_map = dict(list(key_map.items())[: a.limit])
    if not key_map:
        raise SystemExit("no universe symbols with a usable ISIN")
    client = CandleClient(token)
    cur = d0
    total = {"bars_fetched": 0, "bars_written": 0, "skipped_existing": 0, "no_data": set()}
    while cur <= d1:
        end = min(cur + timedelta(days=a.chunk_days - 1), d1)
        s = pull(client, key_map, cur, end, a.interval, a.source, a.overwrite, dump_raw=a.dump_raw)
        log.info("%s..%s: fetched %d, wrote %d, already had %d, no data for %d instruments",
                 cur, end, s["bars_fetched"], s["bars_written"], s["skipped_existing"], len(s["no_data"]))
        for k in ("bars_fetched", "bars_written", "skipped_existing"):
            total[k] += s[k]
        total["no_data"] |= set(s["no_data"])
        cur = end + timedelta(days=1)
    print(f"done: wrote {total['bars_written']} bars ({total['skipped_existing']} already present); "
          f"no data for: {sorted(total['no_data']) or 'none'}")


if __name__ == "__main__":
    main()
