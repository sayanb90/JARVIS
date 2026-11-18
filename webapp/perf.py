"""
Times every API route of the running viewer, as each tab calls it (SCRUM-206: "loading takes forever").

  python -m webapp.perf [--base http://127.0.0.1:8000] [--date 2026-11-18] [--report logs/viewer-perf.txt]

Credentials from WEBAPP_USER / WEBAPP_PASSWORD (the environment or .env).  Each request runs twice: the first is what a viewer sees when it opens the
tab (cold caches), the second what the 15 s refresh costs.  Prints one line per request (seconds, kilobytes, status), the slowest first, then the
viewer's own /api/perf table (calls, mean and slowest per route, with the slowest query).  Read only: it only does GET requests.
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request


def _env(path: str = ".env") -> None:
    try:
        for line in open(path):
            k, _, v = line.strip().partition("=")
            if k and not k.startswith("#") and k not in os.environ:
                os.environ[k] = v.strip().strip("'\"")
    except OSError:
        pass


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="python -m webapp.perf")
    ap.add_argument("--base", default="http://127.0.0.1:8000")
    ap.add_argument("--date", default=None, help="default: the newest day with data")
    ap.add_argument("--report", default=None, help="also write the report to this file")
    ap.add_argument("--timeout", type=float, default=180.0)
    a = ap.parse_args(argv)
    _env()
    auth = None
    if os.environ.get("WEBAPP_USER") and os.environ.get("WEBAPP_PASSWORD"):
        auth = "Basic " + base64.b64encode(f"{os.environ['WEBAPP_USER']}:{os.environ['WEBAPP_PASSWORD']}".encode()).decode()
    lines: list[str] = []

    def out(s: str = "") -> None:
        print(s, flush=True)
        lines.append(s)

    def get(path: str, **params):
        q = urllib.parse.urlencode({k: v for k, v in params.items() if v not in (None, "")})
        req = urllib.request.Request(a.base + path + ("?" + q if q else ""))
        if auth:
            req.add_header("Authorization", auth)
        t0 = time.perf_counter()
        try:
            with urllib.request.urlopen(req, timeout=a.timeout) as r:
                body = r.read()
                return time.perf_counter() - t0, len(body), r.status, body
        except urllib.error.HTTPError as e:
            return time.perf_counter() - t0, 0, e.code, b""
        except Exception as e:                                          # noqa: BLE001
            return time.perf_counter() - t0, 0, type(e).__name__, b""

    def js(body: bytes):
        try:
            return json.loads(body)
        except ValueError:
            return None

    t, kb, st, body = get("/api/meta")
    meta = js(body)
    if not meta:
        out(f"cannot read {a.base}/api/meta (status {st}); is the viewer running and are WEBAPP_USER / WEBAPP_PASSWORD set?")
        return 2
    days = meta.get("days") or []
    day = a.date or (days[0] if days else None)
    out(f"viewer {a.base}   day {day}   days with data {len(days)}   candle days {len(meta.get('candle_days') or [])}")
    plan: list[tuple[str, str, dict]] = []
    if day:
        syms = js(get("/api/symbols", date=day)[3]) or []
        symbols = syms.get("symbols", syms) if isinstance(syms, dict) else syms
        names = [s["symbol"] if isinstance(s, dict) else s for s in symbols]
        eq = names[0] if names else None
        fo = js(get("/api/fo/meta", date=day)[3]) or {}
        unders = fo.get("underlyings") or []
        und = "NIFTY" if "NIFTY" in unders else (unders[0] if unders else None)
        chain = js(get("/api/fo/chain", date=day, underlying=und)[3]) if und else None
        contract = None
        if chain:
            fut = (chain.get("futures") or [{}])[0].get("symbol")
            contract = fut
        plan += [("Equities", "/api/day", {"date": day, "symbol": eq, "width": 1}), ("Equities (ticks)", "/api/day", {"date": day, "symbol": eq, "width": 0}),
                 ("Market", "/api/overview", {"date": day}), ("Candles", "/api/candles/symbols", {"date": day}),
                 ("Candles", "/api/candles", {"date": day, "symbol": eq, "minutes": 1})]
        if und:
            plan += [("Derivatives", "/api/fo/chain", {"date": day, "underlying": und})]
            if contract:
                plan += [("Derivatives", "/api/fo/greeks", {"date": day, "symbol": contract, "width": 1}), ("Derivatives", "/api/day", {"date": day, "symbol": contract, "width": 1})]
        for u in ("NIFTY", "BANKNIFTY"):                                     # the signals do not depend on the lake's derivative bars
            plan += [("Signals", "/api/signals/live", {"underlying": u, "date": day}),
                     ("Signals", "/api/signals/series", {"underlying": u, "signal": "book_pressure", "bucket": "1s", "date": day}),
                     ("Signals", "/api/signals/contracts", {"underlying": u, "signal": "book_pressure", "bucket": "1s", "date": day}),
                     ("Signals", "/api/signals/chain", {"underlying": u, "date": day}),
                     ("Signals", "/api/signals/smile", {"underlying": u, "date": day}),
                     ("Signals", "/api/signals/history", {"underlying": u, "signal": "skew", "bucket": "1min"})]
    plan += [("History", "/api/history/instruments", {}), ("History", "/api/history/volume", {}), ("Data", "/api/coverage", {}), ("Data", "/api/coverage/markets", {})]
    rows = []
    for tab, path, params in plan:
        if any(v is None for v in params.values()):
            continue
        t1, kb1, st1, body1 = get(path, **params)
        t2, kb2, st2, _ = get(path, **params)
        extra = ""
        if path == "/api/signals/live" and body1:
            j = js(body1) or {}
            extra = f" load={j.get('load', {}).get('files_done')}/{j.get('load', {}).get('files_total')} signals={len(j.get('signals', []))} store={j.get('store', {}).get('path')}"
        rows.append((t1, t2, kb1 / 1024, st1, tab, path, params, extra))
    out()
    out(f"{'first':>7} {'again':>7} {'KB':>8}  status  tab / request")
    for t1, t2, kb, st, tab, path, params, extra in sorted(rows, key=lambda r: -r[0]):
        q = " ".join(f"{k}={v}" for k, v in params.items() if k != "date")
        out(f"{t1:7.2f} {t2:7.2f} {kb:8.0f}  {st!s:>6}  {tab}: {path} {q}{extra}")
    t, kb, st, body = get("/api/perf")
    pj = js(body)
    if pj:
        out()
        out("the viewer's own timings since it started (route, calls, mean s, slowest s, the slowest query):")
        for r in pj["routes"]:
            out(f"  {r['route']:<28} {r['calls']:>4} {r['mean_s']:>7.2f} {r['max_s']:>7.2f}  {r['slowest_query']}")
    if a.report:
        with open(a.report, "w") as f:
            f.write("\n".join(lines) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
