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


_CHILD = r"""
import json, os, resource, sys, time
sys.path.insert(0, os.getcwd())
day, sym, und, store = {day!r}, {sym!r}, {und!r}, {store!r}
t0 = time.perf_counter()
rows = None
{body}
def _hwm_mb():                                  # this process's own peak (ru_maxrss would also carry the parent's peak across exec)
    try:
        for line in open("/proc/self/status"):
            if line.startswith("VmHWM:"):
                return int(line.split()[1]) // 1024
    except OSError:
        pass
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss // 1024
sys.stdout.write("\n@@RESULT " + json.dumps({{"secs": round(time.perf_counter() - t0, 2), "rss_mb": _hwm_mb(), "rows": rows}}) + "\n")
"""

DIRECT_CALLS = [
    ("symbols (the equity list)", "from data_layer.query import Lake\nrows = len(Lake().bar_symbols(day))"),
    ("day, 1 second bars of one equity", "from data_layer.query import Lake\nrows = len(Lake().day_bars(day, sym))"),
    ("day, raw ticks of one equity", "from data_layer.query import Lake\nrows = len(Lake().day_ticks(day, sym))"),
    ("market overview (every equity)", "from data_layer.query import Lake\nrows = len(Lake().day_overview(day))"),
    ("candles of one equity", "from data_layer.query import Lake\nrows = len(Lake().minute_candles(day, sym))"),
    ("F&O chain", "from data_layer.query import Lake\nrows = len(Lake().fo_chain(day, und) or {})"),
    ("Signals tab: the first read of the day (NIFTY)", "from webapp import signals_view\nfrom datetime import date\nrows = len(signals_view.summary(store, und, date.fromisoformat(day), wait_s=600)['signals'])"),
    ("Signals tab: BANKNIFTY after NIFTY (same process)", "from webapp import signals_view\nfrom datetime import date\nrows = len(signals_view.summary(store, und, date.fromisoformat(day), wait_s=600)['signals'])\nrows += len(signals_view.summary(store, 'BANKNIFTY', date.fromisoformat(day), wait_s=600)['signals'])"),
]


def direct(day, limit, report) -> int:
    import subprocess
    from pathlib import Path
    lines = []

    def out(s=""):
        print(s, flush=True)
        lines.append(s)
        if report:                                                              # after every line: a crash or a stop keeps what was measured
            with open(report, "w") as fh:
                fh.write("\n".join(lines) + "\n")

    from data_layer.query import Lake
    q = Lake()
    days = q.bar_days("1s")
    day = day or (days[0] if days else None)
    syms = q.bar_symbols(day)["symbol"].tolist() if day else []
    sym = next((s for s in syms if s.endswith(".NS")), syms[0] if syms else "")
    store = os.environ.get("JARVIS_SIGNAL_STORE") or str(Path.cwd() / "store")
    out(f"direct timings (each call in its own process, stopped after {limit:.0f} s)   day {day}   equity {sym}   store {store}")
    out(f"{'seconds':>8} {'peak MB':>8} {'rows':>9}  call")
    for name, body in DIRECT_CALLS:
        code = _CHILD.format(day=day, sym=sym, und="NIFTY", store=store, body=body.replace("\\n", "\n"))
        try:
            r = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, timeout=limit)
            hit = [x for x in r.stdout.replace("\r", "\n").splitlines() if x.startswith("@@RESULT ")]
            if r.returncode == 0 and hit:                                       # a progress bar or any other output before the marker is ignored
                j = json.loads(hit[-1][len("@@RESULT "):])
                out(f"{j['secs']:8.1f} {j['rss_mb']:8d} {j['rows']!s:>9}  {name}")
            else:
                out(f"{'failed':>8} {'':>8} {'':>9}  {name}: {(r.stderr or '').strip().splitlines()[-1:] }")
        except subprocess.TimeoutExpired:
            out(f"{'>' + str(int(limit)) + ' s':>8} {'':>8} {'':>9}  {name}: stopped")
    if report:
        with open(report, "w") as f:
            f.write("\n".join(lines) + "\n")
    return 0


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="python -m webapp.perf")
    ap.add_argument("--base", default="http://127.0.0.1:8000")
    ap.add_argument("--date", default=None, help="default: the newest day with data")
    ap.add_argument("--report", default=None, help="also write the report to this file")
    ap.add_argument("--timeout", type=float, default=180.0)
    ap.add_argument("--direct", action="store_true", help="time each heavy lake call and the Signals load in a process of its own (no viewer, no HTTP): seconds and peak memory")
    ap.add_argument("--limit", type=float, default=300.0, help="--direct: seconds before a call is stopped")
    a = ap.parse_args(argv)
    _env()
    if a.direct:
        return direct(a.date, a.limit, a.report)
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
        out(f"viewer memory now {pj.get('rss_mb_now')} MB (route, calls, mean s, slowest s, memory after, the most one call added):")
        for r in pj["routes"]:
            out(f"  {r['route']:<28} {r['calls']:>4} {r['mean_s']:>7.2f} {r['max_s']:>7.2f} {r.get('rss_mb_max', 0):>6} MB {r.get('most_added_mb', 0):>5} MB  {r['slowest_query']}")
    if a.report:
        with open(a.report, "w") as f:
            f.write("\n".join(lines) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
