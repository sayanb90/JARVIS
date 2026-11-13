"""
JARVIS intraday viewer: a small read-only web app over the Parquet lake.

  python -m webapp                      # http://127.0.0.1:8000, lake from $WEBAPP_LAKE / $JARVIS_LAKE / ./lake
  python -m webapp --lake lake_demo     # synthetic demo data (see webapp/demo.py)

API (JSON):
  GET /api/meta                                    days available, demo flag, allowed precisions (width 0 = raw ticks, else bar seconds)
  GET /api/symbols?date=YYYY-MM-DD                 equities with bars that day (F&O contracts are listed under /api/fo/*)
  GET /api/day?date=&symbol=&width=1&vol_window=300   summary + chart series for one instrument (equity or F&O); width=0 returns every tick
  GET /api/candles/symbols?date=                   equities with minute candles that day, and from which source (live feed, REST candle API)
  GET /api/candles?date=&symbol=&minutes=1&bins=40&source=auto   candles, summary and volume profile for one equity (Candles section)
  GET /api/overview?date=                          one row per equity: volume, spread, volatility, ...
  GET /api/fo/meta?date=                           underlyings with derivative bars that day
  GET /api/fo/chain?date=&underlying=&expiry=      futures + option chain snapshot (section 17)
  GET /api/coverage                                what the lake holds per dataset, gaps, historical run, snapshot, storage (Data tab)
  GET /api/coverage/markets                        per-market ranges of the daily candles and 1-minute bars (slower, cached)
  GET /api/history/instruments                     instruments with daily candles or 1-minute bars (History tab)
  GET /api/history/bars?symbol=&tf=&span=|start=&end=  candles and volume for one instrument (tf 1d, 1m, 5m, 15m, 60m)
  GET /api/history/volume?market=&date=&sort=&n=   the most traded tickers on a day

Admin (data browser, read-only SQL, subscription list) is at /admin when ADMIN_USER and ADMIN_PASSWORD are set;
see webapp/admin.py.  The app never writes to the lake.  It binds to 127.0.0.1 by default; put a reverse proxy with TLS in front of it
for remote access, and/or set WEBAPP_USER and WEBAPP_PASSWORD to require HTTP Basic auth.
"""

from __future__ import annotations

import base64
import os
import re
import secrets
import time
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd
from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import FileResponse, JSONResponse

from analytics.microstructure import day_summary, rolling_realised_vol, series_payload, to_ist_strings
from analytics.profile import candle_summary, candles_payload, volume_profile
from . import markets as mkt
from data_layer import live_book
from data_layer.lake import LAKE_DIR
from data_layer import tick_store
from data_layer.query import Lake
from . import coverage as coverage_mod
from . import history as history_mod

STATIC = Path(__file__).parent / "static"
WIDTHS = (0, 1, 5, 15, 30, 60, 300)              # precision offered in the UI: 0 = raw ticks (no aggregation), else a bar width in seconds
VOL_WINDOWS = (60, 300, 900, 1800)
CANDLE_MINUTES = (1, 3, 5, 15, 30, 60)           # candle size offered in the Candles section
PROFILE_BINS = (20, 40, 60, 100)                 # about how many price levels the volume profile is split into
SYMBOL_RE = re.compile(r"^[A-Za-z0-9&._-]{1,32}$")
UNDERLYING_RE = re.compile(r"^[A-Za-z0-9&]{1,20}$")
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _json_safe(obj):
    """Replace NaN/inf with None recursively (browsers reject NaN in JSON)."""
    if isinstance(obj, dict):
        return {k: _json_safe(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_json_safe(v) for v in obj]
    if isinstance(obj, float) and not np.isfinite(obj):
        return None
    if isinstance(obj, (np.floating, np.integer)):
        return _json_safe(obj.item())
    return obj


def _check_date(d: str) -> str:
    if not DATE_RE.match(d):
        raise HTTPException(422, "date must be YYYY-MM-DD")
    try:
        date.fromisoformat(d)
    except ValueError:
        raise HTTPException(422, "not a real calendar date")
    return d


def _check_symbol(s: str) -> str:
    if not SYMBOL_RE.match(s):
        raise HTTPException(422, "invalid symbol")
    return s


def create_app(lake_dir: Path | str | None = None, interval: str = "1s") -> FastAPI:
    lake_path = Path(lake_dir or os.getenv("WEBAPP_LAKE") or os.getenv("JARVIS_LAKE") or LAKE_DIR)
    user, password = os.getenv("WEBAPP_USER"), os.getenv("WEBAPP_PASSWORD")

    def auth(request: Request) -> None:
        if not (user and password) or request.url.path.startswith("/admin"):
            return                                   # /admin has its own, separate credentials (webapp/admin.py)
        header = request.headers.get("authorization", "")
        ok = False
        if header.lower().startswith("basic "):
            try:
                u, _, p = base64.b64decode(header[6:]).decode().partition(":")
                ok = secrets.compare_digest(u, user) & secrets.compare_digest(p, password)
            except Exception:
                ok = False
        if not ok:
            raise HTTPException(401, "authentication required", headers={"WWW-Authenticate": 'Basic realm="JARVIS"'})

    app = FastAPI(title="JARVIS intraday viewer", docs_url=None, redoc_url=None, dependencies=[Depends(auth)])
    app.add_middleware(GZipMiddleware, minimum_size=1000)
    app.state.lake_path = lake_path
    from .admin import admin_router
    admin = admin_router(lake_path)
    if admin is not None:                            # only when ADMIN_USER and ADMIN_PASSWORD are set
        app.include_router(admin)
    overview_cache: dict[tuple, list] = {}

    def lake() -> Lake:
        return Lake(lake_path)            # cheap: registers views over parquet files, reads nothing

    @app.get("/api/meta")
    def meta():
        q = lake()
        return {"demo": (lake_path / "DEMO").exists(), "interval": interval, "days": q.bar_days(interval), "candle_days": q.candle_days(interval),
                "widths": list(WIDTHS), "vol_windows": list(VOL_WINDOWS), "candle_minutes": list(CANDLE_MINUTES), "profile_bins": list(PROFILE_BINS)}

    @app.get("/api/symbols")
    def symbols(date: str):
        _check_date(date)
        q = lake()
        df = q.bar_symbols(date, interval)
        if df.empty:
            raise HTTPException(404, f"no bars for {date}")
        df = df[~df["symbol"].isin(q.fo_symbols()) & ~df["symbol"].map(tick_store.is_index)]   # stocks only; F&O has its own tab
        if df.empty:
            raise HTTPException(404, f"no equity bars for {date}")
        return df.to_dict("records")

    @app.get("/api/day")
    def day(date: str, symbol: str, width: int = Query(1), vol_window: int = Query(300)):
        _check_date(date); _check_symbol(symbol)
        if width not in WIDTHS:
            raise HTTPException(422, f"width must be one of {WIDTHS}")
        if vol_window not in VOL_WINDOWS:
            raise HTTPException(422, f"vol_window must be one of {VOL_WINDOWS}")
        q = lake()
        if width == 0:                                              # ticks: the raw rows, no aggregation (SCRUM-131)
            df = q.day_ticks(date, symbol)
            if df.empty:
                raise HTTPException(404, f"no raw ticks for {symbol} on {date} (precision Ticks needs the day's raw ticks)")
            series = series_payload(df, vol_window, ms=True)
            summary = day_summary(df)
            one = q.day_bars(date, symbol, interval, 1)             # volatility is estimated from 1-second bars, then shown at each tick
            if not one.empty:
                one = one.sort_values("ts").reset_index(drop=True)
                vol = pd.DataFrame({"ts": pd.to_datetime(one["ts"]).astype("datetime64[ns]"), "v": rolling_realised_vol(one, vol_window) * 100})
                at = pd.DataFrame({"ts": pd.to_datetime(df.sort_values("ts")["ts"]).astype("datetime64[ns]")})
                mapped = pd.merge_asof(at, vol, on="ts")["v"]
                series["vol_ann"] = [None if np.isnan(v) else round(float(v), 6) for v in mapped]
                summary["realised_vol_pct"] = day_summary(one).get("realised_vol_pct")
        else:
            df = q.day_bars(date, symbol, interval, width)
            if df.empty:
                raise HTTPException(404, f"no bars for {symbol} on {date}")
            series, summary = series_payload(df, max(vol_window, width * 2)), day_summary(df)
        return JSONResponse(_json_safe({
            "symbol": symbol, "date": date, "width": width, "vol_window": vol_window,
            "summary": summary,
            "has_book": bool(df[["bid", "ask"]].notna().all(axis=1).any()),
            "has_oi": bool("oi" in df.columns and df["oi"].notna().any()),
            "series": series}))

    @app.get("/api/candles/symbols")
    def candle_symbols(date: str):
        _check_date(date)
        q = lake()
        skip = q.fo_symbols()
        live, rest = q.bar_symbols(date, interval), q.bar_symbols(date, "1m")
        have = {s: {"symbol": s, "live": False, "rest": False} for s in set(live["symbol"]) | set(rest["symbol"])}
        for s in live["symbol"]:
            have[s]["live"] = True
        for s in rest["symbol"]:
            have[s]["rest"] = True
        info = cached("universe-info", 600, mkt.universe_info)
        rows = []
        for s, r in sorted(have.items()):
            if s in skip or tick_store.is_index(s):
                continue
            u = info.get(s) or {}
            market = u.get("market") or mkt.market_of(s)
            rows.append({**r, "market": market, "name": u.get("name", ""), "group": u.get("sector") or "Other"})
        if not rows:
            raise HTTPException(404, f"no minute candles for {date}")
        return rows

    @app.get("/api/candles")
    def candles(date: str, symbol: str, minutes: int = Query(1), bins: int = Query(40), source: str = Query("auto")):
        _check_date(date); _check_symbol(symbol)
        if minutes not in CANDLE_MINUTES:
            raise HTTPException(422, f"minutes must be one of {CANDLE_MINUTES}")
        if bins not in PROFILE_BINS:
            raise HTTPException(422, f"bins must be one of {PROFILE_BINS}")
        if source not in ("auto", "live", "rest"):
            raise HTTPException(422, "source must be auto, live or rest")
        df, used = lake().minute_candles(date, symbol, minutes, source, interval)
        if df.empty:
            raise HTTPException(404, f"no {'minute ' if source == 'auto' else source + ' '}candles for {symbol} on {date}")
        u = (cached("universe-info", 600, mkt.universe_info)).get(symbol) or {}
        ex = mkt.exchange_for(symbol, u.get("market"))
        return JSONResponse(_json_safe({
            "symbol": symbol, "date": date, "minutes": minutes, "bins": bins, "source": used, "name": u.get("name", ""),
            "exchange": mkt.public(ex, date),
            "summary": candle_summary(df, minutes, ex["tz"], mkt.session_minutes(ex)), "series": candles_payload(df, ex["tz"]),
            "profile": volume_profile(df, bins)}))

    @app.get("/api/overview")
    def overview(date: str):
        _check_date(date)
        q = lake()
        if interval == "1s":
            fingerprint = q.fingerprint(date)                      # the day's tick files (what the collector writes) and any legacy bars
        else:
            folder = lake_path / "bars" / f"interval={interval}" / f"date={date}"
            fingerprint = tuple((f.name, f.stat().st_mtime_ns) for f in sorted(folder.glob("*.parquet")))
        if not fingerprint:
            raise HTTPException(404, f"no bars for {date}")
        key = (date, fingerprint)
        if key not in overview_cache:
            overview_cache.clear()          # keep only the latest day in memory
            rows = q.day_overview(date, interval, equities_only=True)      # futures and options are left out before any bar is built
            rows = rows[~rows["symbol"].isin(q.fo_symbols())]
            overview_cache[key] = _json_safe(rows.to_dict("records"))
        return overview_cache[key]

    @app.get("/api/fo/meta")
    def fo_meta(date: str):
        _check_date(date)
        return {"underlyings": lake().fo_underlyings(date, interval)}

    @app.get("/api/fo/chain")
    def fo_chain(date: str, underlying: str, expiry: str | None = None):
        _check_date(date)
        expiry = expiry or None               # an empty string means "no preference", same as omitting it
        if not UNDERLYING_RE.match(underlying):
            raise HTTPException(422, "invalid underlying")
        if expiry is not None and not DATE_RE.match(expiry):
            raise HTTPException(422, "expiry must be YYYY-MM-DD")
        data = lake().fo_chain(date, underlying, expiry, interval)
        if not data:
            raise HTTPException(404, f"no derivatives for {underlying} on {date}")
        return JSONResponse(_json_safe(data))

    @app.get("/api/fo/greeks")
    def fo_greeks(date: str, symbol: str, width: int = Query(1)):
        _check_date(date); _check_symbol(symbol)
        if width not in WIDTHS:
            raise HTTPException(422, f"width must be one of {WIDTHS}")
        df = lake().greeks_series(date, symbol, width)
        if df.empty:
            return {"symbol": symbol, "date": date, "width": width, "available": False, "series": {}}
        series = {"ts": to_ist_strings(df["ts"], ms=(width == 0))}
        for g in Lake.GREEKS:
            series[g] = [None if pd.isna(v) else round(float(v), 6) for v in df[g]]
        return JSONResponse(_json_safe({"symbol": symbol, "date": date, "width": width, "available": True,
                                        "series": series, "computed": list(df.attrs.get("computed", []))}))

    @app.get("/api/live_book")
    def live_book_api(symbol: str):
        """Latest five bid and ask levels from the running collector (memory only, never stored)."""
        _check_symbol(symbol)
        if (lake_path / "DEMO").exists():
            from . import demo
            return demo.demo_book(lake(), symbol)
        book = live_book.fetch(symbol)
        if book is None:
            raise HTTPException(503, "no live book: the feed is not running, or this instrument is not subscribed")
        return JSONResponse(_json_safe(book))

    cov_cache: dict[str, tuple[float, object]] = {}

    def cached(key: str, ttl: float, fn):
        now = time.monotonic()
        hit = cov_cache.get(key)
        if hit and now - hit[0] < ttl:
            return hit[1]
        val = _json_safe(fn())
        cov_cache[key] = (now, val)
        return val

    @app.get("/api/coverage")
    def coverage_api():
        """What the lake holds per dataset (first and last day, gaps, the last 60 days), the historical run, the research snapshot and storage.
        Reads folder names and a few small files only; cached for a minute."""
        return cached("coverage", 60.0, lambda: coverage_mod.coverage(lake_path))

    @app.get("/api/coverage/markets")
    def coverage_markets():
        """Per-market ranges of the daily candles and the 1-minute bars (reads the bars tables with a memory cap; cached for ten minutes)."""
        return cached("markets", 600.0, lambda: coverage_mod.market_ranges(lake_path))

    def history_call(fn, *args, **kw):
        try:
            return fn(lake_path, *args, **kw)
        except history_mod.HistoryError as exc:
            raise HTTPException(404 if str(exc).startswith("no ") else 422, str(exc))

    @app.get("/api/history/instruments")
    def history_instruments():
        """Every instrument the History tab can chart (stocks with daily candles; stocks and indices with 1-minute bars)."""
        return cached("instruments", 600.0, lambda: history_mod.instruments(lake_path))

    @app.get("/api/history/bars")
    def history_bars(symbol: str, tf: str = "1d", start: str | None = None, end: str | None = None, span: int | None = Query(None, ge=1, le=40000)):
        """Candles and volume for one instrument: tf 1d, 1m, 5m, 15m or 60m, over `span` calendar days back from its last day (or start..end);
        an intraday range is clipped to what a chart can show."""
        return JSONResponse(_json_safe(history_call(history_mod.get_bars, symbol, tf, start, end, span)))

    @app.get("/api/history/volume")
    def history_volume(market: str | None = None, date: str | None = None, sort: str = "volume", n: int = 20):
        """The most traded tickers on a day (default: the latest), ranked by volume, relative volume, change or traded value."""
        return JSONResponse(_json_safe(history_call(history_mod.top_volume, market or None, date or None, n, sort)))

    @app.get("/vendor/plotly.min.js", include_in_schema=False)
    def plotly_js():
        import plotly
        f = Path(plotly.__file__).parent / "package_data" / "plotly.min.js"
        return FileResponse(f, media_type="application/javascript", headers={"Cache-Control": "public, max-age=86400"})

    @app.get("/", include_in_schema=False)
    def index():
        # no-cache (not no-store): the browser must revalidate via ETag/Last-Modified every time rather than
        # guess freshness heuristically, since this page and app.js change often -- a stale copy otherwise
        # survives reloads and silently keeps running old JS against a new API.
        return FileResponse(STATIC / "index.html", headers={"Cache-Control": "no-cache"})

    @app.get("/static/{name}", include_in_schema=False)
    def static_file(name: str):
        # explicit route (not a mounted sub-app) so the optional basic auth also covers it; no path separators allowed
        f = STATIC / name
        if "/" in name or "\\" in name or not f.is_file() or f.parent.resolve() != STATIC.resolve():
            raise HTTPException(404)
        return FileResponse(f, headers={"Cache-Control": "no-cache"})

    return app


app = create_app()
