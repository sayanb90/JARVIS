"""
JARVIS intraday viewer: a small read-only web app over the Parquet lake.

  python -m webapp                      # http://127.0.0.1:8000, lake from $WEBAPP_LAKE / $JARVIS_LAKE / ./lake
  python -m webapp --lake lake_demo     # synthetic demo data (see webapp/demo.py)

API (JSON):
  GET /api/meta                                    days available, demo flag, allowed bar widths
  GET /api/symbols?date=YYYY-MM-DD                 instruments with bars that day
  GET /api/day?date=&symbol=&width=1&vol_window=300   summary + chart series for one instrument
  GET /api/overview?date=                          one row per instrument: volume, spread, volatility, ...

The app never writes to the lake.  It binds to 127.0.0.1 by default; put a reverse proxy with TLS in front of it
for remote access, and/or set WEBAPP_USER and WEBAPP_PASSWORD to require HTTP Basic auth.
"""

from __future__ import annotations

import base64
import os
import re
import secrets
from datetime import date
from pathlib import Path

import numpy as np
from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import FileResponse, JSONResponse

from analytics.microstructure import day_summary, series_payload
from data_layer.lake import LAKE_DIR
from data_layer.query import Lake

STATIC = Path(__file__).parent / "static"
WIDTHS = (1, 5, 15, 30, 60, 300)                 # bar widths in seconds offered in the UI
VOL_WINDOWS = (60, 300, 900, 1800)
SYMBOL_RE = re.compile(r"^[A-Za-z0-9&._-]{1,32}$")
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
        if not (user and password):
            return
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
    overview_cache: dict[tuple, list] = {}

    def lake() -> Lake:
        return Lake(lake_path)            # cheap: registers views over parquet files, reads nothing

    @app.get("/api/meta")
    def meta():
        return {"demo": (lake_path / "DEMO").exists(), "interval": interval, "days": lake().bar_days(interval),
                "widths": list(WIDTHS), "vol_windows": list(VOL_WINDOWS)}

    @app.get("/api/symbols")
    def symbols(date: str):
        _check_date(date)
        df = lake().bar_symbols(date, interval)
        if df.empty:
            raise HTTPException(404, f"no bars for {date}")
        return df.to_dict("records")

    @app.get("/api/day")
    def day(date: str, symbol: str, width: int = Query(1), vol_window: int = Query(300)):
        _check_date(date); _check_symbol(symbol)
        if width not in WIDTHS:
            raise HTTPException(422, f"width must be one of {WIDTHS}")
        if vol_window not in VOL_WINDOWS:
            raise HTTPException(422, f"vol_window must be one of {VOL_WINDOWS}")
        df = lake().day_bars(date, symbol, interval, width)
        if df.empty:
            raise HTTPException(404, f"no bars for {symbol} on {date}")
        return JSONResponse(_json_safe({
            "symbol": symbol, "date": date, "width": width, "vol_window": vol_window,
            "summary": day_summary(df),
            "has_book": bool(df[["bid", "ask"]].notna().all(axis=1).any()),
            "has_oi": bool("oi" in df.columns and df["oi"].notna().any()),
            "series": series_payload(df, max(vol_window, width * 2))}))

    @app.get("/api/overview")
    def overview(date: str):
        _check_date(date)
        folder = lake_path / "bars" / f"interval={interval}" / f"date={date}"
        files = sorted(folder.glob("*.parquet"))
        if not files:
            raise HTTPException(404, f"no bars for {date}")
        key = (date, tuple((f.name, f.stat().st_mtime_ns) for f in files))
        if key not in overview_cache:
            overview_cache.clear()          # keep only the latest day in memory
            overview_cache[key] = _json_safe(lake().day_overview(date, interval).to_dict("records"))
        return overview_cache[key]

    @app.get("/vendor/plotly.min.js", include_in_schema=False)
    def plotly_js():
        import plotly
        f = Path(plotly.__file__).parent / "package_data" / "plotly.min.js"
        return FileResponse(f, media_type="application/javascript", headers={"Cache-Control": "public, max-age=86400"})

    @app.get("/", include_in_schema=False)
    def index():
        return FileResponse(STATIC / "index.html")

    @app.get("/static/{name}", include_in_schema=False)
    def static_file(name: str):
        # explicit route (not a mounted sub-app) so the optional basic auth also covers it; no path separators allowed
        f = STATIC / name
        if "/" in name or "\\" in name or not f.is_file() or f.parent.resolve() != STATIC.resolve():
            raise HTTPException(404)
        return FileResponse(f)

    return app


app = create_app()
