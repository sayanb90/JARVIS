"""
JARVIS admin: data browser, read-only SQL console and the subscription list, at /admin on the viewer's host.

Separate from the TensorT Labs admin console (its own repo and subdomain).  Off unless ADMIN_USER and ADMIN_PASSWORD
are set; then every /admin path needs those credentials (HTTP Basic, over HTTPS via the reverse proxy).

Tables are grouped into services, the way a kdb+ setup splits them across ports.  Here they are only a catalog:
the data stays one Parquet lake read by DuckDB, so a "service" is a schema name, not a process.

  reference     universe, equity instrument table, F&O contract table, F&O watchlist, selected contracts
  market        raw ticks (live feed), REST minute bars, legacy 1-second bars
  aggregates    daily OHLC, 1-second and 1-minute bars derived from ticks (per day)
  fundamentals  income statement, balance sheet, cash flow, ratios, pillar metrics, fund meta
  analytics     signals and features (none yet)

Everything is read-only except the subscription list upload, which writes data/universe.local.csv (versioned).
SQL runs in a DuckDB sandbox: one SELECT statement, reads only inside the lake, configuration locked, row, time and
memory limits.  Pages are fetched one at a time, so opening page 2 of a day of ticks reads 200 rows, not the day.
"""

from __future__ import annotations

import base64
import logging
import re
import secrets
import threading
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
import pyarrow.parquet as pq
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import FileResponse, JSONResponse

from data_layer import fmp_client, universe as uni
from data_layer import lake as lake_mod
from data_layer.query import Lake
from data_layer.upstox import fo_instruments as fo

log = logging.getLogger("jarvis.admin")
STATIC = Path(__file__).parent / "static"
IST = timezone(timedelta(hours=5, minutes=30))
PAGE_SIZES = (50, 100, 200, 500)
QUERY_TIMEOUT_S = 20.0
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
NAME_RE = re.compile(r"^[a-z0-9_]{1,40}$")
SYMBOL_RE = re.compile(r"^[A-Za-z0-9&._|-]{1,40}$")
SERVICES = [("reference", "Reference"), ("market", "Market data"), ("aggregates", "Aggregates"),
            ("fundamentals", "Fundamentals"), ("analytics", "Analytics")]


@dataclass(frozen=True)
class Table:
    service: str
    name: str
    title: str
    about: str
    kind: str                  # days: lake/<dir>/date=YYYY-MM-DD/*.parquet | file: one parquet | years: ohlc | frame: CSV | derived
    location: str = ""         # relative to the lake (days, file, years) or a loader key (frame)
    time_col: str | None = None
    width_s: int = 0           # derived bars width


CATALOG = [
    Table("reference", "universe", "Equity universe", "Instruments the collectors follow (all markets; India is the live feed's list).", "frame", "universe"),
    Table("reference", "eq_instruments", "NSE equity instruments", "Every NSE cash-equity listing from the Upstox file: ticker, ISIN, Upstox key.", "file", "instruments/eq.parquet"),
    Table("reference", "fo_contracts", "F&O contracts", "Every futures and options contract seen in the Upstox file, expired ones kept.", "file", "instruments/fo.parquet"),
    Table("reference", "fo_watchlist", "F&O watchlist", "Underlyings and the expiry/strike window to collect.", "frame", "fo_watchlist"),
    Table("reference", "fo_selected", "F&O selected contracts", "Today's contracts chosen from the watchlist (what the derivatives feed subscribes).", "frame", "fo_selected"),
    Table("market", "ticks", "Ticks", "Every update from the live feed: last price, cumulative volume, best bid/ask and sizes, open interest.", "days", "ticks", "ts"),
    Table("market", "bars_1m_rest", "Minute bars (REST)", "One-minute candles pulled from the Upstox candle API after the close.", "days", "bars/interval=1m", "ts"),
    Table("market", "bars_1s_legacy", "1-second bars (legacy)", "Bars written by the feed before it stored raw ticks.", "days", "bars/interval=1s", "ts"),
    Table("aggregates", "ohlc_daily", "Daily OHLC", "Five years of daily bars for India, US, UK and EU (Yahoo).", "years", "ohlc", "date"),
    Table("aggregates", "bars_1s", "1-second bars", "Derived from the day's ticks on read.", "derived", "ticks", "ts", 1),
    Table("aggregates", "bars_1m", "1-minute bars", "Derived from the day's ticks on read.", "derived", "ticks", "ts", 60),
    *[Table("fundamentals", n, n.replace("_", " ").capitalize(), f"FMP {n.replace('_', ' ')} (India).", "file", f"fundamentals/{n}.parquet",
            "date" if n in ("income_statement", "balance_sheet", "cash_flow", "ratios") else None)
      for n in ("income_statement", "balance_sheet", "cash_flow", "ratios", "pillar_metrics", "fund_meta")],
]
BY_KEY = {(t.service, t.name): t for t in CATALOG}


def _ist_today() -> date:
    return datetime.now(IST).date()


def _safe(v):
    if v is None or v is pd.NaT or v is pd.NA:
        return None
    if isinstance(v, float):
        return v if np.isfinite(v) else None
    if isinstance(v, (np.floating,)):
        return _safe(float(v))
    if isinstance(v, (np.integer,)):
        return int(v)
    if isinstance(v, (pd.Timestamp, datetime)):
        return v.isoformat(sep=" ", timespec="milliseconds" if v.microsecond else "seconds")
    if isinstance(v, date):
        return v.isoformat()
    if isinstance(v, (bytes, bytearray)):
        return v.hex()
    if isinstance(v, (list, tuple, dict)):
        return str(v)
    return v


def _rows(df: pd.DataFrame) -> list[list]:
    return [[_safe(v) for v in row] for row in df.itertuples(index=False, name=None)]


class Catalog:
    """Where each table's files are, which days exist, and the SQL that reads one page of it."""

    def __init__(self, lake_dir: Path, data_dir: Path) -> None:
        self.lake, self.data = Path(lake_dir), Path(data_dir)

    # -- discovery ----------------------------------------------------------
    def days(self, t: Table) -> list[str]:
        if t.kind in ("days", "derived"):
            base = self.lake / t.location
            return sorted((p.name.split("=", 1)[1] for p in base.glob("date=*") if any(p.glob("*.parquet"))), reverse=True)
        return []

    def files(self, t: Table, day: str | None = None) -> list[Path]:
        if t.kind in ("days", "derived"):
            return sorted((self.lake / t.location / f"date={day}").glob("*.parquet")) if day else \
                sorted((self.lake / t.location).glob("date=*/*.parquet"))
        if t.kind == "file":
            p = self.lake / t.location
            return [p] if p.exists() else []
        if t.kind == "years":
            return sorted((self.lake / t.location).glob("year=*/ohlc.parquet"))
        return []

    def frame(self, t: Table) -> pd.DataFrame:
        try:
            if t.location == "universe":
                df = pd.DataFrame(fmp_client.read_universe(None))
                if len(df):
                    df["source"] = fmp_client.active_universe_csv().name
                return df
            if t.location == "fo_watchlist":
                p = self.data / "fo_watchlist.csv"
                return pd.read_csv(p) if p.exists() else pd.DataFrame()
            if t.location == "fo_selected":
                p = self.data / "fo_universe.csv"
                return pd.read_csv(p) if p.exists() else pd.DataFrame()
        except Exception as exc:                       # noqa: BLE001 -- a bad CSV shows as an empty table, not a 500
            log.warning("could not read %s: %s", t.location, exc)
        return pd.DataFrame()

    def available(self, t: Table) -> bool:
        if t.kind == "frame":
            return not self.frame(t).empty
        return bool(self.files(t))

    def file_rows(self, files: list[Path]) -> int:
        n = 0
        for f in files:
            try:
                n += pq.ParquetFile(f).metadata.num_rows       # footer only: no data read
            except Exception:                                  # noqa: BLE001 -- a file being written right now
                pass
        return n

    # -- one table as SQL -----------------------------------------------------
    @staticmethod
    def _q(p: Path) -> str:
        return "'" + str(p).replace("'", "''") + "'"

    def source_sql(self, t: Table, day: str | None) -> str | None:
        files = self.files(t, day)
        if t.kind in ("days", "file", "years"):
            if not files:
                return None
            return f"read_parquet([{', '.join(self._q(f) for f in files)}], union_by_name=true, hive_partitioning=false)"
        if t.kind == "derived":
            if not day or not files:
                return None
            return Lake._ticks_bars_sql(f"{self.lake / t.location / f'date={day}'}/*.parquet", t.width_s)
        return None


class Sandbox:
    """A DuckDB connection that sees each table as <service>.<name> and can read nothing outside the lake."""

    def __init__(self, cat: Catalog, memory: str = "1GB", threads: int = 2) -> None:
        self.con = duckdb.connect(config={"threads": threads, "memory_limit": memory})
        self.views: dict[str, list[str]] = {}
        for svc, _ in SERVICES:
            self.con.execute(f"CREATE SCHEMA {svc}")
        for t in CATALOG:
            sql = None
            if t.kind == "days" and cat.files(t):
                sql = (f"SELECT * FROM read_parquet({cat._q(cat.lake / t.location / 'date=*' / '*.parquet')}, "
                       f"hive_partitioning=true, union_by_name=true)")
            elif t.kind in ("file", "years") and cat.files(t):
                sql = f"SELECT * FROM {cat.source_sql(t, None)}"
            elif t.kind == "frame":
                df = cat.frame(t)
                if not df.empty:
                    self.con.register(f"_frame_{t.name}", df)
                    sql = f"SELECT * FROM _frame_{t.name}"
            if sql:
                self.con.execute(f"CREATE VIEW {t.service}.{t.name} AS {sql}")
                cols = [r[0] for r in self.con.execute(f"DESCRIBE {t.service}.{t.name}").fetchall()]
                self.views[f"{t.service}.{t.name}"] = cols
        lake = str(cat.lake.resolve()).replace("'", "''")
        self.con.execute(f"SET allowed_directories=['{lake}']")
        self.con.execute("SET enable_external_access=false")
        self.con.execute("SET lock_configuration=true")

    def run(self, sql: str, params: list | None = None, timeout: float = QUERY_TIMEOUT_S) -> pd.DataFrame:
        timer = threading.Timer(timeout, self.con.interrupt)
        timer.start()
        try:
            return self.con.execute(sql, params or []).df()
        except duckdb.InterruptException:
            raise HTTPException(408, f"query stopped after {timeout:.0f} s; filter by date or symbol, or aggregate first")
        finally:
            timer.cancel()


def validate_select(sql: str) -> str:
    """One read-only SELECT (or WITH ... SELECT); anything else is refused before it runs."""
    sql = (sql or "").strip().rstrip(";").strip()
    if not sql:
        raise HTTPException(422, "empty query")
    if len(sql) > 20_000:
        raise HTTPException(422, "query too long")
    try:
        stmts = duckdb.extract_statements(sql)
    except duckdb.Error as exc:
        raise HTTPException(422, f"syntax error: {str(exc).splitlines()[0][:300]}")
    if len(stmts) != 1:
        raise HTTPException(422, "one statement at a time")
    if stmts[0].type != duckdb.StatementType.SELECT:
        raise HTTPException(422, f"read-only console: {stmts[0].type.name} statements are not allowed, only SELECT")
    return sql


def admin_router(lake_dir: Path, data_dir: Path | None = None) -> APIRouter | None:
    import os
    user, password = os.getenv("ADMIN_USER"), os.getenv("ADMIN_PASSWORD")
    if not (user and password):
        return None                                   # admin off: /admin is simply not there
    data_dir = Path(data_dir or lake_mod.DATA_DIR)
    cat = Catalog(lake_dir, data_dir)

    def auth(request: Request) -> None:
        header = request.headers.get("authorization", "")
        ok = False
        if header.lower().startswith("basic "):
            try:
                u, _, p = base64.b64decode(header[6:]).decode().partition(":")
                ok = secrets.compare_digest(u, user) & secrets.compare_digest(p, password)
            except Exception:                         # noqa: BLE001
                ok = False
        if not ok:
            time.sleep(0.5)                           # slows password guessing; the proxy rate-limits as well
            raise HTTPException(401, "authentication required", headers={"WWW-Authenticate": 'Basic realm="JARVIS admin"'})

    def same_origin(request: Request) -> None:
        # a page on another site cannot set this header without a CORS preflight, which this app never answers:
        # so a logged-in browser cannot be tricked into changing the subscription list
        if request.headers.get("x-jarvis-admin") != "1":
            raise HTTPException(403, "missing admin header")

    r = APIRouter(prefix="/admin", dependencies=[Depends(auth)], include_in_schema=False)
    noindex = {"X-Robots-Tag": "noindex, nofollow", "Cache-Control": "no-store"}

    def table_or_404(service: str, name: str) -> Table:
        if not (NAME_RE.match(service) and NAME_RE.match(name)) or (service, name) not in BY_KEY:
            raise HTTPException(404, "no such table")
        return BY_KEY[(service, name)]

    # -- pages ----------------------------------------------------------------
    @r.get("")
    @r.get("/")
    def page():
        return FileResponse(STATIC / "admin.html", headers=noindex)

    @r.get("/static/{name}")
    def asset(name: str):
        if name not in ("admin.js", "admin.css", "style.css"):     # served here too: the viewer's own /static may sit behind other credentials
            raise HTTPException(404)
        return FileResponse(STATIC / name, headers=noindex)

    # -- catalog ----------------------------------------------------------------
    @r.get("/api/catalog")
    def catalog():
        out = []
        for svc, title in SERVICES:
            tables = []
            for t in [t for t in CATALOG if t.service == svc]:
                days = cat.days(t)
                tables.append({"name": t.name, "title": t.title, "about": t.about, "kind": t.kind,
                               "days": days[:400], "available": cat.available(t), "time_col": t.time_col,
                               "query_name": None if t.kind == "derived" else f"{t.service}.{t.name}"})
            out.append({"service": svc, "title": title, "tables": tables})
        return JSONResponse(out, headers=noindex)

    # -- one page of a table --------------------------------------------------------
    @r.get("/api/table")
    def table(service: str, name: str, day: str | None = None, page: int = Query(1, ge=1, le=1_000_000),
              size: int = Query(200), symbol: str | None = None, order: str = Query("stored")):
        t = table_or_404(service, name)
        if size not in PAGE_SIZES:
            raise HTTPException(422, f"size must be one of {PAGE_SIZES}")
        if order not in ("stored", "asc", "desc"):
            raise HTTPException(422, "order must be stored, asc or desc")
        if symbol and not SYMBOL_RE.match(symbol):
            raise HTTPException(422, "invalid symbol")
        if t.kind in ("days", "derived"):
            days = cat.days(t)
            if day is None:
                day = days[0] if days else None
            elif not DATE_RE.match(day):
                raise HTTPException(422, "day must be YYYY-MM-DD")
        else:
            day = None
        t0 = time.perf_counter()
        sb = Sandbox(cat)
        if t.kind == "frame":
            df = cat.frame(t)
            if df.empty:
                return {"columns": [], "rows": [], "total": 0, "page": 1, "pages": 0, "day": day, "elapsed_ms": 0}
            sb.con.register("_frame", df)
            src = "_frame"
        else:
            src = cat.source_sql(t, day)
            if src is None:
                return {"columns": [], "rows": [], "total": 0, "page": 1, "pages": 0, "day": day, "elapsed_ms": 0}
        where, params = [], []
        cols = [c[0] for c in sb.con.execute(f"DESCRIBE SELECT * FROM {src}").fetchall()]
        if symbol:
            if "symbol" not in cols:
                raise HTTPException(422, "this table has no symbol column")
            where.append("symbol = ?"); params.append(symbol)
        star = "* EXCLUDE (date)" if t.kind == "days" and "date" in cols else "*"   # the day is already chosen above
        sql = f"SELECT {star} FROM {src}" + (f" WHERE {' AND '.join(where)}" if where else "")
        tc = t.time_col if t.time_col in cols else None
        if order != "stored" and tc:
            sql += f' ORDER BY "{tc}" {"DESC" if order == "desc" else "ASC"}'
        # total: from the Parquet footers when nothing is filtered (reads no data), else a count of the filter
        if not where and t.kind in ("days", "file", "years"):
            total = cat.file_rows(cat.files(t, day))
        else:
            total = int(sb.run(f"SELECT count(*) AS n FROM ({sql})", params)["n"].iloc[0])
        pages = max(1, -(-total // size))
        page = min(page, pages)
        df = sb.run(f"SELECT * FROM ({sql}) LIMIT {size} OFFSET {(page - 1) * size}", params)
        return JSONResponse({"columns": list(df.columns), "rows": _rows(df), "total": total, "page": page, "pages": pages,
                             "size": size, "day": day, "elapsed_ms": round((time.perf_counter() - t0) * 1000)}, headers=noindex)

    # -- read-only SQL ----------------------------------------------------------
    @r.get("/api/schema")
    def schema():
        return JSONResponse(Sandbox(cat).views, headers=noindex)

    @r.post("/api/query", dependencies=[Depends(same_origin)])
    async def query(request: Request):
        body = await request.json()
        sql = validate_select(str(body.get("sql", "")))
        page = int(body.get("page", 1) or 1)
        size = int(body.get("size", 200) or 200)
        if size not in PAGE_SIZES or not 1 <= page <= 1_000_000:
            raise HTTPException(422, "bad page or size")
        t0 = time.perf_counter()
        sb = Sandbox(cat)
        try:
            df = sb.run(f"SELECT * FROM ({sql}) AS q LIMIT {size + 1} OFFSET {(page - 1) * size}")
        except HTTPException:
            raise
        except duckdb.Error as exc:
            raise HTTPException(422, str(exc).splitlines()[0][:400])
        more = len(df) > size
        ms = round((time.perf_counter() - t0) * 1000)
        log.info("admin query by %s (%d ms, page %d): %s", user, ms, page, " ".join(sql.split())[:500])
        return JSONResponse({"columns": list(df.columns), "rows": _rows(df.head(size)), "page": page, "size": size,
                             "has_more": more, "elapsed_ms": ms}, headers=noindex)

    # -- subscription list --------------------------------------------------------
    def today_ticks() -> tuple[str | None, pd.DataFrame]:
        days = cat.days(BY_KEY[("market", "ticks")])
        if not days:
            return None, pd.DataFrame(columns=["symbol", "ticks", "first_tick", "last_tick", "last_price"])
        d = days[0]
        df = Sandbox(cat).run(f"""
            SELECT symbol, count(*)::BIGINT AS ticks, min(ts) AS first_tick, max(ts) AS last_tick, arg_max(ltp, ts) AS last_price
            FROM read_parquet({cat._q(cat.lake / 'ticks' / f'date={d}' / '*.parquet')}, union_by_name=true) GROUP BY symbol""")
        return d, df

    @r.get("/api/universe")
    def universe_snapshot():
        current = fmp_client.read_universe("IN")
        eq = fo.load_eq_table(cat.lake)
        meta = {r.symbol: r for r in eq.itertuples()} if len(eq) else {}
        day, ticks = today_ticks()
        tk = {r.symbol: r for r in ticks.itertuples()}
        rows = []
        for r in current:
            t = r["symbol"][:-3] if r["symbol"].endswith(".NS") else r["symbol"]
            m, k = meta.get(t), tk.get(r["symbol"])
            isin = r.get("isin", "")
            rows.append({"symbol": r["symbol"], "ticker": t, "name": r.get("name") or (m.name if m else ""), "isin": isin,
                         "instrument_key": f"NSE_EQ|{isin}" if uni.ISIN_RE.match(isin) else None,
                         "listed": bool(m) if meta else None, "tick_size": m.tick_size if m else None,
                         "ticks": int(k.ticks) if k else 0, "first_tick": _safe(k.first_tick) if k else None,
                         "last_tick": _safe(k.last_tick) if k else None, "last_price": _safe(k.last_price) if k else None})
        fo_sel = cat.frame(BY_KEY[("reference", "fo_selected")])
        fo_rows = []
        if len(fo_sel):
            grp = fo_sel.groupby(["underlying", "instrument_type"]).size().unstack(fill_value=0)
            exp = fo_sel.groupby("underlying")["expiry"].agg(lambda s: ", ".join(sorted(set(map(str, s)))[:4]))
            fo_ticks = ticks[ticks["symbol"].isin(set(fo_sel["symbol"]))] if len(ticks) else ticks
            fo_ticks = fo_ticks.merge(fo_sel[["symbol", "underlying"]], on="symbol") if len(fo_ticks) else fo_ticks
            for u in grp.index:
                g = fo_ticks[fo_ticks["underlying"] == u] if len(fo_ticks) else fo_ticks
                fo_rows.append({"underlying": u, "futures": int(grp.loc[u].get("FUT", 0)), "calls": int(grp.loc[u].get("CE", 0)),
                                "puts": int(grp.loc[u].get("PE", 0)), "expiries": exp.get(u, ""),
                                "ticking": int(len(g)), "ticks": int(g["ticks"].sum()) if len(g) else 0})
        watch = cat.frame(BY_KEY[("reference", "fo_watchlist")])
        return JSONResponse(_jsonable({
            "source": fmp_client.active_universe_csv().name, "eq_table": bool(meta), "eq_listings": len(eq),
            "tick_day": day, "equities": rows, "fo": fo_rows, "watchlist": watch.to_dict("records") if len(watch) else [],
            "history": uni.history(data_dir / "universe_history"),
        }), headers=noindex)

    @r.post("/api/universe/preview", dependencies=[Depends(same_origin)])
    async def universe_preview(request: Request):
        body = await request.json()
        return JSONResponse(_jsonable(_plan(body, cat)), headers=noindex)

    @r.post("/api/universe/apply", dependencies=[Depends(same_origin)])
    async def universe_apply(request: Request):
        body = await request.json()
        plan = _plan(body, cat)                       # mapped again here: the browser's preview is never trusted
        if not plan["rows"]:
            raise HTTPException(422, "none of the tickers could be mapped; nothing changed")
        res = uni.write_active_universe(plan["india"], data_dir / "universe_history")
        log.warning("admin %s set the India universe: %d instruments (+%d / -%d); backup %s",
                    user, len(plan["india"]), len(plan["added"]), len(plan["removed"]), res["backup"])
        return JSONResponse(_jsonable({**res, "added": plan["added"], "removed": plan["removed"],
                                       "skipped": [u["ticker"] for u in plan["unknown"]] + plan["invalid"]}), headers=noindex)

    @r.post("/api/universe/restore", dependencies=[Depends(same_origin)])
    async def universe_restore(request: Request):
        body = await request.json()
        try:
            res = uni.restore(str(body.get("name", "")), data_dir / "universe_history")
        except ValueError as exc:
            raise HTTPException(404, str(exc))
        log.warning("admin %s restored universe version %s", user, body.get("name"))
        return JSONResponse(_jsonable(res), headers=noindex)

    return r


def _plan(body: dict, cat: Catalog) -> dict:
    tickers = body.get("tickers")
    if isinstance(tickers, str):
        tickers = uni.parse_ticker_text(tickers)
    if not isinstance(tickers, list) or not tickers:
        raise HTTPException(422, "send a list of tickers")
    tickers = [uni.normalise_ticker(str(t)) for t in tickers][: uni.MAX_TICKERS + 1]
    tickers = list(dict.fromkeys(t for t in tickers if t))
    mode = body.get("mode", "replace")
    if mode not in ("replace", "add"):
        raise HTTPException(422, "mode must be replace or add")
    current = fmp_client.read_universe(None)
    eq = fo.load_eq_table(cat.lake)
    m = uni.map_tickers(tickers, eq, current)
    plan = uni.plan_india(m["rows"], current, mode)
    return {**m, **plan, "mode": mode, "eq_table": len(eq) > 0, "requested": len(tickers)}


def _jsonable(obj):
    if isinstance(obj, dict):
        return {k: _jsonable(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_jsonable(v) for v in obj]
    return _safe(obj)
