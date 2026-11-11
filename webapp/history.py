"""
Prices and volume per instrument, from the lake's bars: the viewer's History tab.

  daily candles   `lake/ohlc` (Yahoo, adjusted by Yahoo for splits and dividends) for every stock; for the Indian indices, which Yahoo does not
                  carry, the days are rolled up from the 1-minute bars
  intraday        `lake/bars/interval=1m` (Upstox for India, Yahoo for the other markets), rolled up to 1, 5, 15 or 60 minutes on the day's own
                  open, so a 60-minute bar starts at the session open, not at the top of the hour

Read-only.  Every query is parameterised (a symbol never reaches the SQL text) and reads only the day folders in the requested range, with a
memory cap.  Indices carry no traded volume (the feed sends zero), which the payload says.  Times of Indian instruments are shown in IST,
the others in UTC (the payload names the zone).
"""

from __future__ import annotations

import re
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

import duckdb
import numpy as np
import pandas as pd

INSTRUMENT_RE = re.compile(r"^[A-Za-z0-9&._: -]{1,48}$")
INDEX_PREFIX = "NSE_INDEX:"
TFS = ("1d", "1m", "5m", "15m", "60m")
MINUTES = {"1m": 1, "5m": 5, "15m": 15, "60m": 60}
MAX_SPAN_DAYS = {"1m": 30, "5m": 90, "15m": 180, "60m": 730}           # a longer request is clipped to this many calendar days
DEFAULT_SPAN_DAYS = {"1d": 365, "1m": 5, "5m": 30, "15m": 90, "60m": 365}
MAX_DAILY_ROWS = 8000
SORTS = ("volume", "rvol", "change", "value")
IST = timedelta(hours=5, minutes=30)
DB_MEMORY = "1GB"


class HistoryError(ValueError):
    """A request the lake cannot answer (the message is for the person at the page)."""


def _con() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute(f"SET memory_limit='{DB_MEMORY}'")
    con.execute("SET threads=2")
    return con


def _ohlc_glob(lake: Path) -> Optional[str]:
    return f"{lake}/ohlc/year=*/ohlc.parquet" if any((lake / "ohlc").glob("year=*/ohlc.parquet")) else None


def _bar_days(lake: Path) -> list[date]:
    base = lake / "bars" / "interval=1m"
    if not base.is_dir():
        return []
    out = []
    for f in base.glob("date=*"):
        try:
            if f.is_dir() and any(f.glob("*.parquet")):
                out.append(date.fromisoformat(f.name[5:]))
        except ValueError:
            continue
    return sorted(out)


def _files(lake: Path, start: date, end: date, days: list[date]) -> list[str]:
    out = []
    for d in days:
        if start <= d <= end:
            out += sorted(str(p) for p in (lake / "bars" / "interval=1m" / f"date={d}").glob("*.parquet"))
    return out


def _check_symbol(symbol: str) -> str:
    if not INSTRUMENT_RE.match(symbol or ""):
        raise HistoryError("invalid instrument name")
    return symbol


def _names() -> dict[str, tuple[str, str]]:
    """{symbol: (name, sector)} from the universe files; best effort (a missing file just means no names)."""
    try:
        from data_layer.fmp_client import read_universe
        return {r["symbol"]: (r.get("name", "") or "", r.get("sector", "") or "") for r in read_universe(None)}
    except Exception:                                              # noqa: BLE001
        return {}


# ---------------------------------------------------------------------------------------------- the instrument list
def instruments(lake: Path) -> list[dict]:
    """Every instrument the History tab can chart: stocks with daily candles, and stocks and indices with 1-minute bars."""
    lake = Path(lake)
    found: dict[str, dict] = {}
    con = _con()
    glob = _ohlc_glob(lake)
    if glob:
        for sym, mk, a, b, n in con.execute(f"SELECT symbol, market, min(date), max(date), count(*) FROM read_parquet('{glob}', hive_partitioning=false) "
                                            f"GROUP BY 1, 2").fetchall():
            found[sym] = {"symbol": sym, "market": mk, "daily_first": str(a)[:10], "daily_last": str(b)[:10], "daily_days": int(n), "minute": False}
    days = _bar_days(lake)
    seen_markets: set[str] = set()
    for d in reversed(days[-15:]):                                  # the symbols with 1-minute bars: newest day folders, until every market is seen
        rows = con.execute(f"SELECT DISTINCT symbol, market FROM read_parquet('{lake}/bars/interval=1m/date={d}/*.parquet', union_by_name=true)").fetchall()
        new_markets = {m for _, m in rows} - seen_markets
        for sym, mk in rows:
            if mk in new_markets or sym in found:
                rec = found.setdefault(sym, {"symbol": sym, "market": mk, "daily_first": None, "daily_last": None, "daily_days": 0})
                rec["minute"] = True
        seen_markets |= new_markets
    names = _names()
    out = []
    for sym, rec in found.items():
        rec.setdefault("minute", False)
        name, sector = names.get(sym, ("", ""))
        if sym.startswith(INDEX_PREFIX):
            name, sector = sym[len(INDEX_PREFIX):], "Index"
        kind = "index" if sym.startswith(INDEX_PREFIX) else "stock"
        out.append({**rec, "name": name, "sector": sector, "kind": kind, "group": "Indices" if kind == "index" else (sector or "Other")})
    return sorted(out, key=lambda r: (r["kind"] != "index", r["market"] != "IN", r["market"], r["symbol"]))


# ---------------------------------------------------------------------------------------------- bars for one instrument
def _latest_day_with(con, lake: Path, symbol: str, days: list[date]) -> Optional[date]:
    for d in reversed(days[-15:]):
        if con.execute(f"SELECT 1 FROM read_parquet('{lake}/bars/interval=1m/date={d}/*.parquet', union_by_name=true) WHERE symbol = ? LIMIT 1", [symbol]).fetchone():
            return d
    return None


def _market_of(con, lake: Path, symbol: str) -> Optional[str]:
    glob = _ohlc_glob(lake)
    if glob:
        r = con.execute(f"SELECT market FROM read_parquet('{glob}', hive_partitioning=false) WHERE symbol = ? LIMIT 1", [symbol]).fetchone()
        if r:
            return r[0]
    return "IN" if symbol.startswith(INDEX_PREFIX) else None


def _parse_day(text: Optional[str], what: str) -> Optional[date]:
    if not text:
        return None
    try:
        return date.fromisoformat(text)
    except ValueError:
        raise HistoryError(f"{what} must be YYYY-MM-DD")


def get_bars(lake: Path, symbol: str, tf: str = "1d", start: Optional[str] = None, end: Optional[str] = None, span: Optional[int] = None) -> dict:
    """`span` (calendar days) is the range to show counted back from the last day the instrument has; start and end override it."""
    lake = Path(lake)
    _check_symbol(symbol)
    if tf not in TFS:
        raise HistoryError(f"timeframe must be one of {', '.join(TFS)}")
    s0, e0 = _parse_day(start, "start"), _parse_day(end, "end")
    if s0 and e0 and s0 > e0:
        raise HistoryError("start is after end")
    con = _con()
    is_index = symbol.startswith(INDEX_PREFIX)
    clipped = False
    days = _bar_days(lake)
    if tf == "1d" and not is_index:
        glob = _ohlc_glob(lake)
        if not glob:
            raise HistoryError("no daily candles in the lake yet")
        last = con.execute(f"SELECT max(date) FROM read_parquet('{glob}', hive_partitioning=false) WHERE symbol = ?", [symbol]).fetchone()[0]
        if last is None:
            raise HistoryError(f"no daily candles for {symbol}")
        end_d = e0 or pd.Timestamp(last).date()
        start_d = s0 or end_d - timedelta(days=span or DEFAULT_SPAN_DAYS["1d"])
        df = con.execute(f"SELECT CAST(date AS DATE) AS t, open, high, low, close, volume FROM read_parquet('{glob}', hive_partitioning=false) "
                         f"WHERE symbol = ? AND date BETWEEN ? AND ? ORDER BY date", [symbol, start_d, end_d]).df()
        if len(df) > MAX_DAILY_ROWS:
            df, clipped = df.iloc[-MAX_DAILY_ROWS:], True
        source, adjusted = "ohlc", True
    else:
        if not days:
            raise HistoryError("no 1-minute bars in the lake yet")
        last = _latest_day_with(con, lake, symbol, days) if e0 is None else e0
        if last is None:
            raise HistoryError(f"no 1-minute bars for {symbol}" + (" (indices come from Upstox)" if is_index else ""))
        end_d = e0 or last
        days_back = span or (DEFAULT_SPAN_DAYS[tf] if tf != "1d" else 3650)
        start_d = s0 or end_d - timedelta(days=days_back)
        cap = MAX_SPAN_DAYS.get(tf)
        if cap and (end_d - start_d).days > cap:
            start_d, clipped = end_d - timedelta(days=cap), True
        files = _files(lake, start_d, end_d, days)
        if not files:
            raise HistoryError(f"no 1-minute bars between {start_d} and {end_d}")
        if tf == "1d":                                                # an index: the day's candle from its 1-minute bars
            df = con.execute("SELECT CAST(ts AS DATE) AS t, arg_min(open, ts) AS open, max(high) AS high, min(low) AS low, arg_max(close, ts) AS close, "
                             "sum(volume) AS volume FROM read_parquet(?, union_by_name=true) WHERE symbol = ? GROUP BY 1 ORDER BY 1", [files, symbol]).df()
        else:
            w = MINUTES[tf]
            df = con.execute("WITH b AS (SELECT ts, open, high, low, close, volume, min(ts) OVER (PARTITION BY CAST(ts AS DATE)) AS day_open "
                             "FROM read_parquet(?, union_by_name=true) WHERE symbol = ?) "
                             "SELECT day_open + to_minutes(CAST(floor(date_diff('minute', day_open, ts) / ?) * ? AS BIGINT)) AS t, arg_min(open, ts) AS open, "
                             "max(high) AS high, min(low) AS low, arg_max(close, ts) AS close, sum(volume) AS volume FROM b GROUP BY 1 ORDER BY 1",
                             [files, symbol, w, w]).df()
        source, adjusted = "bars_1m", False
    if df.empty:
        raise HistoryError(f"no data for {symbol} in that range")
    market = _market_of(con, lake, symbol)
    intraday = tf != "1d"
    t = pd.to_datetime(df["t"])
    tz = "UTC"
    if market == "IN" and intraday:
        t, tz = t + IST, "IST"
    missing, session = [], None
    present = set(t.dt.date)
    d = min(present)
    while d <= max(present):
        if d.weekday() < 5 and d not in present:
            missing.append(d.isoformat())
        d += timedelta(days=1)
    if intraday:
        per_day = pd.DataFrame({"d": t.dt.date, "m": t.dt.hour * 60 + t.dt.minute})
        first, lastm = per_day.groupby("d")["m"].min(), per_day.groupby("d")["m"].max()
        session = [int(first.median()), int(lastm.median()) + MINUTES[tf]]
    vol = df["volume"].fillna(0).astype("float64")
    out = {
        "symbol": symbol, "market": market, "tf": tf, "tz": tz, "source": source, "adjusted": adjusted, "clipped": clipped,
        "from": str(t.iloc[0])[:19].replace(" ", "T") if intraday else str(t.iloc[0].date()), "to": str(t.iloc[-1])[:19].replace(" ", "T") if intraday else str(t.iloc[-1].date()),
        "has_volume": bool((vol > 0).any()), "is_index": is_index, "missing_days": missing, "session": session, "bars": int(len(df)),
        "t": [x.strftime("%Y-%m-%d %H:%M") if intraday else x.strftime("%Y-%m-%d") for x in t],
        "open": df["open"].astype("float64").round(6).tolist(), "high": df["high"].astype("float64").round(6).tolist(),
        "low": df["low"].astype("float64").round(6).tolist(), "close": df["close"].astype("float64").round(6).tolist(), "volume": vol.tolist(),
    }
    return out


# ---------------------------------------------------------------------------------------------- volume per ticker
def top_volume(lake: Path, market: Optional[str] = None, day: Optional[str] = None, n: int = 20, sort: str = "volume") -> dict:
    lake = Path(lake)
    if sort not in SORTS:
        raise HistoryError(f"sort must be one of {', '.join(SORTS)}")
    if market and not re.fullmatch(r"[A-Z]{2,3}", market):
        raise HistoryError("invalid market")
    if sort == "value" and not market:
        raise HistoryError("traded value is in each market's own currency: pick one market to rank by it")
    n = max(1, min(int(n), 200))
    glob = _ohlc_glob(lake)
    if not glob:
        raise HistoryError("no daily candles in the lake yet")
    con = _con()
    q = f"FROM read_parquet('{glob}', hive_partitioning=false)"
    mk = " AND market = ?" if market else ""
    mp = [market] if market else []
    latest = con.execute(f"SELECT max(date) {q} WHERE 1 = 1{mk}", mp).fetchone()[0]
    if latest is None:
        raise HistoryError("no daily candles for that market")
    d = _parse_day(day, "date") or pd.Timestamp(latest).date()
    df = con.execute(
        f"WITH w AS (SELECT symbol, market, CAST(date AS DATE) AS d, close, volume, lag(close) OVER (PARTITION BY symbol ORDER BY date) AS prev_close, "
        f"avg(volume) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING) AS avg20 "
        f"{q} WHERE date BETWEEN ? AND ?{mk}) SELECT symbol, market, close, volume, close * volume AS value, "
        f"(close / prev_close - 1) * 100 AS change_pct, volume / NULLIF(avg20, 0) AS rvol, avg20 FROM w WHERE d = ? AND volume > 0",
        [d - timedelta(days=45), d, *mp, d]).df()
    if df.empty:
        raise HistoryError(f"no volume for {d}" + (f" in {market}" if market else ""))
    key = {"volume": "volume", "rvol": "rvol", "change": "change_pct", "value": "value"}[sort]
    df = df.sort_values(key, ascending=False, na_position="last").head(n)
    names = _names()
    rows = [{**{k: (None if (isinstance(v, float) and not np.isfinite(v)) else v) for k, v in r.items()}, "name": names.get(r["symbol"], ("", ""))[0]}
            for r in df.to_dict("records")]
    return {"date": d.isoformat(), "latest": pd.Timestamp(latest).date().isoformat(), "market": market, "sort": sort, "rows": rows,
            "note": "Volume is shares traded that day. Relative volume is the day's volume over the average of the 20 sessions before it."}
