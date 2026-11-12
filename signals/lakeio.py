"""
Read the raw inputs of a Context from a lake directory or a research snapshot (same layout; never written to).

  load_raw(lake_dir, underlying, d0, d1, option_from=None, ticks=True)

  index_1m, vix_1m  lake/bars/interval=1m           NSE_INDEX:Nifty 50 (or Nifty Bank) and NSE_INDEX:India VIX, every day d0..d1
  fut_1m            lake/bars/interval=1m           <U>_FUT_* (expired futures from the backfill, current ones from the candle job)
  opt_1m            lake/bars/interval=1m           <U>_<strike>_CE|PE_* from `option_from` (defaults to d0) to d1
  contracts         lake/instruments/fo_expired.parquet and fo.parquet
  ticks_fo          lake/ticks/segment=fo/date=*    option and future ticks of U (live, from 2026-11-03)
  participant       lake/positioning/participant_oi.parquet, known the evening it is published (PARTICIPANT_KNOWN_UTC)
  events            data/reference/market_events.csv
Only the day folders in range are opened.  Minute bars keep ts (naive UTC bar start), symbol, open, high, low, close, volume, oi.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Optional

import duckdb
import pandas as pd

from signals.options.core import INDEX_SYMBOL, VIX_SYMBOL

PARTICIPANT_KNOWN_UTC = pd.Timedelta(hours=15)            # 20:30 IST: NSE posts the file in the evening; used from the next session
TICK_COLUMNS = ("ts", "fts", "symbol", "ltp", "vol_today", "bid", "ask", "bid_qty", "ask_qty", "oi", "iv", "delta", "gamma")


def _day_files(base: Path, d0: date, d1: date) -> list[str]:
    out = []
    if not base.is_dir():
        return out
    for folder in sorted(base.glob("date=*")):
        try:
            d = date.fromisoformat(folder.name.split("=", 1)[1])
        except ValueError:
            continue
        if d0 <= d <= d1:
            out += [str(f) for f in sorted(folder.glob("*.parquet"))]
    return out


def _q(files: list[str]) -> str:
    return "[" + ", ".join("'" + f.replace("'", "''") + "'" for f in files) + "]"


def read_bars(lake_dir: Path, d0: date, d1: date, where: str, params: Optional[list] = None, con=None) -> pd.DataFrame:
    files = _day_files(Path(lake_dir) / "bars" / "interval=1m", d0, d1)
    if not files:
        return pd.DataFrame(columns=["ts", "symbol", "open", "high", "low", "close", "volume", "oi"])
    con = con or duckdb.connect()
    cols = {r[0] for r in con.execute(f"DESCRIBE SELECT * FROM read_parquet({_q(files)}, union_by_name=true)").fetchall()}
    oi = "oi" if "oi" in cols else "CAST(NULL AS BIGINT) AS oi"
    sql = (f"SELECT ts, symbol, open, high, low, close, volume, {oi} FROM read_parquet({_q(files)}, union_by_name=true) WHERE {where} "
           "QUALIFY row_number() OVER (PARTITION BY symbol, ts ORDER BY ts) = 1 ORDER BY symbol, ts")
    return con.execute(sql, params or []).df()


def read_ticks(lake_dir: Path, d0: date, d1: date, where: str, params: Optional[list] = None, con=None) -> pd.DataFrame:
    base = Path(lake_dir) / "ticks"
    files = _day_files(base / "segment=fo", d0, d1) + _day_files(base, d0, d1)
    if not files:
        return pd.DataFrame(columns=list(TICK_COLUMNS))
    con = con or duckdb.connect()
    cols = {r[0] for r in con.execute(f"DESCRIBE SELECT * FROM read_parquet({_q(files)}, union_by_name=true)").fetchall()}
    sel = ", ".join(c if c in cols else f"NULL AS {c}" for c in TICK_COLUMNS)
    return con.execute(f"SELECT {sel} FROM read_parquet({_q(files)}, union_by_name=true) WHERE {where} ORDER BY fts NULLS LAST, ts",
                       params or []).df()


def read_contracts(lake_dir: Path, underlying: str) -> pd.DataFrame:
    parts = []
    for name in ("fo_expired.parquet", "fo.parquet"):
        p = Path(lake_dir) / "instruments" / name
        if p.exists():
            df = pd.read_parquet(p)
            keep = [c for c in ("symbol", "underlying", "instrument_type", "strike", "expiry", "lot_size") if c in df.columns]
            parts.append(df[keep])
    if not parts:
        return pd.DataFrame(columns=["symbol", "underlying", "instrument_type", "strike", "expiry", "lot_size"])
    c = pd.concat(parts, ignore_index=True)
    c = c[c["underlying"] == underlying].drop_duplicates("symbol", keep="last")
    c["expiry"] = pd.to_datetime(c["expiry"]).dt.date
    return c


def read_participant(lake_dir: Path) -> pd.DataFrame:
    p = Path(lake_dir) / "positioning" / "participant_oi.parquet"
    if not p.exists():
        return pd.DataFrame()
    df = pd.read_parquet(p)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["known_at"] = pd.to_datetime(df["date"]) + PARTICIPANT_KNOWN_UTC
    return df


def read_events() -> pd.DataFrame:
    try:
        from data_layer import market_events
        ev = market_events.load()
    except Exception:                                                     # noqa: BLE001 -- events are optional context
        return pd.DataFrame(columns=["date", "kind", "time_ist", "description"])
    return pd.DataFrame([{"date": e.day, "kind": e.kind, "time_ist": e.time_ist.strftime("%H:%M") if e.time_ist else "",
                          "description": e.description} for e in ev])


def load_raw(lake_dir: Path, underlying: str, d0: date, d1: date, option_from: Optional[date] = None, ticks: bool = True) -> dict:
    con = duckdb.connect()
    idx = INDEX_SYMBOL[underlying]
    index_1m = read_bars(lake_dir, d0, d1, "symbol = ?", [idx], con)
    vix_1m = read_bars(lake_dir, d0, d1, "symbol = ?", [VIX_SYMBOL], con)
    fut_1m = read_bars(lake_dir, d0, d1, "regexp_matches(symbol, ?)", [f"^{underlying}_FUT_"], con)
    opt_1m = read_bars(lake_dir, option_from or d0, d1, "regexp_matches(symbol, ?)", [f"^{underlying}_[0-9.]+_(CE|PE)_"], con)
    raw = {"index_1m": index_1m, "vix_1m": vix_1m, "fut_1m": fut_1m, "opt_1m": opt_1m, "contracts": read_contracts(lake_dir, underlying),
           "participant": read_participant(lake_dir), "events": read_events()}
    if ticks:
        t = read_ticks(lake_dir, option_from or d0, d1, "regexp_matches(symbol, ?)", [f"^{underlying}_([0-9.]+_(CE|PE)|FUT)_"], con)
        for c in ("ts", "fts"):
            if c in t.columns:
                t[c] = pd.to_datetime(t[c])
        if len(t) and t["fts"].isna().all():
            t["fts"] = t["ts"]
        raw["ticks_fo"] = t
    for k in ("index_1m", "vix_1m", "fut_1m", "opt_1m"):
        raw[k]["ts"] = pd.to_datetime(raw[k]["ts"])
    return raw
