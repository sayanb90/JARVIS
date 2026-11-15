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
  rates             lake/reference/rates.parquet (SCRUM-186): MIBOR or repo, used by the option chain from the next session
  weights           lake/reference/index_weights.parquet (SCRUM-183): dated constituent weights of U
  stocks_1m         lake/bars/interval=1m           the TOP_N_STOCKS heaviest constituents of U, for the basket signals (SCRUM-191)
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
TICK_COLUMNS = ("ts", "fts", "symbol", "ltp", "vol_today", "ltq", "bid", "ask", "bid_qty", "ask_qty", "oi", "iv", "delta", "gamma",
                *(f"{s}{i}{q}" for s in ("bid", "ask") for i in range(2, 6) for q in ("", "_qty")))
INDEX_NAMES = {"NIFTY": ("NIFTY50", "NIFTY"), "BANKNIFTY": ("NIFTYBANK", "BANKNIFTY"),
               "FINNIFTY": ("NIFTYFIN", "NIFTYFINANCIALSERVICES", "FINNIFTY")}
TOP_N_STOCKS = 15                                        # constituents by weight whose minutes are loaded for the basket signals


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


def norm_index(name: str) -> str:
    return "".join(ch for ch in str(name).upper() if ch.isalnum())


def blend_weights(w: pd.DataFrame) -> pd.DataFrame:
    """One weight set per snapshot date from the collector's sources (SCRUM-183): the official factsheet top ten where published (latest
    on or before the date), the approximate market-cap weights for the other members rescaled to fill the rest of 100 %.  A table with
    a plain `weight` column and no `source` (tests, older files) is returned as is."""
    if "source" not in w.columns:
        return w
    w = w[w["source"].isin(["approx_mcap", "factsheet_top10"]) & w["weight"].notna()]
    out = []
    for d in sorted(w["date"].unique()):
        approx = w[(w["source"] == "approx_mcap") & (w["date"] <= d)]
        top = w[(w["source"] == "factsheet_top10") & (w["date"] <= d)]
        a = approx[approx["date"] == approx["date"].max()] if len(approx) else approx
        t = top[top["date"] == top["date"].max()] if len(top) else top
        if a.empty and t.empty:
            continue
        official = dict(zip(t["symbol"], t["weight"]))
        rest = a[~a["symbol"].isin(official)]
        room = max(0.0, 1.0 - sum(official.values()))
        scale = room / rest["weight"].sum() if len(rest) and rest["weight"].sum() > 0 else 0.0
        rows = [(s_, wt) for s_, wt in official.items()] + [(s_, wt * scale) for s_, wt in zip(rest["symbol"], rest["weight"])]
        out.append(pd.DataFrame(rows, columns=["symbol", "weight"]).assign(date=d))
    return pd.concat(out, ignore_index=True) if out else pd.DataFrame(columns=["date", "symbol", "weight"])


def read_weights(lake_dir: Path, underlying: str) -> pd.DataFrame:
    """Dated index weights of `underlying` (lake/reference/index_weights.parquet, SCRUM-183): date, symbol (lake symbol, e.g. HDFCBANK.NS),
    weight (fraction), known_at (that day's close).  Empty when the collector has not run."""
    p = Path(lake_dir) / "reference" / "index_weights.parquet"
    if not p.exists():
        return pd.DataFrame(columns=["date", "symbol", "weight", "known_at"])
    w = pd.read_parquet(p)
    names = set(INDEX_NAMES.get(underlying, (underlying,)))
    w = w[w["index"].map(norm_index).isin(names)].copy()
    col = "weight_pct" if "weight_pct" in w.columns else "weight"
    w["weight"] = pd.to_numeric(w[col], errors="coerce") / (100.0 if col == "weight_pct" else 1.0)
    w["symbol"] = w["symbol"].astype(str).map(lambda x: x if "." in x else f"{x}.NS")
    w["date"] = pd.to_datetime(w["date"]).dt.date
    w = blend_weights(w)
    w["known_at"] = pd.to_datetime(w["date"]) + pd.Timedelta(hours=10)
    return w[["date", "symbol", "weight", "known_at"]]


def read_rates(lake_dir: Path) -> pd.DataFrame:
    """lake/reference/rates.parquet (SCRUM-186): date, series, value_pct.  Empty when the collector has not run."""
    p = Path(lake_dir) / "reference" / "rates.parquet"
    if not p.exists():
        return pd.DataFrame(columns=["date", "series", "value_pct"])
    return pd.read_parquet(p, columns=["date", "series", "value_pct"])


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
    weights = read_weights(lake_dir, underlying)
    stocks_1m = pd.DataFrame(columns=["ts", "symbol", "open", "high", "low", "close", "volume"])
    if len(weights):
        top = weights.sort_values("weight", ascending=False).groupby("date").head(TOP_N_STOCKS)
        syms = sorted(set(top["symbol"]))
        stocks_1m = read_bars(lake_dir, d0, d1, "list_contains(?, symbol)", [syms], con)
    raw = {"index_1m": index_1m, "vix_1m": vix_1m, "fut_1m": fut_1m, "opt_1m": opt_1m, "contracts": read_contracts(lake_dir, underlying),
           "participant": read_participant(lake_dir), "events": read_events(), "weights": weights, "stocks_1m": stocks_1m,
           "rates": read_rates(lake_dir)}
    if ticks:
        t = read_ticks(lake_dir, option_from or d0, d1, "regexp_matches(symbol, ?)", [f"^{underlying}_([0-9.]+_(CE|PE)|FUT)_"], con)
        for c in ("ts", "fts"):
            if c in t.columns:
                t[c] = pd.to_datetime(t[c])
        if len(t) and t["fts"].isna().all():
            t["fts"] = t["ts"]
        raw["ticks_fo"] = t
    for k in ("index_1m", "vix_1m", "fut_1m", "opt_1m", "stocks_1m"):
        raw[k]["ts"] = pd.to_datetime(raw[k]["ts"])
    return raw
