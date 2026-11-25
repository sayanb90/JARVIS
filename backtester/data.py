"""
What a backtest reads (SCRUM-217): the signals as they were known, minute by minute, and the prices of the contracts it trades.  Nothing is written.

  signal_panel(store, underlying, d0, d1, needs, grid)   one column per (signal, column), as-of joined on known_at to the decision times `grid`
  Prices(lake, underlying, d0, d1)                        index and futures minutes for the range, an option's minutes on demand, the contracts

Where a signal's rows come from, in this order for each day (the first that holds the day wins):
  <store>/signals/<id>/v<n>/intraday/bucket=1m/          the live engine's replay of the tick era (from 2026-11-03)
  <store>/signals/<id>/v<n>/intraday/source=candles/bucket=1m/   snapshot signals at every minute from candles (to 2026-11-02)
  <store>/signals/<id>/v<n>/                            the batch history: 1-minute signals per minute, snapshot signals at 09:20/12:00/15:15, daily
A per-contract signal (rows per symbol) is read at its front future's row.  A value is used from its known_at on, and not longer than `stale`
(15 minutes for an intraday series, 5 days for a daily one), so yesterday's last value never drives this morning's entry.
"""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
from typing import Optional

import duckdb
import pandas as pd

LAYERS = ("intraday/bucket=1m", "intraday/source=candles/bucket=1m", "")
STALE_INTRADAY = pd.Timedelta(minutes=15)
STALE_DAILY = pd.Timedelta(days=5)


def _version_dir(store: Path, sid: str) -> Optional[Path]:
    base = Path(store) / "signals" / sid
    vs = sorted((p for p in base.glob("v*") if p.is_dir() and p.name[1:].isdigit()), key=lambda p: int(p.name[1:]))
    return vs[-1] if vs else None


def _layer_files(vdir: Path, layer: str, underlying: str, d0: date, d1: date) -> dict:
    """{day: file} of one layer."""
    base = (vdir / layer if layer else vdir) / f"underlying={underlying}"
    out = {}
    for folder in base.glob("date=*") if base.is_dir() else []:
        try:
            d = date.fromisoformat(folder.name.split("=", 1)[1])
        except ValueError:
            continue
        f = folder / "part.parquet"
        if d0 <= d <= d1 and f.exists():
            out[d] = f
    return out


def read_signal(store, sid: str, underlying: str, d0: date, d1: date, column: str) -> pd.DataFrame:
    """(known_at, value) of one column of one signal, every row known in [d0, d1], each day from its finest layer."""
    vdir = _version_dir(Path(store), sid)
    if vdir is None:
        return pd.DataFrame(columns=["known_at", "value"])
    chosen: dict = {}
    for layer in LAYERS:
        for d, f in _layer_files(vdir, layer, underlying, d0, d1).items():
            chosen.setdefault(d, f)
    if not chosen:
        return pd.DataFrame(columns=["known_at", "value"])
    parts = []
    for d in sorted(chosen):
        df = pd.read_parquet(chosen[d])
        if column not in df.columns:
            continue
        if "known_at" not in df.columns:                          # older outputs: a bar's value is known at its end, a daily one at the next session
            if "ts" in df.columns:
                df["known_at"] = pd.to_datetime(df["ts"]) + pd.Timedelta(minutes=1)
            else:
                df["known_at"] = pd.to_datetime(df["date"]) + pd.Timedelta(days=1, hours=3, minutes=45)
        if "symbol" in df.columns and df["symbol"].notna().any():  # per contract: the front future's row
            fut = df[df["symbol"].astype(str).str.contains("_FUT_", regex=False)]
            if len(fut):
                order = "expiry" if "expiry" in fut.columns else "symbol"
                df = fut.sort_values(["known_at", order]).drop_duplicates("known_at", keep="first")
            else:
                df = df.groupby("known_at", as_index=False)[column].median()
        parts.append(df[["known_at", column]].rename(columns={column: "value"}))
    if not parts:
        return pd.DataFrame(columns=["known_at", "value"])
    out = pd.concat(parts, ignore_index=True)
    out["known_at"] = pd.to_datetime(out["known_at"]).astype("datetime64[ns]")
    out["value"] = pd.to_numeric(out["value"], errors="coerce")
    return out.dropna(subset=["known_at"]).sort_values("known_at").drop_duplicates("known_at", keep="last").reset_index(drop=True)


def signal_panel(store, underlying: str, d0: date, d1: date, needs: list, grid: pd.Series) -> pd.DataFrame:
    """One row per decision time in `grid` (naive UTC), one column '<signal>.<column>' per need: the newest value known at that time."""
    panel = pd.DataFrame({"t": pd.to_datetime(grid).astype("datetime64[ns]")}).sort_values("t").reset_index(drop=True)
    for sid, col in needs:
        s = read_signal(store, sid, underlying, d0 - timedelta(days=10), d1, col)
        key = f"{sid}.{col}"
        if not len(s):
            panel[key] = float("nan")
            continue
        gaps = s["known_at"].diff().dropna()
        stale = STALE_INTRADAY if len(gaps) and gaps.median() < pd.Timedelta(hours=1) else STALE_DAILY
        m = pd.merge_asof(panel[["t"]], s, left_on="t", right_on="known_at", direction="backward", tolerance=stale)
        panel[key] = m["value"].to_numpy()
    return panel


class Prices:
    """Minute bars of the index (for the strike at the money), the futures (read once for the range) and an option (read for one day when it is traded)."""

    def __init__(self, lake, underlying: str, d0: date, d1: date):
        from signals.lakeio import read_bars, read_contracts
        from signals.options.core import INDEX_SYMBOL
        self.lake, self.underlying = Path(lake), underlying
        self._read_bars = read_bars
        con = duckdb.connect()
        self.index = read_bars(self.lake, d0, d1, "symbol = ?", [INDEX_SYMBOL[underlying]], con)
        self.futures = read_bars(self.lake, d0, d1, "regexp_matches(symbol, ?)", [f"^{underlying}_FUT_"], con)
        con.close()
        for df in (self.index, self.futures):
            df["ts"] = pd.to_datetime(df["ts"]).astype("datetime64[ns]")
        self.contracts = read_contracts(self.lake, underlying)
        self._day_cache: dict = {}

    def days(self) -> list:
        return sorted(set(self.index["ts"].dt.date))

    def bars(self, symbol: str, day: date) -> pd.DataFrame:
        """One contract's minutes of one day, ordered (empty when it did not trade)."""
        key = (symbol, day)
        if key not in self._day_cache:
            if "_FUT_" in symbol:
                df = self.futures[(self.futures["symbol"] == symbol) & (self.futures["ts"].dt.date == day)]
            else:
                df = self._read_bars(self.lake, day, day, "symbol = ?", [symbol])
                df["ts"] = pd.to_datetime(df["ts"]).astype("datetime64[ns]")
            if len(self._day_cache) > 64:
                self._day_cache.clear()
            self._day_cache[key] = df.sort_values("ts").reset_index(drop=True)
        return self._day_cache[key]

    def choose(self, kind: str, day: date, spot: float, right: str = "CE", strike_offset: int = 0, expiry: str = "nearest",
               min_days: int = 0) -> Optional[dict]:
        """The contract a rule trades on `day`: {symbol, expiry, strike, lot_size}, or None.  Options: the strike nearest `spot` in the chosen expiry,
        then `strike_offset` strikes up (+) or down (-)."""
        c = self.contracts
        if not len(c):
            return None
        itype = "FUT" if kind == "future" else right
        c = c[(c["instrument_type"] == itype) & (c["expiry"] >= day + timedelta(days=min_days))]
        if not len(c):
            return None
        exps = sorted(set(c["expiry"]))
        if expiry == "next" and len(exps) > 1:
            e = exps[1]
        elif expiry == "monthly":
            first = exps[0]
            same_month = [x for x in exps if (x.year, x.month) == (first.year, first.month)]
            e = same_month[-1]
        else:
            e = exps[0]
        c = c[c["expiry"] == e].sort_values("strike")
        if kind == "future":
            row = c.iloc[0]
        else:
            strikes = c["strike"].to_numpy()
            i = int(abs(strikes - spot).argmin()) + strike_offset
            if not 0 <= i < len(c):
                return None
            row = c.iloc[i]
        return {"symbol": row["symbol"], "expiry": row["expiry"], "strike": float(row["strike"]) if row["strike"] == row["strike"] else None,
                "lot_size": int(row["lot_size"])}
