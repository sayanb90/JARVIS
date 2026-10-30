"""
Intraday microstructure measures on 1-second (or coarser) bars with an L1 book snapshot.

All functions are pure pandas: no I/O, no lake access.  Input frames have the bar columns
ts, open, high, low, close, volume, vwap, n_ticks, bid, ask, bid_qty, ask_qty (UTC timestamps).

Definitions (so charts and tests agree):
  mid          (bid + ask) / 2
  spread       ask - bid                      in price units
  spread_bps   spread / mid * 10,000          in basis points
  imbalance    (bid_qty - ask_qty) / (bid_qty + ask_qty), in [-1, 1]; +1 means all the size is on the bid
  realised volatility
               sqrt(sum of squared log returns between consecutive bar closes, scaled to a trading year)
               Over a window of W seconds: sqrt(sum(r^2) / W * SESSION_SECONDS * TRADING_DAYS).
               Seconds with no bar contribute nothing, which is right: the price did not move.
               Caveat: at 1-second sampling the bid-ask bounce inflates this versus a 1-minute estimate.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

SESSION_SECONDS = 22_500        # 09:15-15:30 IST
TRADING_DAYS = 250
IST_OFFSET = pd.Timedelta(hours=5, minutes=30)
SESSION_OPEN_OFFSET = pd.Timedelta(hours=3, minutes=45)       # 09:15 IST as a time of day in UTC


def add_book_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """Add mid, spread, spread_bps, imbalance.  Crossed, locked or missing books give NaN, never a wrong number."""
    out = df.copy()
    for c in ("bid", "ask", "bid_qty", "ask_qty"):          # tolerate None / object columns from loosely typed sources
        out[c] = pd.to_numeric(out[c], errors="coerce")
    ok = (out["bid"] > 0) & (out["ask"] > 0) & (out["ask"] >= out["bid"])
    out["mid"] = ((out["bid"] + out["ask"]) / 2).where(ok)
    out["spread"] = (out["ask"] - out["bid"]).where(ok)
    out["spread_bps"] = (out["spread"] / out["mid"] * 1e4).where(ok)
    size = (out["bid_qty"].astype("float64") + out["ask_qty"].astype("float64"))
    out["imbalance"] = ((out["bid_qty"].astype("float64") - out["ask_qty"].astype("float64")) / size).where(ok & (size > 0))
    return out


def log_returns(close: pd.Series) -> pd.Series:
    """Log return between consecutive bar closes (first value NaN)."""
    c = close.astype("float64")
    valid = (c > 0) & (c.shift(1) > 0)
    return np.log((c / c.shift(1)).where(valid))


def rolling_realised_vol(df: pd.DataFrame, window_s: int) -> pd.Series:
    """
    Annualised realised volatility (a fraction, 0.25 = 25%) over the trailing `window_s` seconds, per bar.

    Early in the session the window is not full, so the sum of squared returns is scaled by the time actually
    elapsed since the 09:15 IST open (not the full window), and values are NaN for the first
    min(window_s, 60) seconds, where an estimate from a handful of returns would be meaningless.
    """
    if window_s <= 0:
        raise ValueError("window_s must be positive")
    ts = pd.DatetimeIndex(df["ts"])
    r2 = (log_returns(df["close"]) ** 2).fillna(0.0)
    rv = pd.Series(r2.values, index=ts).rolling(f"{int(window_s)}s", min_periods=2).sum()
    elapsed = (ts - (ts.normalize() + SESSION_OPEN_OFFSET)).total_seconds().to_numpy(dtype="float64")
    denom = np.clip(elapsed, 1.0, float(window_s))
    vol = np.sqrt(rv.to_numpy() / denom * SESSION_SECONDS * TRADING_DAYS)
    vol = np.where(elapsed < min(window_s, 60), np.nan, vol)
    return pd.Series(vol, index=df.index)


def rolling_mean(df: pd.DataFrame, col: str, window_s: int) -> pd.Series:
    """Trailing time-window mean of `col` per bar (NaNs ignored); used to make 1-second L1 series readable."""
    if window_s <= 0:
        raise ValueError("window_s must be positive")
    s = pd.Series(df[col].astype("float64").values, index=pd.DatetimeIndex(df["ts"]))
    return pd.Series(s.rolling(f"{int(window_s)}s", min_periods=1).mean().values, index=df.index)


def day_realised_vol(df: pd.DataFrame) -> float:
    """Annualised realised volatility over the whole session from the sum of squared returns."""
    r = log_returns(df["close"]).dropna()
    if r.empty:
        return float("nan")
    return float(np.sqrt((r ** 2).sum() * TRADING_DAYS))


def _oi_summary(d: pd.DataFrame) -> dict:
    """Open interest at the first and last bar that carries it (futures and options only; empty for cash equities)."""
    if "oi" not in d.columns:
        return {}
    oi = pd.to_numeric(d["oi"], errors="coerce").dropna()
    if oi.empty:
        return {}
    return {"oi_last": int(oi.iloc[-1]), "oi_first": int(oi.iloc[0]), "oi_change": int(oi.iloc[-1] - oi.iloc[0])}


def day_summary(df: pd.DataFrame) -> dict:
    """One-row summary of a day for one instrument (values are plain floats/ints/None, JSON-safe)."""
    if df.empty:
        return {}
    d = add_book_metrics(df).sort_values("ts")
    vol = d["volume"].fillna(0).astype("float64")
    has_vwap = d["vwap"].notna() & (vol > 0)
    if has_vwap.any():
        vwap = float((d.loc[has_vwap, "vwap"] * vol[has_vwap]).sum() / vol[has_vwap].sum())
    elif vol.sum() > 0:
        vwap = float((d["close"] * vol).sum() / vol.sum())
    else:
        vwap = None
    sp = d["spread_bps"].dropna()
    book = d[["bid", "ask"]].notna().all(axis=1)
    crossed = ((d["ask"] < d["bid"]) & book).sum()

    def num(x):
        return None if x is None or (isinstance(x, float) and np.isnan(x)) else float(x)

    rv = day_realised_vol(d)
    return {
        "bars": int(len(d)),
        "open": num(d["open"].iloc[0]), "high": num(d["high"].max()), "low": num(d["low"].min()),
        "close": num(d["close"].iloc[-1]),
        "return_pct": num((d["close"].iloc[-1] / d["open"].iloc[0] - 1) * 100),
        "volume": int(vol.sum()), "vwap": vwap,
        "avg_spread_bps": num(sp.mean()) if len(sp) else None,
        "median_spread_bps": num(sp.median()) if len(sp) else None,
        "avg_bid_qty": num(d["bid_qty"].astype("float64").mean()),
        "avg_ask_qty": num(d["ask_qty"].astype("float64").mean()),
        "book_coverage_pct": num(book.mean() * 100),
        "crossed_books": int(crossed),
        "realised_vol_pct": None if np.isnan(rv) else rv * 100,
        **_oi_summary(d),
    }


def to_ist_strings(ts: pd.Series) -> list[str]:
    """UTC timestamps -> exchange wall-clock strings ('YYYY-MM-DD HH:MM:SS') for chart axes."""
    return (pd.to_datetime(ts) + IST_OFFSET).dt.strftime("%Y-%m-%d %H:%M:%S").tolist()


def series_payload(df: pd.DataFrame, vol_window_s: int = 300) -> dict:
    """Column-oriented chart payload for one instrument/day.  NaN becomes null."""
    d = add_book_metrics(df.sort_values("ts").reset_index(drop=True))
    d["cum_volume"] = d["volume"].fillna(0).astype("float64").cumsum()
    d["vol_ann"] = rolling_realised_vol(d, vol_window_s) * 100
    for c in ("bid_qty", "ask_qty", "imbalance", "spread_bps"):          # smoothed over the same window as the volatility
        d[c + "_avg"] = rolling_mean(d, c, vol_window_s)
    cols = ["open", "high", "low", "close", "volume", "cum_volume", "vwap", "n_ticks", "bid", "ask",
            "bid_qty", "ask_qty", "mid", "spread", "spread_bps", "imbalance", "vol_ann",
            "bid_qty_avg", "ask_qty_avg", "imbalance_avg", "spread_bps_avg"]
    if "oi" in d.columns and pd.to_numeric(d["oi"], errors="coerce").notna().any():
        d["oi"] = pd.to_numeric(d["oi"], errors="coerce").ffill()           # OI only changes occasionally; carry it forward
        cols.append("oi")
    payload = {"ts": to_ist_strings(d["ts"])}
    for c in cols:
        col = d[c].astype("float64").round(6)
        payload[c] = [None if np.isnan(v) else float(v) for v in col]
    return payload
