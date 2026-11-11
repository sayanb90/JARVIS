"""
Minute candles and the volume profile built from them, for the viewer's Candles section.

  candles_payload(df)        column-oriented candle series (IST times, running VWAP) for the charts
  candle_summary(df, ...)    the day in one dict: OHLC, volume, VWAP, how many candles were captured out of how many the session holds
  volume_profile(df, bins)   volume traded at each price level, split into up and down candles, with the point of control and value area

How the profile is built: a candle only tells us its high, low and volume, not where inside the range the volume traded.  Each candle's
volume is therefore spread evenly over the price levels its range covers (a candle with no range puts everything on one level).  That is
the usual way to get a profile from candles; with more, shorter candles it approaches the real volume-at-price.  Volume of an up candle
(close at or above open) goes to the "up" side, the rest to "down", so the two stacked bars show who was pushing at each level.

Point of control (POC) = the level with the most volume.  Value area = the levels around the POC that hold `value_area` (70%) of the
day's volume, grown outwards from the POC one level at a time on the side with more volume; VAL and VAH are its lower and upper edges.
"""

from __future__ import annotations

import math

import numpy as np
import pandas as pd

from .microstructure import to_ist_strings

SESSION_MINUTES = 375                    # 09:15 to 15:30 IST
MAX_LEVELS = 400                         # a guard against a mis-priced symbol producing millions of levels


def _clean(df: pd.DataFrame) -> pd.DataFrame:
    d = df.sort_values("ts").reset_index(drop=True)
    for c in ("open", "high", "low", "close", "volume"):
        d[c] = pd.to_numeric(d[c], errors="coerce")
    d["volume"] = d["volume"].fillna(0.0)
    return d.dropna(subset=["open", "high", "low", "close"]).reset_index(drop=True)


def _running_vwap(d: pd.DataFrame) -> pd.Series:
    typical = (d["high"] + d["low"] + d["close"]) / 3
    pv = (typical * d["volume"]).cumsum()
    vol = d["volume"].cumsum()
    return (pv / vol.where(vol > 0)).astype("float64")


def _num(x):
    return None if x is None or (isinstance(x, float) and not math.isfinite(x)) else float(x)


def candles_payload(df: pd.DataFrame) -> dict:
    """Candle series for the charts.  NaN becomes null; `up` marks candles that closed at or above their open."""
    d = _clean(df)
    if d.empty:
        return {"ts": []}
    d["vwap"] = _running_vwap(d)
    out = {"ts": to_ist_strings(d["ts"])}
    for c in ("open", "high", "low", "close", "volume", "vwap"):
        out[c] = [None if np.isnan(v) else float(v) for v in d[c].astype("float64").round(6)]
    out["up"] = (d["close"] >= d["open"]).tolist()
    return out


def candle_summary(df: pd.DataFrame, minutes: int = 1) -> dict:
    d = _clean(df)
    if d.empty:
        return {}
    d["vwap"] = _running_vwap(d)
    expected = max(SESSION_MINUTES // max(int(minutes), 1), 1)
    peak = d.loc[d["volume"].idxmax()]
    return {
        "candles": int(len(d)), "expected": expected, "coverage_pct": _num(min(len(d) / expected, 1.0) * 100),
        "open": _num(d["open"].iloc[0]), "high": _num(d["high"].max()), "low": _num(d["low"].min()), "close": _num(d["close"].iloc[-1]),
        "return_pct": _num((d["close"].iloc[-1] / d["open"].iloc[0] - 1) * 100),
        "volume": int(d["volume"].sum()), "vwap": _num(d["vwap"].iloc[-1]),
        "up_candles": int((d["close"] >= d["open"]).sum()), "down_candles": int((d["close"] < d["open"]).sum()),
        "peak_volume": int(peak["volume"]), "peak_time": to_ist_strings(pd.Series([peak["ts"]]))[0][11:16],
        "first_time": to_ist_strings(d["ts"].iloc[:1])[0][11:16], "last_time": to_ist_strings(d["ts"].iloc[-1:])[0][11:16],
    }


def _nice_step(raw: float, tick: float) -> float:
    """Round a bin width up to 1, 2 or 5 times a power of ten, and never below the tick size."""
    raw = max(raw, tick)
    exp = math.floor(math.log10(raw))
    for m in (1, 2, 5, 10):
        step = m * 10 ** exp
        if step >= raw - 1e-12:
            return max(round(step, 10), tick)
    return raw


def volume_profile(df: pd.DataFrame, bins: int = 40, value_area: float = 0.70, tick: float = 0.05) -> dict:
    """Volume at price for the candles in `df`, about `bins` levels tall.  Empty when there is no volume."""
    d = _clean(df)
    d = d[d["volume"] > 0]
    if d.empty:
        return {}
    lo, hi = float(d["low"].min()), float(d["high"].max())
    step = _nice_step((hi - lo) / max(int(bins), 1), tick)
    start = math.floor(lo / step + 1e-9) * step
    n = max(int(math.ceil((hi - start) / step - 1e-9)), 1)
    while n > MAX_LEVELS:                                                   # a very wide day: coarsen rather than refuse
        step *= 2
        start = math.floor(lo / step + 1e-9) * step
        n = max(int(math.ceil((hi - start) / step - 1e-9)), 1)
    up, down = np.zeros(n), np.zeros(n)
    for o, h, l, c, v in zip(d["open"], d["high"], d["low"], d["close"], d["volume"]):
        side = up if c >= o else down
        b0, b1 = int((l - start) / step + 1e-9), int((h - start) / step - 1e-9)
        b0, b1 = min(max(b0, 0), n - 1), min(max(b1, 0), n - 1)
        if h <= l or b1 <= b0:
            side[b0] += v
            continue
        edges = start + step * np.arange(b0, b1 + 2)
        overlap = np.clip(np.minimum(edges[1:], h) - np.maximum(edges[:-1], l), 0, None)
        total = overlap.sum()
        side[b0:b1 + 1] += v * overlap / total if total > 0 else 0
    vol = up + down
    poc = int(vol.argmax())
    target, got, a, b = value_area * vol.sum(), vol[poc], poc, poc
    while got < target and (a > 0 or b < n - 1):                            # grow the value area towards the heavier neighbour
        below = vol[a - 1] if a > 0 else -1.0
        above = vol[b + 1] if b < n - 1 else -1.0
        if above >= below:
            b += 1; got += vol[b]
        else:
            a -= 1; got += vol[a]
    centres = start + step * (np.arange(n) + 0.5)
    r = lambda arr: [float(x) for x in np.round(arr, 4)]                    # noqa: E731
    return {"step": float(round(step, 6)), "prices": r(centres), "volume": r(vol), "up": r(up), "down": r(down),
            "poc": float(round(centres[poc], 4)), "poc_volume": float(round(vol[poc], 2)),
            "val": float(round(start + step * a, 4)), "vah": float(round(start + step * (b + 1), 4)),
            "value_area_pct": float(round(got / vol.sum() * 100, 2)), "total": float(round(vol.sum(), 2))}
