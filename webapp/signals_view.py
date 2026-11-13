"""
Live Signals tab (SCRUM-199): the latest bucket of every live signal for one underlying, with its time-of-day z-score where the signal has
one, a short series for a sparkline, and the daily signals from the last close.  Read only: the live table (<store>/live/date=D/) and
the signal store; nothing is written.

  summary(store, underlying, day=None, now=None) -> dict
      cadence           the bucket widths seen ("1min", "10s")
      newest            known_at of the newest row of any signal
      stale             True when the newest row is older than two buckets of the finest width (the runner or the feed stopped)
      signals           one entry per signal and bucket width: signal, bucket, ts, known_at, values {col: v}, z {col: z},
                        headline (the column the sparkline shows), series [[known_at, v], ...] for the day (at most 400 points)
      daily             the last row of each daily signal in the store: signal, date, values
"""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

SKIP = {"signal", "bucket", "underlying", "symbol", "ts", "known_at", "date", "snap", "expiry", "next_expiry", "fc_at", "fc1_at", "pt_at",
        "iv7_how", "strike", "cp", "t", "next_t"}
HEADLINE = {"vrp": "vrp_diff", "vrp_intraday": "vrp_intraday", "rv_over_iv": "rv_over_iv", "implied_move": "move_ratio", "skew": "skew1_s",
            "term_slope": "slope", "gex": "gex_us", "gex_full": "gex_us", "pcr_near": "pcr_oi", "pcr_full": "pcr_oi", "rr25": "rr25",
            "fut_ofi": "ofi_norm", "mlofi_future": "mlofi", "book_pressure": "qi", "depth_features": "depth_imb", "signed_flow": "delta_flow",
            "residual_iv": "resid", "curvature": "curv", "cp_iv_gap": "gap", "event_variance": "event_var", "noise_band": "signal_value"}
MAX_POINTS = 400


def _num(v) -> Optional[float]:
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return None if not np.isfinite(f) else round(f, 6)


def _values(row: pd.Series) -> dict:
    return {c: _num(v) for c, v in row.items() if c not in SKIP and not c.endswith("_tod_z") and _num(v) is not None}


def _headline(sid: str, df: pd.DataFrame) -> Optional[str]:
    h = HEADLINE.get(sid)
    if h in df.columns:
        return h
    num = [c for c in df.columns if c not in SKIP and not c.endswith("_tod_z") and pd.api.types.is_numeric_dtype(df[c])]
    return num[0] if num else None


def summary(store: Path, underlying: str, day: Optional[date] = None, now: Optional[datetime] = None) -> dict:
    from signals.options.stream import read_live
    day = day or datetime.utcnow().date()
    now = pd.Timestamp(now or datetime.utcnow())
    live = read_live(store, day)
    out = {"underlying": underlying, "date": str(day), "cadence": [], "newest": None, "stale": True, "signals": [], "daily": []}
    if len(live):
        live = live[live["underlying"] == underlying] if "underlying" in live.columns else live
    if len(live):
        live["known_at"] = pd.to_datetime(live["known_at"])
        widths = sorted(live["bucket"].dropna().unique(), key=lambda b: pd.Timedelta(b)) if "bucket" in live.columns else ["1min"]
        out["cadence"] = list(widths)
        newest = live["known_at"].max()
        out["newest"] = newest.isoformat()
        out["stale"] = bool(now - newest > 2 * pd.Timedelta(widths[0]))
        for (sid, b), g in live.groupby(["signal", "bucket"]):
            g = g.sort_values("known_at")
            if "symbol" in g.columns and g["symbol"].nunique() > 1:          # per contract: show the front future's, else skip the series
                fut = g[g["symbol"].astype(str).str.contains("_FUT_")]
                g = fut[fut["symbol"] == fut["symbol"].iloc[-1]] if len(fut) else g.iloc[0:0]
            if g.empty:
                continue
            last = g.iloc[-1]
            head = _headline(sid, g)
            ser = g[["known_at", head]].dropna() if head else pd.DataFrame()
            if len(ser) > MAX_POINTS:
                ser = ser.iloc[np.linspace(0, len(ser) - 1, MAX_POINTS).astype(int)]
            out["signals"].append({
                "signal": sid, "bucket": b, "ts": pd.Timestamp(last["ts"]).isoformat() if "ts" in g.columns and pd.notna(last.get("ts")) else None,
                "known_at": last["known_at"].isoformat(), "values": _values(last),
                "z": {c[:-6]: _num(last[c]) for c in g.columns if c.endswith("_tod_z") and _num(last[c]) is not None},
                "headline": head, "series": [[k.isoformat(), _num(v)] for k, v in zip(ser["known_at"], ser[head])] if head else []})
    out["signals"].sort(key=lambda s: (s["signal"], pd.Timedelta(s["bucket"])))
    out["daily"] = _daily(store, underlying, day)
    return out


def _daily(store: Path, underlying: str, day: date) -> list[dict]:
    from signals import catalogue  # noqa: F401
    from signals.framework import REGISTRY
    rows = []
    for sid, (spec, _) in sorted(REGISTRY.items()):
        if spec.grid != "daily" or spec.kind != "signal":
            continue
        base = Path(store) / "signals" / sid / f"v{spec.version}" / f"underlying={underlying}"
        files = sorted(f for f in base.glob("date=*/part.parquet") if f.parent.name.split("=", 1)[1] < str(day)) if base.is_dir() else []
        if not files:
            continue
        df = pd.read_parquet(files[-1])
        if not len(df):
            continue
        last = df.iloc[-1]
        rows.append({"signal": sid, "date": files[-1].parent.name.split("=", 1)[1], "values": _values(last)})
    return rows
