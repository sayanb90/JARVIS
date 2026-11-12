"""
The overnight against intraday split of option returns (SCRUM-178, signal A4): our own replication of Bhat, Pandey and Rao (2024) on
Nifty and of Muravyev and Ni (2020) on the S&P 500, who find delta-hedged option sellers earn the premium overnight and give some back
during the day.  It decides the holding rule for long options.

  day_night_returns (study)   for each session and each near-ATM call and put of the front expiries: delta-hedged return from the close
                              (15:29 bar) to the next open (09:16 bar), and from that open to that day's close, as a fraction of the
                              option price at the start of the leg.  Hedged with the future at the start-of-leg delta.
  summarise_day_night(df)     mean, t-statistic and count per era, underlying, leg and days-to-expiry bucket.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from signals.framework import Context, Spec, register
from signals.options.core import era_of

CLOSE_BAR = pd.Timedelta(hours=9, minutes=59)    # 15:29 IST bar
OPEN_BAR = pd.Timedelta(hours=3, minutes=46)     # 09:16 IST bar (the 09:15 bar carries the auction noise)
DTE_BUCKETS = [-0.5, 0.5, 1.5, 3.5, 7.5, 30.5]
DTE_LABELS = ["0", "1", "2-3", "4-7", "8+"]


def _leg(ch: pd.DataFrame, t0: pd.Timestamp, t1: pd.Timestamp, max_k: float) -> pd.DataFrame:
    a = ch[ch["ts"] == t0]
    b = ch[ch["ts"] == t1]
    if a.empty or b.empty:
        return pd.DataFrame()
    a = a[(a["k"].abs() <= max_k) & a["delta"].notna() & (a["age"] <= 2)]
    m = a.merge(b[["expiry", "strike", "cp", "close", "fwd"]], on=["expiry", "strike", "cp"], suffixes=("", "_1"))
    if m.empty:
        return m
    m["ret"] = ((m["close_1"] - m["close"]) - m["delta"] * (m["fwd_1"] - m["fwd"])) / m["close"]
    return m


@register(Spec(id="day_night_returns", version=1, family="holding", grid="daily", kind="study", inputs=("chain",), params={"max_k": 0.01},
               tickets=("SCRUM-178",),
               doc="A4: delta-hedged returns of near-ATM options overnight (close to next open) and intraday (open to close)."))
def day_night_returns(ctx: Context, p: dict) -> pd.DataFrame:
    ch = ctx.get("chain")
    if ch is None or not len(ch):
        return pd.DataFrame()
    days = sorted(ch["ts"].dt.normalize().unique())
    rows = []
    for prev, day in zip(days[:-1], days[1:]):
        for leg, t0, t1 in (("overnight", prev + CLOSE_BAR, day + OPEN_BAR), ("intraday", day + OPEN_BAR, day + CLOSE_BAR)):
            m = _leg(ch, t0, t1, p["max_k"])
            if len(m):
                m = m.assign(leg=leg, date=pd.Timestamp(day).date(), known_at=t1 + pd.Timedelta(minutes=1))
                rows.append(m[["date", "known_at", "leg", "expiry", "strike", "cp", "k", "t", "close", "ret"]])
    out = pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()
    if len(out):
        out["dte"] = (pd.to_datetime(out["expiry"]) - pd.to_datetime(out["date"])).dt.days
        out["dte_bucket"] = pd.cut(out["dte"], DTE_BUCKETS, labels=DTE_LABELS).astype(str)
        out["era"] = out["date"].map(era_of)
    return out


def summarise_day_night(df: pd.DataFrame) -> pd.DataFrame:
    """Mean delta-hedged return, its t-statistic and the count, per era, leg and days-to-expiry bucket (and underlying if present)."""
    if df is None or not len(df):
        return pd.DataFrame()
    keys = [k for k in ("underlying", "era", "leg", "dte_bucket") if k in df.columns]
    g = df.groupby(keys)["ret"]
    out = g.agg(["mean", "std", "count"]).reset_index()
    out["t_stat"] = out["mean"] / (out["std"] / np.sqrt(out["count"]))
    return out
