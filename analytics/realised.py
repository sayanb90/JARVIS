"""
Realised volatility from 1-minute bars (SCRUM-174): the realised leg of every volatility-premium signal.

  daily_measures(minutes)      per session: close-to-close parts (overnight, intraday), realised variance from 1-minute returns, upside
                               and downside semivariance, bipower variation and a jump test, Parkinson, Garman-Klass, Rogers-Satchell
  yang_zhang(daily, n)         Yang-Zhang variance over a trailing window of n sessions (handles the overnight gap)
  har_forecast(daily, ...)     HAR-RV forecasts of the average daily variance over the next h sessions, refitted walk-forward
  tod_profile(minutes, ...)    the share of a session's variance still to come at each minute, from past sessions only

Inputs are naive-UTC minute bars of one instrument with columns ts, open, high, low, close.  Variances are per session (not annualised);
multiply by SESSIONS_PER_YEAR to annualise.  Every rolling quantity at session d uses sessions before d only, or d itself where the value
is known at d's close, so a value never uses data from after the time it is stamped with (`known_at`).
"""

from __future__ import annotations

import math
from typing import Iterable, Optional

import numpy as np
import pandas as pd

SESSIONS_PER_YEAR = 252
MU1 = math.sqrt(2.0 / math.pi)                    # E|Z| for a standard normal
JUMP_Z = 3.09                                      # one-sided 0.1 % critical value for the Huang-Tauchen ratio test
SESSION_CLOSE_UTC = pd.Timedelta(hours=10)         # 15:30 IST


def _session_frame(minutes: pd.DataFrame) -> pd.DataFrame:
    m = minutes[["ts", "open", "high", "low", "close"]].dropna(subset=["close"]).copy()
    m["ts"] = pd.to_datetime(m["ts"])
    m = m.sort_values("ts")
    m["day"] = m["ts"].dt.normalize()
    return m


def daily_measures(minutes: pd.DataFrame) -> pd.DataFrame:
    """One row per session: date, open, high, low, close, n, r_on (overnight log return), r_id (open to close), rv (sum of squared
    1-minute log returns), rs_up, rs_down, bv (bipower variation), jump (max(rv - bv, 0)), jump_flag, parkinson, garman_klass,
    rogers_satchell, known_at (session close)."""
    m = _session_frame(minutes)
    if m.empty:
        return pd.DataFrame()
    m["r"] = np.log(m["close"]).groupby(m["day"]).diff()
    g = m.groupby("day")
    out = pd.DataFrame({
        "open": g["open"].first(), "high": g["high"].max(), "low": g["low"].min(), "close": g["close"].last(), "n": g["close"].size(),
    })
    r2 = m["r"] ** 2
    out["rv"] = r2.groupby(m["day"]).sum(min_count=1)
    out["rs_up"] = r2.where(m["r"] > 0, 0.0).groupby(m["day"]).sum(min_count=1)
    out["rs_down"] = r2.where(m["r"] < 0, 0.0).groupby(m["day"]).sum(min_count=1)
    ar = m["r"].abs()
    ar_lag = ar.groupby(m["day"]).shift(1)
    out["bv"] = (ar * ar_lag).groupby(m["day"]).sum(min_count=1) / (MU1 ** 2)
    ar_l2 = ar.groupby(m["day"]).shift(2)
    mu43 = 2 ** (2 / 3) * math.gamma(7 / 6) / math.gamma(0.5)
    tq_raw = ((ar * ar_lag * ar_l2) ** (4 / 3)).groupby(m["day"]).sum(min_count=1)
    n = out["n"].astype(float)
    tq = n * (mu43 ** -3) * (n / (n - 2).clip(lower=1)) * tq_raw
    out["jump"] = (out["rv"] - out["bv"]).clip(lower=0.0)
    with np.errstate(divide="ignore", invalid="ignore"):
        rj = (out["rv"] - out["bv"]) / out["rv"]
        denom = np.sqrt(((math.pi / 2) ** 2 + math.pi - 5) / n * np.maximum(1.0, tq / out["bv"] ** 2))
        out["jump_z"] = rj / denom
    out["jump_flag"] = out["jump_z"] > JUMP_Z
    lo, hi, op, cl = np.log(out["low"]), np.log(out["high"]), np.log(out["open"]), np.log(out["close"])
    out["parkinson"] = (hi - lo) ** 2 / (4 * math.log(2))
    out["garman_klass"] = 0.5 * (hi - lo) ** 2 - (2 * math.log(2) - 1) * (cl - op) ** 2
    out["rogers_satchell"] = (hi - cl) * (hi - op) + (lo - cl) * (lo - op)
    out["r_id"] = cl - op
    out["r_on"] = op - np.log(out["close"].shift(1))
    out["r_cc"] = cl - np.log(out["close"].shift(1))
    out = out.reset_index().rename(columns={"day": "date"})
    out["known_at"] = out["date"] + SESSION_CLOSE_UTC
    out["date"] = out["date"].dt.date
    return out


def yang_zhang(daily: pd.DataFrame, n: int = 20) -> pd.Series:
    """Yang-Zhang per-session variance over the trailing n sessions ending at (and including) each row."""
    o = daily["r_on"]
    c = daily["r_id"]
    rs = daily["rogers_satchell"]
    k = 0.34 / (1.34 + (n + 1) / (n - 1))
    var_o = o.rolling(n, min_periods=n).var()
    var_c = c.rolling(n, min_periods=n).var()
    return var_o + k * var_c + (1 - k) * rs.rolling(n, min_periods=n).mean()


def total_variance(daily: pd.DataFrame) -> pd.Series:
    """A session's variance including the overnight move: rv + r_on^2 (the first session has no overnight part)."""
    return daily["rv"] + daily["r_on"].fillna(0.0) ** 2


def _har_design(v: pd.Series) -> pd.DataFrame:
    return pd.DataFrame({"c": 1.0, "d": v, "w": v.rolling(5, min_periods=5).mean(), "m": v.rolling(22, min_periods=22).mean()})


def har_forecast(daily: pd.DataFrame, horizons: Iterable[int] = (1, 5, 22), min_obs: int = 250, refit_every: int = 21,
                 col: Optional[str] = None) -> pd.DataFrame:
    """Walk-forward HAR-RV.  For each session t and horizon h: the forecast of the mean daily variance over sessions t+1..t+h, from a
    regression fitted only on pairs whose target window ended by t (so nothing after t's close is used).  Coefficients are refitted every
    `refit_every` sessions on an expanding window; rows before `min_obs` sessions of history are NaN.  Forecasts are floored at a tenth
    of the trailing 22-session mean, so a negative fitted value cannot appear."""
    v = (daily[col] if col else total_variance(daily)).astype(float).reset_index(drop=True)
    X = _har_design(v)
    out = pd.DataFrame({"date": daily["date"].values, "known_at": daily["known_at"].values})
    for h in horizons:
        y = pd.Series([v.iloc[i + 1:i + 1 + h].mean() if i + h < len(v) else np.nan for i in range(len(v))])
        fc = np.full(len(v), np.nan)
        beta = None
        for t in range(len(v)):
            if t >= min_obs and (beta is None or (t - min_obs) % refit_every == 0):
                last = t - h                                   # target of row `last` ends at t: known by t's close
                rows = X.iloc[:last + 1].join(y.iloc[:last + 1].rename("y")).dropna()
                if len(rows) >= 60:
                    A = rows[["c", "d", "w", "m"]].to_numpy()
                    beta, *_ = np.linalg.lstsq(A, rows["y"].to_numpy(), rcond=None)
            if beta is not None and not X.iloc[t].isna().any():
                f = float(X.iloc[t].to_numpy() @ beta)
                fc[t] = max(f, 0.1 * float(X.iloc[t]["m"]))
        out[f"har_{h}"] = fc
    return out


def minute_returns(minutes: pd.DataFrame) -> pd.DataFrame:
    """Within-session 1-minute log returns with the minute index from the open (0 = 03:45-03:46 UTC bar's return from the previous bar)."""
    m = _session_frame(minutes)
    m["r"] = np.log(m["close"]).groupby(m["day"]).diff()
    m["minute"] = ((m["ts"] - m["day"]) / pd.Timedelta(minutes=1)).astype(int) - (3 * 60 + 45)
    return m.dropna(subset=["r"])


def tod_profile(minutes: pd.DataFrame, day_tags: Optional[pd.DataFrame] = None, window: int = 120, min_days: int = 20,
                session_minutes: int = 375) -> pd.DataFrame:
    """Point-in-time time-of-day variance profile.  For each session d (and its tag, e.g. expiry day or not, given in `day_tags` with
    columns date and tag), the share of a session's within-day variance that falls after each minute, estimated from the previous
    `window` sessions with the same tag (needs `min_days`).  Returns date, minute, remaining (share still to come after that minute)."""
    r = minute_returns(minutes)
    r["date"] = r["day"].dt.date
    if day_tags is not None and len(day_tags):
        r = r.merge(day_tags[["date", "tag"]], on="date", how="left")
    else:
        r["tag"] = "all"
    r["tag"] = r["tag"].fillna("all")
    day_rv = r.groupby("date")["r"].apply(lambda x: float((x ** 2).sum()))
    r["share"] = r["r"] ** 2 / r["date"].map(day_rv)
    grid = pd.DataFrame({"minute": np.arange(session_minutes)})
    per_day = (r.pivot_table(index="date", columns="minute", values="share", aggfunc="sum")
               .reindex(columns=grid["minute"], fill_value=0.0).fillna(0.0))
    tags = r.groupby("date")["tag"].first()
    dates = sorted(per_day.index)
    rows = []
    for i, d in enumerate(dates):
        same = [x for x in dates[:i] if tags[x] == tags[d]][-window:]
        if len(same) < min_days:
            continue
        mean_share = per_day.loc[same].mean(axis=0).to_numpy()
        tot = mean_share.sum()
        if tot <= 0:
            continue
        cum = np.cumsum(mean_share) / tot
        rows.append(pd.DataFrame({"date": d, "minute": grid["minute"], "remaining": 1.0 - cum, "tag": tags[d]}))
    return pd.concat(rows, ignore_index=True) if rows else pd.DataFrame(columns=["date", "minute", "remaining", "tag"])


def annualise(var_per_session) -> np.ndarray:
    return np.sqrt(np.asarray(var_per_session, dtype=float) * SESSIONS_PER_YEAR)
