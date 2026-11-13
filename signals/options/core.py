"""
Shared inputs for the option signals (SCRUM-173, SCRUM-174): the option chain with forward, IV and Greeks; at-the-money summaries and
smile points; the continuous front future; daily realised measures and HAR forecasts; eras; the time-of-day variance profile.

The option chain (provider "chain"), one row per (minute, expiry, strike, call or put):
  ts, expiry, strike, cp ("C"/"P"), close, volume, oi, age (minutes since the last trade; 0 = traded in this minute), lot, t (years to
  expiry at the end of the minute, expiry at 15:30 IST), fwd (forward for that expiry and minute), fwd_src ("parity", "future", "spot"),
  k = ln(strike / fwd), iv, delta, gamma, vega, and, on live days, bid, ask, bid_qty, ask_qty, iv_vendor, delta_vendor, gamma_vendor.

How the forward is found, per (minute, expiry): put-call parity on the strikes where both the call and the put traded within PARITY_AGE
minutes, F = K + e^(rT)(C - P), median over the PARITY_PAIRS strikes with the smallest |C - P| (the most liquid, least model-dependent
pairs).  If no pair qualifies: the front future carried back to the option's expiry, F = Fut * e^(-r (T_fut - T)); else the index carried
forward.  A last trade older than STALE_MAX minutes is not carried; IV is computed only where age <= IV_AGE and the premium is at least
MIN_IV_PRICE (below about 20 ticks the 0.05 tick dominates and the IV is noise: on synthetic data a 2 % out-of-the-money put on its last
day shows +20 volatility points of error from rounding alone).
"""

from __future__ import annotations

from datetime import date, time
from typing import Optional

import numpy as np
import pandas as pd

from analytics import black76, realised
from data_layer.greeks_calc import parse_future, parse_option
from signals.framework import Context, provider

EXPIRY_UTC = pd.Timedelta(hours=10)              # 15:30 IST
YEAR = pd.Timedelta(days=365)
OPEN_UTC = pd.Timedelta(hours=3, minutes=45)
STALE_MAX = 5
IV_AGE = 2
PARITY_AGE = 1
MIN_IV_PRICE = 1.0                               # rupees: below this the 0.05 tick dominates the price and the IV is noise
PARITY_PAIRS = 3
SNAP_TIMES_IST = ("09:20", "12:00", "15:15")
MIN_T_IV7 = 1.0 / 365                            # expiries closer than a day are not used for the 7-day constant-maturity IV
CM_DAYS = 7
SMILE_POINTS = {"m2": 0.98, "m1": 0.99, "p1": 1.01, "p2": 1.02}
HARD_BREAKS = (("2024-11-20", "one weekly per exchange"), ("2025-09-01", "NSE expiries on Tuesday"))
INDEX_SYMBOL = {"NIFTY": "NSE_INDEX:Nifty 50", "BANKNIFTY": "NSE_INDEX:Nifty Bank", "FINNIFTY": "NSE_INDEX:Nifty Fin Service",
                "SENSEX": "BSE_INDEX:SENSEX"}
VIX_SYMBOL = "NSE_INDEX:India VIX"


def snap_utc(day, hhmm_ist: str) -> pd.Timestamp:
    h, m = (int(x) for x in hhmm_ist.split(":"))
    return pd.Timestamp(day) + pd.Timedelta(hours=h, minutes=m) - pd.Timedelta(hours=5, minutes=30)


def era_of(day) -> str:
    d = pd.Timestamp(day)
    label = "pre_2024-11-20"
    for when, _ in HARD_BREAKS:
        if d >= pd.Timestamp(when):
            label = f"from_{when}"
    return label


# ---------------------------------------------------------------------------------------------- contracts
@provider("contract_table")
def contract_table(ctx: Context) -> pd.DataFrame:
    """Contracts of this underlying: symbol, instrument_type (CE, PE, FUT), strike, expiry (date), lot_size.  From the raw `contracts`
    frame when given, else parsed from the symbols seen in the option and future minutes."""
    c = ctx.raw.get("contracts")
    if c is not None and len(c):
        c = c[c["underlying"] == ctx.underlying].copy()
        c["expiry"] = pd.to_datetime(c["expiry"]).dt.date
        return c[["symbol", "instrument_type", "strike", "expiry", "lot_size"]].drop_duplicates("symbol")
    rows = []
    for df, kind in ((ctx.raw.get("opt_1m"), "opt"), (ctx.raw.get("fut_1m"), "fut")):
        if df is None or not len(df):
            continue
        for s in df["symbol"].unique():
            if kind == "opt" and (p := parse_option(s)) and p[0] == ctx.underlying:
                rows.append((s, p[2], p[1], p[3], np.nan))
            elif kind == "fut" and (p := parse_future(s)) and p[0] == ctx.underlying:
                rows.append((s, "FUT", np.nan, p[1], np.nan))
    return pd.DataFrame(rows, columns=["symbol", "instrument_type", "strike", "expiry", "lot_size"])


def _years(expiry: pd.Series, ts: pd.Series) -> np.ndarray:
    exp_dt = pd.to_datetime(expiry) + EXPIRY_UTC
    return ((exp_dt - (pd.to_datetime(ts) + pd.Timedelta(minutes=1))) / YEAR).to_numpy(dtype=float)


# ---------------------------------------------------------------------------------------------- underlying, futures
@provider("spot")
def spot(ctx: Context) -> pd.DataFrame:
    """Index minutes of this underlying: ts, open, high, low, close."""
    df = ctx.raw.get("index_1m")
    if df is None or not len(df):
        return pd.DataFrame(columns=["ts", "open", "high", "low", "close"])
    if "symbol" in df.columns:
        df = df[df["symbol"] == INDEX_SYMBOL.get(ctx.underlying, ctx.underlying)]
    out = df[["ts", "open", "high", "low", "close"]].copy()
    out["ts"] = pd.to_datetime(out["ts"])
    return out.sort_values("ts").drop_duplicates("ts").reset_index(drop=True)


@provider("vix")
def vix(ctx: Context) -> pd.DataFrame:
    df = ctx.raw.get("vix_1m")
    if df is None or not len(df):
        return pd.DataFrame(columns=["ts", "close"])
    if "symbol" in df.columns:
        df = df[df["symbol"] == VIX_SYMBOL]
    out = df[["ts", "open", "high", "low", "close"]].copy()
    out["ts"] = pd.to_datetime(out["ts"])
    return out.sort_values("ts").drop_duplicates("ts").reset_index(drop=True)


@provider("futures")
def futures(ctx: Context) -> pd.DataFrame:
    """Every future minute with its expiry: ts, symbol, expiry, close, volume, oi."""
    df = ctx.raw.get("fut_1m")
    if df is None or not len(df):
        return pd.DataFrame(columns=["ts", "symbol", "expiry", "close", "volume", "oi"])
    ct = ctx.get("contract_table")
    f = df.merge(ct[ct["instrument_type"] == "FUT"][["symbol", "expiry"]], on="symbol", how="inner")
    f["ts"] = pd.to_datetime(f["ts"])
    for c in ("volume", "oi"):
        if c not in f.columns:
            f[c] = np.nan
    return f[["ts", "symbol", "expiry", "close", "volume", "oi"]].sort_values(["ts", "expiry"]).reset_index(drop=True)


@provider("fut_front")
def fut_front(ctx: Context) -> pd.DataFrame:
    """The continuous front future: at each minute the contract with the nearest expiry after today (on its expiry day the next one
    takes over), plus the next contract's close and open interest for basis and roll.  Columns ts, symbol, expiry, close, volume, oi,
    next_close, next_oi, t_fut."""
    f = ctx.get("futures")
    if not len(f):
        return pd.DataFrame(columns=["ts", "symbol", "expiry", "close", "volume", "oi", "next_close", "next_oi", "t_fut"])
    f = f.copy()
    f["day"] = f["ts"].dt.date
    f = f[pd.to_datetime(f["expiry"]) > pd.to_datetime(f["day"])]
    f["rank"] = f.groupby("ts")["expiry"].rank(method="dense")
    front = f[f["rank"] == 1].drop(columns=["rank", "day"])
    nxt = f[f["rank"] == 2][["ts", "close", "oi"]].rename(columns={"close": "next_close", "oi": "next_oi"})
    out = front.merge(nxt, on="ts", how="left")
    out["t_fut"] = _years(out["expiry"], out["ts"])
    return out.drop_duplicates("ts").reset_index(drop=True)


# ---------------------------------------------------------------------------------------------- the chain
def _grid_fill(o: pd.DataFrame, minutes: pd.DataFrame, stale_max: int) -> pd.DataFrame:
    """Carry each contract's last trade forward inside its session for up to `stale_max` minutes, with its age."""
    if o.empty:
        return o
    o = o.copy()
    o["day"] = o["ts"].dt.normalize()
    mins = minutes[["ts"]].copy()
    mins["day"] = mins["ts"].dt.normalize()
    parts = []
    carry = [c for c in ("close", "oi", "bid", "ask", "bid_qty", "ask_qty", "iv_vendor", "delta_vendor", "gamma_vendor") if c in o.columns]
    for (day, sym), g in o.groupby(["day", "symbol"], sort=False):
        grid = mins.loc[mins["day"] == day, "ts"]
        if grid.empty:
            grid = g["ts"]
        g = g.set_index("ts").sort_index()
        g = g[~g.index.duplicated(keep="last")]
        traded = pd.Series(np.arange(len(g)), index=g.index)
        idx = grid[(grid >= g.index.min())].sort_values()
        r = g.reindex(idx)
        pos = traded.reindex(idx).ffill(limit=stale_max)
        last_ts = pd.Series(g.index, index=g.index).reindex(idx).ffill(limit=stale_max)
        r[carry] = r[carry].ffill(limit=stale_max)
        r["volume"] = r["volume"].fillna(0) if "volume" in r.columns else 0
        r["age"] = ((r.index - pd.to_datetime(last_ts.values)) / pd.Timedelta(minutes=1)).astype(float)
        r = r[pos.notna().to_numpy()]
        r["symbol"] = sym
        parts.append(r.reset_index().rename(columns={"index": "ts"}))
    out = pd.concat(parts, ignore_index=True) if parts else o.iloc[:0]
    return out.drop(columns=["day"], errors="ignore")


def rate_table(rates: pd.DataFrame | None) -> pd.DataFrame:
    """Daily rate for the chain (SCRUM-186): overnight MIBOR where imported, else the RBI repo rate, as a fraction.  Each value is used
    from the next session on (a day's rate is fetched during that day), so a session never sees its own fixing.  Columns date, r."""
    if rates is None or not len(rates):
        return pd.DataFrame(columns=["date", "r"])
    x = rates.copy()
    x["date"] = pd.to_datetime(x["date"]).dt.date
    for series in ("mibor_overnight", "repo"):
        s = x[x["series"] == series]
        if len(s):
            s = s.sort_values("date")
            return pd.DataFrame({"date": s["date"].to_numpy(), "r": s["value_pct"].astype(float).to_numpy() / 100.0})
    return pd.DataFrame(columns=["date", "r"])


def rate_by_row(ts: pd.Series, table: pd.DataFrame, default: float) -> np.ndarray:
    """The rate in force at each timestamp: the last table value dated before that day, else `default`."""
    if table is None or not len(table):
        return np.full(len(ts), float(default))
    days = pd.to_datetime(pd.to_datetime(ts).dt.date).astype("datetime64[ns]")
    tab = pd.DataFrame({"d": (pd.to_datetime(table["date"]) + pd.Timedelta(days=1)).astype("datetime64[ns]"),
                        "r": table["r"].astype(float)}).sort_values("d")
    left = pd.DataFrame({"d": days.to_numpy(), "i": np.arange(len(ts))}).sort_values("d")
    m = pd.merge_asof(left, tab, on="d", direction="backward").sort_values("i")
    return m["r"].fillna(float(default)).to_numpy()


def parity_forward(ch: pd.DataFrame, r: float, max_age: float = PARITY_AGE, pairs: int = PARITY_PAIRS) -> pd.DataFrame:
    """Per (ts, expiry): the forward from put-call parity on the freshest, most at-the-money pairs.  Columns ts, expiry, fwd_parity."""
    c = ch[(ch["cp"] == "C") & (ch["age"] <= max_age)][["ts", "expiry", "strike", "close", "t"] + (["r"] if "r" in ch.columns else [])]
    p = ch[(ch["cp"] == "P") & (ch["age"] <= max_age)][["ts", "expiry", "strike", "close"]]
    m = c.merge(p, on=["ts", "expiry", "strike"], suffixes=("_c", "_p"))
    if m.empty:
        return pd.DataFrame(columns=["ts", "expiry", "fwd_parity"])
    m["diff"] = (m["close_c"] - m["close_p"]).abs()
    rr = m["r"] if "r" in m.columns else r
    m["f"] = m["strike"] + np.exp(rr * m["t"]) * (m["close_c"] - m["close_p"])
    m = m.sort_values(["ts", "expiry", "diff"])
    m = m.groupby(["ts", "expiry"]).head(pairs)
    return m.groupby(["ts", "expiry"], as_index=False)["f"].median().rename(columns={"f": "fwd_parity"})


def build_chain(opt: pd.DataFrame, contracts: pd.DataFrame, spot_m: pd.DataFrame, fut: pd.DataFrame, r: float,
                stale_max: int = STALE_MAX, iv_age: int = IV_AGE, rates: pd.DataFrame | None = None) -> pd.DataFrame:
    """`r` is the flat rate used where `rates` (from rate_table) has no value yet."""
    if opt is None or not len(opt):
        return pd.DataFrame()
    o = opt.copy()
    o["ts"] = pd.to_datetime(o["ts"])
    if "volume" not in o.columns:
        o["volume"] = 0
    if "oi" not in o.columns:
        o["oi"] = np.nan
    ct = contracts[contracts["instrument_type"].isin(["CE", "PE"])][["symbol", "instrument_type", "strike", "expiry", "lot_size"]]
    minutes = spot_m[["ts"]] if len(spot_m) else o[["ts"]].drop_duplicates()
    g = _grid_fill(o, minutes, stale_max)
    g = g.merge(ct, on="symbol", how="inner")
    g["cp"] = np.where(g["instrument_type"] == "CE", "C", "P")
    g["lot"] = g["lot_size"]
    g["t"] = _years(g["expiry"], g["ts"])
    g = g[g["t"] > 0].copy()
    g["r"] = rate_by_row(g["ts"], rates, r)
    r = g["r"].to_numpy()
    pf = parity_forward(g, r)
    g = g.merge(pf, on=["ts", "expiry"], how="left")
    g["fwd_parity"] = pd.to_numeric(g["fwd_parity"], errors="coerce").astype(float)
    g["fwd"], g["fwd_src"] = g["fwd_parity"].copy(), np.where(g["fwd_parity"].notna(), "parity", None)
    if len(fut):
        ff = fut[["ts", "close", "t_fut"]].rename(columns={"close": "fut_close"})
        g = g.merge(ff, on="ts", how="left")
        fb = g["fut_close"] * np.exp(-r * (g["t_fut"] - g["t"]))
        use = g["fwd"].isna() & fb.notna()
        g.loc[use, "fwd"], g.loc[use, "fwd_src"] = fb[use], "future"
    if len(spot_m):
        sp = spot_m[["ts", "close"]].rename(columns={"close": "spot_close"})
        g = g.merge(sp, on="ts", how="left")
        sb = g["spot_close"] * np.exp(r * g["t"])
        use = g["fwd"].isna() & sb.notna()
        g.loc[use, "fwd"], g.loc[use, "fwd_src"] = sb[use], "spot"
    g["k"] = np.log(g["strike"] / g["fwd"])
    is_call = (g["cp"] == "C").to_numpy()
    fresh = ((g["age"] <= iv_age) & (g["close"] >= MIN_IV_PRICE)).to_numpy()
    iv = black76.implied_vol(is_call, g["close"].to_numpy(dtype=float), g["fwd"].to_numpy(dtype=float), g["strike"].to_numpy(dtype=float),
                             g["t"].to_numpy(dtype=float), r)
    g["iv"] = np.where(fresh, iv, np.nan)
    gk = black76.greeks(is_call, g["fwd"].to_numpy(dtype=float), g["strike"].to_numpy(dtype=float), g["t"].to_numpy(dtype=float),
                        g["iv"].to_numpy(dtype=float), r)
    g["delta"], g["gamma"], g["vega"] = gk["delta"], gk["gamma"], gk["vega"]
    keep = ["ts", "expiry", "strike", "cp", "close", "volume", "oi", "age", "lot", "t", "r", "fwd", "fwd_src", "k", "iv", "delta", "gamma", "vega"]
    keep += [c for c in ("bid", "ask", "bid_qty", "ask_qty", "iv_vendor", "delta_vendor", "gamma_vendor", "src") if c in g.columns]
    return g[keep].sort_values(["ts", "expiry", "strike", "cp"]).reset_index(drop=True)


@provider("chain")
def chain(ctx: Context) -> pd.DataFrame:
    return build_chain(ctx.raw.get("opt_1m"), ctx.get("contract_table"), ctx.get("spot"), ctx.get("fut_front"), ctx.r,
                       rates=rate_table(ctx.raw.get("rates")))


# ---------------------------------------------------------------------------------------------- ATM and smile summaries
def otm_strikes(ch: pd.DataFrame) -> pd.DataFrame:
    """One row per (ts, expiry, strike): the OTM option's IV (put below the forward, call above), falling back to the other side,
    with both sides' close, OI and volume."""
    c = ch[ch["cp"] == "C"].set_index(["ts", "expiry", "strike"])
    p = ch[ch["cp"] == "P"].set_index(["ts", "expiry", "strike"])
    j = c[["fwd", "t", "iv", "close", "oi", "volume"]].join(p[["fwd", "t", "iv", "close", "oi", "volume"]], how="outer", lsuffix="_c", rsuffix="_p")
    j["fwd"] = j["fwd_c"].fillna(j["fwd_p"])
    j["t"] = j["t_c"].fillna(j["t_p"])
    j = j.drop(columns=["fwd_c", "fwd_p", "t_c", "t_p"]).reset_index()
    j["k"] = np.log(j["strike"] / j["fwd"])
    put_side = j["k"] < 0
    j["iv_otm"] = np.where(put_side, j["iv_p"].fillna(j["iv_c"]), j["iv_c"].fillna(j["iv_p"]))
    return j


def interp_at(j: pd.DataFrame, targets: pd.DataFrame, value: str = "iv_otm") -> np.ndarray:
    """Linear interpolation of `value` in k, per (ts, expiry), at each target row's k (columns ts, expiry, k).  NaN outside the strikes
    that have a value: never extrapolated."""
    src = j.dropna(subset=[value, "k"])[["ts", "expiry", "k", value]].sort_values("k")
    tg = targets[["ts", "expiry", "k"]].reset_index(drop=True).copy()
    tg["_i"] = np.arange(len(tg))
    tg = tg.sort_values("k")
    lo = pd.merge_asof(tg, src.rename(columns={"k": "k_lo", value: "v_lo"}), left_on="k", right_on="k_lo", by=["ts", "expiry"],
                       direction="backward")
    hi = pd.merge_asof(tg, src.rename(columns={"k": "k_hi", value: "v_hi"}), left_on="k", right_on="k_hi", by=["ts", "expiry"],
                       direction="forward")
    m = lo.merge(hi[["_i", "k_hi", "v_hi"]], on="_i").sort_values("_i")
    span = (m["k_hi"] - m["k_lo"]).to_numpy()
    w = np.where(span > 0, (m["k"] - m["k_lo"]).to_numpy() / np.where(span > 0, span, 1), 0.0)
    return (m["v_lo"] + w * (m["v_hi"] - m["v_lo"])).to_numpy()


@provider("smile")
def smile(ctx: Context) -> pd.DataFrame:
    """Per (ts, expiry): fwd, t, atm_iv (interpolated at k = 0), the IVs at SMILE_POINTS (strike / forward), the straddle at the strike
    nearest the forward, and that strike."""
    ch = ctx.get("chain")
    if ch is None or not len(ch):
        return pd.DataFrame()
    j = otm_strikes(ch)
    g = j.groupby(["ts", "expiry"], as_index=False).agg(fwd=("fwd", "first"), t=("t", "first"))
    g["k"] = 0.0
    g["atm_iv"] = interp_at(j, g)
    for name, mny in SMILE_POINTS.items():
        g["k"] = float(np.log(mny))
        g[f"iv_{name}"] = interp_at(j, g)
    g = g.drop(columns=["k"])
    j["dist"] = (j["strike"] - j["fwd"]).abs()
    near = j.loc[j.groupby(["ts", "expiry"])["dist"].idxmin(), ["ts", "expiry", "strike", "close_c", "close_p"]]
    near["straddle"] = near["close_c"] + near["close_p"]
    g = g.merge(near[["ts", "expiry", "strike", "straddle"]].rename(columns={"strike": "atm_strike"}), on=["ts", "expiry"], how="left")
    return g.sort_values(["ts", "t"]).reset_index(drop=True)


@provider("atm")
def atm(ctx: Context) -> pd.DataFrame:
    """Per minute: the front expiry (nearest, 0DTE included) and its ATM IV, straddle and smile points; the next expiry's ATM IV; and
    iv7, the 7-day constant-maturity ATM IV by linear interpolation of total variance across the expiries at least a day out (flat
    beyond the listed expiries, marked in iv7_how)."""
    sm = ctx.get("smile")
    if sm is None or not len(sm):
        return pd.DataFrame()
    sm = sm.sort_values(["ts", "t"])
    sm["rank"] = sm.groupby("ts").cumcount()
    front = sm[sm["rank"] == 0].drop(columns=["rank"])
    nxt = sm[sm["rank"] == 1][["ts", "expiry", "t", "atm_iv"]].rename(columns={"expiry": "next_expiry", "t": "next_t", "atm_iv": "next_atm_iv"})
    out = front.merge(nxt, on="ts", how="left")
    t7 = CM_DAYS / 365.0
    cm = sm[(sm["t"] >= MIN_T_IV7) & sm["atm_iv"].notna()][["ts", "t", "atm_iv"]].copy()
    cm["w"] = cm["atm_iv"] ** 2 * cm["t"]
    below = cm[cm["t"] <= t7].sort_values("t").groupby("ts").last()
    above = cm[cm["t"] >= t7].sort_values("t").groupby("ts").first()
    both = below.join(above, lsuffix="_a", rsuffix="_b", how="outer")
    iv7 = pd.Series(np.nan, index=both.index)
    how = pd.Series(None, index=both.index, dtype=object)
    has_both = both["t_a"].notna() & both["t_b"].notna()
    span = (both["t_b"] - both["t_a"])
    interp = has_both & (span > 0)
    w = both["w_a"] + (both["w_b"] - both["w_a"]) * (t7 - both["t_a"]) / span.where(span > 0)
    iv7[interp] = np.sqrt(w[interp].clip(lower=0) / t7)
    how[interp] = "interp"
    exact = has_both & ~interp
    iv7[exact], how[exact] = both.loc[exact, "atm_iv_a"], "exact"
    only_a, only_b = both["t_a"].notna() & both["t_b"].isna(), both["t_a"].isna() & both["t_b"].notna()
    iv7[only_a], how[only_a] = both.loc[only_a, "atm_iv_a"], "flat"
    iv7[only_b], how[only_b] = both.loc[only_b, "atm_iv_b"], "flat"
    out = out.merge(pd.DataFrame({"ts": both.index, "iv7": iv7.values, "iv7_how": how.values}), on="ts", how="left")
    return out.reset_index(drop=True)


@provider("atm_snaps")
def atm_snaps(ctx: Context) -> pd.DataFrame:
    """`atm` at the snapshot times (09:20, 12:00, 15:15 IST): the minute bar that ends at the snapshot.  Columns date, snap, known_at, ..."""
    a = ctx.get("atm")
    if a is None or not len(a):
        return pd.DataFrame()
    a = a.copy()
    a["date"] = pd.to_datetime(a["ts"]).dt.date
    if ctx.snaps == "all":                     # every bar (SCRUM-198): snap is the IST time the bar ends, known at that time
        a["known_at"] = pd.to_datetime(a["ts"]) + pd.Timedelta(minutes=1)
        a["snap"] = (a["known_at"] + pd.Timedelta(hours=5, minutes=30)).dt.strftime("%H:%M")
        return a.reset_index(drop=True)
    rows = []
    for d, g in a.groupby("date"):
        for s in SNAP_TIMES_IST:
            at = snap_utc(d, s)
            hit = g[pd.to_datetime(g["ts"]) == at - pd.Timedelta(minutes=1)]
            if len(hit):
                r = hit.iloc[0].to_dict()
                r.update(date=d, snap=s, known_at=at)
                rows.append(r)
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------------------------- daily realised, eras, profile
@provider("daily")
def daily(ctx: Context) -> pd.DataFrame:
    """Daily realised measures of the index (analytics.realised.daily_measures), Yang-Zhang over 20 sessions, total variance, HAR
    forecasts (1, 5, 22 sessions ahead), all per session with known_at = the close."""
    sp = ctx.get("spot")
    if sp is None or not len(sp):
        return pd.DataFrame()
    d = realised.daily_measures(sp)
    d["tv"] = realised.total_variance(d)
    d["yz20"] = realised.yang_zhang(d, 20)
    h = realised.har_forecast(d)
    return d.merge(h.drop(columns=["known_at"]), on="date", how="left")


@provider("expiries")
def expiries(ctx: Context) -> pd.DataFrame:
    """Option expiries of this underlying with their lot size: expiry, lot."""
    ct = ctx.get("contract_table")
    o = ct[ct["instrument_type"].isin(["CE", "PE"])]
    return o.groupby("expiry", as_index=False)["lot_size"].max().rename(columns={"lot_size": "lot"}).sort_values("expiry")


@provider("eras")
def eras(ctx: Context) -> pd.DataFrame:
    """Per session: era (hard breaks), front option expiry (from the contracts), days to it, whether today is an expiry day, the
    expiry weekday, whether weeklies are listed (the next two expiries within 8 days of each other) and the front lot size."""
    sp = ctx.get("spot")
    days = sorted(pd.to_datetime(sp["ts"]).dt.date.unique()) if len(sp) else []
    ex = ctx.get("expiries")
    exps = sorted(pd.to_datetime(ex["expiry"]).dt.date) if len(ex) else []
    lots = dict(zip(pd.to_datetime(ex["expiry"]).dt.date, ex["lot"])) if len(ex) else {}
    rows = []
    for d in days:
        fut = [e for e in exps if e >= d]
        front = fut[0] if fut else None
        weekly = len(fut) >= 2 and (fut[1] - fut[0]).days <= 8
        rows.append({"date": d, "known_at": pd.Timestamp(d) + OPEN_UTC, "era": era_of(d), "front_expiry": front,
                     "dte": (front - d).days if front else np.nan, "is_expiry": front == d,
                     "expiry_weekday": front.weekday() if front else np.nan, "weekly": weekly, "lot": lots.get(front, np.nan)})
    return pd.DataFrame(rows)


@provider("tod")
def tod(ctx: Context) -> pd.DataFrame:
    """Time-of-day variance profile per session, point in time, separate for expiry and other days."""
    sp = ctx.get("spot")
    er = ctx.get("eras")
    if not len(sp):
        return pd.DataFrame()
    tags = er[["date", "is_expiry"]].assign(tag=lambda x: np.where(x["is_expiry"], "expiry", "other"))[["date", "tag"]] if len(er) else None
    return realised.tod_profile(sp, tags, window=120, min_days=10)


def minute_of_session(ts: pd.Series) -> pd.Series:
    ts = pd.to_datetime(ts)
    return ((ts - ts.dt.normalize() - OPEN_UTC) / pd.Timedelta(minutes=1)).astype(int)
