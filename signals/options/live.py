"""
Live-only option microstructure (SCRUM-181): needs the level-1 feed recorded from 2026-11-03.  Recorded nightly from now, not trusted
before `min_sample` sessions (25 to 50 weekly expiries).  Every spec here is live_only.

Inputs: raw `ticks_fo` (ts or fts, symbol, ltp, vol_today, bid, ask, bid_qty, ask_qty, oi, iv, delta, gamma) and optionally `fills`
(our own paper or real fills: ts, symbol, side +1 buy / -1 sell, price, qty).

  chain_live (provider)  1-minute option rows from the ticks (last trade, last quote, OI, vendor IV and Greeks, volume from the day
                         counter), run through the same chain builder as the history so our IV sits next to Upstox's
  spread_gate       C2   per contract-minute: relative spread (ask - bid) / mid and its z-score against the previous sessions in the same
                         15-minute slot and moneyness band
  fair_value_gap    C1   per contract-minute: the mid moved by delta x the future's microprice change since the last minute, against the
                         quoted ask and bid: a positive buy_edge means the ask has not caught up with the future yet
  book_pressure     C3, C4  microprice minus mid (relative) and queue imbalance (bid_qty - ask_qty) / (bid_qty + ask_qty), last in the minute
  fut_ofi           C5   order flow imbalance at the best level on the front future (Cont, Kukanov, Stoikov 2014), per minute, over mean depth
  cost_log (study)  C6   effective and 5-minute realised spread of our own fills against the prevailing quote
  illiquidity       C7   relative spread x a touch-size penalty (max(1, TOUCH_REF lots / touch lots))
  amihud            C7   history proxy per contract-session: mean |1-minute return| / premium traded (crore), near money
  signed_flow       C8, B4  Lee-Ready signed volume per contract-minute (trade price against the prevailing mid; tick rule at the mid),
                         delta x lot and vega weighted, summed per minute over the chain
  pcr_full, gex_full, pin_full   B2, B5, B1 on all strikes with Upstox gamma and OI
  rr25              A12  25-delta put IV minus 25-delta call IV per expiry (Upstox delta and IV)
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from signals.framework import Context, Spec, provider, register
from signals.options.core import STALE_MAX, _years, build_chain, minute_of_session, rate_table
from signals.options.positioning import gex_table, pcr_table

MIN_SAMPLE = 40
TOUCH_REF = 10


def _ticks(ctx: Context) -> pd.DataFrame:
    t = ctx.raw.get("ticks_fo")
    if t is None or not len(t):
        return pd.DataFrame()
    t = t.copy()
    t["t_evt"] = pd.to_datetime(t["fts"] if "fts" in t.columns else t["ts"])
    return t.sort_values("t_evt", kind="stable")                 # stable: ticks with the same event time keep their arrival order


@provider("ticks_meta")
def ticks_meta(ctx: Context) -> pd.DataFrame:
    """Option and future ticks of this underlying with contract facts, quote measures and the minute they fall in."""
    t = _ticks(ctx)
    if t.empty:
        return t
    ct = ctx.get("contract_table")
    t = t.merge(ct[["symbol", "instrument_type", "strike", "expiry", "lot_size"]], on="symbol", how="inner")
    t["mid"] = (t["bid"] + t["ask"]) / 2
    t["spread"] = t["ask"] - t["bid"]
    t["rel_spread"] = t["spread"] / t["mid"]
    tot = (t["bid_qty"] + t["ask_qty"]).replace(0, np.nan)
    t["microprice"] = (t["bid"] * t["ask_qty"] + t["ask"] * t["bid_qty"]) / tot
    t["qi"] = (t["bid_qty"] - t["ask_qty"]) / tot
    t["minute"] = t["t_evt"].dt.floor(ctx.bucket)          # the bucket start: a minute, or 10 s (Context.bucket)
    return t


@provider("chain_live")
def chain_live(ctx: Context) -> pd.DataFrame:
    t = ctx.get("ticks_meta")
    if t is None or not len(t):
        return pd.DataFrame()
    o = t[t["instrument_type"].isin(["CE", "PE"])].copy()
    o["day"] = o["minute"].dt.normalize()
    o["dv"] = o.groupby(["day", "symbol"])["vol_today"].diff().clip(lower=0).fillna(0) if "vol_today" in o.columns else 0
    agg = o.groupby(["minute", "symbol"]).agg(close=("ltp", "last"), volume=("dv", "sum"), oi=("oi", "last"), bid=("bid", "last"), ask=("ask", "last"),
                                              bid_qty=("bid_qty", "last"), ask_qty=("ask_qty", "last"), iv_vendor=("iv", "last"),
                                              delta_vendor=("delta", "last"), gamma_vendor=("gamma", "last")).reset_index().rename(columns={"minute": "ts"})
    agg["close"] = agg["close"].fillna((agg["bid"] + agg["ask"]) / 2)
    agg["src"] = "tick"
    rates = rate_table(ctx.raw.get("rates"))
    if ctx.bucket_td >= pd.Timedelta(minutes=1):
        return build_chain(agg, ctx.get("contract_table"), ctx.get("spot"), ctx.get("fut_front"), ctx.r, rates=rates)
    # Sub-minute buckets (10 s): the index and future minute bars are too coarse, so the grid is the option buckets themselves and the
    # forward falls back to the front future's bucket mid from its own ticks.  Staleness is still counted in minutes.
    steps = int(pd.Timedelta(minutes=STALE_MAX) / ctx.bucket_td)
    return build_chain(agg, ctx.get("contract_table"), pd.DataFrame(columns=["ts", "close"]), _fut_buckets(t), ctx.r, stale_max=steps,
                       rates=rates)


def _fut_buckets(t: pd.DataFrame) -> pd.DataFrame:
    """The front future per bucket from its ticks (last mid, else last trade), shaped like fut_front."""
    f = t[t["instrument_type"] == "FUT"]
    if f.empty:
        return pd.DataFrame(columns=["ts", "close", "t_fut"])
    f = f[f["expiry"] == f.groupby(f["minute"].dt.normalize())["expiry"].transform("min")]
    g = f.groupby("minute").agg(close=("mid", "last"), ltp=("ltp", "last"), expiry=("expiry", "first")).reset_index().rename(columns={"minute": "ts"})
    g["close"] = g["close"].fillna(g["ltp"])
    g["t_fut"] = _years(g["expiry"], g["ts"])
    return g[["ts", "close", "t_fut"]]


def _spec(sid, doc, inputs=("ticks_fo",), grid="1m", kind="signal", params=None):
    return Spec(id=sid, version=1, family="live", grid=grid, inputs=inputs, kind=kind, params=params or {}, live_only=True,
                min_sample=MIN_SAMPLE, tickets=("SCRUM-181",), doc=doc)


def _per_minute(t: pd.DataFrame, opts_only: bool = True) -> pd.DataFrame:
    if opts_only:
        t = t[t["instrument_type"].isin(["CE", "PE"])]
    return t.groupby(["minute", "symbol"]).agg(expiry=("expiry", "first"), strike=("strike", "first"), cp=("instrument_type", "first"),
                                               rel_spread=("rel_spread", "median"), mid=("mid", "last"), bid=("bid", "last"), ask=("ask", "last"),
                                               microprice=("microprice", "last"), qi=("qi", "last"), bid_qty=("bid_qty", "last"),
                                               ask_qty=("ask_qty", "last"), lot=("lot_size", "first")).reset_index()


@register(_spec("spread_gate", "C2: relative spread per contract-minute and its z-score against previous sessions in the same 15-minute slot "
                "and moneyness band.", params={"window": 20}))
def spread_gate(ctx: Context, p: dict) -> pd.DataFrame:
    t = ctx.get("ticks_meta")
    ch = ctx.get("chain_live")
    if t is None or not len(t) or ch is None or not len(ch):
        return pd.DataFrame()
    m = _per_minute(t)
    m["cp"] = np.where(m["cp"] == "CE", "C", "P")
    m = m.merge(ch[["ts", "expiry", "strike", "cp", "k"]].rename(columns={"ts": "minute"}), on=["minute", "expiry", "strike", "cp"], how="left")
    m["band"] = (m["k"].abs() / 0.005).round().clip(upper=8)
    m["slot"] = m["minute"].dt.floor("15min") - m["minute"].dt.normalize()
    m["day"] = m["minute"].dt.normalize()
    daily = m.groupby(["day", "slot", "band"])["rel_spread"].agg(["mean", "std"]).reset_index().sort_values("day")
    daily["base_mean"] = daily.groupby(["slot", "band"])["mean"].transform(lambda s: s.shift(1).rolling(p["window"], min_periods=3).mean())
    daily["base_std"] = daily.groupby(["slot", "band"])["mean"].transform(lambda s: s.shift(1).rolling(p["window"], min_periods=3).std())
    m = m.merge(daily[["day", "slot", "band", "base_mean", "base_std"]], on=["day", "slot", "band"], how="left")
    m["z"] = (m["rel_spread"] - m["base_mean"]) / m["base_std"].replace(0, np.nan)
    m["known_at"] = m["minute"] + ctx.bucket_td
    return m.rename(columns={"minute": "ts"})[["ts", "known_at", "symbol", "expiry", "strike", "cp", "k", "rel_spread", "z"]]


@register(_spec("fair_value_gap", "C1: option mid moved by delta x the future's microprice change since the last minute, against the quoted "
                "ask (buy edge) and bid (sell edge), as a fraction of the mid."))
def fair_value_gap(ctx: Context, p: dict) -> pd.DataFrame:
    t = ctx.get("ticks_meta")
    ch = ctx.get("chain_live")
    if t is None or not len(t) or ch is None or not len(ch):
        return pd.DataFrame()
    fut = t[t["instrument_type"] == "FUT"]
    if fut.empty:
        return pd.DataFrame()
    front = fut[fut["expiry"] == fut.groupby("minute")["expiry"].transform("min")]
    fmp = front.groupby("minute")["microprice"].last().rename("fut_mp")
    m = _per_minute(t)
    m["cp"] = np.where(m["cp"] == "CE", "C", "P")
    m = m.merge(ch[["ts", "expiry", "strike", "cp", "delta"]].rename(columns={"ts": "minute"}), on=["minute", "expiry", "strike", "cp"], how="left")
    m = m.join(fmp, on="minute").sort_values("minute")
    g = m.groupby("symbol")
    m["fair"] = g["mid"].shift(1) + m["delta"] * (m["fut_mp"] - g["fut_mp"].shift(1))
    m["buy_edge"] = (m["fair"] - m["ask"]) / m["mid"]
    m["sell_edge"] = (m["bid"] - m["fair"]) / m["mid"]
    m["known_at"] = m["minute"] + ctx.bucket_td
    return m.rename(columns={"minute": "ts"})[["ts", "known_at", "symbol", "expiry", "strike", "cp", "fair", "buy_edge", "sell_edge"]].dropna(subset=["fair"])


@register(_spec("book_pressure", "C3, C4: microprice minus mid as a fraction of the mid, and queue imbalance, last in each contract-minute."))
def book_pressure(ctx: Context, p: dict) -> pd.DataFrame:
    t = ctx.get("ticks_meta")
    if t is None or not len(t):
        return pd.DataFrame()
    m = _per_minute(t, opts_only=False)
    m["micro_rel"] = (m["microprice"] - m["mid"]) / m["mid"]
    m["known_at"] = m["minute"] + ctx.bucket_td
    return m.rename(columns={"minute": "ts"})[["ts", "known_at", "symbol", "expiry", "strike", "cp", "micro_rel", "qi"]]


@register(_spec("fut_ofi", "C5: best-level order flow imbalance of the front future per minute (Cont, Kukanov, Stoikov), over mean depth."))
def fut_ofi(ctx: Context, p: dict) -> pd.DataFrame:
    t = ctx.get("ticks_meta")
    if t is None or not len(t):
        return pd.DataFrame()
    f = t[t["instrument_type"] == "FUT"].copy()
    if f.empty:
        return pd.DataFrame()
    f = f[f["expiry"] == f.groupby(f["minute"].dt.normalize())["expiry"].transform("min")].sort_values("t_evt", kind="stable")
    b, a, qb, qa = f["bid"], f["ask"], f["bid_qty"], f["ask_qty"]
    day = f["t_evt"].dt.normalize()                                  # each session starts afresh: no difference across the night
    pb, pa, pqb, pqa = (x.groupby(day).shift(1) for x in (b, a, qb, qa))
    e = (np.where(b >= pb, qb, 0) - np.where(b <= pb, pqb, 0) - np.where(a <= pa, qa, 0) + np.where(a >= pa, pqa, 0))
    f["e"] = np.where(pb.isna(), 0, e)
    f["depth"] = (qb + qa) / 2
    g = f.groupby("minute").agg(ofi=("e", "sum"), depth=("depth", "mean"), mid=("mid", "last")).reset_index()
    g["ofi_norm"] = g["ofi"] / g["depth"].replace(0, np.nan)
    g["known_at"] = g["minute"] + ctx.bucket_td
    return g.rename(columns={"minute": "ts"})


@register(_spec("cost_log", "C6: effective spread 2 x side x (price - mid) / mid and the 5-minute realised spread of our own fills.",
                inputs=("fills", "ticks_fo"), kind="study"))
def cost_log(ctx: Context, p: dict) -> pd.DataFrame:
    fills = ctx.raw.get("fills")
    t = ctx.get("ticks_meta")
    cols = ["ts", "known_at", "symbol", "side", "price", "qty", "mid", "effective_spread", "realised_spread_5m"]
    if fills is None or not len(fills) or t is None or not len(t):
        return pd.DataFrame(columns=cols)
    q = t[["t_evt", "symbol", "mid"]].sort_values("t_evt")
    f = fills.copy()
    f["ts"] = pd.to_datetime(f["ts"])
    f = pd.merge_asof(f.sort_values("ts"), q, left_on="ts", right_on="t_evt", by="symbol", direction="backward")
    later = q.rename(columns={"mid": "mid_5m", "t_evt": "t5"})
    f["t_after"] = f["ts"] + pd.Timedelta(minutes=5)
    f = pd.merge_asof(f.sort_values("t_after"), later, left_on="t_after", right_on="t5", by="symbol", direction="backward")
    f["effective_spread"] = 2 * f["side"] * (f["price"] - f["mid"]) / f["mid"]
    f["realised_spread_5m"] = 2 * f["side"] * (f["price"] - f["mid_5m"]) / f["mid"]
    f["known_at"] = f["t_after"]
    return f[cols]


@register(_spec("illiquidity", "C7: relative spread x max(1, 10 lots / touch size in lots), per contract-minute.", params={"touch_ref": TOUCH_REF}))
def illiquidity(ctx: Context, p: dict) -> pd.DataFrame:
    t = ctx.get("ticks_meta")
    if t is None or not len(t):
        return pd.DataFrame()
    m = _per_minute(t)
    touch_lots = np.minimum(m["bid_qty"], m["ask_qty"]) / m["lot"].replace(0, np.nan)
    m["illiq"] = m["rel_spread"] * np.maximum(1.0, p["touch_ref"] / touch_lots.replace(0, np.nan))
    m["known_at"] = m["minute"] + ctx.bucket_td
    return m.rename(columns={"minute": "ts"})[["ts", "known_at", "symbol", "expiry", "strike", "cp", "rel_spread", "illiq"]]


@register(Spec(id="amihud", version=1, family="live", grid="daily", inputs=("chain",), params={"max_k": 0.03}, tickets=("SCRUM-181",),
               doc="C7 history proxy: per near-money contract-session, mean |1-minute return| over premium traded (crore)."))
def amihud(ctx: Context, p: dict) -> pd.DataFrame:
    ch = ctx.get("chain")
    if ch is None or not len(ch):
        return pd.DataFrame()
    c = ch[(ch["k"].abs() <= p["max_k"]) & (ch["age"] == 0)].sort_values("ts").copy()
    c["date"] = c["ts"].dt.date
    key = ["date", "expiry", "strike", "cp"]
    c["ar"] = np.log(c["close"]).groupby([c[k] for k in key]).diff().abs()
    c["prem_cr"] = c["close"] * c["volume"] * c["lot"].fillna(1) / 1e7
    g = c.groupby(key).agg(ar=("ar", "mean"), prem=("prem_cr", "sum"), k=("k", "median")).reset_index()
    g["amihud"] = g["ar"] / g["prem"].replace(0, np.nan)
    g["known_at"] = pd.to_datetime(g["date"]) + pd.Timedelta(hours=10)
    return g


def lee_ready(t: pd.DataFrame) -> pd.DataFrame:
    """Sign each volume increment: trade price above the prevailing mid (the quote before this update) is a buy, below a sell, at the
    mid the tick rule (up from the last different price = buy).  Needs t_evt, symbol, ltp, vol_today, bid, ask."""
    t = t.sort_values(["symbol", "t_evt"]).copy()
    g = t.groupby("symbol")
    t["dv"] = g["vol_today"].diff().clip(lower=0).fillna(0)
    prev_mid = (g["bid"].shift(1) + g["ask"].shift(1)) / 2
    tick = np.sign(t["ltp"] - g["ltp"].shift(1)).replace(0, np.nan)
    tick = tick.groupby(t["symbol"]).ffill().fillna(0)
    sign = np.where(t["ltp"] > prev_mid, 1, np.where(t["ltp"] < prev_mid, -1, tick))
    t["sign"] = np.where(prev_mid.isna(), tick, sign)
    t["signed"] = t["sign"] * t["dv"]
    return t


@register(_spec("signed_flow", "C8, B4: Lee-Ready signed option volume per minute over the chain, delta x lot and vega weighted."))
def signed_flow(ctx: Context, p: dict) -> pd.DataFrame:
    t = ctx.get("ticks_meta")
    ch = ctx.get("chain_live")
    if t is None or not len(t) or ch is None or not len(ch):
        return pd.DataFrame()
    o = lee_ready(t[t["instrument_type"].isin(["CE", "PE"])])
    m = o.groupby(["minute", "expiry", "strike", "instrument_type"]).agg(signed=("signed", "sum"), lot=("lot_size", "first")).reset_index()
    m["cp"] = np.where(m["instrument_type"] == "CE", "C", "P")
    m = m.merge(ch[["ts", "expiry", "strike", "cp", "delta", "vega"]].rename(columns={"ts": "minute"}), on=["minute", "expiry", "strike", "cp"], how="left")
    m["delta_flow"] = m["signed"] * m["delta"].fillna(0) * m["lot"].fillna(1)
    m["vega_flow"] = m["signed"] * m["vega"].fillna(0) * m["lot"].fillna(1) / 100.0
    out = m.groupby("minute").agg(signed_volume=("signed", "sum"), delta_flow=("delta_flow", "sum"), vega_flow=("vega_flow", "sum")).reset_index()
    out["known_at"] = out["minute"] + ctx.bucket_td
    return out.rename(columns={"minute": "ts"})


def _live_gamma_chain(ctx: Context) -> pd.DataFrame:
    ch = ctx.get("chain_live")
    if ch is None or not len(ch):
        return pd.DataFrame()
    ch = ch.copy()
    ch["gamma_use"] = ch["gamma_vendor"].where(ch["gamma_vendor"].notna() & (ch["gamma_vendor"] != 0), ch["gamma"])
    return ch


@register(_spec("pcr_full", "B2 full chain: put/call OI and volume ratios over all strikes, front expiry.", inputs=("ticks_fo",), grid="snap"))
def pcr_full(ctx: Context, p: dict) -> pd.DataFrame:
    return pcr_table(ctx.get("chain_live"), np.inf, ctx.snaps, ctx.bucket_td)


@register(_spec("gex_full", "B5 full chain: gamma exposure over all strikes and both expiries with Upstox gamma (our gamma where it is "
                "missing), US and participant conventions, and the flip level.", inputs=("ticks_fo", "participant"), grid="snap"))
def gex_full(ctx: Context, p: dict) -> pd.DataFrame:
    ch = _live_gamma_chain(ctx)
    if not len(ch):
        return ch
    return gex_table(ch, ctx.raw.get("participant"), np.inf, ctx.r, gamma_col="gamma_use", snaps=ctx.snaps, bucket=ctx.bucket_td,
                     since=ctx.since)


@register(_spec("pin_full", "B1 full chain: on expiry day, the strike with the most OI (calls + puts) over all strikes, and the forward's "
                "distance from it.", inputs=("ticks_fo",)))
def pin_full(ctx: Context, p: dict) -> pd.DataFrame:
    ch = ctx.get("chain_live")
    if ch is None or not len(ch):
        return pd.DataFrame()
    first = ch.groupby("ts")["expiry"].transform("min")
    c = ch[(ch["expiry"] == first) & (pd.to_datetime(ch["expiry"]) == ch["ts"].dt.normalize())]
    if c.empty:
        return pd.DataFrame()
    s = c.groupby(["ts", "strike"], as_index=False).agg(oi=("oi", "sum"), fwd=("fwd", "first"))
    top = s.loc[s.groupby("ts")["oi"].idxmax()].rename(columns={"strike": "pin_oi"})
    top["dist_oi"] = (top["fwd"] - top["pin_oi"]) / top["fwd"]
    top["known_at"] = top["ts"] + ctx.bucket_td
    return top[["ts", "known_at", "pin_oi", "fwd", "dist_oi"]]


@register(_spec("rr25", "A12: 25-delta put IV minus 25-delta call IV per expiry, from Upstox delta and IV, at the snapshots.", grid="snap"))
def rr25(ctx: Context, p: dict) -> pd.DataFrame:
    from signals.options.positioning import _at_snaps
    ch = ctx.get("chain_live")
    if ch is None or not len(ch) or "delta_vendor" not in ch.columns:
        return pd.DataFrame()
    s = _at_snaps(ch.dropna(subset=["delta_vendor", "iv_vendor"]), ctx.snaps, ctx.bucket_td)
    rows = []
    for (d, sn, ka, e), g in s.groupby(["date", "snap", "known_at", "expiry"]):
        c = g[g["cp"] == "C"].sort_values("delta_vendor")
        pp = g[g["cp"] == "P"].sort_values("delta_vendor")
        iv_c = np.interp(0.25, c["delta_vendor"], c["iv_vendor"]) if len(c) >= 2 and c["delta_vendor"].min() <= 0.25 <= c["delta_vendor"].max() else np.nan
        iv_p = np.interp(-0.25, pp["delta_vendor"], pp["iv_vendor"]) if len(pp) >= 2 and pp["delta_vendor"].min() <= -0.25 <= pp["delta_vendor"].max() else np.nan
        rows.append({"date": d, "snap": sn, "known_at": ka, "expiry": e, "iv_25c": iv_c, "iv_25p": iv_p, "rr25": iv_p - iv_c})
    return pd.DataFrame(rows)


def minute_index(ts: pd.Series) -> pd.Series:
    return minute_of_session(ts)


# ---------------------------------------------------------------------------------------------- order book levels 2 to 5 (SCRUM-193)
LEVELS = 5


def _book_cols(t: pd.DataFrame) -> bool:
    return all(c in t.columns for c in ("bid2", "ask2", "bid2_qty", "ask2_qty")) and t["bid2"].notna().any()


def _level(t: pd.DataFrame, side: str, i: int, what: str) -> pd.Series:
    if i == 1:
        col = side if what == "px" else f"{side}_qty"
    else:
        col = f"{side}{i}" if what == "px" else f"{side}{i}_qty"
    return t[col] if col in t.columns else pd.Series(np.nan, index=t.index)


@register(Spec(id="depth_features", version=1, family="live", grid="1m", inputs=("ticks_fo",), live_only=True, min_sample=MIN_SAMPLE,
               params={"levels": LEVELS, "withdraw_window": 15}, tickets=("SCRUM-193",),
               doc="Per contract-minute from the 5-level book: depth imbalance over 5 levels, book slope (quantity per unit of price "
                   "away from the mid), total depth and its change against the previous 15 minutes (liquidity withdrawal), and "
                   "quote refills after trades at the touch (an iceberg proxy).  Empty until JARVIS_DEPTH5 is on."))
def depth_features(ctx: Context, p: dict) -> pd.DataFrame:
    t = ctx.get("ticks_meta")
    if t is None or not len(t) or not _book_cols(t):
        return pd.DataFrame()
    t = t.sort_values("t_evt", kind="stable").copy()
    bq = sum(_level(t, "bid", i, "qty").fillna(0) for i in range(1, p["levels"] + 1))
    aq = sum(_level(t, "ask", i, "qty").fillna(0) for i in range(1, p["levels"] + 1))
    t["depth_imb"] = (bq - aq) / (bq + aq).replace(0, np.nan)
    t["depth"] = bq + aq
    far_b = _level(t, "bid", p["levels"], "px")
    far_a = _level(t, "ask", p["levels"], "px")
    span = ((far_a - far_b) / t["mid"]).replace(0, np.nan)
    t["slope"] = (bq + aq) / span                                  # quantity per relative price distance across the visible book
    g = t.groupby(["symbol", t["t_evt"].dt.normalize()])
    prev_bid_q = g["bid_qty"].shift(1)
    prev_bid = g["bid"].shift(1)
    sold_at_bid = (t["ltp"] <= prev_bid) & (g["vol_today"].diff() > 0) if "vol_today" in t.columns else pd.Series(False, index=t.index)
    t["refill"] = (sold_at_bid & (t["bid"] == prev_bid) & (t["bid_qty"] >= prev_bid_q)).astype(int)
    m = t.groupby(["minute", "symbol"]).agg(depth_imb=("depth_imb", "mean"), slope=("slope", "median"), depth=("depth", "mean"),
                                             refills=("refill", "sum"), cp=("instrument_type", "first"), strike=("strike", "first"),
                                             expiry=("expiry", "first")).reset_index()
    m = m.sort_values("minute")
    n = max(1, int(pd.Timedelta(minutes=p["withdraw_window"]) / ctx.bucket_td))          # the window is in minutes whatever the bucket
    base = m.groupby(["symbol", m["minute"].dt.normalize()])["depth"].transform(lambda x: x.shift(1).rolling(n, min_periods=min(5, n)).median())
    m["depth_change"] = m["depth"] / base - 1
    m["known_at"] = m["minute"] + ctx.bucket_td
    return m.rename(columns={"minute": "ts"})


@register(Spec(id="mlofi_future", version=1, family="live", grid="1m", inputs=("ticks_fo",), live_only=True, min_sample=MIN_SAMPLE,
               params={"levels": LEVELS}, tickets=("SCRUM-193",),
               doc="Multi-level order flow imbalance of the front future per minute (Xu, Gould, Howison; Cont et al.): the best-level OFI "
                   "computed at each of 5 levels, each normalised by mean depth.  Empty until JARVIS_DEPTH5 is on."))
def mlofi_future(ctx: Context, p: dict) -> pd.DataFrame:
    t = ctx.get("ticks_meta")
    if t is None or not len(t) or not _book_cols(t):
        return pd.DataFrame()
    f = t[t["instrument_type"] == "FUT"].copy()
    if f.empty:
        return pd.DataFrame()
    f = f[f["expiry"] == f.groupby(f["minute"].dt.normalize())["expiry"].transform("min")].sort_values("t_evt", kind="stable")
    day = f["t_evt"].dt.normalize()
    out = None
    for i in range(1, p["levels"] + 1):
        b, a = _level(f, "bid", i, "px"), _level(f, "ask", i, "px")
        qb, qa = _level(f, "bid", i, "qty"), _level(f, "ask", i, "qty")
        pb, pa, pqb, pqa = (x.groupby(day).shift(1) for x in (b, a, qb, qa))
        e = (np.where(b >= pb, qb, 0) - np.where(b <= pb, pqb, 0) - np.where(a <= pa, qa, 0) + np.where(a >= pa, pqa, 0))
        f[f"e{i}"] = np.where(pb.isna(), 0, np.nan_to_num(e))
        f[f"d{i}"] = (qb + qa) / 2
        agg = f.groupby("minute").agg(**{f"ofi{i}": (f"e{i}", "sum"), f"depth{i}": (f"d{i}", "mean")})
        agg[f"ofi{i}_norm"] = agg[f"ofi{i}"] / agg[f"depth{i}"].replace(0, np.nan)
        out = agg if out is None else out.join(agg)
    out = out.reset_index().rename(columns={"minute": "ts"})
    out["mlofi"] = out[[f"ofi{i}_norm" for i in range(1, p["levels"] + 1)]].mean(axis=1)
    out["known_at"] = out["ts"] + ctx.bucket_td
    return out
