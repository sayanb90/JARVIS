"""
A synthetic market with known answers, for tests and for a smoke run of the whole catalogue without the lake.

make_raw() returns the raw inputs a Context expects for one underlying:
  index_1m   GBM index minutes (volatility `sigma`, an overnight share of the variance) for `index_days` sessions
  vix_1m     a VIX-like level around 100 * sigma
  fut_1m     two monthly futures (last Tuesday of the month), priced at the index carried at rate r
  opt_1m     options on the two nearest weekly expiries (Tuesdays) for the last `option_days` sessions: strikes on a fixed ladder around
             the index, priced by Black-76 with a smile sigma(k) = sigma + skew * (-k), some minutes without a trade (thin strikes)
  contracts  every option and future with strike, expiry and lot
  ticks_fo   level-1 ticks (bid, ask, sizes, last price, cumulative volume, open interest, vendor IV and Greeks) for the last `tick_days`
  participant  daily participant positions (Client, DII, FII, Pro)
  events     a few events
  weights, stocks_1m  n_stocks constituents that track the index (small idiosyncratic noise) and their weights
Everything is deterministic for a given seed.
"""

from __future__ import annotations

from datetime import date, timedelta

import numpy as np
import pandas as pd

from analytics import black76

OPEN = pd.Timedelta(hours=3, minutes=45)
SESSION = 375


def _tuesdays(first: date, last: date) -> list[date]:
    d = first + timedelta(days=(1 - first.weekday()) % 7)
    out = []
    while d <= last:
        out.append(d)
        d += timedelta(days=7)
    return out


def _last_tuesdays(first: date, months: int) -> list[date]:
    out = []
    y, m = first.year, first.month
    for _ in range(months):
        nxt = date(y + (m // 12), m % 12 + 1, 1)
        d = nxt - timedelta(days=1)
        while d.weekday() != 1:
            d -= timedelta(days=1)
        out.append(d)
        y, m = nxt.year, nxt.month
    return out


def _sym(u, k, cp, e):
    return f"{u}_{int(k)}_{cp}_{e:%d}_{e:%b}_{e:%y}".upper()


def make_raw(underlying: str = "NIFTY", end: str = "2026-03-31", index_days: int = 300, option_days: int = 12, tick_days: int = 3,
             sigma: float = 0.13, skew: float = 0.6, r: float = 0.065, s0: float = 24000.0, step: float = 100.0, n_strikes: int = 4,
             lot: int = 65, seed: int = 7, plant_pattern: bool = False, n_stocks: int = 5) -> dict[str, pd.DataFrame]:
    """`plant_pattern` plants the expiry-day pattern of SEBI's 2025 order on the last Tuesday (an expiry) of the option days: the index
    and its constituents pushed up 0.9 % over the first three hours on heavy constituent buying, put OI built in the morning, then sold
    down 1.3 % into the close."""
    rng = np.random.default_rng(seed)
    days = list(pd.bdate_range(end=end, periods=index_days))
    mins = pd.to_timedelta(np.arange(SESSION), unit="min")
    tuesdays = [d for d in days[-option_days:] if d.weekday() == 1]
    plant_day = tuesdays[-1] if (plant_pattern and tuesdays) else None
    push = np.where(np.arange(SESSION) < 180, 0.009 / 180, -0.013 / (SESSION - 180))
    idx_rows, px = [], s0
    for d in days:
        px *= np.exp(rng.normal(-0.5 * sigma ** 2 * 0.2 / 252, sigma * np.sqrt(0.2 / 252)))
        r_m = rng.normal(0, sigma * np.sqrt(0.8 / 252 / SESSION), SESSION)
        if plant_day is not None and d == plant_day:
            r_m = r_m + push
        c = px * np.exp(np.cumsum(r_m))
        o = np.r_[px, c[:-1]]
        idx_rows.append(pd.DataFrame({"ts": d + OPEN + mins, "open": o, "high": np.maximum(o, c) * (1 + 1e-5), "low": np.minimum(o, c) * (1 - 1e-5), "close": c}))
        px = c[-1]
    index_1m = pd.concat(idx_rows, ignore_index=True)
    index_1m["symbol"] = {"NIFTY": "NSE_INDEX:Nifty 50", "BANKNIFTY": "NSE_INDEX:Nifty Bank"}.get(underlying, underlying)
    vix = index_1m[["ts"]].copy()
    vix["close"] = 100 * sigma * (1 + 0.05 * np.sin(np.arange(len(vix)) / 5000.0)) + rng.normal(0, 0.05, len(vix))
    vix["open"], vix["high"], vix["low"] = vix["close"], vix["close"] + 0.02, vix["close"] - 0.02
    vix["symbol"] = "NSE_INDEX:India VIX"

    first_d, last_d = days[0].date(), days[-1].date()
    fut_exps = _last_tuesdays(first_d - timedelta(days=31), 16)
    contracts, fut_rows = [], []
    spot = index_1m.set_index("ts")["close"]
    for e in fut_exps:
        sym = f"{underlying}_FUT_{e:%d}_{e:%b}_{e:%y}".upper()
        contracts.append({"symbol": sym, "underlying": underlying, "instrument_type": "FUT", "strike": np.nan, "expiry": e, "lot_size": lot})
        live = spot[(spot.index.normalize() <= pd.Timestamp(e)) & (spot.index.normalize() > pd.Timestamp(e) - pd.Timedelta(days=95))]
        if not len(live):
            continue
        t = ((pd.Timestamp(e) + pd.Timedelta(hours=10) - (live.index + pd.Timedelta(minutes=1))) / pd.Timedelta(days=365)).to_numpy()
        fut_rows.append(pd.DataFrame({"ts": live.index, "symbol": sym, "open": live.values * np.exp(r * t), "high": live.values * np.exp(r * t),
                                      "low": live.values * np.exp(r * t), "close": live.values * np.exp(r * t),
                                      "volume": rng.integers(50, 500, len(live)), "oi": 1_000_000 + rng.integers(-1000, 1000, len(live)).cumsum()}))
    fut_1m = pd.concat(fut_rows, ignore_index=True) if fut_rows else pd.DataFrame()

    opt_days = days[-option_days:]
    weekly = _tuesdays(opt_days[0].date(), last_d + timedelta(days=14))
    ladder_mid = round(float(spot[spot.index.normalize() == opt_days[0]].iloc[0]) / step) * step
    strikes = ladder_mid + step * np.arange(-n_strikes, n_strikes + 1)
    opt_rows, oi_state = [], {}
    for d in opt_days:
        exps = [e for e in weekly if e >= d.date()][:2]
        s_day = spot[spot.index.normalize() == d]
        for e in exps:
            t = ((pd.Timestamp(e) + pd.Timedelta(hours=10) - (s_day.index + pd.Timedelta(minutes=1))) / pd.Timedelta(days=365)).to_numpy()
            t = np.maximum(t, 1e-6)
            F = s_day.values * np.exp(r * t)
            for k in strikes:
                kk = np.log(k / F)
                vol = sigma + skew * np.maximum(-kk, 0) * 1.0 + 0.3 * skew * np.maximum(kk, 0)
                for cp in ("C", "P"):
                    sym = _sym(underlying, k, "CE" if cp == "C" else "PE", e)
                    p = black76.price(cp == "C", F, k, t, vol, r)
                    p = np.round(np.maximum(p, 0.05) / 0.05) * 0.05
                    away = abs(k - ladder_mid) / step
                    traded = rng.random(SESSION) > min(0.6, 0.08 * away)
                    oi0 = oi_state.get(sym, int(200_000 / (1 + away)))
                    oi = oi0 + rng.integers(-500, 600, SESSION).cumsum()
                    if plant_day is not None and d == plant_day and cp == "P":
                        oi = oi + np.where(np.arange(SESSION) < 180, 3000, 0).cumsum()
                    oi_state[sym] = int(oi[-1])
                    vol_n = rng.poisson(2000 / (1 + away), SESSION)
                    opt_rows.append(pd.DataFrame({"ts": s_day.index[traded], "symbol": sym, "open": p[traded], "high": p[traded], "low": p[traded],
                                                  "close": p[traded], "volume": vol_n[traded], "oi": oi[traded]}))
    opt_1m = pd.concat(opt_rows, ignore_index=True)
    for e in weekly:
        for k in strikes:
            for cp in ("CE", "PE"):
                contracts.append({"symbol": _sym(underlying, k, cp, e), "underlying": underlying, "instrument_type": cp, "strike": float(k),
                                  "expiry": e, "lot_size": lot})
    contracts = pd.DataFrame(contracts)

    stock_rows, weight_rows = [], []
    wts = np.array([0.30, 0.25, 0.20, 0.15, 0.10])[:n_stocks]
    wts = wts / wts.sum() if len(wts) else wts
    base = index_1m.set_index("ts")["close"]
    for i in range(len(wts)):
        sym = f"S{i + 1}.NS"
        p0 = 500.0 * (i + 2)
        noise = np.cumsum(rng.normal(0, 2e-5, len(base)))
        c = p0 * base.to_numpy() / base.iloc[0] * np.exp(noise)
        o = np.r_[c[0], c[:-1]]
        vol = rng.poisson(5000, len(base)).astype(float)
        if plant_day is not None:
            on = (base.index.normalize() == plant_day) & ((base.index - plant_day - OPEN) < pd.Timedelta(minutes=180))
            vol[on] *= 5
        stock_rows.append(pd.DataFrame({"ts": base.index, "symbol": sym, "open": o, "high": np.maximum(o, c), "low": np.minimum(o, c),
                                        "close": c, "volume": vol.astype("int64")}))
        weight_rows.append({"date": days[0].date(), "symbol": sym, "weight": float(wts[i])})
    stocks_1m = pd.concat(stock_rows, ignore_index=True) if stock_rows else pd.DataFrame()
    weights = pd.DataFrame(weight_rows)
    if len(weights):
        weights["known_at"] = pd.to_datetime(weights["date"]) + pd.Timedelta(hours=10)

    tick_rows = []
    o_last = opt_1m[opt_1m["ts"].dt.normalize().isin(opt_days[-tick_days:])]
    meta = contracts.set_index("symbol")
    for sym, g in o_last.groupby("symbol"):
        g = g.sort_values("ts")
        strike_v, cp_v, exp_v = meta.loc[sym, "strike"], meta.loc[sym, "instrument_type"], meta.loc[sym, "expiry"]
        tt = np.maximum(((pd.Timestamp(exp_v) + pd.Timedelta(hours=10) - (g["ts"] + pd.Timedelta(minutes=1))) / pd.Timedelta(days=365)).to_numpy(), 1e-6)
        FF = spot.reindex(g["ts"]).to_numpy() * np.exp(r * tt)
        kk = np.log(strike_v / FF)
        vol_true = sigma + skew * np.maximum(-kk, 0) + 0.3 * skew * np.maximum(kk, 0)
        gk = black76.greeks(cp_v == "CE", FF, strike_v, tt, vol_true, r)
        half = np.maximum(0.05, np.round(g["close"].to_numpy() * 0.004 / 0.05) * 0.05)
        ts = g["ts"] + pd.Timedelta(seconds=30)
        ltp = g["close"].to_numpy() + rng.choice([-1, 1], len(g)) * half
        tick_rows.append(pd.DataFrame({"ts": ts, "fts": ts, "symbol": sym, "ltp": ltp, "vol_today": g.groupby(g["ts"].dt.normalize())["volume"].cumsum().to_numpy(),
                                       "bid": g["close"].to_numpy() - half, "ask": g["close"].to_numpy() + half,
                                       "bid_qty": rng.integers(65, 6500, len(g)), "ask_qty": rng.integers(65, 6500, len(g)), "oi": g["oi"].to_numpy(),
                                       "ltq": lot * rng.integers(1, 40, len(g)),
                                       "iv": np.round(vol_true, 4), "delta": np.round(gk["delta"], 4), "gamma": np.round(gk["gamma"], 4)}))
    f_last = fut_1m[fut_1m["ts"].dt.normalize().isin(opt_days[-tick_days:])] if len(fut_1m) else pd.DataFrame()
    if len(f_last):
        for sym, g in f_last.groupby("symbol"):
            ts = g["ts"] + pd.Timedelta(seconds=30)
            tick_rows.append(pd.DataFrame({"ts": ts, "fts": ts, "symbol": sym, "ltp": g["close"].to_numpy(), "vol_today": g["volume"].cumsum().to_numpy(),
                                           "ltq": lot * rng.integers(1, 10, len(g)),
                                           "bid": g["close"].to_numpy() - 0.05, "ask": g["close"].to_numpy() + 0.05,
                                           "bid_qty": rng.integers(65, 3000, len(g)), "ask_qty": rng.integers(65, 3000, len(g)), "oi": g["oi"].to_numpy(),
                                           "iv": np.nan, "delta": np.nan, "gamma": np.nan}))
    ticks_fo = pd.concat(tick_rows, ignore_index=True) if tick_rows else pd.DataFrame()

    part_rows = []
    for d in days:
        base = {"Client": (300_000, 250_000, 280_000, 320_000), "DII": (1_000, 2_000, 500, 400), "FII": (90_000, 110_000, 100_000, 80_000),
                "Pro": (150_000, 140_000, 160_000, 160_000)}
        for p, (cl, pl, cs, ps) in base.items():
            j = rng.integers(-5000, 5000, 4)
            part_rows.append({"date": d.date(), "participant": p, "option_index_call_long": cl + j[0], "option_index_put_long": pl + j[1],
                              "option_index_call_short": cs + j[2], "option_index_put_short": ps + j[3],
                              "future_index_long": 50_000, "future_index_short": 50_000})
    participant = pd.DataFrame(part_rows)
    participant["known_at"] = pd.to_datetime(participant["date"]) + pd.Timedelta(hours=16)

    events = pd.DataFrame([{"date": opt_days[-3].date(), "kind": "rbi_mpc", "time_ist": "10:00", "description": "synthetic MPC"},
                           {"date": days[-60].date(), "kind": "budget", "time_ist": "11:00", "description": "synthetic budget"}])
    return {"index_1m": index_1m, "vix_1m": vix, "fut_1m": fut_1m, "opt_1m": opt_1m, "contracts": contracts, "ticks_fo": ticks_fo,
            "participant": participant, "events": events, "weights": weights, "stocks_1m": stocks_1m}
