"""
The viewer's Signals tab (SCRUM-199, built out in SCRUM-206): every live signal of an underlying, its evolution through the day, the per-contract
signals by strike, the chain views, and the signals' history across days.  Read only: the live table (<store>/live/date=D/, read once and kept by
webapp/signals_store.py), the signal store written by the nightly job (<store>/signals/...) and the lake's option ticks (for the IV smile).

  summary(store, underlying, day)            one entry per signal and bucket width: family, doc, kind (single | contract | events), the latest values,
                                             the time-of-day z, a series for the sparkline; the days that have live rows; the daily signals
  series(store, underlying, signal, bucket)  the day's series of every numeric column (and its z), and the previous session's for comparison
  contracts(store, underlying, signal, bucket, expiry, column)   a per-contract signal by strike (latest snapshot) and as strike x time
  chain(store, underlying)                   the chain signals (skew, curvature, risk reversal, term slope, PCR, GEX, VRP ...) as small series
  smile(lake, underlying, day, expiry)       the IV smile from the option ticks: the latest, and by strike over the last hours
  history(store, underlying, signal)         one row per day (open, close, low, high, mean of each column) and the headline over the last sessions
"""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

from webapp import signals_store as live

SKIP = {"signal", "bucket", "underlying", "symbol", "ts", "known_at", "date", "snap", "expiry", "next_expiry", "fc_at", "fc1_at", "pt_at",
        "iv7_how", "strike", "cp", "t", "next_t", "instrument_type", "oi_symbol", "oi_cp"}
HEADLINE = {"vrp": "vrp_diff", "vrp_intraday": "vrp_intraday", "rv_over_iv": "rv_over_iv", "implied_move": "move_ratio", "skew": "skew1_s",
            "term_slope": "slope", "gex": "gex_us", "gex_full": "gex_us", "pcr_near": "pcr_oi", "pcr_full": "pcr_oi", "rr25": "rr25",
            "fut_ofi": "ofi_norm", "mlofi_future": "mlofi", "book_pressure": "qi", "depth_features": "depth_imb", "signed_flow": "delta_flow",
            "residual_iv": "resid", "curvature": "curv", "cp_iv_gap": "gap", "event_variance": "event_var", "noise_band": "signal_value",
            "illiquidity": "illiq", "spread_gate": "rel_spread", "fair_value_gap": "buy_edge", "block_prints": "lots"}
MAX_POINTS = 400
BUS_ONLY = {"t_vendor", "t_recv", "t_emit", "provisional"}
FAMILY_LABEL = {"live": "Book and tape", "flow": "Order flow", "surface": "Volatility surface", "vol_gate": "Volatility gate", "positioning": "Positioning",
                "expiry": "Expiry", "direction": "Direction"}
FAMILY_ORDER = ["live", "flow", "surface", "vol_gate", "positioning", "expiry", "direction"]
# the chain panel: (title, [(signal, column)])
CHAIN_PANELS = [
    ("Skew and shape", [("skew", "skew1_s"), ("curvature", "curv"), ("rr25", "rr25"), ("cp_iv_gap", "gap")]),
    ("Term structure and events", [("term_slope", "slope"), ("event_variance", "event_var"), ("residual_iv", "resid")]),
    ("Positioning", [("pcr_near", "pcr_oi"), ("pcr_full", "pcr_oi"), ("gex", "gex_us"), ("gex_full", "gex_us"), ("pin", None), ("pin_full", None)]),
    ("Volatility", [("vrp", "vrp_diff"), ("vrp_intraday", "vrp_intraday"), ("implied_move", "move_ratio"), ("rv_over_iv", "rv_over_iv"), ("vol_size", None)]),
]
SERIES_COLUMNS = 8


def _num(v) -> Optional[float]:
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return None if not np.isfinite(f) else round(f, 6)


def _values(row: pd.Series) -> dict:
    return {c: _num(v) for c, v in row.items() if c not in SKIP and c not in BUS_ONLY and not c.endswith("_tod_z")
            and _num(v) is not None}


def _numeric_columns(df: pd.DataFrame) -> list:
    return [c for c in df.columns if c not in SKIP and c not in BUS_ONLY and not c.endswith("_tod_z") and pd.api.types.is_numeric_dtype(df[c])]


def _headline(sid: str, df: pd.DataFrame) -> Optional[str]:
    h = HEADLINE.get(sid)
    if h in df.columns:
        return h
    num = _numeric_columns(df)
    return num[0] if num else None


def _meta(sid: str) -> dict:
    """family, label and doc of a signal from the registry (empty for a signal the registry does not know)."""
    try:
        from signals import catalogue  # noqa: F401
        from signals.framework import REGISTRY
        spec = REGISTRY[sid][0]
        return {"family": spec.family, "family_label": FAMILY_LABEL.get(spec.family, spec.family.replace("_", " ").title()), "doc": spec.doc or ""}
    except Exception:                                                                     # noqa: BLE001
        return {"family": "other", "family_label": "Other", "doc": ""}


def _ts(x) -> str:
    return pd.Timestamp(x).isoformat()


def _thin(df: pd.DataFrame, n: int) -> pd.DataFrame:
    """At most n rows, evenly spread, the newest always included."""
    if len(df) <= n:
        return df
    return df.iloc[np.unique(np.linspace(0, len(df) - 1, n).astype(int))]


def _section(ld: "live.LiveDay", sid: str, bucket: str, und: str):
    """(kind, frame) the tab draws a signal's day from: the rows of a single-row signal; the futures rows of a per-contract signal when it has
    them, else the cross-section median of each numeric column at every kept snapshot; the events of block prints."""
    f = ld.frame("single", sid, bucket, und)
    if f is not None:
        return "single", f
    ev = ld.frame("events", sid, bucket, und)
    if ev is not None:
        return "events", ev
    fut = ld.frame("fut", sid, bucket, und)
    hist, last = ld.frame("hist", sid, bucket, und), ld.frame("last", sid, bucket, und)
    parts = [x for x in (hist, last) if x is not None]
    if parts:
        opt = pd.concat(parts, ignore_index=True).drop_duplicates(subset=["known_at", "symbol"], keep="last")
        num = _numeric_columns(opt)
        med = opt.groupby("known_at")[num].median().reset_index() if num else pd.DataFrame({"known_at": opt["known_at"].unique()})
        med["contracts"] = opt.groupby("known_at")["symbol"].nunique().to_numpy()
        return "contract", (fut if fut is not None and len(fut) else med).sort_values("known_at").reset_index(drop=True)
    if fut is not None:
        return "contract", fut
    return None, None


def _day(day) -> date:
    return day or datetime.utcnow().date()


def summary(store: Path, underlying: str, day: Optional[date] = None, now: Optional[datetime] = None, wait_s: float = 20.0) -> dict:
    day = _day(day)
    now = pd.Timestamp(now or datetime.utcnow())
    ld = live.get_day(store, day, wait_s)
    out = {"underlying": underlying, "date": str(day), "cadence": [], "newest": None, "stale": True, "signals": [], "daily": [],
           "days": live.live_days(store), "load": ld.progress()}
    widths, newest = set(), None
    for sid, bucket, _kind in ld.signals(underlying):
        kind, df = _section(ld, sid, bucket, underlying)
        if df is None or df.empty:
            continue
        widths.add(bucket)
        last = df.iloc[-1]
        newest = last["known_at"] if newest is None else max(newest, last["known_at"])
        head = _headline(sid, df)
        ser = _thin(df[["known_at", head]].dropna(), MAX_POINTS) if head and kind != "events" else pd.DataFrame()
        entry = {"signal": sid, "bucket": bucket, "kind": kind, **_meta(sid),
                 "ts": pd.Timestamp(last["ts"]).isoformat() if "ts" in df.columns and pd.notna(last.get("ts")) else None,
                 "known_at": last["known_at"].isoformat(), "values": _values(last),
                 "z": {c[:-6]: _num(last[c]) for c in df.columns if c.endswith("_tod_z") and _num(last[c]) is not None},
                 "headline": head, "columns": _numeric_columns(df), "n": int(len(df)),
                 "series": [[k.isoformat(), _num(v)] for k, v in zip(ser["known_at"], ser[head])] if head and kind != "events" else []}
        if kind == "contract":
            lastsnap = ld.frame("last", sid, bucket, underlying)
            entry["contracts"] = int(len(lastsnap)) if lastsnap is not None else 0
            entry["front_future"] = ld.frame("fut", sid, bucket, underlying) is not None
        if kind == "events":
            entry["events"] = int(len(df))
        out["signals"].append(entry)
    if widths:
        out["cadence"] = sorted(widths, key=pd.Timedelta)
        out["newest"] = newest.isoformat()
        out["stale"] = bool(now - newest > max(2 * pd.Timedelta(out["cadence"][0]), pd.Timedelta(seconds=10)))        # the tab refreshes every 3 s: 2 s was always 'stale' on a live 1 s day
    fam = {f: i for i, f in enumerate(FAMILY_ORDER)}
    out["signals"].sort(key=lambda s: (fam.get(s["family"], 99), s["signal"], pd.Timedelta(s["bucket"])))
    out["daily"] = _daily(store, underlying, day)
    return out


def _layer(bucket: str) -> str:
    return {"1s": "1s", "10s": "10s"}.get(bucket, "1m")


def _spec(sid: str):
    try:
        from signals import catalogue  # noqa: F401
        from signals.framework import REGISTRY
        return REGISTRY[sid][0]
    except Exception:                                                                     # noqa: BLE001
        return None


def _intraday_files(store: Path, sid: str, bucket: str, underlying: str) -> list:
    spec = _spec(sid)
    if spec is None:
        return []
    base = Path(store) / "signals" / sid / f"v{spec.version}" / "intraday" / f"bucket={_layer(bucket)}" / f"underlying={underlying}"
    return sorted(base.glob("date=*/part.parquet")) if base.is_dir() else []


def _read_cols(path: Path, cols: list) -> Optional[pd.DataFrame]:
    import pyarrow.parquet as pq
    try:
        have = set(pq.ParquetFile(path).schema.names)
        want = list(dict.fromkeys(c for c in cols if c in have))
        return pd.read_parquet(path, columns=want) if "known_at" in want else None
    except Exception:                                                                     # noqa: BLE001
        return None


def series(store: Path, underlying: str, signal: str, bucket: str, day: Optional[date] = None, columns: Optional[list] = None,
           max_points: int = 1500, wait_s: float = 20.0) -> dict:
    """The day's series of a signal: every numeric column (and its time-of-day z), the previous session's for comparison."""
    day = _day(day)
    ld = live.get_day(store, day, wait_s)
    kind, df = _section(ld, signal, bucket, underlying)
    out = {"underlying": underlying, "signal": signal, "bucket": bucket, "date": str(day), "kind": kind, **_meta(signal), "columns": [], "t": [],
           "series": {}, "z": {}, "prev": None, "n": 0, "load": ld.progress()}
    if df is None or df.empty:
        return out
    allc = _numeric_columns(df)
    head = _headline(signal, df)
    cols = [c for c in (columns or [head] + [c for c in allc if c != head]) if c in allc][:SERIES_COLUMNS]
    if kind == "events":
        out.update({"columns": allc, "n": int(len(df)), "events": [{"known_at": _ts(r["known_at"]), **{c: (r[c] if isinstance(r[c], str) else _num(r[c]))
                                                                                                       for c in df.columns if c in ("symbol", "instrument_type", "lots", "ltp", "sign", "oi_symbol", "oi_strike", "oi_cp", "oi_jump")}}
                                                                    for _, r in df.tail(200).iterrows()]})
        return out
    th = _thin(df, max_points)
    out.update({"columns": allc, "headline": head, "n": int(len(df)), "t": [_ts(x) for x in th["known_at"]],
                "series": {c: [_num(v) for v in th[c]] for c in cols},
                "z": {c: [_num(v) for v in th[c + "_tod_z"]] for c in cols if c + "_tod_z" in th.columns}})
    if kind == "single" or (kind == "contract" and ld.frame("fut", signal, bucket, underlying) is not None):
        out["prev"] = _previous(store, signal, bucket, underlying, day, cols, max_points)
    return out


def _previous(store: Path, sid: str, bucket: str, und: str, day: date, cols: list, n: int) -> Optional[dict]:
    """The previous session's series from the nightly history, shifted onto `day` so the two overlay by time of day."""
    files = [f for f in _intraday_files(store, sid, bucket, und) if f.parent.name.split("=", 1)[1] < str(day)]
    if not files:
        return None
    f = files[-1]
    df = _read_cols(f, ["known_at", "symbol"] + cols)
    if df is None or df.empty:
        return None
    if "symbol" in df.columns:
        if df["known_at"].duplicated().any():
            df = df[live.is_future(df["symbol"])]
    prev_day = pd.Timestamp(f.parent.name.split("=", 1)[1])
    df["known_at"] = pd.to_datetime(df["known_at"]) + (pd.Timestamp(day) - prev_day)
    th = _thin(df.sort_values("known_at"), n)
    return {"date": f.parent.name.split("=", 1)[1], "t": [_ts(x) for x in th["known_at"]],
            "series": {c: [_num(v) for v in th[c]] for c in cols if c in th.columns}}


def contracts(store: Path, underlying: str, signal: str, bucket: str, day: Optional[date] = None, expiry: Optional[str] = None,
              column: Optional[str] = None, wait_s: float = 20.0) -> dict:
    """A per-contract signal by strike: the newest snapshot (calls and puts), and one column as strike x time (one snapshot a minute)."""
    day = _day(day)
    ld = live.get_day(store, day, wait_s)
    last, hist = ld.frame("last", signal, bucket, underlying), ld.frame("hist", signal, bucket, underlying)
    out = {"underlying": underlying, "signal": signal, "bucket": bucket, "date": str(day), **_meta(signal), "columns": [], "expiries": [], "expiry": None,
           "column": None, "snapshot": None, "heat": None, "load": ld.progress()}
    if last is None and hist is None:
        return out
    ref = last if last is not None else hist
    num = _numeric_columns(ref)
    out["columns"] = num
    exps = sorted({str(e) for f in (hist, last) if f is not None and "expiry" in f.columns for e in f["expiry"].dropna().unique()})
    out["expiries"] = exps
    expiry = expiry if expiry in exps else (exps[0] if exps else None)
    out["expiry"] = expiry
    column = column if column in num else _headline(signal, ref)
    out["column"] = column

    def pick(df: pd.DataFrame) -> pd.DataFrame:
        if expiry is not None and "expiry" in df.columns:
            df = df[df["expiry"].astype(str) == expiry]
        return df

    parts = [x for x in (hist, last) if x is not None]
    if parts:
        # the newest row of every contract (not one instant: at 1 s only the few contracts that traded in the last second have a row, and the
        # nearest expiry may have none of them); the per-row time tells how old each value is
        both = pick(pd.concat(parts, ignore_index=True)).sort_values("known_at", kind="stable")
        snap = both.drop_duplicates(subset=[c for c in ("symbol", "strike", "cp") if c in both.columns] or ["symbol"], keep="last")
        rows = []
        for _, r in snap.sort_values([c for c in ("strike", "cp") if c in snap.columns] or ["symbol"]).iterrows():
            rows.append({"symbol": r.get("symbol"), "strike": _num(r.get("strike")), "cp": r.get("cp"), "expiry": str(r.get("expiry", "")), "at": _ts(r.get("known_at")),
                         "values": {c: _num(r.get(c)) for c in num if _num(r.get(c)) is not None},
                         "z": {c[:-6]: _num(r.get(c)) for c in snap.columns if c.endswith("_tod_z") and _num(r.get(c)) is not None}})
        out["snapshot"] = {"known_at": _ts(snap["known_at"].max()) if len(snap) else None, "rows": rows}
    if hist is not None and column is not None and "strike" in hist.columns and "cp" in hist.columns:
        h = pick(hist)
        h = pd.concat([h, pick(last)], ignore_index=True).drop_duplicates(subset=["known_at", "symbol"], keep="last") if last is not None else h
        h = h[h["strike"].notna()]
        if len(h):
            cp = h["cp"].astype(str).str[0]                                             # CE / PE / C / P
            strikes = sorted(h["strike"].unique())
            times = sorted(h["known_at"].unique())
            if len(times) > 400:
                times = times[np.unique(np.linspace(0, len(times) - 1, 400).astype(int))]
            heat = {"strikes": [float(s) for s in strikes], "times": [_ts(t) for t in times]}
            for side, label in (("C", "call"), ("P", "put")):
                x = h[cp == side]
                piv = x.pivot_table(index="strike", columns="known_at", values=column, aggfunc="last").reindex(index=strikes, columns=times)
                heat[label] = [[_num(v) for v in row] for row in piv.to_numpy()]
            out["heat"] = heat
    return out


def chain(store: Path, underlying: str, day: Optional[date] = None, max_points: int = 600, wait_s: float = 20.0) -> dict:
    """The chain signals as small series: panels of (signal, column) with the finest bucket width each has today."""
    day = _day(day)
    ld = live.get_day(store, day, wait_s)
    avail = {}
    for sid, bucket, _k in ld.signals(underlying):
        avail.setdefault(sid, []).append(bucket)
    panels = []
    for title, items in CHAIN_PANELS:
        charts = []
        for sid, col in items:
            if sid not in avail:
                continue
            bucket = sorted(avail[sid], key=pd.Timedelta)[0]
            kind, df = _section(ld, sid, bucket, underlying)
            if df is None or df.empty or kind == "events":
                continue
            c = col if col in df.columns else _headline(sid, df)
            if c is None:
                continue
            th = _thin(df[["known_at", c]].dropna(), max_points)
            z = df[c + "_tod_z"].iloc[-1] if c + "_tod_z" in df.columns else None
            charts.append({"signal": sid, "bucket": bucket, "column": c, **_meta(sid), "last": _num(df[c].iloc[-1]), "z": _num(z),
                           "t": [_ts(x) for x in th["known_at"]], "v": [_num(v) for v in th[c]]})
        if charts:
            panels.append({"title": title, "charts": charts})
    return {"underlying": underlying, "date": str(day), "panels": panels, "load": ld.progress()}


def smile(lake: Path, underlying: str, day: Optional[date] = None, expiry: Optional[str] = None, minutes: int = 240, step: int = 5) -> dict:
    """The IV smile from the option ticks (vendor IV): by strike at the newest bucket, and by strike and time (last IV of each `step` minutes)."""
    import duckdb
    from data_layer.tick_store import day_files
    day = _day(day)
    out = {"underlying": underlying, "date": str(day), "expiries": [], "expiry": None, "latest": None, "over_time": None, "atm": []}
    inst_files = [Path(lake) / "instruments" / n for n in ("fo.parquet", "fo_expired.parquet") if (Path(lake) / "instruments" / n).exists()]
    if not inst_files:
        return out
    inst = pd.concat([pd.read_parquet(f, columns=["symbol", "underlying", "instrument_type", "strike", "expiry"]) for f in inst_files], ignore_index=True)
    inst = inst[(inst["underlying"] == underlying) & inst["instrument_type"].isin(["CE", "PE"])].drop_duplicates("symbol")
    if inst.empty:
        return out
    inst["expiry"] = inst["expiry"].astype(str).str[:10]
    files = [f for f in day_files(Path(lake), day, "fo") if not f.name.endswith(".tmp")]
    if day == datetime.utcnow().date():                                                      # today: only the files of the window
        import time as _t
        files = [f for f in files if _t.time() - f.stat().st_mtime < (minutes + 20) * 60]
    if not files:
        return out
    con = duckdb.connect()
    con.execute("SET threads=2")
    con.execute("SET memory_limit='500MB'")
    q = "[" + ", ".join("'" + str(f).replace("'", "''") + "'" for f in files) + "]"
    cols = {r[0] for r in con.execute(f"DESCRIBE SELECT * FROM read_parquet({q}, union_by_name=true)").fetchall()}
    if "iv" not in cols:
        return out
    tcol = "fts" if "fts" in cols else "ts"
    syms = "[" + ", ".join("'" + s.replace("'", "''") + "'" for s in inst["symbol"]) + "]"
    df = con.execute(f"""
        SELECT symbol, time_bucket(INTERVAL '{int(step)} minutes', CAST({tcol} AS TIMESTAMP)) AS tb, arg_max(iv, CAST({tcol} AS TIMESTAMP)) AS iv
        FROM read_parquet({q}, union_by_name=true)
        WHERE iv IS NOT NULL AND iv > 0 AND list_contains({syms}, symbol)
        GROUP BY symbol, tb ORDER BY tb""").df()
    con.close()
    if df.empty:
        return out
    df = df.merge(inst[["symbol", "strike", "expiry", "instrument_type"]], on="symbol")
    if df["iv"].median() > 3:                                                                # the vendor sends percent
        df["iv"] = df["iv"] / 100
    df = df[df["tb"] >= df["tb"].max() - pd.Timedelta(minutes=minutes)]
    exps = sorted(df["expiry"].unique())
    out["expiries"] = exps
    expiry = expiry if expiry in exps else exps[0]
    out["expiry"] = expiry
    d = df[df["expiry"] == expiry]
    newest = d["tb"].max()
    cur = d[d["tb"] >= newest - pd.Timedelta(minutes=2 * step)].sort_values("tb").drop_duplicates("symbol", keep="last")
    strikes = sorted(cur["strike"].unique())
    byk = {side: cur[cur["instrument_type"] == side].set_index("strike")["iv"] for side in ("CE", "PE")}
    both = [k for k in strikes if k in byk["CE"].index and k in byk["PE"].index]
    atm = min(both, key=lambda k: abs(byk["CE"][k] - byk["PE"][k])) if both else (strikes[len(strikes) // 2] if strikes else None)
    keep = sorted(strikes, key=lambda k: abs(k - atm))[:61] if atm is not None else strikes
    keep = sorted(keep)
    out["latest"] = {"at": _ts(newest), "atm_strike": _num(atm), "strikes": [float(k) for k in keep],
                     "call": [_num(byk["CE"].get(k)) for k in keep], "put": [_num(byk["PE"].get(k)) for k in keep]}
    d = d[d["strike"].isin(keep)]
    times = sorted(d["tb"].unique())
    m = d.groupby(["strike", "tb"])["iv"].mean().unstack("tb").reindex(index=keep, columns=times)
    out["over_time"] = {"times": [_ts(t) for t in times], "strikes": [float(k) for k in keep], "iv": [[_num(v) for v in row] for row in m.to_numpy()]}
    near = m.loc[atm] if atm is not None and atm in m.index else None
    out["atm"] = [{"t": _ts(t), "iv": _num(v)} for t, v in zip(times, near)] if near is not None else []
    return out


def history(store: Path, underlying: str, signal: str, bucket: str = "1min", days: int = 30, paths: int = 8, points: int = 120) -> dict:
    """A signal across days from the nightly store: one summary row per day for every numeric column, and the headline of the last sessions
    on one time-of-day axis."""
    out = {"underlying": underlying, "signal": signal, "bucket": bucket, **_meta(signal), "columns": [], "headline": None, "days": [], "paths": [],
           "available": False}
    spec = _spec(signal)
    if spec is None:
        return out
    if spec.grid == "daily":
        base = Path(store) / "signals" / signal / f"v{spec.version}" / f"underlying={underlying}"
        files = sorted(base.glob("date=*/part.parquet"))[-days:] if base.is_dir() else []
        rows = []
        for f in files:
            df = pd.read_parquet(f)
            if len(df):
                rows.append({"date": f.parent.name.split("=", 1)[1], "values": _values(df.iloc[-1])})
        out.update({"available": bool(rows), "kind": "daily", "days": rows})
        return out
    files = _intraday_files(store, signal, bucket, underlying)[-days:]
    summary_rows, path_rows, columns, head = [], [], [], None
    for f in files:
        df = _read_cols(f, _wanted(f))
        if df is None or df.empty:
            continue
        if "symbol" in df.columns and df["known_at"].duplicated().any():
            df = df[live.is_future(df["symbol"])]
            if df.empty:
                continue
        df["known_at"] = pd.to_datetime(df["known_at"])
        num = _numeric_columns(df)
        if not num:
            continue
        head = head or _headline(signal, df)
        columns = columns or num
        stats = {c: {"open": _num(df[c].dropna().iloc[0]) if df[c].notna().any() else None, "close": _num(df[c].dropna().iloc[-1]) if df[c].notna().any() else None,
                     "low": _num(df[c].min()), "high": _num(df[c].max()), "mean": _num(df[c].mean())} for c in num[:SERIES_COLUMNS + 4]}
        day_s = f.parent.name.split("=", 1)[1]
        summary_rows.append({"date": day_s, "n": int(len(df)), "columns": stats})
        if head in df.columns:
            th = _thin(df[["known_at", head]].dropna().sort_values("known_at"), points)
            base = pd.Timestamp(day_s)
            path_rows.append({"date": day_s, "minutes": [round((t - base).total_seconds() / 60, 1) for t in th["known_at"]], "v": [_num(v) for v in th[head]]})
    out.update({"available": bool(summary_rows), "kind": "intraday", "columns": columns, "headline": head, "days": summary_rows, "paths": path_rows[-paths:]})
    return out


def _wanted(path: Path) -> list:
    import pyarrow.parquet as pq
    try:
        return [c for c in pq.ParquetFile(path).schema.names if c not in SKIP or c in ("known_at", "symbol")]
    except Exception:                                                                     # noqa: BLE001
        return []


def _daily(store: Path, underlying: str, day: date) -> list:
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
        rows.append({"signal": sid, "date": files[-1].parent.name.split("=", 1)[1], "values": _values(last), **_meta(sid)})
    return rows


class BusLatest:
    """The newest row of every live signal from the engine's bus (final at bucket close, provisional on the engine's timer), so the
    tab shows the bucket as it forms instead of waiting for the store (written every 15 s).  A background thread subscribes to the
    engines' publishers ("signals-<U>"); summary() overlays what it holds as `now` on each signal.  Read only."""

    def __init__(self, names=None, env=None):
        import threading
        from data_layer import bus
        self.sub = bus.Subscriber(names or bus.signal_names(), topics=["signal", "provisional"], env=env)
        self.latest: dict = {}
        self.lock = threading.Lock()
        self._stop = threading.Event()
        self.thread = threading.Thread(target=self._run, daemon=True, name="signals-bus")
        self.thread.start()

    def _run(self) -> None:
        while not self._stop.is_set():
            for topic, m in self.sub.poll(timeout_ms=200):
                self.ingest(topic, m)

    def ingest(self, topic: str, m: dict) -> None:
        cols = m.get("cols", [])
        with self.lock:
            for r in m.get("rows", []):
                row = dict(zip(cols, r))
                sym = row.get("symbol")
                if sym is not None and "_FUT_" not in str(sym):         # per contract: the futures only (as the series does)
                    continue
                key = (row.get("underlying"), row.get("signal"), row.get("bucket"))
                old = self.latest.get(key)
                if old is None or (row.get("known_at") or 0) >= (old[1].get("known_at") or 0):
                    self.latest[key] = (topic, row)

    def overlay(self, out: dict) -> dict:
        with self.lock:
            for s in out.get("signals", []):
                hit = self.latest.get((out["underlying"], s["signal"], s["bucket"]))
                if hit is None:
                    continue
                topic, row = hit
                ka = pd.Timestamp(row["known_at"], unit="ns") if isinstance(row.get("known_at"), int) else pd.Timestamp(row.get("known_at"))
                if s.get("known_at") and ka <= pd.Timestamp(s["known_at"]) and topic == "signal":
                    continue
                s["now"] = {"known_at": ka.isoformat(), "provisional": topic == "provisional", "values": _values(pd.Series(row))}
        return out

    def close(self) -> None:
        self._stop.set()
        self.thread.join(timeout=1)
        self.sub.close()
