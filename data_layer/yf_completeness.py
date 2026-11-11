"""
Four-year completeness of the Yahoo fundamentals, symbol by symbol (SCRUM-163), and the backfill it drives.

  python -m data_layer.yf_fundamentals check [--min-complete 0.9]

For every symbol in the universe it answers one question: for each of its latest four fiscal years, is everything the pillars need
there?  The needed items per year are revenue, net income, free cash flow, a share count and long-term debt, and for non-financial
companies also EBIT and invested capital (the ROIC inputs); each may come from the fallback chain in yf_pillars.

Status of a symbol:
  complete      four fiscal years, each with every needed item, in sequence (about a year apart), the latest not older than 18 months
  short         every year present is complete, but fewer than four years exist (a young listing; Yahoo may add years later)
  incomplete    a needed item is missing in at least one of the latest four years (the items and years are listed)
  gap           a fiscal year is missing in the middle of the sequence
  stale         the latest fiscal year ended more than 18 months ago (the company stopped reporting, or Yahoo stopped updating)
  fetch_failed  the last fetch did not return all three statements
  pending       in the universe, never fetched yet
  no_data       fetched, but no statement values stored

The statuses short, incomplete, gap and stale are what the daily job backfills: such a symbol is fetched again every GAP_RETRY_DAYS (a
week), not every day, because Yahoo mostly fills gaps slowly and a young company stays short until it has four years.  fetch_failed is
retried after 20 hours, pending first of all.  The state is read from what is stored, not from dates, so a day the job did not run
is caught up by the next run (see select_due and --catch-up).
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import pandas as pd

from . import yf_fundamentals as yfc
from . import yf_pillars as pil

GAP_RETRY_DAYS = 7
STALE_DAYS = 548                      # 18 months
YEAR_RANGE = (270, 460)               # days between two fiscal year ends that still count as consecutive
RETRY_STATUSES = ("short", "incomplete", "gap", "stale", "no_data")
REQUIRED = ("revenue", "net_income", "fcf", "shares", "ltd")
REQUIRED_NON_FINANCIAL = ("ebit", "invested_capital")


def symbol_status(periods: list[dict], profile: dict, today: date) -> dict:
    """{"status", "periods", "missing", "latest_fy", "age_days", "reason"} for one symbol's stored fiscal years."""
    if not periods:
        return {"status": "no_data", "periods": 0, "missing": "", "latest_fy": None, "age_days": None, "reason": "no statement values stored"}
    rows = [pil.derive_period(p) for p in sorted(periods, key=lambda p: p["period_end"])]
    window = rows[-pil.WINDOW:]
    financial = pil._s(profile.get("sector")) in pil.FINANCIAL_SECTORS
    need = REQUIRED + (() if financial else REQUIRED_NON_FINANCIAL)
    missing = []
    for r in window:
        lacking = [k for k in need if r.get(k) is None]
        if lacking:
            missing.append(f"{r['period_end'][:4]}:{'+'.join(lacking)}")
    latest = date.fromisoformat(window[-1]["period_end"])
    age = (today - latest).days
    ends = [date.fromisoformat(r["period_end"]) for r in window]
    gaps = [f"{a}..{b}" for a, b in zip(ends, ends[1:]) if not YEAR_RANGE[0] <= (b - a).days <= YEAR_RANGE[1]]
    out = {"periods": len(window), "missing": ";".join(missing), "latest_fy": latest.isoformat(), "age_days": age}
    if age > STALE_DAYS:
        return {**out, "status": "stale", "reason": f"the latest fiscal year ended {age} days ago"}
    if gaps:
        return {**out, "status": "gap", "reason": "fiscal years not consecutive: " + ", ".join(gaps)}
    if missing:
        return {**out, "status": "incomplete", "reason": "missing " + "; ".join(missing)}
    if len(window) < pil.WINDOW:
        return {**out, "status": "short", "reason": f"only {len(window)} fiscal years available"}
    return {**out, "status": "complete", "reason": ""}


def check(symbols: dict[str, str], lake_dir: Path | None = None, today: Optional[date] = None, write: bool = True) -> pd.DataFrame:
    """One row per symbol of `symbols` ({symbol: market}) and per stored symbol outside it: status and detail.  Written to
    lake/yf_fundamentals/completeness.parquet."""
    today = today or datetime.now(timezone.utc).date()
    periods = pil.load_periods(lake_dir)
    profiles = pil.load_profiles(lake_dir)
    lg_path = yfc.store_dir(lake_dir) / "fetch_log.parquet"
    lg = pd.read_parquet(lg_path) if lg_path.exists() else pd.DataFrame(columns=yfc.LOG_COLUMNS)
    failed: dict[str, str] = {}
    attempted: set[str] = set()
    last_attempt: dict[str, str] = {}
    if not lg.empty:
        lg = lg.copy()
        lg["t"] = pd.to_datetime(lg["fetched_at"])
        for sym, g in lg.groupby("symbol"):
            attempted.add(sym)
            last_attempt[sym] = str(g["t"].max())[:19]
            stmts = g[g["part"].isin(list(yfc.STATEMENTS))].sort_values("t").groupby("part").tail(1)
            bad = stmts[~stmts["ok"].astype(bool) | (stmts["rows"].fillna(0) <= 0)]
            if len(stmts) < len(yfc.STATEMENTS) or len(bad):
                failed[sym] = ", ".join(f"{r.part}: {r.error_class or 'no rows'}" for r in bad.itertuples()) or "statements missing"
    rows = []
    for sym in sorted(set(symbols) | set(periods)):
        market = symbols.get(sym) or pil._s((profiles.get(sym) or {}).get("market"))
        if sym not in attempted:
            res = {"status": "pending", "periods": 0, "missing": "", "latest_fy": None, "age_days": None, "reason": "never fetched"}
        elif sym in failed:
            res = {"status": "fetch_failed", "periods": len(periods.get(sym, [])), "missing": "", "latest_fy": None, "age_days": None,
                   "reason": failed[sym]}
        else:
            res = symbol_status(periods.get(sym, []), profiles.get(sym, {}), today)
        rows.append({"symbol": sym, "market": market, **res, "last_attempt": last_attempt.get(sym), "checked": today.isoformat()})
    df = pd.DataFrame(rows, columns=["symbol", "market", "status", "periods", "missing", "latest_fy", "age_days", "reason", "last_attempt", "checked"])
    if write and len(df):
        p = yfc.store_dir(lake_dir) / "completeness.parquet"
        p.parent.mkdir(parents=True, exist_ok=True)
        tmp = p.with_suffix(".parquet.tmp")
        df.to_parquet(tmp, index=False)
        tmp.replace(p)
    return df


def gap_symbols(lake_dir: Path | None = None, today: Optional[date] = None) -> set[str]:
    """Symbols whose stored statements are fetched but incomplete: the ones the daily job backfills, each every GAP_RETRY_DAYS."""
    today = today or datetime.now(timezone.utc).date()
    periods = pil.load_periods(lake_dir)
    profiles = pil.load_profiles(lake_dir)
    return {s for s, p in periods.items() if symbol_status(p, profiles.get(s, {}), today)["status"] in RETRY_STATUSES}


def summary(df: pd.DataFrame) -> dict:
    """Counts per status, overall and per market; `complete_share` is among the symbols fetched at least once."""
    if df.empty:
        return {"symbols": 0, "by_status": {}, "by_market": {}, "complete_share": None}
    by_status = {k: int(v) for k, v in df["status"].value_counts().items()}
    fetched = df[df["status"] != "pending"]
    by_market = {}
    for m, g in df.groupby("market"):
        f = g[g["status"] != "pending"]
        by_market[m or "?"] = {"symbols": len(g), "complete": int((g["status"] == "complete").sum()),
                               "complete_share": round(float((f["status"] == "complete").mean()), 3) if len(f) else None,
                               **{k: int(v) for k, v in g["status"].value_counts().items() if k != "complete"}}
    return {"symbols": len(df), "by_status": by_status, "by_market": by_market,
            "complete_share": round(float((fetched["status"] == "complete").mean()), 3) if len(fetched) else None}
