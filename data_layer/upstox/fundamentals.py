"""
Upstox fundamentals loader (SCRUM-103): everything the /v2/fundamentals API returns for a company, keyed by ISIN.

  python -m data_layer.upstox.fundamentals run --set full --max-age-days 20     # all endpoints, skip companies fetched recently
  python -m data_layer.upstox.fundamentals run --set daily                      # key ratios only (a daily snapshot builds their history)
  python -m data_layer.upstox.fundamentals run --isin INE002A01018 --endpoints profile,key_ratios
  python -m data_layer.upstox.fundamentals status

Endpoints (per company): income statement (yearly, quarterly, and the full line-item statement), balance sheet (and full),
cash flow (and full), key ratios (company and sector value, today only), shareholding, corporate actions, profile, peers.
Upstox gives no filing dates and no ROIC, share count, market cap, capex or free cash flow; those come from FMP.

Storage, under lake/upstox_fundamentals/:
  versions.parquet   one row per (isin, endpoint, item, period, value) ever seen: value, text, change, first_seen, last_seen.
                     A restated value is a NEW row with its own first_seen, so a backtest can ask "what did we know on day X".
                     first_seen is the day WE first saw it (Upstox publishes no filing date), reliable only from the first load.
  fetch_log.parquet  one row per call: isin, symbol, endpoint, fetched_at, ok, status, error_code, rows
  raw/date=YYYY-MM-DD/<isin>.json.gz   the complete payloads of that day's fetch, so a parser change never loses data

The shared rate limiter (50 a second, 500 a minute, 2,000 per 30 minutes, with a margin) paces the calls: a full pass over 500
companies is about 5,500 calls, roughly 90 minutes.  The loader never raises on a bad response; failures are in the log.
"""

from __future__ import annotations

import argparse
import gzip
import json
import logging
import math
import os
import sys
import threading
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Iterable, Optional

import pandas as pd

from .. import lake as lake_mod

log = logging.getLogger(__name__)

# name -> (path under /v2/fundamentals/<isin>/, query parameters)
ENDPOINTS: dict[str, tuple[str, dict]] = {
    "income_yearly": ("income-statement", {"time_period": "yearly"}),
    "income_yearly_full": ("income-statement", {"time_period": "yearly", "fs": "true"}),
    "income_quarterly": ("income-statement", {"time_period": "quarterly"}),
    "balance": ("balance-sheet", {}),
    "balance_full": ("balance-sheet", {"fs": "true"}),
    "cashflow": ("cash-flow", {}),
    "cashflow_full": ("cash-flow", {"fs": "true"}),
    "key_ratios": ("key-ratios", {}),
    "shareholding": ("share-holdings", {}),
    "corporate_actions": ("corporate-actions", {}),
    "profile": ("profile", {}),
    "competitors": ("competitors", {}),            # asks for an instrument key (NSE_EQ|ISIN), not an ISIN; unverified until the first run
}
SETS = {"daily": ("key_ratios",), "full": tuple(ENDPOINTS)}
VERSION_COLUMNS = ["isin", "endpoint", "item", "period", "value", "text", "change", "first_seen", "last_seen"]
LOG_COLUMNS = ["isin", "symbol", "endpoint", "fetched_at", "ok", "status", "error_code", "rows"]
_IO_LOCK = threading.Lock()


def store_dir(lake_dir: Path | None = None) -> Path:
    return (lake_dir or lake_mod.LAKE_DIR) / "upstox_fundamentals"


# ---------------------------------------------------------------------------------------------- parsing
def _num(x) -> Optional[float]:
    if isinstance(x, bool) or x is None:
        return None
    if isinstance(x, (int, float)):
        return None if isinstance(x, float) and math.isnan(x) else float(x)
    if isinstance(x, str):
        try:
            v = float(x.replace(",", "").strip())
            return None if math.isnan(v) else v
        except ValueError:
            return None
    return None


def _row(item, period, value=None, change=None, text=None) -> dict:
    v = _num(value)
    if v is None and value is not None and text is None:
        text = value if isinstance(value, str) else json.dumps(value, default=str, sort_keys=True)
    c = _num(change)
    if c is None and change is not None:
        text = (text + " | " if text else "") + "change: " + str(change)
    return {"item": None if item is None else str(item), "period": "" if period is None else str(period), "value": v, "text": text, "change": c}


def _history(item, hist) -> list[dict]:
    return [_row(item, h.get("period"), h.get("value"), h.get("change")) for h in (hist or []) if isinstance(h, dict)]


def parse(endpoint: str, data) -> list[dict]:
    """The rows (item, period, value, text, change) of one endpoint's `data`.  Anything that does not fit a known shape is
    kept as JSON text, never dropped; the raw payload is stored as well."""
    rows: list[dict] = []
    if data is None:
        return rows
    if endpoint.startswith(("income", "cashflow")):
        key = "income_statement" if endpoint.startswith("income") else "cash_flow"
        for blk in (data or {}).get(key) or []:
            rows += _history(blk.get("category"), blk.get("history"))
        for blk in (data or {}).get("full_statement") or []:
            rows += _history("full:" + str(blk.get("particular")), blk.get("history"))
    elif endpoint.startswith("balance"):
        for h in (data or {}).get("history") or []:
            rows += [_row("total_asset", h.get("period"), h.get("total_asset")), _row("total_liability", h.get("period"), h.get("total_liability"))]
            extra = {k: v for k, v in h.items() if k not in ("period", "total_asset", "total_liability")}
            rows += [_row(k, h.get("period"), v) for k, v in extra.items()]
        for blk in (data or {}).get("full_statement") or []:
            rows += _history("full:" + str(blk.get("particular")), blk.get("history"))
    elif endpoint == "key_ratios":
        for x in data or []:
            rows.append(_row(x.get("name"), "snapshot", x.get("company_value"), x.get("sector_value")))     # change = the sector's value
    elif endpoint == "shareholding":
        for blk in data or []:
            rows += _history(blk.get("category"), blk.get("history"))
    elif endpoint == "corporate_actions":
        for x in data or []:
            if not isinstance(x, dict):
                rows.append(_row("action", "", text=json.dumps(x, default=str)))
                continue
            period = next((x[k] for k in ("expiry_date", "ex_date", "record_date", "date") if x.get(k)), "")
            rows.append(_row(x.get("name") or x.get("type") or "action", period, x.get("amount"), text=json.dumps(x, default=str, sort_keys=True)))
    elif endpoint == "profile":
        if isinstance(data, dict):
            rows += [_row(k, "snapshot", v) for k, v in data.items()]
        else:
            rows.append(_row("profile", "snapshot", text=json.dumps(data, default=str, sort_keys=True)))
    else:                                                                 # competitors and anything new
        for x in data if isinstance(data, list) else [data]:
            rows.append(_row("competitor", "snapshot", text=x if isinstance(x, str) else json.dumps(x, default=str, sort_keys=True)))
    return rows


# ---------------------------------------------------------------------------------------------- storage
def _read(path: Path, columns: list[str]) -> pd.DataFrame:
    return pd.read_parquet(path) if path.exists() else pd.DataFrame(columns=columns)


def _write(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".parquet.tmp")
    df.to_parquet(tmp, index=False)
    os.replace(tmp, path)


def _vkey(df: pd.DataFrame) -> pd.Series:
    def f(x):
        return "" if x is None or (isinstance(x, float) and math.isnan(x)) else format(x, ".10g") if isinstance(x, (int, float)) else str(x)
    cols = ["isin", "endpoint", "item", "period", "value", "text", "change"]
    return pd.Series(["\x1f".join(f(v) for v in r) for r in df[cols].itertuples(index=False, name=None)], index=df.index)


def merge_versions(existing: pd.DataFrame, new: pd.DataFrame, today: str) -> tuple[pd.DataFrame, int]:
    """Fold freshly parsed rows into the version table.  A row already there (same isin, endpoint, item, period, value, text,
    change) only gets its last_seen moved to `today`; anything else is a new version with first_seen = last_seen = today.
    Returns the table and the number of new versions."""
    if new.empty:
        return existing, 0
    new = new.copy()
    new["first_seen"] = new["last_seen"] = today
    existing = existing.copy()
    existing["_k"], new["_k"] = (_vkey(existing) if len(existing) else pd.Series(dtype=object)), _vkey(new)
    new = new.drop_duplicates("_k")
    known = set(existing["_k"])
    seen_now = set(new["_k"])
    existing.loc[existing["_k"].isin(seen_now), "last_seen"] = today
    fresh = new[~new["_k"].isin(known)]
    out = pd.concat([existing, fresh], ignore_index=True).drop(columns="_k")
    return out[VERSION_COLUMNS], len(fresh)


def write_raw(lake_dir: Path | None, isin: str, payload: dict, day: str) -> Path:
    p = store_dir(lake_dir) / "raw" / f"date={day}" / f"{isin}.json.gz"
    p.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(p, "wt", encoding="utf-8") as fh:
        json.dump(payload, fh, default=str)
    return p


# ---------------------------------------------------------------------------------------------- fetching
def fetch_isin(rest, isin: str, endpoints: Iterable[str], instrument_key: str | None = None) -> dict:
    """{endpoint: Resp}.  Peers are asked for with the instrument key, everything else with the ISIN."""
    out = {}
    for ep in endpoints:
        path, params = ENDPOINTS[ep]
        who = (instrument_key or f"NSE_EQ|{isin}") if ep == "competitors" else isin
        out[ep] = rest.get(f"/v2/fundamentals/{who}/{path}", params or None, tag=f"fund.{ep}")
    return out


def _recent(log_df: pd.DataFrame, endpoints: set[str], cutoff: datetime) -> set[str]:
    """ISINs whose every requested endpoint was fetched successfully since `cutoff`."""
    if log_df.empty:
        return set()
    ok = log_df[(log_df["ok"]) & (pd.to_datetime(log_df["fetched_at"]) >= cutoff) & (log_df["endpoint"].isin(endpoints))]
    counts = ok.groupby("isin")["endpoint"].nunique()
    return set(counts[counts >= len(endpoints)].index)


def run(rest, isins: dict[str, str], endpoints: Iterable[str] = SETS["full"], lake_dir: Path | None = None,
        max_age_days: Optional[float] = None, now: Optional[datetime] = None, save_every: int = 25,
        stop: Optional[threading.Event] = None, progress: Optional[Callable[[str], None]] = None) -> dict:
    """
    Load fundamentals for {isin: symbol}.  Companies whose requested endpoints were all fetched within `max_age_days` are
    skipped.  Versions and the log are saved every `save_every` companies and at the end, so a stopped run keeps its work.
    """
    now = now or datetime.now(timezone.utc).replace(tzinfo=None)
    day = now.strftime("%Y-%m-%d")
    endpoints = list(endpoints)
    unknown = [e for e in endpoints if e not in ENDPOINTS]
    if unknown:
        raise ValueError(f"unknown endpoints {unknown}; choose from {sorted(ENDPOINTS)}")
    base = store_dir(lake_dir)
    vpath, lpath = base / "versions.parquet", base / "fetch_log.parquet"
    versions, fetch_log = _read(vpath, VERSION_COLUMNS), _read(lpath, LOG_COLUMNS)
    if max_age_days is not None:
        skip = _recent(fetch_log, set(endpoints), now - timedelta(days=max_age_days))
        todo = {i: s for i, s in isins.items() if i not in skip}
    else:
        todo = dict(isins)
    summary = {"companies": 0, "skipped": len(isins) - len(todo), "calls": 0, "ok": 0, "failed": {}, "rows": 0, "new_versions": 0, "stopped": False}
    new_log: list[dict] = []
    pending: list[pd.DataFrame] = []

    def flush() -> None:
        nonlocal versions, fetch_log, new_log, pending
        with _IO_LOCK:
            if pending:
                versions, added = merge_versions(versions, pd.concat(pending, ignore_index=True), day)
                summary["new_versions"] += added
                _write(versions, vpath)
                pending = []
            if new_log:
                fetch_log = pd.concat([fetch_log, pd.DataFrame(new_log)], ignore_index=True)
                _write(fetch_log, lpath)
                new_log = []

    try:
        for n, (isin, symbol) in enumerate(todo.items(), 1):
            if stop is not None and stop.is_set():
                summary["stopped"] = True
                break
            resps = fetch_isin(rest, isin, endpoints, instrument_key=f"NSE_EQ|{isin}")
            raw, rows_all = {}, []
            for ep, r in resps.items():
                summary["calls"] += 1
                rows: list[dict] = []
                if r.ok:
                    try:
                        rows = parse(ep, (r.data or {}).get("data") if isinstance(r.data, dict) else None)
                        summary["ok"] += 1
                    except Exception as exc:                              # noqa: BLE001 -- one bad shape must not stop the run
                        log.warning("could not parse %s for %s: %s", ep, isin, exc)
                        r.ok, r.error_code, r.error_message = False, "PARSE", f"{type(exc).__name__}: {exc}"[:200]
                if not r.ok:
                    key = f"{ep}:{r.status}:{r.error_code}"
                    summary["failed"][key] = summary["failed"].get(key, 0) + 1
                raw[ep] = {"ok": r.ok, "status": r.status, "error_code": r.error_code, "data": r.data if r.ok else None}
                for x in rows:
                    rows_all.append({"isin": isin, "endpoint": ep, **x})
                new_log.append({"isin": isin, "symbol": symbol, "endpoint": ep, "fetched_at": now.isoformat(timespec="seconds"),
                                "ok": bool(r.ok), "status": int(r.status), "error_code": r.error_code or "", "rows": len(rows)})
            if any(v["ok"] for v in raw.values()):
                write_raw(lake_dir, isin, raw, day)
            if rows_all:
                pending.append(pd.DataFrame(rows_all))
                summary["rows"] += len(rows_all)
            summary["companies"] += 1
            if progress and n % 25 == 0:
                progress(f"{n}/{len(todo)} companies, {summary['calls']} calls, {sum(summary['failed'].values())} failed")
            if n % save_every == 0:
                flush()
    finally:
        flush()
    return summary


def status(lake_dir: Path | None = None) -> dict:
    base = store_dir(lake_dir)
    versions, fetch_log = _read(base / "versions.parquet", VERSION_COLUMNS), _read(base / "fetch_log.parquet", LOG_COLUMNS)
    out = {"companies": int(versions["isin"].nunique()) if len(versions) else 0, "versions": len(versions), "calls_logged": len(fetch_log)}
    if len(fetch_log):
        last = fetch_log.sort_values("fetched_at").groupby(["isin", "endpoint"]).tail(1)
        out["last_fetch"] = str(last["fetched_at"].max())
        out["failing_now"] = {k: int(v) for k, v in last[~last["ok"]].groupby("endpoint").size().items()}
    return out


# ---------------------------------------------------------------------------------------------- command line
def _token() -> str:
    from dotenv import load_dotenv
    from .feed import current_token
    load_dotenv(lake_mod.ENV_FILE, override=True)
    return current_token() or os.getenv("UPSTOX_ANALYTICS_TOKEN", "")


def main(argv: Optional[list[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Load Upstox company fundamentals (all endpoints) into the lake")
    sub = ap.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run", help="fetch and store")
    r.add_argument("--set", choices=sorted(SETS), default="full", help="daily = key ratios only; full = every endpoint")
    r.add_argument("--endpoints", default=None, help="comma-separated endpoint names instead of --set")
    r.add_argument("--isin", nargs="*", default=None, help="only these ISINs (default: the equity universe)")
    r.add_argument("--limit", type=int, default=None)
    r.add_argument("--max-age-days", type=float, default=None, help="skip companies whose endpoints were all fetched this recently")
    sub.add_parser("status", help="what is stored and what is failing")
    a = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    if a.cmd == "status":
        print(json.dumps(status(), indent=2, default=str))
        return 0
    token = _token()
    if not token:
        print("No Upstox token: set UPSTOX_FEED_TOKEN, UPSTOX_ACCESS_TOKEN or UPSTOX_ANALYTICS_TOKEN in .env (see data_layer.upstox.auth).")
        return 2
    from .rest import UpstoxRest
    from .instruments import segment_key_map
    if a.isin:
        isins = {i: i for i in a.isin}
    else:
        km = segment_key_map("eq")
        isins = {k.split("|", 1)[1]: s for k, s in km.items() if "|" in k}
    if a.limit:
        isins = dict(list(isins.items())[: a.limit])
    eps = [e.strip() for e in a.endpoints.split(",")] if a.endpoints else list(SETS[a.set])
    stop = threading.Event()
    import signal
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())
    log.info("fundamentals: %d companies, endpoints %s, max age %s days", len(isins), ",".join(eps), a.max_age_days)
    s = run(UpstoxRest(_token), isins, eps, max_age_days=a.max_age_days, stop=stop, progress=log.info)
    log.info("fundamentals done: %s", json.dumps(s))
    return 0 if not s["stopped"] else 130


if __name__ == "__main__":
    sys.exit(main())
