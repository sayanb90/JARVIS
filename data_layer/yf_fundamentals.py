"""
Fundamentals from Yahoo Finance for the whole universe (SCRUM-161): annual statements, company profile, dividends and splits.

  python -m data_layer.yf_fundamentals run [--limit 100] [--market US,UK] [--symbols AAPL,HSBA.L] [--refresh-days 30]
  python -m data_layer.yf_fundamentals status

Why Yahoo (decision record SCRUM-138): the previous vendor's free plan covered US exchanges only and its global tier was expensive; Yahoo covers every
market in the universe through the open-source `yfinance` library.  That library is not an API: it runs on our side and reads Yahoo's
own website endpoints, which Yahoo can change at any time.  So this module is built to notice breakage instead of hiding it:

  * every call of every symbol is classified: ok, empty (Yahoo answered nothing), throttled (HTTP 429), exception, or parse (the
    answer came back in a shape we do not recognise).  The classes are the first thing to look at when data thins out;
  * a symbol is complete only when all three statements came back; one that did not is retried before anything else, and is never
    marked fresh (the lesson of the earlier rotation, which stamped failed fetches as done);
  * a failed or empty answer never overwrites stored values;
  * the complete payload of every symbol is kept (raw/date=.../<symbol>.json.gz), so a parser fix can be replayed without calling
    Yahoo again.

Storage, under lake/yf_fundamentals/:
  versions.parquet   (symbol, statement, item, period_end, value, currency, first_seen, last_seen): the items the pillars use.  A restated
                     value is a new row with its own first_seen; an unchanged one only moves last_seen.
  profile.parquet    one row per symbol: price currency, reporting currency, market cap, shares, sector, type, name, exchange.
  actions.parquet    dividends and splits (symbol, date, kind, value).
  fetch_log.parquet  one row per call: symbol, part, fetched_at, ok, error_class, rows, attempts, latency_s, message.
  raw/date=YYYY-MM-DD/<symbol>.json.gz
Refresh: a symbol is due when it has never been fetched, when its last attempt failed (retried after MIN_RETRY_HOURS), or when its last
complete fetch is older than --refresh-days.  Run a slice a day and a full pass takes about a month.
"""

from __future__ import annotations

import argparse
import gzip
import json
import logging
import os
import re
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Iterable, Optional

import pandas as pd
import warnings

# yfinance 1.7 on pandas 3 prints a deprecation warning for every call (an `inplace` keyword it uses); with two thousand symbols that would
# bury the real lines of the cron logs
warnings.filterwarnings("ignore", message=r".*inplace keyword.*")

from . import lake as lake_mod

log = logging.getLogger(__name__)

OK, EMPTY, THROTTLED, EXCEPTION, PARSE = "ok", "empty", "throttled", "exception", "parse"
STATEMENTS = {"income": "financials", "balance": "balance-sheet", "cashflow": "cash-flow"}      # our name -> yfinance's key group
PARTS = ("income", "balance", "cashflow", "info", "dividends", "splits")
MIN_RETRY_HOURS = 20
REFRESH_DAYS = 30

# the items the pillars and the checks use; the raw payload keeps every item Yahoo sent
KEEP = {
    "income": ("TotalRevenue", "EBIT", "EBITDA", "OperatingIncome", "PretaxIncome", "TaxProvision", "TaxRateForCalcs", "NetIncome",
               "NetIncomeCommonStockholders", "NetIncomeContinuousOperations", "DilutedAverageShares", "BasicAverageShares",
               "InterestExpense", "DilutedEPS", "NormalizedEBITDA"),
    "balance": ("TotalAssets", "TotalLiabilitiesNetMinorityInterest", "TotalDebt", "LongTermDebt", "CurrentDebt",
                "LongTermDebtAndCapitalLeaseObligation", "StockholdersEquity", "CommonStockEquity", "MinorityInterest", "InvestedCapital",
                "TotalCapitalization", "NetDebt", "CashAndCashEquivalents", "CashCashEquivalentsAndShortTermInvestments",
                "OrdinarySharesNumber", "ShareIssued", "WorkingCapital"),
    "cashflow": ("OperatingCashFlow", "CapitalExpenditure", "FreeCashFlow", "RepurchaseOfCapitalStock", "CashDividendsPaid",
                 "IssuanceOfCapitalStock"),
}
VERSION_COLUMNS = ["symbol", "statement", "item", "period_end", "value", "currency", "first_seen", "last_seen"]
LOG_COLUMNS = ["symbol", "part", "fetched_at", "ok", "error_class", "rows", "attempts", "latency_s", "message"]
PROFILE_COLUMNS = ["symbol", "market", "currency", "financial_currency", "market_cap", "shares_outstanding", "sector", "quote_type",
                   "name", "exchange", "fetched_at"]
ACTION_COLUMNS = ["symbol", "date", "kind", "value"]
INFO_KEYS = {"currency": "currency", "financialCurrency": "financial_currency", "marketCap": "market_cap",
             "sharesOutstanding": "shares_outstanding", "sector": "sector", "quoteType": "quote_type", "longName": "name",
             "exchange": "exchange"}


class ParseError(Exception):
    """Yahoo answered, but not in the shape this module knows."""


def store_dir(lake_dir: Path | None = None) -> Path:
    return (lake_dir or lake_mod.LAKE_DIR) / "yf_fundamentals"


def _now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


# ---------------------------------------------------------------------------------------------- calling Yahoo
def classify_exception(exc: BaseException) -> str:
    text = f"{type(exc).__name__} {exc}".lower()
    return THROTTLED if ("429" in text or "too many requests" in text or "rate limit" in text or "ratelimit" in text) else EXCEPTION


class Part:
    """The outcome of one call: value, class, attempts, latency, message."""

    def __init__(self, name: str, value=None, cls: str = OK, attempts: int = 1, latency: float = 0.0, message: str = "", rows: int = 0) -> None:
        self.name, self.value, self.cls, self.attempts, self.latency, self.message, self.rows = name, value, cls, attempts, latency, message, rows

    @property
    def ok(self) -> bool:
        return self.cls == OK


def _is_empty(v) -> bool:
    if v is None:
        return True
    if isinstance(v, (pd.DataFrame, pd.Series)):
        return v.empty
    if isinstance(v, dict):
        return not v
    return False


def call(name: str, fn: Callable, sleep: Callable[[float], None] = time.sleep, retries: int = 2, backoff_s: float = 5.0,
         empty_ok: bool = False) -> Part:
    """Run one Yahoo call.  A throttled answer is retried after a growing pause; anything else is classified and returned.
    `empty_ok`: an empty answer is a normal answer (dividends and splits of a company that never paid or split)."""
    t0 = time.monotonic()
    for attempt in range(1, retries + 2):
        try:
            value = fn()
        except Exception as exc:                                       # noqa: BLE001 -- Yahoo's failures are data here
            cls = classify_exception(exc)
            if cls == THROTTLED and attempt <= retries:
                sleep(backoff_s * attempt)
                continue
            return Part(name, None, cls, attempt, time.monotonic() - t0, f"{type(exc).__name__}: {str(exc)[:150]}")
        if _is_empty(value):
            return Part(name, value, OK if empty_ok else EMPTY, attempt, time.monotonic() - t0, "" if empty_ok else "Yahoo returned nothing")
        return Part(name, value, OK, attempt, time.monotonic() - t0)
    return Part(name, None, EXCEPTION, retries + 1, time.monotonic() - t0, "gave up")            # not reached


# ---------------------------------------------------------------------------------------------- parsing
def parse_statement(df, statement: str) -> list[dict]:
    """(item, period_end, value) for the items we keep.  Raises ParseError when the table is not rows of items by columns of period ends."""
    if not isinstance(df, pd.DataFrame):
        raise ParseError(f"expected a table, got {type(df).__name__}")
    if not all(isinstance(i, str) for i in df.index):
        raise ParseError("the row labels are not item names")
    try:
        periods = [pd.Timestamp(c).date() for c in df.columns]
    except (ValueError, TypeError) as exc:
        raise ParseError(f"the column labels are not dates: {exc}") from exc
    keep = set(KEEP[statement])
    out = []
    for item in df.index:
        if item not in keep:
            continue
        row = df.loc[item]
        if isinstance(row, pd.DataFrame):                              # a duplicated row label: take the first
            row = row.iloc[0]
        for period, v in zip(periods, row):
            if pd.notna(v):
                try:
                    out.append({"item": item, "period_end": period.isoformat(), "value": float(v)})
                except (TypeError, ValueError):
                    raise ParseError(f"{item} holds a non-number: {v!r}")
    return out


def parse_actions(series, kind: str) -> list[dict]:
    if series is None or len(series) == 0:
        return []
    if not isinstance(series, pd.Series):
        raise ParseError(f"expected a series for {kind}, got {type(series).__name__}")
    out = []
    for d, v in series.items():
        try:
            out.append({"date": pd.Timestamp(d).date().isoformat(), "kind": kind, "value": float(v)})
        except (ValueError, TypeError) as exc:
            raise ParseError(f"{kind}: {exc}") from exc
    return out


def parse_info(info: dict) -> dict:
    out = {}
    for src, dst in INFO_KEYS.items():
        v = info.get(src)
        out[dst] = v if not (isinstance(v, float) and pd.isna(v)) else None
    return out


# ---------------------------------------------------------------------------------------------- one symbol
def fetch_symbol(symbol: str, factory: Callable = None, sleep: Callable[[float], None] = time.sleep, fallback_currency: str = "") -> dict:
    """Call Yahoo for one symbol.  Returns {"parts": {name: Part}, "rows": [...], "actions": [...], "profile": {...}, "raw": {...}}.
    A part that failed keeps its class and message; a table that came back in an unknown shape is reclassified as PARSE."""
    if factory is None:
        import yfinance as yf
        factory = yf.Ticker
    t = factory(symbol)
    parts: dict[str, Part] = {
        "income": call("income", lambda: t.get_income_stmt(freq="yearly"), sleep),
        "balance": call("balance", lambda: t.get_balance_sheet(freq="yearly"), sleep),
        "cashflow": call("cashflow", lambda: t.get_cashflow(freq="yearly"), sleep),
        "info": call("info", lambda: t.get_info(), sleep),
        "dividends": call("dividends", lambda: t.get_dividends(), sleep, empty_ok=True),
        "splits": call("splits", lambda: t.get_splits(), sleep, empty_ok=True),
    }
    profile = {}
    if parts["info"].ok:
        try:
            profile = parse_info(parts["info"].value)
        except Exception as exc:                                       # noqa: BLE001
            parts["info"].cls, parts["info"].message = PARSE, f"{type(exc).__name__}: {str(exc)[:100]}"
    currency = profile.get("financial_currency") or fallback_currency or ""        # a failed info call must not change the currency of stored values
    rows: list[dict] = []
    for name in STATEMENTS:
        p = parts[name]
        if not p.ok:
            continue
        try:
            got = parse_statement(p.value, name)
        except ParseError as exc:
            p.cls, p.message = PARSE, str(exc)[:150]
            continue
        p.rows = len(got)
        if not got:                                                    # a table with none of the items we use: Yahoo changed the names
            p.cls, p.message = PARSE, "none of the expected items are in the table"
            continue
        rows += [{"statement": name, "currency": currency, **r} for r in got]
    actions: list[dict] = []
    for name, kind in (("dividends", "dividend"), ("splits", "split")):
        p = parts[name]
        if p.ok:
            try:
                got = parse_actions(p.value, kind)
            except ParseError as exc:
                p.cls, p.message = PARSE, str(exc)[:150]
                continue
            p.rows = len(got)
            actions += got
    raw = {"symbol": symbol, "fetched_at": _now().isoformat(timespec="seconds"), "info": {k: v for k, v in (parts["info"].value or {}).items()
                                                                                          if isinstance(v, (str, int, float, bool, type(None)))}
           if parts["info"].ok and isinstance(parts["info"].value, dict) else {}}
    for name in STATEMENTS:
        v = parts[name].value
        raw[name] = json.loads(v.to_json(orient="split", date_format="iso")) if isinstance(v, pd.DataFrame) else None
    for name in ("dividends", "splits"):
        v = parts[name].value
        raw[name] = {str(k): float(x) for k, x in v.items()} if isinstance(v, pd.Series) else None
    return {"parts": parts, "rows": rows, "actions": actions, "profile": profile, "raw": raw}


def is_complete(parts: dict[str, Part]) -> bool:
    return all(parts[n].ok and parts[n].rows > 0 for n in STATEMENTS)


# ---------------------------------------------------------------------------------------------- storage
def _read(path: Path, columns: list[str]) -> pd.DataFrame:
    return pd.read_parquet(path) if path.exists() else pd.DataFrame(columns=columns)


def _write(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".parquet.tmp")
    df.to_parquet(tmp, index=False)
    os.replace(tmp, path)


_KEY = ["symbol", "statement", "item", "period_end", "value", "currency"]


def merge_versions(existing: pd.DataFrame, new: pd.DataFrame, today: str) -> tuple[pd.DataFrame, int]:
    """Fold fresh rows into the version table: an identical row only moves last_seen to `today`; a changed value is a new row with its
    own first_seen (the old one stays, with its older last_seen).  Returns the table and the number of new rows."""
    if new.empty:
        return existing, 0
    new = new.drop_duplicates(_KEY).copy()
    new["currency"] = new["currency"].fillna("")
    new["first_seen"] = new["last_seen"] = today
    if existing.empty:
        return new[VERSION_COLUMNS].reset_index(drop=True), len(new)
    ex = existing.copy()
    ex["currency"] = ex["currency"].fillna("")
    seen = ex[_KEY].merge(new[_KEY].assign(_hit=1), on=_KEY, how="left")["_hit"].notna().to_numpy()
    ex.loc[seen, "last_seen"] = today
    fresh = new.merge(ex[_KEY].assign(_known=1), on=_KEY, how="left")
    fresh = fresh[fresh["_known"].isna()].drop(columns="_known")
    out = pd.concat([ex, fresh[VERSION_COLUMNS]], ignore_index=True)
    return out[VERSION_COLUMNS], len(fresh)


def write_raw(lake_dir: Path | None, symbol: str, payload: dict, day: str) -> Path:
    safe = re.sub(r"[^A-Za-z0-9._=-]", "_", symbol)
    p = store_dir(lake_dir) / "raw" / f"date={day}" / f"{safe}.json.gz"
    p.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(p, "wt", encoding="utf-8") as fh:
        json.dump(payload, fh, default=str)
    return p


# ---------------------------------------------------------------------------------------------- what is due
def select_due(symbols: dict[str, str], fetch_log: pd.DataFrame, now: datetime, refresh_days: float = REFRESH_DAYS,
               limit: Optional[int] = None, gap_symbols: Iterable[str] = (), gap_retry_days: float = 7,
               catch_up: Optional[int] = None) -> list[str]:
    """Symbols to fetch now, most urgent first:
      1. never fetched;
      2. the last attempt failed (retried after MIN_RETRY_HOURS, so a failing symbol is not hammered);
      3. fetched but incomplete (`gap_symbols`: a missing year or item, see yf_completeness): retried every `gap_retry_days`;
      4. the last complete fetch is older than `refresh_days`, oldest first.
    The state comes from what is stored, not from the date of the last run, so a day the job did not run is caught up by the next one:
    when more symbols are overdue than `limit` (never fetched, failed, incomplete, or late by more than a week), up to `catch_up` are taken."""
    gaps = set(gap_symbols)
    last_attempt: dict[str, datetime] = {}
    last_complete: dict[str, datetime] = {}
    failed: set[str] = set()
    if not fetch_log.empty:
        lg = fetch_log.copy()
        lg["t"] = pd.to_datetime(lg["fetched_at"])
        for sym, g in lg.groupby("symbol"):
            last_attempt[sym] = g["t"].max()
            stmts = g[g["part"].isin(list(STATEMENTS))]
            latest = stmts.sort_values("t").groupby("part").tail(1)
            if len(latest) == len(STATEMENTS) and latest["ok"].astype(bool).all() and (latest["rows"].fillna(0) > 0).all():
                last_complete[sym] = latest["t"].max()
            else:
                failed.add(sym)
    never, retry, gap, stale = [], [], [], []
    late = 0
    for sym in symbols:
        if sym not in last_attempt:
            never.append(sym)
        elif sym in failed:
            if now - last_attempt[sym] >= timedelta(hours=MIN_RETRY_HOURS):
                retry.append((last_attempt[sym], sym))
        elif sym in gaps and now - last_attempt[sym] >= timedelta(days=gap_retry_days):
            gap.append((last_attempt[sym], sym))
        elif now - last_complete[sym] >= timedelta(days=refresh_days):
            stale.append((last_complete[sym], sym))
            late += int(now - last_complete[sym] >= timedelta(days=refresh_days + 7))
    due = never + [s for _, s in sorted(retry)] + [s for _, s in sorted(gap)] + [s for _, s in sorted(stale)]
    overdue = len(never) + len(retry) + len(gap) + late
    if limit and catch_up and overdue > limit:
        limit = min(overdue, max(catch_up, limit))
    return due[:limit] if limit else due


# ---------------------------------------------------------------------------------------------- the run
def run(symbols: dict[str, str], lake_dir: Path | None = None, factory: Callable = None, limit: Optional[int] = None,
        refresh_days: float = REFRESH_DAYS, pause_s: Optional[float] = None, now: Optional[datetime] = None, catch_up: Optional[int] = None,
        sleep: Callable[[float], None] = time.sleep, stop=None, progress: Optional[Callable[[str], None]] = None,
        save_every: int = 25) -> dict:
    """Fetch the due symbols of {symbol: market}.  Saves every `save_every` symbols and at the end, so a stopped run keeps its work."""
    fixed = now
    now = now or _now()
    day = now.strftime("%Y-%m-%d")
    pause_s = float(os.getenv("JARVIS_YF_FUND_PAUSE_S", "1.0")) if pause_s is None else pause_s
    base = store_dir(lake_dir)
    vpath, lpath, ppath, apath = base / "versions.parquet", base / "fetch_log.parquet", base / "profile.parquet", base / "actions.parquet"
    versions, fetch_log = _read(vpath, VERSION_COLUMNS), _read(lpath, LOG_COLUMNS)
    profiles, actions = _read(ppath, PROFILE_COLUMNS), _read(apath, ACTION_COLUMNS)
    from . import yf_completeness
    gap = yf_completeness.gap_symbols(lake_dir, now.date())
    due = select_due(symbols, fetch_log, now, refresh_days, limit, gap_symbols=gap, catch_up=catch_up)
    summary = {"due": len(due), "symbols": 0, "complete": 0, "partial": 0, "classes": {}, "rows": 0, "new_versions": 0, "stopped": False}
    pend_rows: list[dict] = []
    pend_log: list[dict] = []
    pend_prof: list[dict] = []
    pend_act: list[dict] = []

    def flush() -> None:
        nonlocal versions, fetch_log, profiles, actions, pend_rows, pend_log, pend_prof, pend_act
        if pend_rows:
            versions, added = merge_versions(versions, pd.DataFrame(pend_rows), day)
            summary["new_versions"] += added
            _write(versions, vpath)
            pend_rows = []
        if pend_log:
            fetch_log = pd.concat([fetch_log, pd.DataFrame(pend_log)], ignore_index=True)
            _write(fetch_log, lpath)
            pend_log = []
        if pend_prof:
            profiles = pd.concat([profiles[~profiles["symbol"].isin([p["symbol"] for p in pend_prof])], pd.DataFrame(pend_prof)], ignore_index=True)
            _write(profiles[PROFILE_COLUMNS], ppath)
            pend_prof = []
        if pend_act:
            actions = pd.concat([actions, pd.DataFrame(pend_act)], ignore_index=True).drop_duplicates(["symbol", "date", "kind"], keep="last")
            _write(actions[ACTION_COLUMNS], apath)
            pend_act = []

    try:
        for n, symbol in enumerate(due, 1):
            if stop is not None and stop.is_set():
                summary["stopped"] = True
                break
            stamp = (fixed or _now()).isoformat(timespec="seconds")
            known = profiles[profiles["symbol"] == symbol]["financial_currency"].dropna()
            res = fetch_symbol(symbol, factory, sleep, fallback_currency=str(known.iloc[-1]) if len(known) else "")
            parts = res["parts"]
            for name in PARTS:
                p = parts[name]
                summary["classes"][p.cls] = summary["classes"].get(p.cls, 0) + 1
                pend_log.append({"symbol": symbol, "part": name, "fetched_at": stamp, "ok": p.ok, "error_class": "" if p.ok else p.cls,
                                 "rows": p.rows, "attempts": p.attempts, "latency_s": round(p.latency, 3), "message": p.message[:200]})
            # only what came back is stored: a failed part leaves the stored values alone
            pend_rows += [{"symbol": symbol, **r} for r in res["rows"]]
            pend_act += [{"symbol": symbol, **a} for a in res["actions"]]
            if res["profile"]:
                pend_prof.append({"symbol": symbol, "market": symbols[symbol], "fetched_at": stamp, **res["profile"]})
            if any(parts[nm].ok for nm in PARTS):
                write_raw(lake_dir, symbol, res["raw"], day)
            summary["symbols"] += 1
            summary["rows"] += len(res["rows"])
            summary["complete" if is_complete(parts) else "partial"] += 1
            if progress and n % 25 == 0:
                progress(f"{n}/{len(due)} symbols, {summary['complete']} complete, {summary['partial']} partial")
            if n % save_every == 0:
                flush()
            if pause_s and n < len(due):
                sleep(pause_s)
    finally:
        flush()
    return summary


def status(lake_dir: Path | None = None) -> dict:
    base = store_dir(lake_dir)
    versions, lg = _read(base / "versions.parquet", VERSION_COLUMNS), _read(base / "fetch_log.parquet", LOG_COLUMNS)
    out = {"symbols_with_values": int(versions["symbol"].nunique()) if len(versions) else 0, "versions": len(versions), "calls_logged": len(lg)}
    if len(lg):
        lg = lg.copy()
        lg["day"] = lg["fetched_at"].astype(str).str[:10]
        last = lg["day"].max()
        out["last_run_day"] = last
        out["last_day_classes"] = {k: int(v) for k, v in lg[(lg["day"] == last) & (~lg["ok"].astype(bool))].groupby("error_class").size().items()}
    return out


# ---------------------------------------------------------------------------------------------- command line
def universe_symbols(markets: Optional[Iterable[str]] = None) -> dict[str, str]:
    from .universe_files import read_universe
    out: dict[str, str] = {}
    for m in markets or [m for m in lake_mod.MARKETS if m not in lake_mod.NO_FUNDAMENTALS]:
        for r in read_universe(m):
            out.setdefault(r["symbol"], m)
    return out


def main(argv: list[str] | None = None) -> int:
    from .limits import raise_open_files
    raise_open_files()
    ap = argparse.ArgumentParser(description="Fundamentals from Yahoo Finance (yfinance)")
    sub = ap.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run", help="fetch the symbols that are due")
    r.add_argument("--limit", type=int, default=None, help="at most this many symbols (a slice a day)")
    r.add_argument("--catch-up", type=int, default=None, help="when more symbols are overdue than --limit (missed days, a first pass), take up to this many")
    r.add_argument("--market", default=None, help="comma-separated markets, e.g. US,UK (default: all)")
    r.add_argument("--symbols", default=None, help="comma-separated symbols instead of the universe")
    r.add_argument("--refresh-days", type=float, default=REFRESH_DAYS)
    sub.add_parser("status", help="what is stored and what failed last time")
    c = sub.add_parser("check", help="four-year completeness of every symbol (writes completeness.parquet)")
    c.add_argument("--min-complete", type=float, default=0.9, help="exit 1 when a market has a smaller share of complete symbols")
    c.add_argument("--market", default=None)
    a = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    if a.cmd == "status":
        print(json.dumps(status(), indent=2, default=str))
        return 0
    if a.cmd == "check":
        from . import yf_completeness
        df = yf_completeness.check(universe_symbols([m.strip() for m in a.market.split(",")] if a.market else None))
        sm = yf_completeness.summary(df)
        print(f"{_now():%Y-%m-%d %H:%M:%S} yahoo fundamentals completeness: {sm['symbols']} symbols, by status {sm['by_status']}")
        low = []
        for m, d in sorted(sm["by_market"].items()):
            print(f"  {m:<3} {d['symbols']:>4} symbols, {d['complete']:>4} complete ({d['complete_share'] if d['complete_share'] is not None else 'n/a'} of those fetched)"
                  + "".join(f", {k} {v}" for k, v in d.items() if k not in ("symbols", "complete", "complete_share")))
            if d["complete_share"] is not None and d["complete_share"] < a.min_complete:
                low.append(m)
        bad = df[~df["status"].isin(["complete", "pending"])].head(15)
        for r in bad.itertuples():
            print(f"  {r.symbol:<12} {r.market:<3} {r.status:<12} {r.reason}")
        return 1 if low else 0
    if a.symbols:
        syms = {s.strip(): "?" for s in a.symbols.split(",") if s.strip()}
    else:
        syms = universe_symbols([m.strip() for m in a.market.split(",")] if a.market else None)
    import threading
    import signal
    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())
    log.info("yahoo fundamentals: %d symbols in scope, limit %s, refresh %s days", len(syms), a.limit, a.refresh_days)
    s = run(syms, limit=a.limit, refresh_days=a.refresh_days, stop=stop, progress=log.info, catch_up=a.catch_up)
    log.info("yahoo fundamentals done: %s", json.dumps(s))
    print(f"{_now():%Y-%m-%d %H:%M:%S} yahoo fundamentals: {s['symbols']} symbols ({s['complete']} complete, {s['partial']} partial), "
          f"{s['new_versions']} new values; problems by class: {({k: v for k, v in s['classes'].items() if k != 'ok'})}")
    return 130 if s["stopped"] else 0


if __name__ == "__main__":
    sys.exit(main())
