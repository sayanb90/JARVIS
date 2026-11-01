"""
REST jobs of the probe: one function per dataset in the signal catalogue.

Each job fetches, parses defensively (the shapes come from the documentation, not from live traffic yet), stores the
rows in the probe store, keeps the first raw responses as samples, and returns a `notes` dict of what it learned
(history depth, counts, sizes).  A parser that fails does not stop the job: the failure is recorded and the raw
response is kept, so the shape can be fixed tomorrow without losing the day.

`mode` is "check" (small, one pass) or "run" (the sampling used for the cost measurement).
"""

from __future__ import annotations

import gzip
import json
import logging
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Callable, Optional
from urllib.parse import quote

from .store import ProbeStore
from ..rest import Resp, UpstoxRest

log = logging.getLogger(__name__)

MASTER_URL = "https://assets.upstox.com/market-quote/instruments/exchange/{name}.json.gz"
INDEX_NAMES = ["Nifty 50", "Nifty Bank", "India VIX", "Nifty Fin Service", "Nifty Next 50", "Nifty Midcap Select", "Nifty IT",
               "Nifty Auto", "Nifty Pharma", "Nifty Metal", "Nifty FMCG", "Nifty Energy", "Nifty Realty", "Nifty PSU Bank",
               "Nifty Private Bank", "Nifty Midcap 100", "Nifty Smallcap 100", "Nifty 500"]
GLOBAL_NAMES = ["GIFT NIFTY", "Dow Jones", "S&P", "US Tech 100", "FTSE 100", "DAX", "CAC 40", "Nikkei 225", "Hang Seng",
                "USD INR", "Brent", "WTI"]
UNDERLYINGS = {"NIFTY": "Nifty 50", "BANKNIFTY": "Nifty Bank"}
FII_TYPES = ["NSE_FO|INDEX_FUTURES", "NSE_FO|STOCK_FUTURES", "NSE_FO|INDEX_OPTIONS", "NSE_FO|STOCK_OPTIONS", "NSE_EQ|CASH"]
EXPIRY_KEYWORDS = ["current_week", "next_week", "current_month"]
BLUE_CHIP_ISINS = ["INE002A01018", "INE467B01029", "INE009A01021", "INE040A01034", "INE090A01021"]   # Reliance, TCS, Infosys, HDFC Bank, ICICI Bank


@dataclass
class Ctx:
    rest: UpstoxRest
    store: ProbeStore
    now: Callable[[], datetime] = lambda: datetime.now(timezone.utc).replace(tzinfo=None)
    eq_keys: dict = field(default_factory=dict)          # instrument_key -> symbol
    fo_keys: dict = field(default_factory=dict)
    index_keys: dict = field(default_factory=dict)
    global_keys: dict = field(default_factory=dict)
    underlying_keys: dict = field(default_factory=dict)  # NIFTY -> NSE_INDEX|Nifty 50
    isins: list = field(default_factory=list)
    fund_sample: int = 25
    news_stocks: int = 500
    seen_news: set = field(default_factory=set)
    out: dict = field(default_factory=dict)              # facts shared between jobs (for example expiry dates)

    def today(self) -> date:
        return self.now().date()


# ----------------------------------------------------------------------------------------------------- helpers
def g(d, *path, default=None):
    """Safe nested lookup through dicts and lists."""
    cur = d
    for p in path:
        try:
            cur = cur[p]
        except (KeyError, IndexError, TypeError):
            return default
    return cur


def chunks(seq: list, n: int):
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


def weekdays_back(day: date, n: int) -> date:
    d = day - timedelta(days=n)
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d


def call(ctx: Ctx, path: str, params: Optional[dict] = None, tag: Optional[str] = None, sample: Optional[str] = None) -> Resp:
    r = ctx.rest.get(path, params, tag=tag)
    if sample:
        ctx.store.save_sample(sample, {"path": path, "params": params, "status": r.status, "error": [r.error_code, r.error_message],
                                       "body": r.data})
    return r


def stamp(ctx: Ctx) -> str:
    return ctx.now().strftime("%Y-%m-%dT%H:%M:%SZ")


def safe(parse: Callable, ctx: Ctx, tag: str, r: Resp, notes: dict) -> list:
    """Run a parser; on failure record it and return no rows."""
    try:
        return parse(r.data) if r.ok else []
    except Exception as exc:                                             # noqa: BLE001
        notes.setdefault("parse_errors", {})[tag] = f"{type(exc).__name__}: {exc}"[:200]
        log.warning("parser %s failed: %s", tag, exc)
        return []


# ----------------------------------------------------------------------------------------------------- reference data
def job_instrument_master(ctx: Ctx, mode: str) -> dict:
    """Dated copy of the instrument master (survivorship control) and the keys for indices, VIX and global indices."""
    notes: dict = {}
    for name, store_it in (("NSE", True), ("global", False)):
        status, content = ctx.rest.fetch_raw(MASTER_URL.format(name=name))
        notes[f"{name}_http"] = status
        if status != 200 or not content:
            continue
        if store_it:
            ctx.store.write_blob("instrument_master", f"{name}-{ctx.today()}.json.gz", content, day=ctx.today())
        notes[f"{name}_gz_bytes"] = len(content)
        try:
            items = json.loads(gzip.decompress(content))
        except Exception as exc:                                         # noqa: BLE001
            notes[f"{name}_parse_error"] = str(exc)[:200]
            continue
        notes[f"{name}_records"] = len(items)
        if name == "NSE":
            by_name = {str(i.get("name") or i.get("trading_symbol") or "").lower(): i for i in items if i.get("segment") == "NSE_INDEX"}
            for want in INDEX_NAMES:
                i = by_name.get(want.lower())
                if i:
                    ctx.index_keys[i["instrument_key"]] = f"NSE_INDEX:{want}"
            for u, n in UNDERLYINGS.items():
                i = by_name.get(n.lower())
                if i:
                    ctx.underlying_keys[u] = i["instrument_key"]
            notes["index_keys_found"] = len(ctx.index_keys)
        else:
            for i in items:
                label = f"{i.get('name', '')} {i.get('trading_symbol', '')}".lower()
                for want in GLOBAL_NAMES:
                    if want.lower() in label and i.get("instrument_key") not in ctx.global_keys:
                        ctx.global_keys[i["instrument_key"]] = f"GLOBAL:{i.get('trading_symbol') or want}"
                        break
            notes["global_keys_found"] = len(ctx.global_keys)
    for u, k in {"NIFTY": "NSE_INDEX|Nifty 50", "BANKNIFTY": "NSE_INDEX|Nifty Bank"}.items():
        ctx.underlying_keys.setdefault(u, k)                             # fall back to the documented keys
    return notes


def job_market_info(ctx: Ctx, mode: str) -> dict:
    notes: dict = {}
    today = ctx.today().isoformat()
    candidates = {"holidays": ["/v2/market/holidays"], "status": ["/v2/market/status/NSE", "/v2/market/status/NSE_EQ"],
                  "timings": [f"/v2/market/timings/{today}"]}
    for name, paths in candidates.items():
        for p in paths:
            r = call(ctx, p, tag=f"market.{name}", sample=f"market_{name}")
            if r.ok:
                notes[f"{name}_path"] = p
                data = r.data.get("data") if isinstance(r.data, dict) else r.data
                rows = data if isinstance(data, list) else [data]
                ctx.store.write("market_info", [{"fetched_at": stamp(ctx), "kind": name, "payload": x} for x in rows if x is not None])
                break
        else:
            notes[f"{name}_path"] = None
    return notes


# ----------------------------------------------------------------------------------------------------- M2 daily quote snapshot
QUOTE_FIELDS = ["instrument_token", "symbol", "last_price", "volume", "average_price", "oi", "oi_day_high", "oi_day_low",
                "previous_oi", "net_change", "total_buy_quantity", "total_sell_quantity", "upper_circuit_limit",
                "lower_circuit_limit", "year_high", "year_low", "prev_close_price", "last_trade_time",
                "indicative_equilibrium_price", "indicative_equilibrium_quantity", "indicative_imbalance_quantity_total",
                "indicative_imbalance_quantity_market", "reference_price", "cas_eligible", "timestamp"]


def parse_quotes(body: dict, fetched: str) -> list[dict]:
    rows = []
    for k, q in (body.get("data") or {}).items():
        if not isinstance(q, dict):
            continue
        row = {"key": k, "fetched_at": fetched, **{f: q.get(f) for f in QUOTE_FIELDS}}
        o = q.get("ohlc") or {}
        row.update({"open": o.get("open"), "high": o.get("high"), "low": o.get("low"), "close": o.get("close"),
                    "ohlc_volume": o.get("volume")})
        rows.append(row)
    return rows


def job_quotes(ctx: Ctx, mode: str) -> dict:
    """Full market quote V3 for the equity universe and the futures in the F&O selection (no depth is stored)."""
    keys = list(ctx.eq_keys) + [k for k, s in ctx.fo_keys.items() if "_FUT_" in s]
    if mode == "check":
        keys = keys[:20]
    notes: dict = {"n_inst": len(keys)}
    rows = 0
    for i, part in enumerate(chunks(keys, 500)):
        r = call(ctx, "/v3/market-quote/quotes", {"instrument_key": ",".join(part)}, tag="quotes.full", sample="quotes_full")
        rows += ctx.store.write("quote_snapshot", safe(lambda b: parse_quotes(b, stamp(ctx)), ctx, "quotes", r, notes))[0]
    notes["rows"] = rows
    return notes


# ----------------------------------------------------------------------------------------------------- M5, M14 option data
def parse_option_chain(body: dict, fetched: str, u: str, expiry_param: str) -> list[dict]:
    rows = []
    for item in body.get("data") or []:
        for side, blk in (("CE", item.get("call_options")), ("PE", item.get("put_options"))):
            if not blk:
                continue
            md, gk = blk.get("market_data") or {}, blk.get("option_greeks") or {}
            rows.append({"fetched_at": fetched, "underlying": u, "expiry_param": expiry_param, "expiry": item.get("expiry"),
                         "strike": item.get("strike_price"), "spot": item.get("underlying_spot_price"), "pcr": item.get("pcr"),
                         "side": side, "key": blk.get("instrument_key"), "ltp": md.get("ltp"), "close": md.get("close_price"),
                         "volume": md.get("volume"), "oi": md.get("oi"), "prev_oi": md.get("prev_oi"), "bid": md.get("bid_price"),
                         "bid_qty": md.get("bid_qty"), "ask": md.get("ask_price"), "ask_qty": md.get("ask_qty"),
                         "delta": gk.get("delta"), "gamma": gk.get("gamma"), "theta": gk.get("theta"), "vega": gk.get("vega"),
                         "iv": gk.get("iv"), "pop": gk.get("pop")})
    return rows


def job_option_chain(ctx: Ctx, mode: str) -> dict:
    notes: dict = {"snapshots": 0}
    for u, key in ctx.underlying_keys.items():
        for exp in (["current_week"] if mode == "check" else ["current_week", "current_month"]):
            r = call(ctx, "/v2/option/chain", {"instrument_key": key, "expiry_date": exp}, tag="option.chain", sample="option_chain")
            rows = safe(lambda b: parse_option_chain(b, stamp(ctx), u, exp), ctx, "option_chain", r, notes)
            ctx.store.write("option_chain", rows)
            notes["snapshots"] += 1 if rows else 0
            notes[f"rows_{u}_{exp}"] = len(rows)
    return notes


def _stat_rows(kind: str, body: dict, base: dict) -> list[dict]:
    d = body.get("data") or {}
    if kind in ("pcr", "max_pain"):
        rows = [{**base, "time": None, kind: d.get(kind if kind == "pcr" else "max_pain"), "spot": d.get("spot_closing_price"),
                 "expiry_date": d.get("expiry_date")}]
        for p in d.get("insights") or []:
            rows.append({**base, "time": p.get("time"), kind: p.get(kind if kind == "pcr" else "max_pain"),
                         "spot": p.get("spot_price"), "expiry_date": d.get("expiry_date")})
        return rows
    lst = d.get("call_put_oi_data_list") or []
    return [{**base, "strike": x.get("strike_price"), "call": x.get("call_oi", x.get("call_change_oi")),
             "put": x.get("put_oi", x.get("put_change_oi")), "total_calls": d.get("total_calls", d.get("total_call_change_oi")),
             "total_puts": d.get("total_puts", d.get("total_put_change_oi")), "spot": d.get("spot_closing_price"),
             "expiry_date": d.get("expiry")} for x in lst]


def job_option_stats(ctx: Ctx, mode: str) -> dict:
    """PCR, max pain, OI by strike and change in OI from the Upstox analytics endpoints, plus a history-depth ladder."""
    notes: dict = {"depth": {}}
    day = ctx.today().isoformat()
    expiries = ["current_week"] if mode == "check" else EXPIRY_KEYWORDS
    for u, key in ctx.underlying_keys.items():
        for exp in expiries:
            for kind, path, extra in (("pcr", "/v2/market/pcr", {"bucket_interval": 60}), ("max_pain", "/v2/market/max-pain", {"bucket_interval": 60}),
                                      ("oi", "/v2/market/oi", {}), ("change_oi", "/v2/market/change-oi", {"interval": 1})):
                params = {"instrument_key": key, "expiry": exp, "date": day, **extra}
                r = call(ctx, path, params, tag=f"stats.{kind}", sample=f"stats_{kind}")
                base = {"fetched_at": stamp(ctx), "underlying": u, "expiry_param": exp, "date": day}
                ctx.store.write(f"optstats_{kind}", safe(lambda b: _stat_rows(kind, b, base), ctx, f"stats_{kind}", r, notes))
    # depth ladder: how far back does a past date return data?
    key = ctx.underlying_keys.get("NIFTY")
    if key:
        for label, back in (("1d", 1), ("1w", 7), ("1m", 30), ("3m", 90), ("6m", 180), ("1y", 365)):
            past = weekdays_back(ctx.today(), back).isoformat()
            r = call(ctx, "/v2/market/pcr", {"instrument_key": key, "expiry": "current_month", "date": past, "bucket_interval": 60},
                     tag="stats.pcr_depth")
            pts = len(g(r.data, "data", "insights", default=[]) or []) if r.ok else None
            notes["depth"][label] = {"date": past, "ok": r.ok, "status": r.status, "code": r.error_code, "insight_points": pts}
    return notes


# ----------------------------------------------------------------------------------------------------- M7 flows, M14 smartlists
def job_flows(ctx: Ctx, mode: str) -> dict:
    """FII and DII activity, daily and monthly; also records how far back each goes."""
    notes: dict = {"earliest": {}}
    rows = []
    today = ctx.today()
    jobs = [("fii", "/v2/market/fii", t) for t in FII_TYPES] + [("dii", "/v2/market/dii", "NSE_EQ|CASH")]
    for who, path, dtype in jobs:
        for interval, start in (("1D", today - timedelta(days=30)), ("1M", date(2026, 1, 1))):
            r = call(ctx, path, {"data_type": dtype, "interval": interval, "from": start.isoformat()}, tag=f"flows.{who}.{interval}",
                     sample=f"flows_{who}")
            data = g(r.data, "data", default={}) if r.ok else {}
            for seg, recs in (data.items() if isinstance(data, dict) else []):
                for x in recs or []:
                    rows.append({"fetched_at": stamp(ctx), "who": who, "segment": seg, "interval": interval, **x})
                    ts = x.get("time_stamp")
                    if ts:
                        d = datetime.fromtimestamp(ts / 1000, timezone.utc).date().isoformat()
                        cur = notes["earliest"].get(f"{who}.{interval}")
                        notes["earliest"][f"{who}.{interval}"] = d if cur is None else min(cur, d)
    ctx.store.write("flows", rows)
    notes["rows"] = len(rows)
    return notes


def job_smartlists(ctx: Ctx, mode: str) -> dict:
    notes: dict = {}
    plans = [("options", "INDEX", ["TOP_TRADED", "MOST_ACTIVE", "OI_GAINERS", "OI_LOSERS", "IV_GAINERS", "IV_LOSERS"]),
             ("options", "STOCK", ["MOST_ACTIVE", "OI_GAINERS", "IV_GAINERS"]),
             ("futures", "INDEX", ["PREMIUM", "DISCOUNT", "OI_GAINERS"]), ("futures", "STOCK", ["PREMIUM", "DISCOUNT", "MOST_ACTIVE"])]
    rows = []
    for kind, asset, cats in plans:
        for cat in (cats[:1] if mode == "check" else cats):
            r = call(ctx, f"/v2/market/smartlist/{kind}", {"asset_type": asset, "category": cat, "page_size": 50},
                     tag=f"smartlist.{kind}", sample=f"smartlist_{kind}")
            d = g(r.data, "data", default={}) if r.ok else {}
            for i, x in enumerate(d.get("smartlist") or [], start=1):
                rows.append({"fetched_at": stamp(ctx), "kind": kind, "asset_type": asset, "category": cat, "rank": i,
                             "key": x.get("instrument_key"), "price": g(x, "price", "current"), "close": g(x, "price", "close_price"),
                             "change_pct": g(x, "price", "change_pct"), "metric": g(x, "metric", "current"),
                             "metric_prev": g(x, "metric", "previous"), "metric_change_pct": g(x, "metric", "change_pct"),
                             "list_ts": d.get("time_stamp")})
    ctx.store.write("smartlists", rows)
    notes["rows"] = len(rows)
    return notes


# ----------------------------------------------------------------------------------------------------- M11 news, H14 global
def job_news(ctx: Ctx, mode: str) -> dict:
    keys = list(ctx.eq_keys)[: (30 if mode == "check" else ctx.news_stocks)]
    notes: dict = {"n_inst": len(keys), "new_items": 0, "items_seen": 0}
    rows = []
    for part in chunks(keys, 30):
        r = call(ctx, "/v2/news", {"category": "instrument_keys", "instrument_keys": ",".join(part), "page_size": 100}, tag="news",
                 sample="news")
        d = g(r.data, "data", default={}) if r.ok else {}
        for k, items in (d.items() if isinstance(d, dict) else []):
            for it in items or []:
                notes["items_seen"] += 1
                sig = (k, it.get("published_time"), it.get("heading"))
                if sig in ctx.seen_news:
                    continue
                ctx.seen_news.add(sig)
                rows.append({"fetched_at": stamp(ctx), "key": k, "published_time": it.get("published_time"),
                             "heading": it.get("heading"), "summary": it.get("summary"), "link": it.get("article_link")})
    ctx.store.write("news", rows)
    notes["new_items"] = len(rows)
    return notes


def job_global(ctx: Ctx, mode: str) -> dict:
    notes: dict = {"n_inst": len(ctx.global_keys)}
    if not ctx.global_keys:
        notes["skipped"] = "no global keys found in the instrument file"
        return notes
    r = call(ctx, "/v3/market-quote/ltp", {"instrument_key": ",".join(ctx.global_keys)}, tag="global.ltp", sample="global_ltp")
    rows = []
    for k, q in (g(r.data, "data", default={}) or {}).items() if r.ok else []:
        rows.append({"fetched_at": stamp(ctx), "key": k, "token": q.get("instrument_token"), "last_price": q.get("last_price"),
                     "cp": q.get("cp"), "ltq": q.get("ltq"), "volume": q.get("volume")})
    ctx.store.write("global_cues", rows)
    notes["rows"] = len(rows)
    return notes


# ----------------------------------------------------------------------------------------------------- M9, M10 candles
def _candle_rows(body: dict, key: str, interval: str) -> list[dict]:
    return [{"key": key, "interval": interval, "ts": c[0], "open": c[1], "high": c[2], "low": c[3], "close": c[4],
             "volume": c[5], "oi": c[6] if len(c) > 6 else None} for c in g(body, "data", "candles", default=[]) or []]


def job_candles_sample(ctx: Ctx, mode: str) -> dict:
    """Daily history since 2000 and minute history for a few stocks: sizes, request counts and depth, to cost the backfill."""
    n = 1 if mode == "check" else 5
    sample = list(ctx.eq_keys.items())[:n]
    notes: dict = {"stocks": [s for _, s in sample], "daily": {}, "minute": {}}
    today = ctx.today()
    d_rows = d_reqs = m_rows = m_reqs = 0
    earliest_daily = earliest_min = None
    for key, sym in sample:
        qk = quote(key, safe="")
        start = 2000                                                     # decades: 2000-2009, 2010-2019, 2020-now
        while start <= today.year:
            to, frm = min(date(start + 9, 12, 31), today), date(start, 1, 1)
            r = call(ctx, f"/v3/historical-candle/{qk}/days/1/{to.isoformat()}/{frm.isoformat()}", tag="candles.daily", sample="candles_daily")
            rows = safe(lambda b: _candle_rows(b, key, "day"), ctx, "candles_daily", r, notes)
            d_rows += ctx.store.write("candles_daily", rows)[0]
            d_reqs += 1
            if rows:
                ts = min(x["ts"] for x in rows)
                earliest_daily = ts if earliest_daily is None else min(earliest_daily, ts)
            start += 10
        month_end = date(today.year, today.month, 1) - timedelta(days=1)
        month_start = date(month_end.year, month_end.month, 1)
        for unit_iv in ("1", "5"):
            r = call(ctx, f"/v3/historical-candle/{qk}/minutes/{unit_iv}/{month_end.isoformat()}/{month_start.isoformat()}",
                     tag=f"candles.minute{unit_iv}", sample=f"candles_minute{unit_iv}")
            rows = safe(lambda b: _candle_rows(b, key, f"{unit_iv}m"), ctx, "candles_minute", r, notes)
            m_rows += ctx.store.write("candles_minute", rows)[0] if unit_iv == "1" else 0
            m_reqs += 1
        if mode != "check":                                              # how far back do minutes go?
            r = call(ctx, f"/v3/historical-candle/{qk}/minutes/1/2022-01-31/2022-01-01", tag="candles.minute_depth", sample="candles_minute_depth")
            first = g(r.data, "data", "candles", default=[]) if r.ok else []
            if first:
                ts = min(c[0] for c in first)
                earliest_min = ts if earliest_min is None else min(earliest_min, ts)
    notes["daily"] = {"rows": d_rows, "requests": d_reqs, "earliest": earliest_daily}
    notes["minute"] = {"rows_one_month_1min": m_rows, "requests": m_reqs, "earliest_jan2022_probe": earliest_min}
    notes["n_inst"] = len(sample)
    return notes


# ----------------------------------------------------------------------------------------------------- M8 expired F&O (Plus)
def job_expired_sample(ctx: Ctx, mode: str) -> dict:
    notes: dict = {}
    key = ctx.underlying_keys.get("NIFTY", "NSE_INDEX|Nifty 50")
    r = call(ctx, "/v2/expired-instruments/expiries", {"instrument_key": key}, tag="expired.expiries", sample="expired_expiries")
    notes["expiries_ok"], notes["expiries_status"], notes["expiries_code"] = r.ok, r.status, r.error_code
    expiries = sorted(g(r.data, "data", default=[]) or []) if r.ok else []
    notes["expiries_count"] = len(expiries)
    if expiries:
        notes["expiry_oldest"], notes["expiry_newest"] = expiries[0], expiries[-1]
        ctx.out["expired_expiries"] = expiries
        ctx.store.write("expired_expiries", [{"fetched_at": stamp(ctx), "underlying": "NIFTY", "expiry": e} for e in expiries])
    else:
        return notes
    pick = expiries[-1:] if mode == "check" else expiries[-2:]
    contracts = futures = candle_rows = candle_reqs = 0
    for exp in pick:
        r1 = call(ctx, "/v2/expired-instruments/option/contract", {"instrument_key": key, "expiry_date": exp}, tag="expired.options",
                  sample="expired_options")
        opts = g(r1.data, "data", default=[]) if r1.ok else []
        r2 = call(ctx, "/v2/expired-instruments/future/contract", {"instrument_key": key, "expiry_date": exp}, tag="expired.futures",
                  sample="expired_futures")
        futs = g(r2.data, "data", default=[]) if r2.ok else []
        contracts += len(opts or [])
        futures += len(futs or [])
        ctx.store.write("expired_contracts", [{"fetched_at": stamp(ctx), "underlying": "NIFTY", "expiry": exp, **{
            k: c.get(k) for k in ("instrument_key", "trading_symbol", "instrument_type", "strike_price", "lot_size", "weekly")}}
            for c in (opts or []) + (futs or [])])
        for c in ((opts or [])[:15] + (futs or [])[:1]) if mode != "check" else ((opts or [])[:2]):
            ek = quote(c.get("instrument_key", ""), safe="")
            to = exp
            frm = (date.fromisoformat(exp) - timedelta(days=14)).isoformat()
            rr = call(ctx, f"/v2/expired-instruments/historical-candle/{ek}/1minute/{to}/{frm}", tag="expired.candles", sample="expired_candles")
            rows = safe(lambda b: _candle_rows(b, c.get("instrument_key"), "1m"), ctx, "expired_candles", rr, notes)
            candle_rows += ctx.store.write("expired_candles", rows)[0]
            candle_reqs += 1
    notes.update({"contracts_sampled_expiries": len(pick), "option_contracts": contracts, "future_contracts": futures,
                  "candle_rows": candle_rows, "candle_requests": candle_reqs, "contracts_per_expiry": contracts // max(1, len(pick))})
    return notes


# ----------------------------------------------------------------------------------------------------- F1 to F9 fundamentals
def _statement_rows(isin: str, ep: str, body: dict, fetched: str) -> tuple[list[dict], int]:
    """Long rows (isin, endpoint, item, period, value, change) and the number of periods returned."""
    d, rows, periods = body.get("data"), [], set()

    def add(item, hist):
        for h in hist or []:
            periods.add(h.get("period"))
            rows.append({"isin": isin, "endpoint": ep, "item": item, "period": h.get("period"), "value": h.get("value"),
                         "change": h.get("change"), "fetched_at": fetched})
    if ep.startswith("income") or ep.startswith("cashflow"):
        for blk in (d or {}).get("income_statement" if ep.startswith("income") else "cash_flow", []) or []:
            add(blk.get("category"), blk.get("history"))
        for blk in (d or {}).get("full_statement", []) or []:
            add("full:" + str(blk.get("particular")), blk.get("history"))
    elif ep.startswith("balance"):
        for h in (d or {}).get("history", []) or []:
            periods.add(h.get("period"))
            rows += [{"isin": isin, "endpoint": ep, "item": "total_asset", "period": h.get("period"), "value": h.get("total_asset"),
                      "change": None, "fetched_at": fetched},
                     {"isin": isin, "endpoint": ep, "item": "total_liability", "period": h.get("period"), "value": h.get("total_liability"),
                      "change": None, "fetched_at": fetched}]
        for blk in (d or {}).get("full_statement", []) or []:
            add("full:" + str(blk.get("particular")), blk.get("history"))
    elif ep == "key_ratios":
        for x in d or []:
            rows.append({"isin": isin, "endpoint": ep, "item": x.get("name"), "period": "snapshot", "value": x.get("company_value"),
                         "change": x.get("sector_value"), "fetched_at": fetched})
        periods.add("snapshot")
    elif ep == "shareholding":
        for blk in d or []:
            add(blk.get("category"), blk.get("history"))
    elif ep == "corporate_actions":
        for x in d or []:
            rows.append({"isin": isin, "endpoint": ep, "item": x.get("name"), "period": x.get("expiry_date"), "value": x.get("amount"),
                         "change": x.get("ratio"), "fetched_at": fetched})
            periods.add(x.get("expiry_date"))
    else:                                                                # profile, competitors: keep the payload as text
        rows.append({"isin": isin, "endpoint": ep, "item": "payload", "period": "snapshot", "value": None,
                     "change": json.dumps(d, default=str)[:4000], "fetched_at": fetched})
        periods.add("snapshot")
    return rows, len([p for p in periods if p])


FUND_ENDPOINTS = [("income_yearly", "income-statement", {"time_period": "yearly"}),
                  ("income_yearly_full", "income-statement", {"time_period": "yearly", "fs": "true"}),
                  ("income_quarterly", "income-statement", {"time_period": "quarterly"}),
                  ("balance", "balance-sheet", {}), ("balance_full", "balance-sheet", {"fs": "true"}),
                  ("cashflow", "cash-flow", {}), ("cashflow_full", "cash-flow", {"fs": "true"}),
                  ("key_ratios", "key-ratios", {}), ("shareholding", "share-holdings", {}),
                  ("corporate_actions", "corporate-actions", {}), ("profile", "profile", {}), ("competitors", "competitors", {})]


def job_fundamentals_sample(ctx: Ctx, mode: str) -> dict:
    isins = (ctx.isins or BLUE_CHIP_ISINS)[: (2 if mode == "check" else ctx.fund_sample)]
    notes: dict = {"n_inst": len(isins), "periods": {}, "calls": 0}
    rows_total = 0
    for isin in isins:
        for ep, path, params in FUND_ENDPOINTS:
            r = call(ctx, f"/v2/fundamentals/{isin}/{path}", params or None, tag=f"fund.{ep}", sample=f"fund_{ep}")
            notes["calls"] += 1
            if not r.ok:
                continue
            try:
                rows, periods = _statement_rows(isin, ep, r.data, stamp(ctx))
            except Exception as exc:                                     # noqa: BLE001
                notes.setdefault("parse_errors", {})[ep] = f"{type(exc).__name__}: {exc}"[:200]
                continue
            rows_total += ctx.store.write("fundamentals", rows)[0]
            cur = notes["periods"].setdefault(ep, {"min": periods, "max": periods})
            cur["min"], cur["max"] = min(cur["min"], periods), max(cur["max"], periods)
    notes["rows"] = rows_total
    return notes


JOBS: dict[str, Callable[[Ctx, str], dict]] = {
    "instrument_master": job_instrument_master, "market_info": job_market_info, "quotes": job_quotes,
    "option_chain": job_option_chain, "option_stats": job_option_stats, "flows": job_flows, "smartlists": job_smartlists,
    "news": job_news, "global_cues": job_global, "candles_sample": job_candles_sample, "expired_sample": job_expired_sample,
    "fundamentals_sample": job_fundamentals_sample,
}

# catalogue id, title, and how the probe's cost scales to the production plan (see report.py)
CATALOGUE = {
    "quotes": ("M2", "Daily quote snapshot", {"dataset": "quote_snapshot", "polls_per_day": 1, "scale_to": 500, "scale_basis": "equities in the plan"}),
    "option_chain": ("M14", "Option chain snapshots (deferred)", {"dataset": "option_chain", "polls_per_day": 26}),
    "option_stats": ("M5", "PCR, max pain, OI, change in OI", {"dataset": "optstats_*", "polls_per_day": 7}),
    "flows": ("M7", "FII and DII flows", {"dataset": "flows", "polls_per_day": 1}),
    "smartlists": ("M14", "Smartlists (deferred)", {"dataset": "smartlists", "polls_per_day": 7}),
    "news": ("M11", "News flow", {"dataset": "news", "polls_per_day": 14}),
    "global_cues": ("H14", "Global cues", {"dataset": "global_cues", "polls_per_day": 80}),
    "candles_sample": ("M9, M10", "Candles backfill sample", {"dataset": "candles_*", "one_off": True}),
    "expired_sample": ("M8", "Expired F&O archive sample", {"dataset": "expired_*", "one_off": True}),
    "fundamentals_sample": ("F1 to F9", "Fundamentals sample", {"dataset": "fundamentals", "one_off": True, "scale_to": 500,
                                                               "scale_basis": "stocks, refreshed 4 times a year", "refreshes_per_year": 4}),
    "instrument_master": ("M12", "Instrument master snapshot", {"dataset": "instrument_master", "polls_per_day": 1}),
    "market_info": ("M13", "Trading calendar and status", {"dataset": "market_info", "polls_per_day": 1}),
}
