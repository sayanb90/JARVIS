"""
History backfill from the Upstox REST API (SCRUM-102, SCRUM-101): the live feed only records from the day it started, so this fills
in what came before it, as 1-minute candles.

  python -m data_layer.upstox.backfill spot    --from 2022-01-01 [--to 2026-11-02] [--limit 5] [--dry-run]
  python -m data_layer.upstox.backfill expired --from 2022-11-01 [--underlying NIFTY BANKNIFTY] [--strikes 5] [--dry-run]
  python -m data_layer.upstox.backfill status

spot      1-minute candles of the equity universe and the index keys (Nifty 50, Nifty Bank, India VIX, ...) from the Historical Candle
          Data V3 API (all plans; minute candles exist from January 2022).  One call per instrument and calendar month.
expired   1-minute candles, with open interest, of EXPIRED index options and futures from the Expired Instruments API (Upstox Plus).
          For every past expiry of an underlying: the strikes within `--strikes` steps of the money on any day of the contract's last
          `--window-days` days (the index's low-to-high range over those days, widened by N strikes on each side), calls and puts, and the
          future that expired that day.  So liquid and illiquid strikes are both there, and the band follows the index as it moved.

What a candle is: time, open, high, low, close, volume, open interest.  There is NO bid, ask or size in it: nothing quote-based (spread,
imbalance, touch size) can be rebuilt from this history; that exists only from the live feed (2026-11-03 onwards).

Storage: lake/bars/interval=1m/date=YYYY-MM-DD/bars.parquet, rows tagged src='rest', market 'IN'; the same table the candle tool
(candles.py) writes.  Expired contracts are also listed in lake/instruments/fo_expired.parquet (symbol, keys, strike, expiry, lot size).

Resumable: every (instrument, month) is recorded in lake/backfill/state.json when it is stored; a run that stops (token expired, rate limit,
Ctrl-C, disk guard) continues where it left off.  `ok` and `empty` are not fetched again; `error` is retried by the next run.  All calls
share the Upstox rate limiter (2,000 per 30 minutes), so a full pass takes hours: run it outside the feed window or let it run.

Endpoints (official SDK, upstox-python-sdk 2.30.0):
  GET /v3/historical-candle/{key}/minutes/1/{to}/{from}                       one calendar month per call for 1-minute candles
  GET /v3/historical-candle/{key}/days/1/{to}/{from}                          the index's daily range, to place the money
  GET /v2/expired-instruments/expiries?instrument_key=                        past expiry dates of an underlying
  GET /v2/expired-instruments/option/contract?instrument_key=&expiry_date=    the option contracts of one expiry
  GET /v2/expired-instruments/future/contract?instrument_key=&expiry_date=    the future of one expiry (empty for a weekly expiry)
  GET /v2/expired-instruments/historical-candle/{expired_key}/1minute/{to}/{from}
UNVERIFIED until the first run on the VPS (this was built without network access to Upstox): the field names of the contract lists (read
tolerantly, with fallbacks), how far back the expiry list goes, and the range one expired-candle call accepts (month chunks are used).
`--dump-raw N` saves the first N raw responses to logs/backfill/ so the shapes can be checked; start with `--limit` and `--dry-run`.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import shutil
import sys
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable, Optional
from urllib.parse import quote

import pandas as pd

from .. import lake as lake_mod
from .. import tick_store
from ..ticks import Bar
from .candles import candles_to_bars, ist_today
from .fo_instruments import contract_symbol, expiry_date
from .instruments import INDEX_KEYS, build_key_map
from .ratelimit import RateLimiter
from .rest import Resp, UpstoxRest

log = logging.getLogger(__name__)

MINUTE_HISTORY_START = date(2022, 1, 1)         # Upstox: minute candles are available from January 2022
UNDERLYING_KEYS = {"NIFTY": "NSE_INDEX|Nifty 50", "BANKNIFTY": "NSE_INDEX|Nifty Bank"}
OK, EMPTY, ERROR = "ok", "empty", "error"
FLUSH_BARS = 250_000                            # bars held in memory before they are written and the touched days compacted
MIN_FREE_GB = 15.0                              # stop before the disk the live feed writes to runs low
PLUS_ERROR = "UDAPI1149"                        # "requires an Upstox Plus subscription"
DEFAULT_BUDGET_30MIN = 1500                     # of the 2,000 calls per 30 minutes Upstox allows; the limiter keeps a further 10 % margin
EX_TEMPFAIL = 75                                # exit code: stopped by --max-minutes, run again to continue


class Stop(Exception):
    """The run cannot usefully continue (token rejected, plan missing, disk low).  What was stored stays stored."""


class TimeUp(Stop):
    """--max-minutes ran out.  Not a failure: everything fetched is stored and the next run continues where this one stopped."""


# ---------------------------------------------------------------------------------------------- small helpers
def enc(key: str) -> str:
    return quote(key, safe="")


def month_chunks(d0: date, d1: date) -> list[tuple[date, date]]:
    """Calendar-month pieces of [d0, d1], oldest first: the V3 API serves one month of 1-minute candles per call."""
    out, cur = [], d0
    while cur <= d1:
        nxt = (cur.replace(day=1) + timedelta(days=32)).replace(day=1)
        out.append((cur, min(nxt - timedelta(days=1), d1)))
        cur = nxt
    return out


def free_gb(path: Path) -> float:
    p = path
    while not p.exists():
        p = p.parent
    return shutil.disk_usage(p).free / 1e9


class State:
    """Which pieces are done: {task id: {status, rows, at, msg}} in one JSON file, replaced atomically."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.tasks: dict[str, dict] = json.loads(path.read_text()) if path.exists() else {}
        self._dirty = 0

    def done(self, task: str) -> bool:
        return self.tasks.get(task, {}).get("status") in (OK, EMPTY)

    def mark(self, task: str, status: str, rows: int = 0, msg: str = "") -> None:
        self.tasks[task] = {"status": status, "rows": rows, "at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                            **({"msg": msg[:200]} if msg else {})}
        self._dirty += 1

    def save(self) -> None:
        if not self._dirty:
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(self.tasks, sort_keys=True))
        os.replace(tmp, self.path)
        self._dirty = 0

    def summary(self) -> dict:
        out: dict[str, dict] = {}
        for task, v in self.tasks.items():
            kind = task.split("|", 1)[0]
            s = out.setdefault(kind, {OK: 0, EMPTY: 0, ERROR: 0, "rows": 0})
            s[v["status"]] = s.get(v["status"], 0) + 1
            s["rows"] += v.get("rows", 0)
        return out


@dataclass
class Sink:
    """Buffers bars, writes them as 1-minute segments and compacts the days they touched.  Tasks are marked done only once
    their bars are on disk, so a crash between fetch and write costs a re-fetch, never a hole."""
    lake_dir: Path
    state: State
    bars: list[Bar] = field(default_factory=list)
    pending: list[tuple[str, int]] = field(default_factory=list)
    written: int = 0

    def add(self, task: str, bars: list[Bar]) -> None:
        self.bars += bars
        self.pending.append((task, len(bars)))
        if len(self.bars) >= FLUSH_BARS:
            self.flush()

    def flush(self) -> None:
        if self.bars:
            days = sorted({b.ts.date() for b in self.bars})
            self.written += tick_store.write_bars(self.bars, "1m", self.lake_dir)
            for d in days:
                tick_store.compact_day("1m", d, self.lake_dir)
            self.bars = []
        for task, n in self.pending:
            self.state.mark(task, OK if n else EMPTY, n)
        self.pending = []
        self.state.save()
        if free_gb(self.lake_dir) < MIN_FREE_GB:
            raise Stop(f"less than {MIN_FREE_GB:g} GB free on the lake's disk: stopped so the live feed keeps its space")


class Fetcher:
    """Candle and contract-list calls on top of UpstoxRest (rate limited, retried, never raises on HTTP errors)."""

    def __init__(self, rest: UpstoxRest, dump_raw: int = 0, dump_dir: Path | None = None, anonymous: bool = False,
                 max_minutes: Optional[float] = None, clock=time.monotonic) -> None:
        self.rest, self.dump_raw, self.dump_dir, self.dumped = rest, dump_raw, dump_dir or Path("logs/backfill"), 0
        self.anonymous = anonymous                      # the candle endpoints answered without a token when tested (2026-11-11)
        self.clock = clock
        self.deadline = clock() + max_minutes * 60 if max_minutes else None

    def _time(self) -> None:
        if self.deadline is not None and self.clock() >= self.deadline:
            raise TimeUp("the time limit (--max-minutes) is reached: everything fetched so far is stored; run the same command again to continue")

    def _check(self, r: Resp) -> None:
        if r.status in (401, 403) and r.error_code != PLUS_ERROR:
            if self.anonymous:
                raise Stop(f"Upstox refused a request without a token (HTTP {r.status} {r.error_code}): it answered one on 2026-11-11 but may not "
                           "any more.  Put the token in .env (UPSTOX_FEED_TOKEN) and run again without --anonymous; the backfill resumes")
            raise Stop(f"Upstox rejected the token (HTTP {r.status} {r.error_code}): refresh it and run again; the backfill resumes")
        if r.error_code == PLUS_ERROR:
            raise Stop("the Expired Instruments API answered UDAPI1149: it needs the Upstox Plus plan on the account of this token")

    def _dump(self, name: str, r: Resp) -> None:
        if self.dumped < self.dump_raw:
            self.dump_dir.mkdir(parents=True, exist_ok=True)
            (self.dump_dir / f"{name}.json").write_text(json.dumps(r.data, default=str)[:200_000])
            self.dumped += 1

    def _candles(self, path: str, tag: str, name: str) -> tuple[Optional[list], str]:
        self._time()
        r = self.rest.get(path, tag=tag, auth=not self.anonymous)
        self._check(r)
        self._dump(name, r)
        if not r.ok:
            return None, f"HTTP {r.status} {r.error_code} {r.error_message}".strip()
        data = r.data.get("data") if isinstance(r.data, dict) else None
        return ((data or {}).get("candles") or []) if isinstance(data, dict) else [], ""

    def minute_candles(self, key: str, d0: date, d1: date) -> tuple[Optional[list], str]:
        return self._candles(f"/v3/historical-candle/{enc(key)}/minutes/1/{d1:%Y-%m-%d}/{d0:%Y-%m-%d}", "candles_v3_1m", f"v3-{d0}")

    def day_candles(self, key: str, d0: date, d1: date) -> tuple[Optional[list], str]:
        return self._candles(f"/v3/historical-candle/{enc(key)}/days/1/{d1:%Y-%m-%d}/{d0:%Y-%m-%d}", "candles_v3_day", f"v3-days-{d0}")

    def expired_candles(self, expired_key: str, d0: date, d1: date) -> tuple[Optional[list], str]:
        if self.anonymous:
            raise Stop("the Expired Instruments API needs a token (and Upstox Plus): --anonymous works for `spot` only")
        return self._candles(f"/v2/expired-instruments/historical-candle/{enc(expired_key)}/1minute/{d1:%Y-%m-%d}/{d0:%Y-%m-%d}",
                             "expired_candles_1m", f"expired-{d0}")

    def _list(self, path: str, params: dict, tag: str, name: str) -> tuple[Optional[list], str]:
        self._time()
        r = self.rest.get(path, params, tag=tag)
        self._check(r)
        self._dump(name, r)
        if not r.ok:
            return None, f"HTTP {r.status} {r.error_code} {r.error_message}".strip()
        data = r.data.get("data") if isinstance(r.data, dict) else None
        return (data if isinstance(data, list) else []), ""

    def expiries(self, underlying_key: str) -> tuple[Optional[list], str]:
        return self._list("/v2/expired-instruments/expiries", {"instrument_key": underlying_key}, "expired_expiries", "expiries")

    def option_contracts(self, underlying_key: str, expiry: date) -> tuple[Optional[list], str]:
        return self._list("/v2/expired-instruments/option/contract", {"instrument_key": underlying_key, "expiry_date": f"{expiry:%Y-%m-%d}"},
                          "expired_options", f"options-{expiry}")

    def future_contracts(self, underlying_key: str, expiry: date) -> tuple[Optional[list], str]:
        return self._list("/v2/expired-instruments/future/contract", {"instrument_key": underlying_key, "expiry_date": f"{expiry:%Y-%m-%d}"},
                          "expired_futures", f"futures-{expiry}")


# ---------------------------------------------------------------------------------------------- spot: stocks and indices
def spot_key_map(limit: Optional[int] = None, index: bool = True) -> dict[str, str]:
    """{instrument key: lake symbol}: the index keys first (they are few and everything option-related needs them), then the stocks."""
    stocks = build_key_map()[0]
    if limit is not None:
        stocks = dict(list(stocks.items())[:limit])
    return {**(INDEX_KEYS if index else {}), **stocks}


def run_spot(fetch: Fetcher, key_map: dict[str, str], d0: date, d1: date, state: State, lake_dir: Path, dry_run: bool = False) -> dict:
    """Newest month first, so the most recent history (the part nearest our own ticks) lands first."""
    months = month_chunks(max(d0, MINUTE_HISTORY_START), d1)[::-1]
    todo = [(k, s, a, b) for a, b in months for k, s in key_map.items() if not state.done(f"spot|{s}|{a:%Y-%m}")]
    stats = {"calls_planned": len(todo), "calls": 0, "bars": 0, "empty": 0, "errors": 0}
    if dry_run:
        return stats
    sink = Sink(lake_dir, state)
    try:
        for key, sym, a, b in todo:
            task = f"spot|{sym}|{a:%Y-%m}"
            raw, err = fetch.minute_candles(key, a, b)
            stats["calls"] += 1
            if raw is None:
                state.mark(task, ERROR, msg=err)
                stats["errors"] += 1
                continue
            bars = [x for x in candles_to_bars(raw, sym, 1) if a <= x.ts.date() <= b]
            stats["bars"] += len(bars)
            stats["empty"] += 0 if bars else 1
            sink.add(task, bars)
            if stats["calls"] % 200 == 0:
                log.info("spot: %d of %d calls, %d bars so far (now at %s %s)", stats["calls"], len(todo), stats["bars"], sym, f"{a:%Y-%m}")
    except BaseException:
        _safe_flush(sink)
        raise
    sink.flush()
    return stats


def run_recent(fetch: Fetcher, key_map: dict[str, str], missing: list[date], state: State, lake_dir: Path, dry_run: bool = False) -> dict:
    """Fetch the 1-minute candles of the days in `missing` (open days with no India rows) for every instrument: one call per instrument and
    month, over the range from the oldest to the newest of them, keeping only the missing days.  Nothing here depends on the resume
    state: the point is to ask again for what is not there."""
    wanted = set(missing)
    todo = [(k, s, a, b) for a, b in month_chunks(min(missing), max(missing)) for k, s in key_map.items()]
    stats = {"calls_planned": len(todo), "calls": 0, "bars": 0, "empty": 0, "errors": 0}
    if dry_run:
        return stats
    sink = Sink(lake_dir, state)
    try:
        for key, sym, a, b in todo:
            raw, err = fetch.minute_candles(key, a, b)
            stats["calls"] += 1
            if raw is None:
                stats["errors"] += 1
                log.warning("recent: %s %s..%s: %s", sym, a, b, err)
                continue
            bars = [x for x in candles_to_bars(raw, sym, 1) if x.ts.date() in wanted]
            stats["bars"] += len(bars)
            stats["empty"] += 0 if bars else 1
            sink.add(f"recent|{sym}|{a}", bars)
    except BaseException:
        _safe_flush(sink)
        raise
    sink.flush()
    return stats


def _safe_flush(sink: Sink) -> None:
    """Flush while an error is already propagating: keep what was fetched, and let the original error be the one that is reported."""
    try:
        sink.flush()
    except Stop:
        pass


# ---------------------------------------------------------------------------------------------- expired options and futures
def _f(rec: dict, *names: str):
    for n in names:
        if rec.get(n) not in (None, ""):
            return rec[n]
    return None


def parse_contracts(records: Iterable[dict], underlying: str, expiry: date) -> pd.DataFrame:
    """Contract-list records -> symbol, expired_key, underlying, instrument_type, strike, expiry, lot_size.  Field names are read
    tolerantly (the shapes are unverified); a record without a key or, for an option, without a strike is dropped."""
    rows = []
    for r in records or []:
        if not isinstance(r, dict):
            continue
        key = _f(r, "instrument_key", "expired_instrument_key")
        itype = str(_f(r, "instrument_type", "option_type") or "").upper()
        if not key or itype not in ("CE", "PE", "FUT"):
            continue
        try:
            strike = float(_f(r, "strike_price", "strike")) if itype != "FUT" else None
        except (TypeError, ValueError):
            continue
        exp = expiry_date(_f(r, "expiry", "expiry_date")) or expiry
        rows.append({"symbol": contract_symbol(r, underlying, itype, exp, strike), "expired_key": str(key), "underlying": underlying,
                     "instrument_type": itype, "strike": strike, "expiry": exp,
                     "lot_size": int(float(_f(r, "lot_size", "minimum_lot") or 0))})
    return pd.DataFrame(rows, columns=["symbol", "expired_key", "underlying", "instrument_type", "strike", "expiry", "lot_size"])


def strike_band(strikes: Iterable[float], low: float, high: float, n: int) -> tuple[float, float]:
    """The strikes to keep when the index ranged from `low` to `high`: from `n` steps below the strike at or under the low to `n` steps
    above the strike at or over the high.  The step is the usual gap between listed strikes near the money."""
    ladder = sorted({float(s) for s in strikes})
    if len(ladder) < 2:
        return (ladder[0], ladder[0]) if ladder else (0.0, 0.0)
    near = [s for s in ladder if low * 0.97 <= s <= high * 1.03] or ladder
    gaps = pd.Series(near).diff().dropna()
    step = float(gaps.mode().iloc[0]) if len(gaps) else float(pd.Series(ladder).diff().dropna().min())
    below = [s for s in ladder if s <= low]
    above = [s for s in ladder if s >= high]
    lo = (below[-1] if below else ladder[0]) - n * step
    hi = (above[0] if above else ladder[-1]) + n * step
    return lo, hi


def index_range(daily: pd.DataFrame, d0: date, d1: date) -> Optional[tuple[float, float]]:
    part = daily[(daily["date"] >= d0) & (daily["date"] <= d1)]
    return (float(part["low"].min()), float(part["high"].max())) if len(part) else None


def load_index_daily(fetch: Fetcher, key: str, d0: date, d1: date) -> pd.DataFrame:
    """Daily high and low of the underlying index (one call covers up to a decade), to place the money for each expiry."""
    raw, err = fetch.day_candles(key, d0, d1)
    if raw is None:
        raise Stop(f"could not read the index's daily candles ({err}): the strike band cannot be placed")
    rows = []
    for c in raw:
        try:
            rows.append({"date": pd.Timestamp(c[0]).date(), "high": float(c[2]), "low": float(c[3])})
        except (TypeError, ValueError, IndexError):
            continue
    return pd.DataFrame(rows, columns=["date", "high", "low"])


def save_expired_contracts(df: pd.DataFrame, lake_dir: Path) -> None:
    if df.empty:
        return
    path = lake_dir / "instruments" / "fo_expired.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        df = pd.concat([pd.read_parquet(path), df], ignore_index=True)
    df = df.drop_duplicates("expired_key", keep="last").sort_values(["underlying", "expiry", "instrument_type", "strike"])
    tmp = path.with_suffix(".parquet.tmp")
    df.to_parquet(tmp, index=False)
    os.replace(tmp, path)


def run_expired(fetch: Fetcher, underlyings: list[str], d0: date, d1: date, state: State, lake_dir: Path, strikes: int = 5,
                window_days: int = 21, future_window_days: int = 95, dry_run: bool = False, limit: Optional[int] = None) -> dict:
    """For each past expiry in [d0, d1]: pick the option strikes near the money over the contract's last `window_days` days and pull
    their 1-minute candles for those days; pull the expiring future for its last `future_window_days` days."""
    stats = {"expiries": 0, "contracts": 0, "calls": 0, "bars": 0, "empty": 0, "errors": 0, "no_range": 0}
    sink = Sink(lake_dir, state)
    try:
        for und in underlyings:
            ukey = UNDERLYING_KEYS[und]
            listed, err = fetch.expiries(ukey)
            if listed is None:
                raise Stop(f"could not list the expiries of {und}: {err}")
            exps = sorted({e for e in (expiry_date(x) for x in listed) if e and d0 <= e <= d1}, reverse=True)[:limit]
            log.info("%s: %d expired expiries listed by Upstox, %d inside %s..%s", und, len(listed), len(exps), d0, d1)
            if not exps:
                continue
            daily = load_index_daily(fetch, ukey, min(exps) - timedelta(days=window_days + 7), max(exps))
            for exp in exps:
                stats["expiries"] += 1
                chosen = _expiry_contracts(fetch, und, ukey, exp, daily, strikes, window_days, state, stats)
                if chosen.empty:
                    continue
                if not dry_run:
                    save_expired_contracts(chosen, lake_dir)
                stats["contracts"] += len(chosen)
                for c in chosen.itertuples():
                    win = future_window_days if c.instrument_type == "FUT" else window_days
                    for a, b in month_chunks(exp - timedelta(days=win), exp):
                        task = f"expired|{c.symbol}|{a:%Y-%m-%d}"
                        if state.done(task):
                            continue
                        if dry_run:
                            stats["calls"] += 1
                            continue
                        raw, err = fetch.expired_candles(c.expired_key, a, b)
                        stats["calls"] += 1
                        if raw is None:
                            state.mark(task, ERROR, msg=err)
                            stats["errors"] += 1
                            continue
                        bars = [x for x in candles_to_bars(raw, c.symbol, 1) if a <= x.ts.date() <= b]
                        stats["bars"] += len(bars)
                        stats["empty"] += 0 if bars else 1
                        sink.add(task, bars)
                log.info("%s %s: %d contracts, %d calls and %d bars so far", und, exp, len(chosen), stats["calls"], stats["bars"])
    except BaseException:
        _safe_flush(sink)
        raise
    sink.flush()
    return stats


def _expiry_contracts(fetch: Fetcher, und: str, ukey: str, exp: date, daily: pd.DataFrame, strikes: int, window_days: int,
                      state: State, stats: dict) -> pd.DataFrame:
    """The contracts to pull for one expiry: options inside the strike band, plus the future (none for a weekly expiry)."""
    parts = []
    raw, err = fetch.option_contracts(ukey, exp)
    if raw is None:
        state.mark(f"expired-list|{und}|{exp}", ERROR, msg=err)
        stats["errors"] += 1
    else:
        opts = parse_contracts(raw, und, exp)
        opts = opts[opts["instrument_type"] != "FUT"]
        rng = index_range(daily, exp - timedelta(days=window_days), exp)
        if rng is None and len(opts):
            stats["no_range"] += 1
            log.warning("%s %s: no daily index candles for the window, options skipped", und, exp)
        elif len(opts):
            lo, hi = strike_band(opts["strike"], rng[0], rng[1], strikes)
            parts.append(opts[(opts["strike"] >= lo) & (opts["strike"] <= hi)])
    raw, err = fetch.future_contracts(ukey, exp)
    if raw is None:
        state.mark(f"expired-list-fut|{und}|{exp}", ERROR, msg=err)
        stats["errors"] += 1
    else:
        fut = parse_contracts(raw, und, exp)
        parts.append(fut[fut["instrument_type"] == "FUT"])
    parts = [p for p in parts if len(p)]
    return pd.concat(parts, ignore_index=True) if parts else parse_contracts([], und, exp)


# ---------------------------------------------------------------------------------------------- command line
def estimate(calls: int, budget: int = DEFAULT_BUDGET_30MIN) -> str:
    per_hour = budget * 0.9 * 2
    return f"{calls:,} calls, about {calls / per_hour:.1f} hours at {budget:,} calls per 30 minutes (the Upstox limit is 2,000)"


def main(argv: list[str] | None = None, rest: UpstoxRest | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--from", dest="d_from", type=date.fromisoformat, default=MINUTE_HISTORY_START, help="first day, YYYY-MM-DD")
    common.add_argument("--to", dest="d_to", type=date.fromisoformat, default=None, help="last day (default: yesterday)")
    common.add_argument("--limit", type=int, default=None, help="spot: first N stocks only; expired: newest N expiries per underlying")
    common.add_argument("--dry-run", action="store_true", help="count the calls and stop; nothing is fetched or stored (spot), "
                                                                 "or only the contract lists are read (expired)")
    common.add_argument("--budget-30min", type=int, default=DEFAULT_BUDGET_30MIN, metavar="N",
                        help=f"calls per 30 minutes (Upstox allows 2,000; default {DEFAULT_BUDGET_30MIN}, leaving room for the cron jobs that share the quota)")
    common.add_argument("--max-minutes", type=float, default=None, metavar="M",
                        help="stop cleanly after M minutes (exit code 75); run again to continue")
    common.add_argument("--token-env", default=None, metavar="NAME",
                        help="read the token from this .env variable (default: UPSTOX_FEED_TOKEN, then UPSTOX_ACCESS_TOKEN)")
    common.add_argument("--dump-raw", type=int, default=0, metavar="N", help="save the first N raw responses to logs/backfill/")
    sp = sub.add_parser("spot", parents=[common], help="1-minute candles of the stocks and indices")
    sp.add_argument("--no-index", action="store_true", help="stocks only")
    sp.add_argument("--anonymous", action="store_true", help="send no token (the candle endpoints answered without one on 2026-11-11); "
                                                             "also used when no token is configured")
    ex = sub.add_parser("expired", parents=[common], help="1-minute candles of expired index options and futures (Upstox Plus)")
    ex.add_argument("--underlying", nargs="+", default=list(UNDERLYING_KEYS), choices=list(UNDERLYING_KEYS))
    ex.add_argument("--strikes", type=int, default=5, help="strikes kept on each side of the money (default 5)")
    ex.add_argument("--window-days", type=int, default=21, help="calendar days before expiry pulled for each option (default 21)")
    ex.add_argument("--future-window-days", type=int, default=95, help="calendar days before expiry pulled for each future (default 95)")
    rc = sub.add_parser("recent", parents=[common], help="nightly: the 1-minute candles of any recent day NSE was open with no India rows stored")
    rc.add_argument("--days", type=int, default=14, help="how many days back to look for missing days (default 14)")
    rc.add_argument("--anonymous", action="store_true", help="send no token (also used when none is configured)")
    sub.add_parser("status", help="what has been stored so far")
    a = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    lake_dir = lake_mod.LAKE_DIR
    state = State(lake_dir / "backfill" / "state.json")
    if a.cmd == "status":
        s = state.summary()
        if not s:
            print("nothing backfilled yet")
        for kind, v in sorted(s.items()):
            print(f"{kind:<18} ok {v[OK]:>7,}  empty {v[EMPTY]:>7,}  error {v[ERROR]:>6,}  bars {v['rows']:>13,}")
        return 0
    injected, token = rest is not None, ""
    if rest is None:
        from dotenv import load_dotenv
        load_dotenv(lake_mod.ENV_FILE)
        token = os.getenv(a.token_env, "") if a.token_env else (os.getenv("UPSTOX_FEED_TOKEN") or os.getenv("UPSTOX_ACCESS_TOKEN", ""))
        if not token and a.cmd not in ("spot", "recent"):
            print("no token: run python -m data_layer.upstox.auth first (or name the variable with --token-env)", file=sys.stderr)
            return 2
        if not token:
            print("no token configured: asking for the candles without one (the endpoint allowed it on 2026-11-11)", file=sys.stderr)
        rest = UpstoxRest(lambda: token, limiter=RateLimiter(windows=((1.0, 50), (60.0, 500), (1800.0, a.budget_30min))))
    anonymous = a.cmd in ("spot", "recent") and (a.anonymous or (not injected and not token))
    fetch = Fetcher(rest, a.dump_raw, anonymous=anonymous, max_minutes=a.max_minutes)
    d1 = a.d_to or ist_today() - timedelta(days=1)
    try:
        if a.cmd == "recent":
            from .. import gaps
            missing = gaps.india_missing(lake_dir, ist_today(), span=a.days)
            if not missing:
                print(f"recent: no open NSE day in the last {a.days} is missing from the 1-minute bars: nothing to fetch")
                return 0
            km = spot_key_map(a.limit, True)
            st = run_recent(fetch, km, missing, State(lake_dir / "backfill" / "recent_state.json"), lake_dir, a.dry_run)
            if a.dry_run:
                print(f"recent, {len(missing)} missing day(s) ({missing[0]}..{missing[-1]}), {len(km)} instruments: {estimate(st['calls_planned'], a.budget_30min)}")
                return 0
            gone = gaps.record(lake_dir, "1m", "IN", missing)
            print(f"recent done: days {', '.join(str(d) for d in missing)}; {st['calls']:,} calls, {st['bars']:,} bars stored, {st['empty']:,} empty, "
                  f"{st['errors']:,} errors" + (f"; given up on {', '.join(str(d) for d in gone)} (still empty: taken to be holidays)" if gone else ""))
            return 1 if st["errors"] and not st["bars"] else 0
        if a.cmd == "spot":
            km = spot_key_map(a.limit, not a.no_index)
            if not km:
                print("no instruments: the universe has no symbols with a usable ISIN", file=sys.stderr)
                return 2
            st = run_spot(fetch, km, a.d_from, d1, state, lake_dir, a.dry_run)
            if a.dry_run:
                print(f"spot, {len(km)} instruments, {max(a.d_from, MINUTE_HISTORY_START)}..{d1}: {estimate(st['calls_planned'], a.budget_30min)}")
            else:
                print(f"spot done: {st['calls']:,} calls, {st['bars']:,} bars stored, {st['empty']:,} empty months, {st['errors']:,} errors "
                      f"(errors are retried by the next run)")
        else:
            st = run_expired(fetch, a.underlying, a.d_from, d1, state, lake_dir, a.strikes, a.window_days, a.future_window_days,
                             a.dry_run, a.limit)
            if a.dry_run:
                print(f"expired, {st['expiries']} expiries, {st['contracts']:,} contracts: {estimate(st['calls'], a.budget_30min)}")
            else:
                print(f"expired done: {st['expiries']} expiries, {st['contracts']:,} contracts, {st['calls']:,} calls, {st['bars']:,} bars "
                      f"stored, {st['empty']:,} empty, {st['errors']:,} errors, {st['no_range']} expiries without an index range")
    except TimeUp as exc:
        state.save()
        print(f"TIME LIMIT: {exc}", file=sys.stderr)
        return EX_TEMPFAIL
    except Stop as exc:
        state.save()
        print(f"STOPPED: {exc}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:                                           # pragma: no cover
        state.save()
        print("interrupted: what was stored is kept; run again to continue", file=sys.stderr)
        return 130
    state.save()
    return 0


if __name__ == "__main__":
    sys.exit(main())
