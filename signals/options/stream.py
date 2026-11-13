"""
Futures order-book signals computed tick by tick inside the F&O feed (SCRUM-196), bucketed at 10 seconds and 1 minute.

The batch functions in signals/options/live.py compute the same values from stored ticks after the close; this module keeps only what
the next tick needs (the previous book of each future, the open buckets, a short history of bucket depths), so each update costs a
few microseconds and memory does not grow with the session.  A test feeds recorded ticks through both and compares them.

  BookAccumulator(buckets=("10s", "1min"), levels=5, front={"NIFTY": expiry, ...})
      .update(tick)      -> list of closed rows (one per bucket width and signal whose bucket the tick closed)
      .flush()           -> the rows of every bucket still open (end of session)

A tick is a mapping or object with: fts (event time; ts when missing), symbol, underlying, expiry, bid, ask, bid_qty, ask_qty, ltp,
vol_today and, when the 5-level book is stored, bid2..bid5, ask2..ask5 and their _qty.  Buckets close on event time: a tick from a
later bucket closes the open one.  A tick older than the open bucket (out of order) is counted in `late` and left out.

Rows (dicts) carry signal, bucket ("10s" / "1min"), underlying, symbol, ts (bucket start), known_at (bucket end) and the signal's
columns, named as in the batch outputs:
  fut_ofi        ofi, depth, mid, ofi_norm                       front future of each underlying
  mlofi_future   ofi1..ofi5, depth1..5, ofi1_norm..5, mlofi     front future, needs levels 2-5
  book_pressure  micro_rel, qi                                  every future
  depth_features depth_imb, slope, depth, refills, depth_change every future, needs levels 2-5
"""

from __future__ import annotations

import math
from collections import deque
from datetime import date, datetime
from statistics import median
from typing import Any, Iterable, Optional

import pandas as pd

LEVELS = 5
WITHDRAW_MINUTES = 15
NAN = float("nan")


def _f(x) -> float:
    try:
        v = float(x)
    except (TypeError, ValueError):
        return NAN
    return v


def _get(t: Any, k: str):
    return t.get(k) if isinstance(t, dict) else getattr(t, k, None)


def _ofi_step(b, a, qb, qa, pb, pa, pqb, pqa) -> float:
    """Cont, Kukanov and Stoikov's event: the same formula as live.fut_ofi, with NaN levels counting as zero."""
    if math.isnan(pb):
        return 0.0
    e = (qb if b >= pb else 0.0) - (pqb if b <= pb else 0.0) - (qa if a <= pa else 0.0) + (pqa if a >= pa else 0.0)
    return 0.0 if math.isnan(e) else e


class _Bucket:
    __slots__ = ("start", "e", "depth_sum", "depth_n", "mid", "micro", "qi", "lev_e", "lev_d_sum", "lev_d_n", "imb_sum", "imb_n",
                 "slopes", "dsum", "dn", "refills")

    def __init__(self, start: pd.Timestamp, levels: int):
        self.start = start
        self.e = 0.0
        self.depth_sum = 0.0
        self.depth_n = 0
        self.mid = NAN                    # last non-missing value in the bucket, like pandas' groupby last
        self.micro = NAN
        self.qi = NAN
        self.lev_e = [0.0] * levels
        self.lev_d_sum = [0.0] * levels
        self.lev_d_n = [0] * levels
        self.imb_sum = 0.0
        self.imb_n = 0
        self.slopes: list[float] = []
        self.dsum = 0.0
        self.dn = 0
        self.refills = 0


class _Future:
    """State of one futures contract: its previous book, and per bucket width the open bucket and recent bucket depths."""

    def __init__(self, symbol: str, underlying: str, levels: int, widths: dict[str, pd.Timedelta]):
        self.symbol, self.underlying, self.levels = symbol, underlying, levels
        self.prev: Optional[dict] = None
        self.day: Optional[date] = None
        self.open: dict[str, _Bucket] = {}
        self.depth_hist: dict[str, deque] = {w: deque(maxlen=max(1, int(pd.Timedelta(minutes=WITHDRAW_MINUTES) / td))) for w, td in widths.items()}


class BookAccumulator:
    def __init__(self, buckets: Iterable[str] = ("10s", "1min"), levels: int = LEVELS, front: Optional[dict[str, Any]] = None):
        self.widths = {b: pd.Timedelta(b) for b in buckets}
        self.levels = levels
        self.fixed_front = {u: pd.Timestamp(e).date() for u, e in (front or {}).items()}   # underlying -> front expiry, from the contracts
        self.front: dict[str, date] = dict(self.fixed_front)                               # else the nearest expiry seen today
        self.today: Optional[date] = None
        self.futures: dict[str, _Future] = {}
        self.late = 0
        self.ticks = 0

    # ----------------------------------------------------------------------------------------- input
    def update(self, t: Any) -> list[dict]:
        ts = _get(t, "fts") or _get(t, "ts")
        if ts is None:
            return []
        ts = pd.Timestamp(ts)
        sym = _get(t, "symbol")
        und = _get(t, "underlying") or str(sym).split("_FUT_")[0]
        if self.today != ts.date():
            self.today = ts.date()
            self.front = dict(self.fixed_front)
        fut = self.futures.get(sym)
        if fut is None:
            fut = self.futures[sym] = _Future(sym, und, self.levels, self.widths)
            exp = _get(t, "expiry")
            fut.expiry = pd.Timestamp(exp).date() if exp is not None else None
        if fut.expiry is not None and und not in self.fixed_front and (und not in self.front or fut.expiry < self.front[und]):
            self.front[und] = fut.expiry
        out: list[dict] = []
        if fut.day != ts.date():                                   # a new session: close what is open, forget yesterday's book
            out += self._close_all(fut)
            fut.day, fut.prev = ts.date(), None
            for d in fut.depth_hist.values():
                d.clear()
        for w, td in self.widths.items():
            start = ts.floor(td)
            b = fut.open.get(w)
            if b is not None and start < b.start:
                self.late += 1
                return out
            if b is not None and start > b.start:
                out += self._close(fut, w)
        self.ticks += 1
        book = self._book(t)
        for w in self.widths:
            b = fut.open.get(w)
            if b is None:
                b = fut.open[w] = _Bucket(ts.floor(self.widths[w]), self.levels)
            self._add(fut, b, book)
        fut.prev = book
        return out

    def flush(self) -> list[dict]:
        out: list[dict] = []
        for fut in self.futures.values():
            out += self._close_all(fut)
        return out

    # ----------------------------------------------------------------------------------------- internals
    def _book(self, t: Any) -> dict:
        bk = {"bid": [_f(_get(t, "bid"))], "ask": [_f(_get(t, "ask"))], "bq": [_f(_get(t, "bid_qty"))], "aq": [_f(_get(t, "ask_qty"))],
              "ltp": _f(_get(t, "ltp")), "vol": _f(_get(t, "vol_today"))}
        for i in range(2, self.levels + 1):
            bk["bid"].append(_f(_get(t, f"bid{i}")))
            bk["ask"].append(_f(_get(t, f"ask{i}")))
            bk["bq"].append(_f(_get(t, f"bid{i}_qty")))
            bk["aq"].append(_f(_get(t, f"ask{i}_qty")))
        bk["deep"] = not math.isnan(bk["bid"][1]) if self.levels > 1 else False
        return bk

    def _add(self, fut: _Future, b: _Bucket, bk: dict) -> None:
        bid, ask, bq, aq = bk["bid"][0], bk["ask"][0], bk["bq"][0], bk["aq"][0]
        p = fut.prev
        if p is None:
            b.e += 0.0
        else:
            b.e += _ofi_step(bid, ask, bq, aq, p["bid"][0], p["ask"][0], p["bq"][0], p["aq"][0])
        d = (bq + aq) / 2
        if not math.isnan(d):
            b.depth_sum += d
            b.depth_n += 1
        mid = (bid + ask) / 2
        if not math.isnan(mid):
            b.mid = mid
        tot = bq + aq
        if tot and not math.isnan(tot):
            micro = (bid * aq + ask * bq) / tot
            if not math.isnan(micro):
                b.micro = micro
            qi = (bq - aq) / tot
            if not math.isnan(qi):
                b.qi = qi
        if not bk["deep"]:
            return
        for i in range(self.levels):
            if p is not None:
                b.lev_e[i] += _ofi_step(bk["bid"][i], bk["ask"][i], bk["bq"][i], bk["aq"][i], p["bid"][i], p["ask"][i], p["bq"][i], p["aq"][i])
            di = (bk["bq"][i] + bk["aq"][i]) / 2
            if not math.isnan(di):
                b.lev_d_sum[i] += di
                b.lev_d_n[i] += 1
        sb = sum(0.0 if math.isnan(x) else x for x in bk["bq"])
        sa = sum(0.0 if math.isnan(x) else x for x in bk["aq"])
        if sb + sa:
            b.imb_sum += (sb - sa) / (sb + sa)
            b.imb_n += 1
        span = (bk["ask"][-1] - bk["bid"][-1]) / mid if mid else NAN
        if span and not math.isnan(span):
            b.slopes.append((sb + sa) / span)
        b.dsum += sb + sa
        b.dn += 1
        if p is not None:
            sold = (not math.isnan(bk["ltp"]) and bk["ltp"] <= p["bid"][0] and not math.isnan(bk["vol"]) and not math.isnan(p["vol"])
                    and bk["vol"] - p["vol"] > 0)
            if sold and bid == p["bid"][0] and bq >= p["bq"][0]:
                b.refills += 1

    def _close_all(self, fut: _Future) -> list[dict]:
        out: list[dict] = []
        for w in list(fut.open):
            out += self._close(fut, w)
        return out

    def _close(self, fut: _Future, w: str) -> list[dict]:
        b = fut.open.pop(w)
        td = self.widths[w]
        base = {"bucket": w, "underlying": fut.underlying, "symbol": fut.symbol, "ts": b.start, "known_at": b.start + td}
        rows = [dict(base, signal="book_pressure", micro_rel=(b.micro - b.mid) / b.mid if b.mid else NAN, qi=b.qi)]
        is_front = getattr(fut, "expiry", None) is not None and self.front.get(fut.underlying) == fut.expiry
        depth = b.depth_sum / b.depth_n if b.depth_n else NAN
        if is_front:
            rows.append(dict(base, signal="fut_ofi", ofi=b.e, depth=depth, mid=b.mid, ofi_norm=b.e / depth if depth else NAN))
        if b.dn:
            hist = fut.depth_hist[w]
            dmean = b.dsum / b.dn
            need = min(5, hist.maxlen)
            base_depth = median(hist) if len(hist) >= need else NAN
            rows.append(dict(base, signal="depth_features", depth_imb=b.imb_sum / b.imb_n if b.imb_n else NAN,
                             slope=median(b.slopes) if b.slopes else NAN, depth=dmean, refills=b.refills,
                             depth_change=dmean / base_depth - 1 if base_depth else NAN))
            hist.append(dmean)
            if is_front:
                r = dict(base, signal="mlofi_future")
                norms = []
                for i in range(self.levels):
                    di = b.lev_d_sum[i] / b.lev_d_n[i] if b.lev_d_n[i] else NAN
                    r[f"ofi{i + 1}"], r[f"depth{i + 1}"] = b.lev_e[i], di
                    r[f"ofi{i + 1}_norm"] = b.lev_e[i] / di if di else NAN
                    norms.append(r[f"ofi{i + 1}_norm"])
                ok = [x for x in norms if not math.isnan(x)]
                r["mlofi"] = sum(ok) / len(ok) if ok else NAN
                rows.append(r)
        return rows


def rows_frame(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------------------------- feed glue
MONTHS = {m: i for i, m in enumerate(("JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"), 1)}


def fut_meta(symbol: str) -> Optional[tuple[str, date]]:
    """(underlying, expiry) of a futures symbol such as NIFTY_FUT_25_NOV_26, else None."""
    if "_FUT_" not in symbol:
        return None
    und, rest = symbol.split("_FUT_", 1)
    try:
        d, mon, yy = rest.split("_")
        return und, date(2000 + int(yy), MONTHS[mon.upper()], int(d))
    except (ValueError, KeyError):
        return None


def front_expiries(symbols: Iterable[str], today: date) -> dict[str, date]:
    """Per underlying, the nearest futures expiry on or after `today` among `symbols` (the feed's subscription list)."""
    out: dict[str, date] = {}
    for s in symbols:
        m = fut_meta(s)
        if m and m[1] >= today and (m[0] not in out or m[1] < out[m[0]]):
            out[m[0]] = m[1]
    return out


def live_dir(store: Optional[Any] = None, day: Optional[date] = None):
    """<store>/live/date=D: today's live signal rows (the nightly job replaces them with the batch result)."""
    import os
    from pathlib import Path
    root = Path(store or os.environ.get("JARVIS_SIGNAL_STORE") or "/srv/jarvis-research/store")
    return root / "live" / f"date={day or datetime.utcnow().date()}"


def write_rows(rows: list[dict], store: Optional[Any] = None, prefix: str = "book") -> int:
    """Append closed bucket rows as one parquet segment per write (named <prefix>-<ms>-<pid>), grouped by day.  Returns rows written."""
    import os
    import time as _time
    if not rows:
        return 0
    df = pd.DataFrame(rows)
    n = 0
    for day, part in df.groupby(pd.to_datetime(df["ts"]).dt.date):
        folder = live_dir(store, day)
        folder.mkdir(parents=True, exist_ok=True)
        name = f"{prefix}-{int(_time.time() * 1000)}-{os.getpid()}.parquet"
        tmp = folder / (name + ".tmp")
        part.to_parquet(tmp, index=False)
        tmp.replace(folder / name)
        n += len(part)
    return n


def read_live(store: Optional[Any] = None, day: Optional[date] = None) -> pd.DataFrame:
    folder = live_dir(store, day)
    files = sorted(folder.glob("*.parquet")) if folder.is_dir() else []
    return pd.concat([pd.read_parquet(f) for f in files], ignore_index=True) if files else pd.DataFrame()
