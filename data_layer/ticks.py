"""
Tick / L1 model and tick -> bar aggregator (protocol independent).

A feed adapter (Upstox WebSocket, see docs/RUNBOOK.md section 13) turns each
decoded message into `Tick`s; `BarAggregator` folds them into fixed-width bars
aligned to the NSE session open, and `tick_store` persists them.

Time handling: all timestamps are naive UTC.  The NSE regular session is
09:15-15:30 IST = 03:45-10:00 UTC, so bars are bucketed from 03:45 UTC and a
60-minute bar starts at 09:15 IST, not at the top of the hour.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, time, timedelta
from typing import Optional

SESSION_OPEN_UTC = time(3, 45)
SESSION_CLOSE_UTC = time(10, 0)


@dataclass(slots=True)
class Tick:
    ts: datetime                    # exchange timestamp, naive UTC
    symbol: str                     # lake symbol, e.g. RELIANCE.NS
    ltp: float
    vol_today: Optional[int] = None  # cumulative day volume, if the feed carries it
    bid: Optional[float] = None
    bid_qty: Optional[int] = None
    ask: Optional[float] = None
    ask_qty: Optional[int] = None
    oi: Optional[int] = None
    iv: Optional[float] = None       # option Greeks as delivered by the feed (options only)
    delta: Optional[float] = None
    theta: Optional[float] = None
    gamma: Optional[float] = None
    vega: Optional[float] = None
    rho: Optional[float] = None


@dataclass(slots=True)
class Bar:
    ts: datetime                    # bar start, naive UTC
    symbol: str
    market: str
    open: float
    high: float
    low: float
    close: float
    volume: int
    vwap: Optional[float]           # None for bars that came from the REST candle API
    n_ticks: Optional[int]
    bid: Optional[float]            # last L1 seen inside the bar
    ask: Optional[float]
    bid_qty: Optional[int]
    ask_qty: Optional[int]
    src: str = "live"               # "live" (built from ticks) or "rest" (Upstox candle API)
    oi: Optional[int] = None        # open interest at the last tick in the bar (futures and options only)


class _Acc:
    __slots__ = ("start", "o", "h", "l", "c", "vol", "pv", "n", "bid", "ask", "bq", "aq", "oi")

    def __init__(self, start: datetime, px: float) -> None:
        self.start, self.o, self.h, self.l, self.c = start, px, px, px, px
        self.vol = self.pv = self.n = 0
        self.bid = self.ask = self.bq = self.aq = self.oi = None


class BarAggregator:
    """
    Folds ticks into bars of `interval_s` seconds.

    - Ticks outside the regular session (pre-open auction, post-close) are dropped.
    - Bar volume = delta of the cumulative day volume between consecutive ticks.
      The first tick seen for a symbol on a day only sets the baseline, so volume
      traded before the collector connected is NOT attributed to any bar.
    - Only buckets that saw ticks produce bars (no forward fill): with 1-second bars a quiet
      stock simply has no row for a second in which nothing happened.
    - A bar is emitted when a later bucket's tick arrives for that symbol, or when
      `flush(now)` / `flush_all()` is called (needed for illiquid names).
    """

    def __init__(self, interval_s: int = 1, market: str = "IN") -> None:
        if interval_s <= 0:
            raise ValueError("interval_s must be positive")
        self.interval = timedelta(seconds=interval_s)
        self.market = market
        self._open: dict[str, _Acc] = {}
        self.late_dropped = 0                                   # ticks older than their symbol's open bucket
        self._last_vtt: dict[str, tuple[object, int]] = {}   # symbol -> (date, cumulative volume)

    def _bucket(self, ts: datetime) -> Optional[datetime]:
        t = ts.time()
        if not (SESSION_OPEN_UTC <= t < SESSION_CLOSE_UTC):
            return None
        day_open = datetime.combine(ts.date(), SESSION_OPEN_UTC)
        n = int((ts - day_open) // self.interval)
        return day_open + n * self.interval

    def _to_bar(self, symbol: str, a: _Acc) -> Bar:
        return Bar(a.start, symbol, self.market, a.o, a.h, a.l, a.c, a.vol,
                   (a.pv / a.vol) if a.vol else a.c, a.n, a.bid, a.ask, a.bq, a.aq, oi=a.oi)

    def add(self, t: Tick) -> list[Bar]:
        start = self._bucket(t.ts)
        if start is None:
            return []
        out: list[Bar] = []
        acc = self._open.get(t.symbol)
        if acc is not None and start < acc.start:
            self.late_dropped += 1                      # late tick for an already-closed bucket
            return []
        if acc is not None and start > acc.start:
            out.append(self._to_bar(t.symbol, acc))
            acc = None
        if acc is None:
            acc = self._open[t.symbol] = _Acc(start, t.ltp)

        delta = 0
        if t.vol_today is not None:
            prev = self._last_vtt.get(t.symbol)
            if prev and prev[0] == t.ts.date() and t.vol_today >= prev[1]:
                delta = t.vol_today - prev[1]
            self._last_vtt[t.symbol] = (t.ts.date(), t.vol_today)

        acc.h, acc.l, acc.c = max(acc.h, t.ltp), min(acc.l, t.ltp), t.ltp
        acc.vol += delta
        acc.pv += t.ltp * delta
        acc.n += 1
        if t.bid is not None or t.ask is not None:
            acc.bid, acc.ask, acc.bq, acc.aq = t.bid, t.ask, t.bid_qty, t.ask_qty
        if t.oi is not None:
            acc.oi = t.oi
        return out

    def flush(self, now: datetime) -> list[Bar]:
        """Emit every open bar whose bucket has ended by `now` (naive UTC)."""
        done = [s for s, a in self._open.items() if a.start + self.interval <= now]
        return [self._to_bar(s, self._open.pop(s)) for s in done]

    def flush_all(self) -> list[Bar]:
        bars = [self._to_bar(s, a) for s, a in self._open.items()]
        self._open.clear()
        return bars
