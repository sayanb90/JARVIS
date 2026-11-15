"""
The strategy interface and one plumbing example (SCRUM-154).  The example proves the path from signal rows to orders, risk, fills and
latency records; it is not a tuned or backtested strategy and should not be read as one.

  class Strategy:
      on_signal(row: dict, now_ns) -> list[Order]     a final or provisional signal row from the engine (bus "signal" topic)
      on_tick(tick: dict, now_ns) -> list[Order]      a tick (quotes for pricing and exits)

VrpSkewGate (example): short one lot of the front ATM straddle when implied volatility is rich against the forecast (vrp_ratio above
`enter_ratio`) and the put skew is not steepening (skew's smoothed 1 % skew, skew1_s, below `max_skew`); the ATM strike is the front future rounded to the strike step, the expiry the nearest one seen in the ticks; buy it
back when vrp_ratio falls
below `exit_ratio` or at `flat_at` IST.  One position at a time, both legs at market.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Optional

import pandas as pd

from trading.execution import Order


class Strategy:
    name = "strategy"

    def on_signal(self, row: dict, now_ns: int) -> list[Order]:
        return []

    def on_tick(self, tick: dict, now_ns: int) -> list[Order]:
        return []


def _num(row: dict, key: str) -> float:
    v = row.get(key)
    try:
        return float(v) if v is not None else math.nan
    except (TypeError, ValueError):
        return math.nan


@dataclass
class VrpSkewGate(Strategy):
    underlying: str = "NIFTY"
    lot_size: int = 65
    enter_ratio: float = 1.25
    exit_ratio: float = 1.05
    max_skew: float = 0.02
    flat_at: str = "15:10"                             # IST
    name: str = "vrp_skew_gate"
    strike_step: float = 50.0
    skew: float = math.nan
    straddle: Optional[tuple] = None                   # (call symbol, put symbol) when short
    fut: float = math.nan                              # front future's last price (ATM strike)
    expiries: dict = field(default_factory=dict)       # option expiry tag "31_MAR_26" -> date, from the ticks seen
    decisions: list = field(default_factory=list)

    def on_tick(self, tick: dict, now_ns: int) -> list[Order]:
        s = str(tick.get("symbol", ""))
        if not s.startswith(self.underlying + "_"):
            return []
        parts = s.split("_")
        if parts[1] == "FUT":
            px = _num(tick, "ltp")
            if px == px:
                self.fut = px
        elif len(parts) == 6 and parts[2] in ("CE", "PE"):
            tag = "_".join(parts[3:])
            if tag not in self.expiries:
                self.expiries[tag] = pd.Timestamp(pd.to_datetime(tag, format="%d_%b_%y")).date()
        return []

    def _legs(self, now_ns: int) -> Optional[tuple]:
        today = pd.Timestamp(now_ns).date()
        live = sorted((d, t) for t, d in self.expiries.items() if d >= today)
        if not live or self.fut != self.fut:
            return None
        k = int(round(self.fut / self.strike_step) * self.strike_step)
        tag = live[0][1]
        return (f"{self.underlying}_{k}_CE_{tag}", f"{self.underlying}_{k}_PE_{tag}")

    def on_signal(self, row: dict, now_ns: int) -> list[Order]:
        sid = row.get("signal")
        if sid == "skew":
            v = _num(row, "skew1_s")
            self.skew = v if v == v else _num(row, "skew1")
            return []
        if sid != "vrp":
            return []
        ratio = _num(row, "vrp_ratio")
        ist = (pd.Timestamp(now_ns) + pd.Timedelta(hours=5, minutes=30)).strftime("%H:%M")
        if self.straddle is None and ratio > self.enter_ratio and not (self.skew > self.max_skew) and ist < self.flat_at:
            legs = self._legs(now_ns)
            if legs is None:
                return []
            self.straddle = legs
            self.decisions.append(("enter", now_ns, ratio))
            return [Order(s, -1, 1, self.lot_size, tag=self.name, t_decide=now_ns) for s in legs]
        if self.straddle is not None and (ratio < self.exit_ratio or ist >= self.flat_at):
            legs, self.straddle = self.straddle, None
            self.decisions.append(("exit", now_ns, ratio))
            return [Order(s, +1, 1, self.lot_size, tag=self.name, t_decide=now_ns) for s in legs]
        return []
