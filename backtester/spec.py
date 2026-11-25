"""
A backtest strategy, written as a TOML file (SCRUM-217).  One file says what to trade, when to enter, when to leave, and how much risk to take:

  name = "fut_flow_long"
  underlying = "NIFTY"
  broker = "zerodha"                         # zerodha | upstox (brokerage schedule; statutory charges are the same)

  [instrument]
  kind = "future"                            # future | option
  side = "buy"                               # buy | sell
  right = "CE"                               # option only: CE | PE
  strike = "atm"                             # option only: atm, atm+1, atm-2 ... (strike steps from the money)
  expiry = "nearest"                         # nearest | next | monthly
  min_days_to_expiry = 0                     # skip contracts expiring sooner (0: same-day expiry allowed)

  [entry]                                    # every condition in `all`, and at least one in `any` when it is not empty
  all = [{ signal = "noise_band", column = "z", op = ">", value = 1.5, scale = 0.5 }]
  any = []
  window = ["09:30", "14:45"]                # IST: entries only between these times
  max_trades_per_day = 3

  [exit]                                     # whichever comes first
  any = [{ signal = "noise_band", column = "z", op = "<", value = 0.0 }]
  stop_loss_pct = 0.004                      # adverse move, fraction of the entry price (options: of the premium)
  take_profit_pct = 0.008
  max_hold_minutes = 90
  flat_at = "15:15"                          # IST

  [risk]
  daily_loss_budget = 100000                 # rupees: no new entry once the day's realised loss reaches it
  max_trade_fraction = 0.6                   # no trade may risk more than this share of the budget
  max_lots = 50

  [conviction]                               # score = mean over the entry conditions of how far past the threshold, in `scale` units
  tiers = [{ min_score = 0.0, scale = 0.5 }, { min_score = 1.0, scale = 0.75 }, { min_score = 2.0, scale = 1.0 }]

  [fills]
  half_spread_fut_bps = 1.0                  # paid on every fill, against us: futures in basis points of price
  half_spread_opt_pct = 0.5                  # options in percent of the premium

A condition reads one column of one signal, as known at the decision time (the signal's known_at), never later.
"""

from __future__ import annotations

import operator
import tomllib
from dataclasses import dataclass, field
from datetime import time
from pathlib import Path
from typing import Optional

OPS = {">": operator.gt, ">=": operator.ge, "<": operator.lt, "<=": operator.le, "==": operator.eq, "!=": operator.ne}


@dataclass(frozen=True)
class Condition:
    signal: str
    column: str
    op: str
    value: float
    scale: Optional[float] = None              # units for the conviction margin (default: |value|, or 1 when the value is 0)

    def __post_init__(self):
        if self.op not in OPS:
            raise ValueError(f"condition {self.signal}.{self.column}: op {self.op!r} not in {sorted(OPS)}")

    @property
    def key(self) -> str:
        return f"{self.signal}.{self.column}"

    def holds(self, x: float) -> bool:
        return x == x and bool(OPS[self.op](x, self.value))              # NaN never holds

    def margin(self, x: float) -> float:
        """How far past the threshold, in `scale` units (0 at the threshold, larger the further on the right side)."""
        s = abs(self.scale) if self.scale else (abs(self.value) or 1.0)
        d = (x - self.value) if self.op in (">", ">=") else (self.value - x) if self.op in ("<", "<=") else 0.0
        return d / s


@dataclass
class Instrument:
    kind: str = "future"
    side: str = "buy"
    right: str = "CE"
    strike: str = "atm"
    expiry: str = "nearest"
    min_days_to_expiry: int = 0

    @property
    def direction(self) -> int:
        return 1 if self.side == "buy" else -1

    @property
    def strike_offset(self) -> int:
        s = self.strike.replace(" ", "").lower()
        if s == "atm":
            return 0
        if s.startswith("atm"):
            return int(s[3:])
        raise ValueError(f"strike {self.strike!r}: use atm, atm+N or atm-N")


@dataclass
class Entry:
    all: list = field(default_factory=list)
    any: list = field(default_factory=list)
    window: tuple = (time(9, 20), time(15, 0))
    max_trades_per_day: int = 3


@dataclass
class Exit:
    any: list = field(default_factory=list)
    stop_loss_pct: Optional[float] = None
    take_profit_pct: Optional[float] = None
    max_hold_minutes: Optional[int] = None
    flat_at: time = time(15, 15)


@dataclass
class Risk:
    daily_loss_budget: float = 100_000.0
    max_trade_fraction: float = 0.6
    max_lots: int = 50


@dataclass
class Fills:
    half_spread_fut_bps: float = 1.0
    half_spread_opt_pct: float = 0.5


@dataclass
class Strategy:
    name: str
    underlying: str
    instrument: Instrument
    entry: Entry
    exit: Exit
    risk: Risk = field(default_factory=Risk)
    tiers: list = field(default_factory=lambda: [(0.0, 1.0)])          # [(min_score, scale)], ascending
    fills: Fills = field(default_factory=Fills)
    broker: str = "zerodha"

    def __post_init__(self):
        if self.instrument.kind not in ("future", "option"):
            raise ValueError("instrument.kind must be future or option")
        if self.instrument.side not in ("buy", "sell"):
            raise ValueError("instrument.side must be buy or sell")
        if not (self.entry.all or self.entry.any):
            raise ValueError("entry needs at least one condition")
        if self.instrument.side == "sell" and not self.exit.stop_loss_pct:
            raise ValueError("a sold position needs exit.stop_loss_pct: without a stop its rupee risk is not bounded")
        if self.instrument.kind == "future" and not self.exit.stop_loss_pct:
            raise ValueError("a futures position needs exit.stop_loss_pct: the risk budget sizes it from the stop")
        if not 0 < self.risk.max_trade_fraction <= 1:
            raise ValueError("risk.max_trade_fraction must be in (0, 1]")

    def conditions(self) -> list:
        return [*self.entry.all, *self.entry.any, *self.exit.any]

    def needs(self) -> list:
        """(signal, column) pairs the rules read, each once."""
        return sorted({(c.signal, c.column) for c in self.conditions()})

    def tier_scale(self, score: float) -> float:
        s = 0.0
        for lo, scale in self.tiers:
            if score >= lo:
                s = scale
        return s


def _t(v) -> time:
    if isinstance(v, time):
        return v
    h, m = str(v).split(":")
    return time(int(h), int(m))


def _conds(xs) -> list:
    return [Condition(str(x["signal"]), str(x["column"]), str(x["op"]), float(x["value"]), x.get("scale")) for x in xs or []]


def from_dict(d: dict) -> Strategy:
    i, e, x, r, c, f = (d.get(k, {}) for k in ("instrument", "entry", "exit", "risk", "conviction", "fills"))
    tiers = sorted((float(t["min_score"]), float(t["scale"])) for t in c.get("tiers", [{"min_score": 0.0, "scale": 1.0}]))
    return Strategy(
        name=str(d["name"]), underlying=str(d.get("underlying", "NIFTY")), broker=str(d.get("broker", "zerodha")),
        instrument=Instrument(**{k: v for k, v in i.items() if k in Instrument.__dataclass_fields__}),
        entry=Entry(all=_conds(e.get("all")), any=_conds(e.get("any")), window=tuple(_t(v) for v in e.get("window", ["09:20", "15:00"])),
                    max_trades_per_day=int(e.get("max_trades_per_day", 3))),
        exit=Exit(any=_conds(x.get("any")), stop_loss_pct=x.get("stop_loss_pct"), take_profit_pct=x.get("take_profit_pct"),
                  max_hold_minutes=x.get("max_hold_minutes"), flat_at=_t(x.get("flat_at", "15:15"))),
        risk=Risk(**{k: v for k, v in r.items() if k in Risk.__dataclass_fields__}),
        tiers=tiers, fills=Fills(**{k: v for k, v in f.items() if k in Fills.__dataclass_fields__}))


def load(path) -> Strategy:
    with open(Path(path), "rb") as fh:
        return from_dict(tomllib.load(fh))
