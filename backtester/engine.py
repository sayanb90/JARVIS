"""
The simulation (SCRUM-217): a strategy (backtester/spec.py) replayed minute by minute over historical prices and signals.

Timing, so nothing is known early:
  * a decision is made at the close of a minute bar (bar start + 1 minute) from signal values whose known_at is at or before it;
  * it is filled at the open of the traded contract's next bar (within FILL_WINDOW minutes; no bar in that time: no trade), worse by the half-spread;
  * a stop or target is checked on every bar's high and low: when both are inside one bar the stop is taken (we cannot know which came first); a bar that
    opens past the stop fills at its open (the gap is a real loss, beyond the planned risk);
  * exit conditions, the time stop and the flat time are decided at a bar's close and filled at the next bar's open; the day's last bar closes anything open.

Risk and size:
  * planned rupee risk of one lot = entry price x stop_loss_pct x lot size (a bought option without a stop: the whole premium);
  * a trade may plan to lose at most  daily_loss_budget x max_trade_fraction x conviction scale,  and never more than what is left of the day's budget;
  * lots = that amount / risk of one lot, rounded down, at most max_lots; fewer than one lot: no trade (recorded as skipped);
  * once the day's realised loss reaches the budget, no new entry that day.
Conviction: the mean margin of the entry conditions past their thresholds (spec.Condition.margin), mapped to a size scale by the tiers.
No market impact: positions are assumed small against the book (owner, 25 Nov 2026: within 2-3 % of the book's volume).  One position at a time.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from typing import Optional

import numpy as np
import pandas as pd

from backtester.data import Prices, signal_panel
from backtester.spec import Strategy
from trading.costs import fill_cost

IST = timedelta(hours=5, minutes=30)
FILL_WINDOW = 3                                   # minutes to find the next bar of the traded contract


def _ist(t: pd.Timestamp) -> time:
    return (t + IST).time()


@dataclass
class Trade:
    day: date
    symbol: str
    kind: str
    direction: int
    lots: int
    lot_size: int
    decided_at: pd.Timestamp
    entry_at: pd.Timestamp
    entry_price: float
    score: float
    size_scale: float
    planned_risk: float
    exit_at: Optional[pd.Timestamp] = None
    exit_price: float = math.nan
    exit_reason: str = ""
    costs: float = 0.0

    @property
    def qty(self) -> int:
        return self.lots * self.lot_size

    @property
    def gross(self) -> float:
        return (self.exit_price - self.entry_price) * self.qty * self.direction

    @property
    def net(self) -> float:
        return self.gross - self.costs


@dataclass
class Result:
    strategy: Strategy
    trades: list = field(default_factory=list)
    skipped: list = field(default_factory=list)          # entries the rules wanted but could not size or fill, with the reason
    days: list = field(default_factory=list)


class Backtest:
    def __init__(self, strategy: Strategy, lake, store, d0: date, d1: date, prices: Optional[Prices] = None):
        self.s, self.d0, self.d1 = strategy, d0, d1
        self.prices = prices or Prices(lake, strategy.underlying, d0, d1)
        grid = self.prices.index["ts"] + pd.Timedelta(minutes=1)                                  # decision times: each bar's close
        self.panel = signal_panel(store, strategy.underlying, d0, d1, strategy.needs(), grid).set_index("t")

    # ------------------------------------------------------------------------------------------ rules
    def _row(self, t: pd.Timestamp) -> dict:
        try:
            return self.panel.loc[t].to_dict()
        except KeyError:
            return {}

    def entry_ok(self, row: dict) -> tuple:
        """(the entry rule holds, conviction score)."""
        e = self.s.entry
        vals = lambda c: row.get(c.key, math.nan)                     # noqa: E731
        if not all(c.holds(vals(c)) for c in e.all):
            return False, 0.0
        held_any = [c for c in e.any if c.holds(vals(c))]
        if e.any and not held_any:
            return False, 0.0
        used = [*e.all, *held_any]
        score = float(np.mean([c.margin(vals(c)) for c in used])) if used else 0.0
        return True, score

    def exit_signal(self, row: dict) -> bool:
        return any(c.holds(row.get(c.key, math.nan)) for c in self.s.exit.any)

    # ------------------------------------------------------------------------------------------ prices
    def _hs(self, kind: str) -> float:
        f = self.s.fills
        return f.half_spread_fut_bps / 1e4 if kind == "future" else f.half_spread_opt_pct / 100

    def _fill(self, raw: float, side: int, kind: str) -> float:
        """A market fill at `raw`, worse by the half-spread: a buy pays more, a sell gets less."""
        return raw * (1 + side * self._hs(kind))

    def _cost(self, kind: str, side: int, price: float, qty: int, day: date) -> float:
        return fill_cost("fut" if kind == "future" else "opt", side, price, qty, day, broker=self.s.broker).total

    # ------------------------------------------------------------------------------------------ one day
    def run_day(self, day: date, res: Result) -> None:
        s, ins = self.s, self.s.instrument
        idx = self.prices.index[self.prices.index["ts"].dt.date == day].reset_index(drop=True)
        if not len(idx):
            return
        realised, trades_today, pos = 0.0, 0, None
        bars = None                                                   # the traded contract's minutes, indexed by ts
        k = 0                                                         # position in `idx`
        while k < len(idx):
            bar = idx.iloc[k]
            t_dec = bar["ts"] + pd.Timedelta(minutes=1)
            row = self._row(t_dec)
            if pos is not None:
                pos, realised = self._manage(pos, bars, bar["ts"], t_dec, row, day, realised, res)
                k += 1
                continue
            win = s.entry.window
            budget_left = s.risk.daily_loss_budget + min(realised, 0.0)
            if (trades_today < s.entry.max_trades_per_day and budget_left > 0 and win[0] <= _ist(t_dec) < win[1]
                    and _ist(t_dec) < s.exit.flat_at):
                ok, score = self.entry_ok(row)
                if ok:
                    pos, bars = self._enter(day, t_dec, float(bar["close"]), score, budget_left, res)
                    if pos is not None:
                        trades_today += 1
            k += 1
        if pos is not None:                                            # the index ended before the contract: close at its last bar
            last = bars.iloc[-1]
            self._close(pos, last["ts"] + pd.Timedelta(minutes=1), float(last["close"]), "day end", day)
            realised += pos.net
            res.trades.append(pos)
        res.days.append(day)

    def _enter(self, day: date, t_dec: pd.Timestamp, spot: float, score: float, budget_left: float, res: Result):
        s, ins = self.s, self.s.instrument
        scale = s.tier_scale(score)
        why = None
        c = self.prices.choose(ins.kind, day, spot, ins.right, ins.strike_offset, ins.expiry, ins.min_days_to_expiry)
        if c is None:
            why = "no contract"
        else:
            bars = self.prices.bars(c["symbol"], day)
            nxt = bars[(bars["ts"] >= t_dec) & (bars["ts"] < t_dec + pd.Timedelta(minutes=FILL_WINDOW))] if len(bars) else bars
            if not len(nxt):
                why = "no bar to fill"
            elif scale <= 0:
                why = "conviction below the lowest tier"
            else:
                b = nxt.iloc[0]
                px = self._fill(float(b["open"]), ins.direction, ins.kind)
                per_unit = px * s.exit.stop_loss_pct if s.exit.stop_loss_pct else px             # a bought option without a stop: all of it
                allowed = min(s.risk.daily_loss_budget * s.risk.max_trade_fraction * scale, budget_left)
                lots = min(int(allowed // (per_unit * c["lot_size"])) if per_unit > 0 else 0, s.risk.max_lots)
                if lots < 1:
                    why = f"one lot risks {per_unit * c['lot_size']:,.0f}, more than the {allowed:,.0f} allowed"
                else:
                    t = Trade(day, c["symbol"], ins.kind, ins.direction, lots, c["lot_size"], t_dec, b["ts"], px, score, scale,
                              per_unit * c["lot_size"] * lots)
                    t.costs = self._cost(ins.kind, ins.direction, px, t.qty, day)
                    return t, bars.set_index("ts", drop=False)
        res.skipped.append({"day": day, "decided_at": t_dec, "score": score, "reason": why})
        return None, None

    def _levels(self, pos: Trade) -> tuple:
        sl, tp = self.s.exit.stop_loss_pct, self.s.exit.take_profit_pct
        d = pos.direction
        stop = pos.entry_price * (1 - d * sl) if sl else None
        target = pos.entry_price * (1 + d * tp) if tp else None
        return stop, target

    def _manage(self, pos: Trade, bars: pd.DataFrame, ts: pd.Timestamp, t_dec: pd.Timestamp, row: dict, day: date, realised: float, res: Result):
        """One minute of an open position: stop and target on the bar, then the close-of-bar exits.  Returns (position or None, realised)."""
        s = self.s
        if ts < pos.entry_at or ts not in bars.index:
            return pos, realised
        b = bars.loc[ts]
        stop, target = self._levels(pos)
        d = pos.direction
        o, h, l = float(b["open"]), float(b["high"]), float(b["low"])
        hit = None
        if stop is not None and ((d > 0 and l <= stop) or (d < 0 and h >= stop)):
            gap = (d > 0 and o <= stop) or (d < 0 and o >= stop)
            hit = ("stop", o if gap else stop)
        elif target is not None and ((d > 0 and h >= target) or (d < 0 and l <= target)):
            gap = (d > 0 and o >= target) or (d < 0 and o <= target)
            hit = ("target", o if gap else target)
        if hit:
            self._close(pos, ts, self._fill(hit[1], -d, pos.kind), hit[0], day)
            res.trades.append(pos)
            return None, realised + pos.net
        reason = None
        if self.exit_signal(row):
            reason = "exit signal"
        elif s.exit.max_hold_minutes and t_dec - pos.entry_at >= pd.Timedelta(minutes=s.exit.max_hold_minutes):
            reason = "time stop"
        elif _ist(t_dec) >= s.exit.flat_at:
            reason = "flat time"
        if reason:
            later = bars[bars["ts"] >= t_dec]
            if len(later):
                nb = later.iloc[0]
                self._close(pos, nb["ts"], self._fill(float(nb["open"]), -d, pos.kind), reason, day)
            else:
                self._close(pos, ts + pd.Timedelta(minutes=1), self._fill(float(b["close"]), -d, pos.kind), reason + " (last bar)", day)
            res.trades.append(pos)
            return None, realised + pos.net
        return pos, realised

    def _close(self, pos: Trade, at: pd.Timestamp, price: float, reason: str, day: date) -> None:
        pos.exit_at, pos.exit_price, pos.exit_reason = at, price, reason
        pos.costs += self._cost(pos.kind, -pos.direction, price, pos.qty, day)

    # ------------------------------------------------------------------------------------------ the range
    def run(self, log=None) -> Result:
        res = Result(self.s)
        for day in self.prices.days():
            if self.d0 <= day <= self.d1:
                self.run_day(day, res)
                if log:
                    log(day, len(res.trades))
        return res
