"""
Execution adapters (SCRUM-154).  Every order passes the risk gate first; every fill is costed (trading/costs.py).

  PaperExecution    fills a market order at the opposite side of the latest quote (buy at the ask, sell at the bid) once `delay_ms`
                    has passed (the quote at that moment, not at the decision), a limit order when the quote crosses it; tracks
                    positions, average prices, realised and marked P&L, costs
  UpstoxExecution   real orders through Upstox's order API.  Refused unless JARVIS_LIVE_ORDERS=1 in the environment, which only the
                    owner sets by hand: no agent, script or default turns it on.  SEBI's retail algo rules also need a static IP
                    registered with Upstox and the strategy's orders tagged; both are the owner's step before going live.
"""

from __future__ import annotations

import itertools
import json
import logging
import os
import time
from dataclasses import dataclass, field
from datetime import date
from typing import Callable, Optional

from trading.costs import fill_cost
from trading.risk import RiskGate

log = logging.getLogger("trading.execution")

UPSTOX_ORDER_URL = "https://api-hft.upstox.com/v2/order/place"


class LiveOrdersDisabled(RuntimeError):
    pass


def live_orders_enabled(env=None) -> bool:
    return (os.environ if env is None else env).get("JARVIS_LIVE_ORDERS", "").strip() == "1"


@dataclass
class Order:
    symbol: str
    side: int                      # +1 buy, -1 sell
    lots: int
    lot_size: int
    kind: str = "opt"              # "opt" or "fut"
    limit: Optional[float] = None  # None: market
    tag: str = ""
    t_decide: int = 0
    t_sent: int = 0
    id: str = ""


@dataclass
class Fill:
    order_id: str
    symbol: str
    side: int
    qty: int
    price: float
    t: int
    cost: float


@dataclass
class Position:
    lots: int = 0
    qty: int = 0
    avg: float = 0.0
    realised: float = 0.0


class Execution:
    """The interface: submit() returns the order id (or raises), poll() returns new fills."""

    def submit(self, order: Order, now_ns: int) -> str:
        raise NotImplementedError

    def poll(self, now_ns: int) -> list[Fill]:
        return []


@dataclass
class PaperExecution(Execution):
    risk: RiskGate = field(default_factory=RiskGate)
    delay_ms: int = 250
    day: date = field(default_factory=date.today)
    quotes: dict = field(default_factory=dict)         # symbol -> (bid, ask, t_ns)
    open: list = field(default_factory=list)
    fills: list = field(default_factory=list)
    positions: dict = field(default_factory=dict)      # symbol -> Position
    costs: float = 0.0
    refused: list = field(default_factory=list)
    _ids: itertools.count = field(default_factory=lambda: itertools.count(1))

    def on_quote(self, symbol: str, bid: float, ask: float, t_ns: int) -> None:
        if bid == bid and ask == ask and bid > 0 and ask > 0:
            self.quotes[symbol] = (bid, ask, t_ns)

    def pnl(self) -> float:
        """Realised plus marked-to-mid P&L, after costs."""
        tot = -self.costs
        for s, p in self.positions.items():
            tot += p.realised
            q = self.quotes.get(s)
            if p.qty and q:
                tot += p.qty * ((q[0] + q[1]) / 2 - p.avg)
        return tot

    def submit(self, order: Order, now_ns: int) -> str:
        why = self.risk.check(now_ns, order.symbol, order.side * order.lots, {s: p.lots for s, p in self.positions.items()}, self.pnl())
        if why:
            self.refused.append((order, why))
            raise RuntimeError(f"risk: {why}")
        order.id = order.id or f"P{next(self._ids)}"
        order.t_sent = order.t_sent or now_ns
        self.open.append(order)
        return order.id

    def poll(self, now_ns: int) -> list[Fill]:
        out, still = [], []
        for o in self.open:
            q = self.quotes.get(o.symbol)
            if q is None or now_ns - o.t_sent < self.delay_ms * 1_000_000:
                still.append(o)
                continue
            px = q[1] if o.side > 0 else q[0]
            if o.limit is not None and ((o.side > 0 and px > o.limit) or (o.side < 0 and px < o.limit)):
                still.append(o)
                continue
            qty = o.lots * o.lot_size
            c = fill_cost(o.kind, o.side, px, qty, self.day).total
            f = Fill(o.id, o.symbol, o.side, qty, px, now_ns, c)
            self._book(f, o.lots)
            out.append(f)
        self.open = still
        self.fills += out
        return out

    def _book(self, f: Fill, lots: int) -> None:
        p = self.positions.setdefault(f.symbol, Position())
        signed = f.side * f.qty
        self.costs += f.cost
        if p.qty == 0 or (p.qty > 0) == (signed > 0):
            p.avg = (p.avg * abs(p.qty) + f.price * f.qty) / (abs(p.qty) + f.qty)
        else:
            closed = min(abs(p.qty), f.qty)
            p.realised += closed * (f.price - p.avg) * (1 if p.qty > 0 else -1)
            if f.qty > abs(p.qty):
                p.avg = f.price
        p.qty += signed
        p.lots += f.side * lots
        if p.qty == 0:
            p.avg = 0.0


@dataclass
class UpstoxExecution(Execution):
    """Real orders.  Construction succeeds (so the process can be configured and tested), but submit() refuses unless
    JARVIS_LIVE_ORDERS=1 is set in the environment at the time of the order."""
    instrument_key: Callable[[str], str]               # our symbol -> Upstox instrument key (data_layer/upstox/fo_instruments)
    risk: RiskGate = field(default_factory=RiskGate)
    token: Callable[[], str] = None
    env: Optional[dict] = None
    post: Callable = None                              # injectable for tests: post(url, headers, body) -> (status, json)
    positions: dict = field(default_factory=dict)
    sent: list = field(default_factory=list)

    def submit(self, order: Order, now_ns: int) -> str:
        if not live_orders_enabled(self.env):
            raise LiveOrdersDisabled("real orders are off: JARVIS_LIVE_ORDERS=1 is the owner's switch")
        why = self.risk.check(now_ns, order.symbol, order.side * order.lots, self.positions, 0.0)
        if why:
            raise RuntimeError(f"risk: {why}")
        body = {"quantity": order.lots * order.lot_size, "product": "I", "validity": "DAY",
                "price": 0 if order.limit is None else order.limit, "tag": (order.tag or "jarvis")[:20],
                "instrument_token": self.instrument_key(order.symbol), "order_type": "MARKET" if order.limit is None else "LIMIT",
                "transaction_type": "BUY" if order.side > 0 else "SELL", "disclosed_quantity": 0, "trigger_price": 0, "is_amo": False}
        tok = (self.token or _token)()
        headers = {"Authorization": f"Bearer {tok}", "Content-Type": "application/json", "Accept": "application/json"}
        status, resp = (self.post or _post)(UPSTOX_ORDER_URL, headers, body)
        if status != 200 or resp.get("status") != "success":
            raise RuntimeError(f"order rejected ({status}): {json.dumps(resp)[:300]}")
        oid = (resp.get("data") or {}).get("order_id") or ",".join((resp.get("data") or {}).get("order_ids", []))
        self.positions[order.symbol] = self.positions.get(order.symbol, 0) + order.side * order.lots
        self.sent.append((order, oid, time.time_ns()))
        return oid


def _token() -> str:
    from data_layer.upstox.feed import current_token
    return current_token()


def _post(url: str, headers: dict, body: dict):
    import requests
    r = requests.post(url, headers=headers, json=body, timeout=5)
    try:
        return r.status_code, r.json()
    except ValueError:
        return r.status_code, {"status": "error", "text": r.text[:300]}
