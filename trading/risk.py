"""
Risk limits and the kill switch, checked before every order (paper or real).

  max_orders_per_second   default 5: under SEBI's retail algo threshold (10 orders a second per client, above which the strategy has to
                          be registered with the exchange through the broker)
  max_orders_per_minute   default 60
  max_lots                largest absolute open position per symbol, in lots
  max_total_lots          largest sum of absolute positions, in lots
  daily_loss_stop         realised + marked P&L in rupees below -daily_loss_stop trips the kill switch for the rest of the day

Kill switch: RiskGate.kill(reason) or the file named by JARVIS_KILL_FILE (default /tmp/jarvis-bus/KILL) existing.  Once tripped, every
new order is refused; closing orders are refused too (flattening is a human decision on the broker's screen).
"""

from __future__ import annotations

import os
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional


@dataclass
class Limits:
    max_orders_per_second: int = 5
    max_orders_per_minute: int = 60
    max_lots: int = 2
    max_total_lots: int = 4
    daily_loss_stop: float = 5_000.0

    @classmethod
    def from_env(cls, env=None) -> "Limits":
        e = os.environ if env is None else env
        kw = {}
        for f, cast in (("max_orders_per_second", int), ("max_orders_per_minute", int), ("max_lots", int), ("max_total_lots", int),
                        ("daily_loss_stop", float)):
            v = e.get("JARVIS_RISK_" + f.upper())
            if v:
                kw[f] = cast(v)
        return cls(**kw)


@dataclass
class RiskGate:
    limits: Limits = field(default_factory=Limits)
    kill_file: Optional[Path] = None
    killed: Optional[str] = None
    sent: deque = field(default_factory=deque)                 # order times (ns) of the last minute
    refused: int = 0

    def __post_init__(self):
        if self.kill_file is None:
            self.kill_file = Path(os.environ.get("JARVIS_KILL_FILE", "/tmp/jarvis-bus/KILL"))

    def kill(self, reason: str) -> None:
        self.killed = self.killed or reason

    def check(self, now_ns: int, symbol: str, lots: int, positions: dict, pnl: float) -> Optional[str]:
        """None when the order may go, else the reason it may not.  `lots` is signed; `positions` symbol -> signed lots."""
        if self.kill_file is not None and self.kill_file.exists():
            self.kill(f"kill file {self.kill_file}")
        if pnl < -self.limits.daily_loss_stop:
            self.kill(f"daily loss {pnl:.0f} beyond -{self.limits.daily_loss_stop:.0f}")
        if self.killed:
            return self._no(f"killed: {self.killed}")
        while self.sent and self.sent[0] < now_ns - 60 * 10**9:
            self.sent.popleft()
        if len(self.sent) >= self.limits.max_orders_per_minute:
            return self._no("orders per minute")
        if sum(1 for t in self.sent if t >= now_ns - 10**9) >= self.limits.max_orders_per_second:
            return self._no("orders per second")
        new = positions.get(symbol, 0) + lots
        if abs(new) > self.limits.max_lots:
            return self._no(f"position {new} lots beyond {self.limits.max_lots}")
        total = sum(abs(v) for k, v in positions.items() if k != symbol) + abs(new)
        if total > self.limits.max_total_lots:
            return self._no(f"total {total} lots beyond {self.limits.max_total_lots}")
        self.sent.append(now_ns)
        return None

    def _no(self, why: str) -> str:
        self.refused += 1
        return why
