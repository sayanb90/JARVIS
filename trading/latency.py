"""
Tick-to-trade latency, one record per decision, every leg timed (nanoseconds since the epoch, UTC):

  t_exchange   the exchange's last trade time of the tick that triggered the decision (ltt; absent on some ticks)
  t_vendor     Upstox's message time (fts)
  t_recv       our feed's receive time (rts)
  t_bus        the signal row's bus send time (engine side)
  t_emit       the engine's emit time of the signal row
  t_decide     the strategy's decision time
  t_sent       order handed to the execution adapter
  t_ack        the adapter's acknowledgement (paper: the simulated fill; Upstox: the REST response)

Legs reported: vendor (t_recv - t_vendor), engine (t_emit - t_recv), bus (t_decide - t_bus), strategy (t_sent - t_decide),
order (t_ack - t_sent) and total tick to trade (t_ack - t_vendor).

  python -m signals latency 2026-11-16 [--logs logs]       p50 / p95 / p99 per leg from logs/latency-<day>.jsonl
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable, Optional

import pandas as pd

LEGS = {"vendor": ("t_vendor", "t_recv"), "engine": ("t_recv", "t_emit"), "bus": ("t_bus", "t_decide"),
        "strategy": ("t_decide", "t_sent"), "order": ("t_sent", "t_ack"), "tick_to_trade": ("t_vendor", "t_ack")}


@dataclass
class LatencyRecord:
    decision: str
    symbol: str
    t_exchange: Optional[int] = None
    t_vendor: Optional[int] = None
    t_recv: Optional[int] = None
    t_bus: Optional[int] = None
    t_emit: Optional[int] = None
    t_decide: Optional[int] = None
    t_sent: Optional[int] = None
    t_ack: Optional[int] = None
    mode: str = "paper"


def log_path(logs: Path, day) -> Path:
    return Path(logs) / f"latency-{pd.Timestamp(day).date()}.jsonl"


def append(records: Iterable[LatencyRecord], logs: Path, day) -> int:
    p = log_path(logs, day)
    p.parent.mkdir(parents=True, exist_ok=True)
    n = 0
    with p.open("a") as f:
        for r in records:
            f.write(json.dumps(asdict(r)) + "\n")
            n += 1
    return n


def report(logs: Path, day) -> pd.DataFrame:
    """Per leg: decisions, p50, p95, p99 and max in milliseconds."""
    p = log_path(logs, day)
    if not p.exists():
        return pd.DataFrame(columns=["leg", "n", "p50_ms", "p95_ms", "p99_ms", "max_ms"])
    df = pd.DataFrame([json.loads(x) for x in p.read_text().splitlines() if x.strip()])
    rows = []
    for leg, (a, b) in LEGS.items():
        if a not in df.columns or b not in df.columns:
            continue
        ms = ((df[b] - df[a]) / 1e6).dropna()
        if not len(ms):
            continue
        rows.append({"leg": leg, "n": len(ms), "p50_ms": ms.quantile(0.5), "p95_ms": ms.quantile(0.95), "p99_ms": ms.quantile(0.99),
                     "max_ms": ms.max()})
    return pd.DataFrame(rows)
