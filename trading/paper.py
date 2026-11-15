"""
The paper trader (SCRUM-154): python -m trading paper --underlying NIFTY [--until 10:00]

  bus "signals-<U>" (final and provisional signal rows) ──► Strategy ──► RiskGate ──► PaperExecution ──► fills, positions, P&L
  bus "ticks-*" (quotes)  ─────────────────────────────────┘                                         └─► latency records

Writes logs/paper-<day>.jsonl (orders, fills, refusals, end-of-day P&L) and logs/latency-<day>.jsonl (one record per order).  Real orders
are never sent from here: this process only knows PaperExecution.
"""

from __future__ import annotations

import argparse
import json
import logging
import time
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd

from trading import latency
from trading.execution import PaperExecution
from trading.risk import Limits, RiskGate
from trading.strategy import Strategy, VrpSkewGate

log = logging.getLogger("trading.paper")


class PaperTrader:
    def __init__(self, strategy: Strategy, execution: PaperExecution, logs: Path, day):
        self.strategy, self.ex, self.logs, self.day = strategy, execution, Path(logs), pd.Timestamp(day).date()
        self.pending_lat: dict = {}                    # order id -> LatencyRecord
        self.events: list = []
        self.last_tick: dict = {}

    def on_tick_rows(self, cols: list, rows: list, now_ns: int) -> None:
        ix = {c: i for i, c in enumerate(cols)}
        for r in rows:
            sym = r[ix["symbol"]]
            bid, ask = r[ix["bid"]] if "bid" in ix else None, r[ix["ask"]] if "ask" in ix else None
            t = r[ix["fts"]] if "fts" in ix else now_ns
            if bid is not None and ask is not None:
                self.ex.on_quote(sym, float(bid), float(ask), int(t or now_ns))
            tick = {c: r[i] for c, i in ix.items()}
            self.last_tick = tick
            self._orders(self.strategy.on_tick(tick, now_ns), tick, now_ns, {})

    def on_signal_rows(self, msg: dict, now_ns: int) -> None:
        cols = msg["cols"]
        for r in msg["rows"]:
            row = dict(zip(cols, r))
            self._orders(self.strategy.on_signal(row, now_ns), row, now_ns, {"t_bus": msg.get("sent_ns")})

    def _orders(self, orders, src: dict, now_ns: int, extra: dict) -> None:
        for o in orders:
            o.t_decide = o.t_decide or now_ns
            rec = latency.LatencyRecord(decision=getattr(self.strategy, "name", "strategy"), symbol=o.symbol,
                                        t_vendor=src.get("t_vendor") or src.get("fts"), t_recv=src.get("t_recv") or src.get("rts"),
                                        t_emit=src.get("t_emit"), t_bus=extra.get("t_bus"), t_decide=o.t_decide)
            sent = time.time_ns()
            try:
                oid = self.ex.submit(o, sent)
            except RuntimeError as err:
                self.events.append({"event": "refused", "symbol": o.symbol, "side": o.side, "why": str(err), "t": sent})
                continue
            rec.t_sent = sent
            self.pending_lat[oid] = rec
            self.events.append({"event": "order", "id": oid, "symbol": o.symbol, "side": o.side, "lots": o.lots, "t": sent})

    def poll(self, now_ns: int) -> list:
        fills = self.ex.poll(now_ns)
        done = []
        for f in fills:
            self.events.append({"event": "fill", **asdict(f)})
            rec = self.pending_lat.pop(f.order_id, None)
            if rec is not None:
                rec.t_ack = f.t
                done.append(rec)
        if done:
            latency.append(done, self.logs, self.day)
        return fills

    def close(self) -> dict:
        summary = {"event": "close", "pnl": self.ex.pnl(), "costs": self.ex.costs, "fills": len(self.ex.fills),
                   "refused": len(self.ex.refused), "positions": {s: p.lots for s, p in self.ex.positions.items() if p.lots}}
        self.events.append(summary)
        p = self.logs / f"paper-{self.day}.jsonl"
        p.parent.mkdir(parents=True, exist_ok=True)
        with p.open("a") as f:
            for e in self.events:
                f.write(json.dumps(e, default=str) + "\n")
        self.events = []
        return summary


def run(underlying: str, until: pd.Timestamp, logs: Path, env=None, poll_ms: int = 100) -> dict:
    from data_layer import bus
    sig = bus.Subscriber([f"signals-{underlying}"], topics=["signal", "provisional"], env=env)
    ticks = bus.Subscriber(bus.feed_names(env), topics=["tick"], env=env)
    day = datetime.utcnow().date()
    strat = VrpSkewGate(underlying=underlying, strike_step=50.0 if underlying == "NIFTY" else 100.0,
                        lot_size=65 if underlying == "NIFTY" else 30)
    tr = PaperTrader(strat, PaperExecution(risk=RiskGate(Limits.from_env(env)), day=day), logs, day)
    last_status, n_sig = time.monotonic(), 0
    while pd.Timestamp.utcnow().tz_localize(None) < until:
        now = time.time_ns()
        try:
            for _, m in ticks.poll(timeout_ms=poll_ms):
                tr.on_tick_rows(m["cols"], m["rows"], now)
            for _, m in sig.poll(timeout_ms=1):
                n_sig += len(m.get("rows", []))
                tr.on_signal_rows(m, time.time_ns())
            tr.poll(time.time_ns())
        except Exception:                                   # noqa: BLE001 -- one bad message must not stop the paper session
            log.exception("paper: processing a bus message failed; continuing")
        if time.monotonic() - last_status >= 60:
            last_status = time.monotonic()
            log.info(status_line(tr, n_sig, ticks.missed + sig.missed))
    log.info(status_line(tr, n_sig, ticks.missed + sig.missed))
    return tr.close()


def status_line(tr: "PaperTrader", n_signal_rows: int, missed: int) -> str:
    """One line a minute: signal rows received, orders, fills, refusals, open positions, P&L after costs, kill switch."""
    ex = tr.ex
    pos = {s: p.lots for s, p in ex.positions.items() if p.lots}
    return (f"status paper: signal_rows={n_signal_rows} missed_on_bus={missed} open_orders={len(ex.open)} fills={len(ex.fills)} "
            f"refused={len(ex.refused)} positions={pos} pnl={ex.pnl():.0f} costs={ex.costs:.0f} kill={ex.risk.killed or 'off'}")


def main(argv: Optional[list] = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m trading")
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("paper", help="paper-trade the example strategy on the live bus")
    p.add_argument("--underlying", default="NIFTY")
    p.add_argument("--until", default="10:00", help="UTC")
    p.add_argument("--logs", default="logs")
    a = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    day = datetime.utcnow().date()
    until = pd.Timestamp(datetime.combine(day, datetime.strptime(a.until, "%H:%M").time()))
    print(json.dumps(run(a.underlying, until, Path(a.logs)), default=str))
    return 0
