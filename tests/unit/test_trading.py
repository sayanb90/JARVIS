"""Strategy, paper execution, risk limits, the live-order switch and latency records (SCRUM-154)."""

from datetime import date

import pandas as pd
import pytest

from trading import latency
from trading.costs import fill_cost
from trading.execution import LiveOrdersDisabled, Order, PaperExecution, UpstoxExecution
from trading.paper import PaperTrader
from trading.risk import Limits, RiskGate
from trading.strategy import VrpSkewGate

S = 10**9
T0 = pd.Timestamp("2026-11-16 04:00").value                  # 09:30 IST


def gate(tmp_path, **kw):
    return RiskGate(Limits(**kw), kill_file=tmp_path / "KILL")


def test_costs_follow_the_side_and_the_date():
    sell = fill_cost("opt", -1, 100.0, 650, date(2025, 1, 2))
    buy = fill_cost("opt", +1, 100.0, 650, date(2025, 1, 2))
    assert sell.stt == pytest.approx(65_000 * 0.001) and buy.stt == 0.0
    assert buy.stamp > 0 and sell.stamp == 0.0
    assert sell.gst == pytest.approx(0.18 * (20 + sell.exchange + sell.sebi))
    assert fill_cost("opt", -1, 100.0, 650, date(2024, 6, 3)).stt == pytest.approx(65_000 * 0.000625)


def test_risk_limits_and_kill_switch(tmp_path):
    g = gate(tmp_path, max_orders_per_second=2, max_lots=1)
    assert g.check(T0, "A", 1, {}, 0.0) is None
    assert g.check(T0 + 1, "B", 1, {"A": 1}, 0.0) is None
    assert "per second" in g.check(T0 + 2, "C", 1, {"A": 1, "B": 1}, 0.0)
    assert "position" in g.check(T0 + 2 * S, "A", 1, {"A": 1}, 0.0)
    (tmp_path / "KILL").write_text("stop")
    assert "kill file" in g.check(T0 + 3 * S, "D", 1, {}, 0.0)
    g2 = gate(tmp_path / "x", daily_loss_stop=100)
    assert "daily loss" in g2.check(T0, "A", 1, {}, -150.0)
    assert g2.check(T0 + 10 * S, "A", -1, {}, 0.0).startswith("killed")          # stays killed for the day


def test_paper_fills_at_the_opposite_quote_after_the_delay(tmp_path):
    ex = PaperExecution(risk=gate(tmp_path), delay_ms=250, day=date(2026, 11, 16))
    ex.on_quote("X", 99.0, 101.0, T0)
    ex.submit(Order("X", +1, 1, 65), T0)
    assert ex.poll(T0 + 100_000_000) == []                                      # before the delay
    ex.on_quote("X", 100.0, 102.0, T0 + 200_000_000)
    (f,) = ex.poll(T0 + 300_000_000)
    assert f.price == 102.0 and f.qty == 65                                     # the ask when the delay is over
    ex.on_quote("X", 110.0, 112.0, T0 + S)
    ex.submit(Order("X", -1, 1, 65), T0 + S)
    (g,) = ex.poll(T0 + 2 * S)
    assert g.price == 110.0
    assert ex.positions["X"].qty == 0 and ex.positions["X"].realised == pytest.approx(65 * 8.0)
    assert ex.pnl() == pytest.approx(65 * 8.0 - ex.costs) and ex.costs > 0


def test_paper_limit_order_waits_for_the_quote(tmp_path):
    ex = PaperExecution(risk=gate(tmp_path), delay_ms=0)
    ex.on_quote("X", 99.0, 101.0, T0)
    ex.submit(Order("X", +1, 1, 65, limit=100.0), T0)
    assert ex.poll(T0 + S) == []
    ex.on_quote("X", 98.0, 99.5, T0 + S)
    assert len(ex.poll(T0 + 2 * S)) == 1


def test_real_orders_refused_without_the_owner_switch(tmp_path):
    calls = []
    ex = UpstoxExecution(instrument_key=lambda s: "NSE_FO|1", risk=gate(tmp_path), token=lambda: "tok", env={},
                         post=lambda *a: calls.append(a) or (200, {"status": "success", "data": {"order_id": "1"}}))
    with pytest.raises(LiveOrdersDisabled):
        ex.submit(Order("X", +1, 1, 65), T0)
    ex.env = {"JARVIS_LIVE_ORDERS": "true"}                                     # only the exact value 1 counts
    with pytest.raises(LiveOrdersDisabled):
        ex.submit(Order("X", +1, 1, 65), T0)
    assert calls == []
    ex.env = {"JARVIS_LIVE_ORDERS": "1"}
    assert ex.submit(Order("X", -1, 2, 65, limit=50.0, tag="t"), T0) == "1"
    url, headers, body = calls[0]
    assert body["transaction_type"] == "SELL" and body["quantity"] == 130 and body["order_type"] == "LIMIT"
    assert body["instrument_token"] == "NSE_FO|1" and headers["Authorization"] == "Bearer tok"


def _feed(strat, now):
    strat.on_tick({"symbol": "NIFTY_FUT_24_NOV_26", "ltp": 24_013.0}, now)
    strat.on_tick({"symbol": "NIFTY_24000_CE_17_NOV_26", "ltp": 120.0}, now)
    strat.on_tick({"symbol": "NIFTY_24000_PE_24_NOV_26", "ltp": 150.0}, now)


def test_example_strategy_enters_and_exits():
    s = VrpSkewGate()
    _feed(s, T0)
    assert s.on_signal({"signal": "skew", "skew1_s": 0.03}, T0) == []
    assert s.on_signal({"signal": "vrp", "vrp_ratio": 1.4}, T0) == []          # skew steepening: no entry
    s.on_signal({"signal": "skew", "skew1_s": 0.01}, T0)
    legs = s.on_signal({"signal": "vrp", "vrp_ratio": 1.4}, T0)
    assert [(o.symbol, o.side) for o in legs] == [("NIFTY_24000_CE_17_NOV_26", -1), ("NIFTY_24000_PE_17_NOV_26", -1)]
    assert s.on_signal({"signal": "vrp", "vrp_ratio": 1.2}, T0) == []          # still rich
    out = s.on_signal({"signal": "vrp", "vrp_ratio": 1.0}, T0 + S)
    assert [o.side for o in out] == [1, 1]


def test_paper_trader_records_latency_per_order(tmp_path):
    s = VrpSkewGate()
    _feed(s, T0)
    ex = PaperExecution(risk=gate(tmp_path), delay_ms=0, day=date(2026, 11, 16))
    tr = PaperTrader(s, ex, tmp_path / "logs", "2026-11-16")
    tr.on_tick_rows(["symbol", "fts", "bid", "ask"], [["NIFTY_24000_CE_17_NOV_26", T0, 119.0, 121.0],
                                                     ["NIFTY_24000_PE_17_NOV_26", T0, 149.0, 151.0]], T0)
    msg = {"sent_ns": T0 + 5_000_000, "cols": ["signal", "vrp_ratio", "t_vendor", "t_recv", "t_emit"],
           "rows": [["vrp", 1.5, T0 - 300_000_000, T0 - 200_000_000, T0]]}
    tr.on_signal_rows(msg, T0 + 6_000_000)
    assert len(tr.poll(pd.Timestamp.utcnow().value)) == 2
    summary = tr.close()
    assert summary["fills"] == 2 and summary["positions"] == {"NIFTY_24000_CE_17_NOV_26": -1, "NIFTY_24000_PE_17_NOV_26": -1}
    rep = latency.report(tmp_path / "logs", "2026-11-16").set_index("leg")
    assert rep.loc["engine", "n"] == 2 and rep.loc["engine", "p50_ms"] == pytest.approx(200.0)
    assert {"vendor", "bus", "strategy", "order", "tick_to_trade"} <= set(rep.index)
