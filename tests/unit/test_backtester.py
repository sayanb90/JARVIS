"""The backtester (SCRUM-217): rule-based entries and exits on minute signals, filled on the next bar, real costs, a rupee risk budget and conviction
sizing; on a synthetic lake and signal store."""

import json
import math
from datetime import date, time

import numpy as np
import pandas as pd
import pytest

from backtester import report, spec
from backtester.engine import Backtest
from signals.options.core import INDEX_SYMBOL
from trading.costs import fill_cost

DAYS = [date(2026, 11, 23), date(2026, 11, 24)]
FUT = "NIFTY_FUT_26_NOV_26"
CE = "NIFTY_24000_CE_2026-11-26"
CE_UP = "NIFTY_24050_CE_2026-11-26"
LOT = 75


def minutes(day):
    return pd.date_range(pd.Timestamp(day) + pd.Timedelta(hours=3, minutes=45), periods=375, freq="1min")


def bars(day, symbol, price, step=0.0, spread=1.0, override=None):
    ts = minutes(day)
    p = price + step * np.arange(len(ts))
    df = pd.DataFrame({"ts": ts, "symbol": symbol, "open": p, "high": p + spread, "low": p - spread, "close": p, "volume": 100, "oi": 1000,
                       "market": "IN"})
    for i, vals in (override or {}).items():
        for k, v in vals.items():
            df.loc[i, k] = v
    return df


def make_lake(root, fut_over=None, step=0.5):
    for d in DAYS:
        folder = root / "bars" / "interval=1m" / f"date={d}"
        folder.mkdir(parents=True)
        pd.concat([bars(d, INDEX_SYMBOL["NIFTY"], 24000, step), bars(d, FUT, 24100, step, override=fut_over),
                   bars(d, CE, 100, 0.1), bars(d, CE_UP, 80, 0.1)]).to_parquet(folder / "bars.parquet", index=False)
    inst = root / "instruments"
    inst.mkdir(parents=True)
    pd.DataFrame([{"symbol": FUT, "underlying": "NIFTY", "instrument_type": "FUT", "strike": np.nan, "expiry": "2026-11-26", "lot_size": LOT},
                  {"symbol": CE, "underlying": "NIFTY", "instrument_type": "CE", "strike": 24000.0, "expiry": "2026-11-26", "lot_size": LOT},
                  {"symbol": CE_UP, "underlying": "NIFTY", "instrument_type": "CE", "strike": 24050.0, "expiry": "2026-11-26", "lot_size": LOT}]
                 ).to_parquet(inst / "fo_expired.parquet", index=False)


def make_signal(store, values_by_minute, sid="trend", col="z", known_lag=1, days=DAYS):
    """values_by_minute: {minute index: value}, others 0.  known_at = bar start + known_lag minutes."""
    for d in days:
        ts = minutes(d)
        z = np.zeros(len(ts))
        for i, v in values_by_minute.items():
            z[i] = v
        folder = store / "signals" / sid / "v1" / "underlying=NIFTY" / f"date={d}"
        folder.mkdir(parents=True)
        pd.DataFrame({"ts": ts, "known_at": ts + pd.Timedelta(minutes=known_lag), col: z}).to_parquet(folder / "part.parquet", index=False)


def strat(**over):
    d = {"name": "t", "underlying": "NIFTY", "broker": "zerodha",
         "instrument": {"kind": "future", "side": "buy"},
         "entry": {"all": [{"signal": "trend", "column": "z", "op": ">", "value": 1.0}], "window": ["09:15", "15:00"], "max_trades_per_day": 1},
         "exit": {"any": [{"signal": "trend", "column": "z", "op": "<", "value": -1.0}], "stop_loss_pct": 0.01, "flat_at": "15:15"},
         "risk": {"daily_loss_budget": 100000, "max_trade_fraction": 0.6, "max_lots": 50},
         "fills": {"half_spread_fut_bps": 0.0, "half_spread_opt_pct": 0.0}}
    for k, v in over.items():
        if isinstance(v, dict) and isinstance(d.get(k), dict):
            d[k] = {**d[k], **v}
        else:
            d[k] = v
    return spec.from_dict(d)


def run(tmp_path, s, d1=DAYS[-1]):
    return Backtest(s, tmp_path / "lake", tmp_path / "store", DAYS[0], d1).run()


def test_a_future_is_bought_on_the_signal_and_sold_on_the_exit_signal_at_the_next_bar_with_real_costs(tmp_path):
    make_lake(tmp_path / "lake")
    make_signal(tmp_path / "store", {30: 2.0, 90: -2.0})
    res = run(tmp_path, strat(), d1=DAYS[0])
    assert len(res.trades) == 1
    t = res.trades[0]
    # signal row at bar 30 is known at bar 31's start: decided at the close of bar 30 (= start of 31), filled at bar 31's open
    assert t.entry_at == minutes(DAYS[0])[31] and t.entry_price == pytest.approx(24100 + 0.5 * 31)
    assert t.exit_reason == "exit signal" and t.exit_at == minutes(DAYS[0])[91] and t.exit_price == pytest.approx(24100 + 0.5 * 91)
    lots = int((100000 * 0.6) // (t.entry_price * 0.01 * LOT))
    assert t.lots == lots
    want = (fill_cost("fut", 1, t.entry_price, t.qty, DAYS[0], broker="zerodha").total
            + fill_cost("fut", -1, t.exit_price, t.qty, DAYS[0], broker="zerodha").total)
    assert t.costs == pytest.approx(want) and t.net == pytest.approx(0.5 * 60 * t.qty - want)


def test_nothing_is_known_before_its_known_at(tmp_path):
    make_lake(tmp_path / "lake")
    make_signal(tmp_path / "store", {30: 2.0, 90: -2.0}, known_lag=6)               # the same values, known five minutes later
    t = run(tmp_path, strat(), d1=DAYS[0]).trades[0]
    assert t.entry_at == minutes(DAYS[0])[36] and t.exit_at == minutes(DAYS[0])[96]


def test_a_stale_value_does_not_carry_into_the_next_morning(tmp_path):
    make_lake(tmp_path / "lake")
    store = tmp_path / "store"
    make_signal(store, {374: 5.0}, days=[DAYS[0]])                                    # true only in the last minute of day one
    folder = store / "signals" / "trend" / "v1" / "underlying=NIFTY" / f"date={DAYS[1]}"
    folder.mkdir(parents=True)
    ts = minutes(DAYS[1])[30:]                                                        # day two's signal starts late
    pd.DataFrame({"ts": ts, "known_at": ts + pd.Timedelta(minutes=1), "z": 0.0}).to_parquet(folder / "part.parquet", index=False)
    res = run(tmp_path, strat(entry={"window": ["09:15", "15:30"]}))
    assert all(t.day != DAYS[1] for t in res.trades)


def test_a_bar_that_touches_stop_and_target_takes_the_stop_and_a_gap_fills_at_the_open(tmp_path):
    entry_open = 24100 + 0.5 * 31
    over = {40: {"high": entry_open * 1.02, "low": entry_open * 0.98}}             # both levels inside one bar
    make_lake(tmp_path / "lake", fut_over=over)
    make_signal(tmp_path / "store", {30: 2.0})
    t = run(tmp_path, strat(exit={"any": [], "stop_loss_pct": 0.01, "take_profit_pct": 0.01}), d1=DAYS[0]).trades[0]
    assert t.exit_reason == "stop" and t.exit_price == pytest.approx(entry_open * 0.99)
    gap = {40: {"open": entry_open * 0.97, "high": entry_open * 0.975, "low": entry_open * 0.96}}
    make_lake(tmp_path / "lake2", fut_over=gap)
    t2 = Backtest(strat(exit={"any": [], "stop_loss_pct": 0.01}), tmp_path / "lake2", tmp_path / "store", DAYS[0], DAYS[0]).run().trades[0]
    assert t2.exit_reason == "stop" and t2.exit_price == pytest.approx(entry_open * 0.97)       # beyond the planned risk


def test_size_follows_the_budget_the_trade_cap_and_conviction_and_entries_stop_when_the_budget_is_spent(tmp_path):
    make_lake(tmp_path / "lake", step=-2.0)                                           # a falling market: every long loses
    make_signal(tmp_path / "store", {20: 1.5, 60: 4.0, 120: 1.5, 200: 1.5})
    s = strat(entry={"max_trades_per_day": 10}, exit={"any": [], "stop_loss_pct": 0.002, "max_hold_minutes": 5},
              conviction={"tiers": [{"min_score": 0.0, "scale": 0.5}, {"min_score": 2.0, "scale": 1.0}]},
              risk={"daily_loss_budget": 20000, "max_trade_fraction": 0.6, "max_lots": 50})
    res = run(tmp_path, s, d1=DAYS[0])
    first = res.trades[0]
    per_lot = first.entry_price * 0.002 * LOT
    assert first.size_scale == 0.5 and first.lots == int((20000 * 0.6 * 0.5) // per_lot)
    second = res.trades[1]
    assert second.score == pytest.approx(3.0) and second.size_scale == 1.0
    assert second.planned_risk <= 20000 * 0.6 + 1e-6
    spent = 0.0
    for t in res.trades:                                                             # each trade fits in what was left of the budget when it started
        assert t.planned_risk <= 20000 + min(spent, 0) + 1e-6
        spent += t.net
    assert len(res.trades) < 4 or spent > -20000                                     # once the budget is gone no new trade starts


def test_an_option_is_chosen_at_the_money_or_steps_away_and_a_bought_option_without_a_stop_risks_its_premium(tmp_path):
    make_lake(tmp_path / "lake", step=0.0)
    make_signal(tmp_path / "store", {30: 2.0, 90: -2.0})
    s = strat(instrument={"kind": "option", "side": "buy", "right": "CE", "strike": "atm"}, exit={"stop_loss_pct": None},
              fills={"half_spread_opt_pct": 1.0})
    t = run(tmp_path, s, d1=DAYS[0]).trades[0]
    assert t.symbol == CE and t.entry_price == pytest.approx((100 + 0.1 * 31) * 1.01)       # paid the half-spread
    assert t.planned_risk == pytest.approx(t.entry_price * LOT * t.lots)
    assert t.lots == int((100000 * 0.6) // (t.entry_price * LOT))
    s2 = strat(instrument={"kind": "option", "side": "buy", "right": "CE", "strike": "atm+1"}, exit={"stop_loss_pct": None})
    assert run(tmp_path, s2, d1=DAYS[0]).trades[0].symbol == CE_UP


def test_rules_that_cannot_bound_their_risk_are_refused():
    with pytest.raises(ValueError, match="stop_loss_pct"):
        strat(instrument={"kind": "option", "side": "sell", "right": "CE", "strike": "atm"}, exit={"stop_loss_pct": None})
    with pytest.raises(ValueError, match="stop_loss_pct"):
        strat(exit={"stop_loss_pct": None})
    with pytest.raises(ValueError, match="op"):
        spec.Condition("a", "b", "~", 1.0)


def test_costs_match_a_hand_computed_contract_note():
    """Futures round trip, 75 x 24,000 = Rs 18 lakh each side, on 2 Dec 2026 (STT 0.05 % sell, NSE 0.00183 %, stamp 0.002 % buy, SEBI Rs 10 / crore,
    GST 18 %); Zerodha caps futures brokerage at 0.03 % (Rs 540 here, so Rs 20), Upstox at 0.05 %.  Options: 75 x Rs 100 = Rs 7,500 premium."""
    d = date(2026, 12, 2)
    v = 75 * 24000.0
    buy = fill_cost("fut", 1, 24000.0, 75, d, broker="zerodha")
    sell = fill_cost("fut", -1, 24000.0, 75, d, broker="zerodha")
    assert buy.brokerage == 20 and sell.stt == pytest.approx(v * 0.0005) and buy.stt == 0
    assert buy.exchange == pytest.approx(v * 0.0000183) and buy.stamp == pytest.approx(v * 0.00002) and sell.stamp == 0
    assert buy.gst == pytest.approx(0.18 * (20 + v * 0.0000183 + v * 1e-6)) and buy.ipft == 0
    small = fill_cost("fut", 1, 100.0, 1, d, broker="zerodha")                       # Rs 100 of value: 0.03 % is less than Rs 20
    assert small.brokerage == pytest.approx(0.03) and fill_cost("fut", 1, 100.0, 1, d, broker="upstox").brokerage == pytest.approx(0.05)
    o = fill_cost("opt", -1, 100.0, 75, d, broker="upstox")
    assert o.brokerage == 20 and o.stt == pytest.approx(7500 * 0.0015) and o.exchange == pytest.approx(7500 * 0.0003553)
    old = fill_cost("opt", -1, 100.0, 75, date(2025, 1, 2))
    assert old.stt == pytest.approx(7500 * 0.001) and old.exchange == pytest.approx(7500 * 0.0003503) and old.ipft == pytest.approx(7500 * 5e-6)
    assert fill_cost("opt", -1, 100.0, 75, date(2023, 2, 1)).stt == pytest.approx(7500 * 0.0005)


def test_the_report_and_the_command_line(tmp_path, capsys):
    make_lake(tmp_path / "lake")
    make_signal(tmp_path / "store", {30: 2.0, 90: -2.0, 200: 2.0, 260: -2.0})
    cfg = tmp_path / "s.toml"
    cfg.write_text('''name = "cli"
underlying = "NIFTY"
broker = "upstox"
[instrument]
kind = "future"
side = "buy"
[entry]
all = [{ signal = "trend", column = "z", op = ">", value = 1.0 }]
window = ["09:15", "15:00"]
max_trades_per_day = 2
[exit]
any = [{ signal = "trend", column = "z", op = "<", value = -1.0 }]
stop_loss_pct = 0.01
[conviction]
tiers = [{ min_score = 0.0, scale = 0.5 }, { min_score = 0.9, scale = 1.0 }]
''')
    from backtester.__main__ import main
    assert main(["run", "--config", str(cfg), "--from", str(DAYS[0]), "--to", str(DAYS[1]), "--lake", str(tmp_path / "lake"),
                 "--store", str(tmp_path / "store"), "--out", str(tmp_path / "out"), "--force"]) == 0
    out = tmp_path / "out" / f"cli-{DAYS[0]}-{DAYS[1]}"
    s = json.loads((out / "summary.json").read_text())
    assert s["trades"] == 4 and s["days"] == 2 and s["net"] == pytest.approx(s["gross"] - s["costs"]) and s["net"] > 0
    assert set(s["by_tier"]) == {"1"} and s["exits"] == {"exit signal": 4}
    daily = pd.read_csv(out / "daily.csv")
    assert list(daily["trades"]) == [2, 2] and daily["cum_net"].iloc[-1] == pytest.approx(s["net"])
    assert len(pd.read_csv(out / "trades.csv")) == 4
