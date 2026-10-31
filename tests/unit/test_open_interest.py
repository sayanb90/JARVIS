"""Open interest end to end: decode -> bar -> Parquet -> roll-up -> API; REST candles; futures verification."""
from __future__ import annotations

import sys
from datetime import date, datetime, timedelta

import numpy as np
import pandas as pd
import pytest
from fastapi.testclient import TestClient

from analytics.microstructure import day_summary, series_payload
from data_layer import tick_store, verify
from data_layer.query import Lake
from data_layer.ticks import Bar, BarAggregator, Tick
from data_layer.upstox import candles, decode, feed
from data_layer.upstox import fo_instruments as fo
from data_layer.upstox.proto import MarketDataFeedV3_pb2 as pb
from tests.bdd.fake_upstox import tick_frame
from tests.helpers import ms
from webapp import demo
from webapp.app import create_app

OPEN = datetime(2026, 10, 23, 3, 45)
DAY = date(2026, 10, 23)


def tick(sec, px, vtt, oi=None, sym="NIFTY_FUT_27_OCT_26"):
    return Tick(OPEN + timedelta(seconds=sec), sym, px, vtt, px - 0.05, 10, px + 0.05, 12, oi)


# -- aggregation ----------------------------------------------------------------
def test_bar_carries_the_last_open_interest_seen_inside_it():
    agg = BarAggregator(1)
    out = agg.add(tick(0, 100, 1000, oi=500)) + agg.add(tick(0, 101, 1010, oi=520)) + agg.add(tick(1, 102, 1020, oi=None))
    out += agg.flush_all()
    assert [b.oi for b in out] == [520, None]                      # a tick with no OI leaves the bar's last value alone


def test_cash_equity_bars_have_no_open_interest():
    agg = BarAggregator(1)
    agg.add(Tick(OPEN, "A.NS", 10.0, 5))
    assert agg.flush_all()[0].oi is None


# -- decoding --------------------------------------------------------------------
def test_decode_reads_oi_from_a_futures_frame_and_zero_means_none():
    key = "NSE_FO|50001"
    t = list(decode.ticks_from_response(decode.parse_frame(tick_frame(key, ms(4, 0, 0), 24500.5, 120, bid=24500.0, oi=9_800_000)),
                                        {key: "NIFTY_FUT_27_OCT_26"}))
    assert t[0].oi == 9_800_000 and t[0].symbol == "NIFTY_FUT_27_OCT_26" and t[0].vol_today == 120
    z = list(decode.ticks_from_response(decode.parse_frame(tick_frame(key, ms(4, 0, 1), 24500.5, 121, oi=0)), {key: "X"}))
    assert z[0].oi is None


def test_decode_reads_oi_from_option_greeks_frames():
    r = pb.FeedResponse(type=pb.live_feed)
    g = r.feeds["NSE_FO|1"].firstLevelWithGreeks
    g.ltpc.ltp, g.ltpc.ltt, g.vtt, g.oi = 180.0, ms(4, 0, 0), 55, 4_200_000
    t = list(decode.ticks_from_response(r, {"NSE_FO|1": "NIFTY_24500_CE_27_OCT_26"}))
    assert t[0].oi == 4_200_000


# -- storage ---------------------------------------------------------------------
def fut_bars(n=10, oi0=1000, sym="NIFTY_FUT_27_OCT_26"):
    return [Bar(OPEN + timedelta(seconds=i), sym, "IN", 100, 101, 99, 100.5, 5, 100.2, 2, 100.4, 100.6, 10, 12, oi=oi0 + i // 3)
            for i in range(n)]


def test_oi_is_stored_as_a_nullable_integer_and_old_files_without_it_still_load(tmp_path):
    old = [Bar(OPEN, "A.NS", "IN", 1, 2, 0.5, 1.5, 3, None, None, None, None, None, None)]            # no oi
    tick_store.write_bars(old, "1s", tmp_path)
    tick_store.write_bars(fut_bars(), "1s", tmp_path)
    tick_store.compact_day("1s", DAY, tmp_path)
    df = Lake(tmp_path).sql("SELECT symbol, oi FROM bars_1s ORDER BY symbol, ts")
    assert str(df.oi.dtype) == "Int64"
    assert df[df.symbol == "A.NS"].oi.isna().all()
    assert df[df.symbol != "A.NS"].oi.tolist() == [1000, 1000, 1000, 1001, 1001, 1001, 1002, 1002, 1002, 1003]


def test_rollup_keeps_the_last_open_interest_of_each_bucket(tmp_path):
    tick_store.write_bars(fut_bars(120), "1s", tmp_path)
    tick_store.compact_day("1s", DAY, tmp_path)
    r = Lake(tmp_path).day_bars("2026-10-23", "NIFTY_FUT_27_OCT_26", width_s=60)
    assert r.oi.tolist() == [1000 + 59 // 3, 1000 + 119 // 3]
    plain = Lake(tmp_path).day_bars("2026-10-23", "NIFTY_FUT_27_OCT_26", width_s=1)
    assert len(plain) == 120 and "oi" in plain.columns


def test_rollup_without_an_oi_column_still_works(tmp_path):
    bars = [Bar(OPEN + timedelta(seconds=i), "A.NS", "IN", 1, 2, 0.5, 1.5, 3, 1.2, 1, None, None, None, None) for i in range(5)]
    tick_store.write_bars(bars, "1s", tmp_path)
    tick_store.compact_day("1s", DAY, tmp_path)
    q = Lake(tmp_path)
    df = q.day_bars("2026-10-23", "A.NS", width_s=60)
    assert len(df) == 1 and "oi" in df.columns and df.oi.isna().all()          # the column exists (all null) after a write


# -- REST candles ------------------------------------------------------------------
def test_candles_read_the_seventh_field_as_open_interest():
    raw = [["2026-10-23T09:15:00+05:30", 100, 101, 99, 100.5, 40, 12345],
           ["2026-10-23T09:16:00+05:30", 100, 101, 99, 100.5, 40, 0],             # zero: cash equity style
           ["2026-10-23T09:17:00+05:30", 100, 101, 99, 100.5, 40],                 # six fields only
           ["2026-10-23T09:18:00+05:30", 100, 101, 99, 100.5, 40, "oops"]]
    b = candles.candles_to_bars(raw, "NIFTY_FUT_27_OCT_26")
    assert [x.oi for x in b] == [12345, None, None, None] and all(x.src == "rest" for x in b)


# -- analytics and the API -----------------------------------------------------------
def test_summary_and_payload_expose_open_interest_only_when_present():
    df = pd.DataFrame([b.__dict__ if hasattr(b, "__dict__") else {} for b in []])
    from dataclasses import asdict
    f = pd.DataFrame([asdict(b) for b in fut_bars(30)])
    s = day_summary(f)
    assert (s["oi_first"], s["oi_last"], s["oi_change"]) == (1000, 1009, 9)
    p = series_payload(f)
    assert p["oi"][0] == 1000 and p["oi"][-1] == 1009
    g = f.drop(columns="oi")
    assert "oi_last" not in day_summary(g) and "oi" not in series_payload(g)
    h = f.assign(oi=pd.array([None] * len(f), dtype="Int64"))
    assert "oi_last" not in day_summary(h) and "oi" not in series_payload(h)
    gap = f.copy(); gap.loc[5:10, "oi"] = pd.NA
    assert all(v is not None for v in series_payload(gap)["oi"])               # carried forward over missing values


def test_api_flags_open_interest_and_symbols_with_underscores_are_accepted(tmp_path):
    tick_store.write_bars(fut_bars(60) + [Bar(OPEN, "A.NS", "IN", 1, 2, 0.5, 1.5, 3, None, None, None, None, None, None)], "1s", tmp_path)
    tick_store.compact_day("1s", DAY, tmp_path)
    c = TestClient(create_app(tmp_path))
    r = c.get("/api/day", params={"date": "2026-10-23", "symbol": "NIFTY_FUT_27_OCT_26"}).json()
    assert r["has_oi"] is True and r["summary"]["oi_last"] == 1019 and r["series"]["oi"][0] == 1000
    e = c.get("/api/day", params={"date": "2026-10-23", "symbol": "A.NS"}).json()
    assert e["has_oi"] is False and "oi" not in e["series"]
    assert {s["symbol"] for s in c.get("/api/symbols", params={"date": "2026-10-23"}).json()} == {"A.NS", "NIFTY_FUT_27_OCT_26"}


def test_demo_derivatives_have_open_interest_and_default_demo_is_unchanged(tmp_path):
    demo.write_demo(tmp_path / "d1", days=1)
    assert "oi" not in Lake(tmp_path / "d1").sql("SELECT * FROM bars_1s LIMIT 1").columns
    demo.write_demo(tmp_path / "d2", days=1, derivatives=True)
    q = Lake(tmp_path / "d2")
    syms = set(q.sql("SELECT DISTINCT symbol FROM bars_1s").symbol)
    contracts = demo.derivative_contracts(date(2026, 10, 23))
    assert {"NIFTY_FUT_24_NOV_26", "NIFTY_24500_CE_27_OCT_26", "BANKNIFTY_54900_PE_24_NOV_26"} <= syms
    assert len(syms) == 10 + len(contracts)
    oi = q.sql("SELECT symbol, count(oi) n, min(oi) lo FROM bars_1s GROUP BY symbol").set_index("symbol")
    assert oi.loc["NIFTY_FUT_24_NOV_26", "n"] > 1000 and oi.loc["RELIANCE.NS", "n"] == 0
    # the demo writes an instrument table, so the derivatives views work on it like on real data
    assert q.fo_underlyings("2026-10-23") == ["BANKNIFTY", "NIFTY"]
    chain = q.fo_chain("2026-10-23", "NIFTY")
    assert chain["expiries"] == ["2026-10-27", "2026-11-24"] and chain["lot_size"] == 75 and len(chain["futures"]) == 2
    assert len(chain["chain"]) == 25 and all(r["ce_oi"] and r["pe_oi"] for r in chain["chain"])
    atm = next(r for r in chain["chain"] if r["strike"] == chain["atm_strike"])
    far = chain["chain"][0]
    assert atm["ce_oi"] + atm["pe_oi"] > far["ce_oi"] + far["pe_oi"]          # open interest is concentrated near the money
    sys.argv = ["demo", "--out", str(tmp_path / "d3"), "--days", "1", "--with-derivatives"]
    demo.main()
    assert (tmp_path / "d3" / "DEMO").exists()


# -- option chain / futures snapshot (query layer + webapp) ------------------------------
def fo_records():
    def r(itype, strike=None, token="t"):
        label = "FUT" if itype == "FUT" else f"{int(strike)} {itype}"
        return {"segment": "NSE_FO", "instrument_type": itype, "instrument_key": f"NSE_FO|{token}",
                "trading_symbol": f"NIFTY {label} 23 NOV 26", "underlying_symbol": "NIFTY",
                "underlying_key": "NSE_INDEX|Nifty 50", "expiry": "2026-11-23", "lot_size": 65,
                "tick_size": 5.0, "weekly": False, "strike_price": strike}
    return [r("FUT", token="f"), r("CE", 24500, "c1"), r("PE", 24500, "p1"), r("CE", 24600, "c2"), r("PE", 24600, "p2")]


def chain_bar(sym, close, oi):
    return Bar(OPEN, sym, "IN", close, close, close, close, 10, close, 1, close - 0.05, close + 0.05, 5, 5, oi=oi)


def seed_fo_chain(lake_dir):
    fo.save_table(fo.parse_master(fo_records()), DAY, lake_dir)
    bars = [chain_bar("NIFTY_FUT_23_NOV_26", 24550.0, 1_000_000), chain_bar("NIFTY_24500_CE_23_NOV_26", 120.0, 500_000),
           chain_bar("NIFTY_24500_PE_23_NOV_26", 90.0, 400_000), chain_bar("NIFTY_24600_CE_23_NOV_26", 80.0, 300_000),
           chain_bar("NIFTY_24600_PE_23_NOV_26", 110.0, 600_000)]
    tick_store.write_bars(bars, "1s", lake_dir)
    tick_store.compact_day("1s", DAY, lake_dir)


def test_fo_underlyings_and_chain_pick_the_right_expiry_atm_and_totals(tmp_path):
    seed_fo_chain(tmp_path)
    q = Lake(tmp_path)
    assert q.fo_underlyings("2026-10-23") == ["NIFTY"]
    assert q.fo_underlyings("2099-01-01") == []                                 # no bars that day
    d = q.fo_chain("2026-10-23", "NIFTY")
    assert d["expiry"] == "2026-11-23" and d["expiries"] == ["2026-11-23"]
    assert d["spot"] == 24550.0 and d["atm_strike"] == 24500                    # nearer to 24500 than 24600
    assert d["total_call_oi"] == 800_000 and d["total_put_oi"] == 1_000_000
    assert d["pcr"] == round(1_000_000 / 800_000, 3)
    assert d["futures"] == [{"symbol": "NIFTY_FUT_23_NOV_26", "expiry": "2026-11-23", "close": 24550.0,
                             "oi": 1_000_000, "oi_change": 0, "volume": 10, "bars": 1}]
    row = next(r for r in d["chain"] if r["strike"] == 24500)
    assert row["ce_symbol"] == "NIFTY_24500_CE_23_NOV_26" and row["ce_close"] == 120.0 and row["ce_oi"] == 500_000
    assert row["pe_close"] == 90.0 and row["pe_oi"] == 400_000
    assert len(d["chain"]) == 2


def test_fo_chain_is_empty_without_a_match(tmp_path):
    assert Lake(tmp_path).fo_chain("2026-10-23", "NIFTY") == {}                 # no instrument table at all
    seed_fo_chain(tmp_path)
    assert Lake(tmp_path).fo_chain("2026-10-23", "RELIANCE") == {}              # underlying not in the table
    assert Lake(tmp_path).fo_chain("2099-01-01", "NIFTY") == {}                 # no bars that day


def test_fo_chain_defaults_to_an_available_expiry_when_the_requested_one_is_missing(tmp_path):
    seed_fo_chain(tmp_path)
    d = Lake(tmp_path).fo_chain("2026-10-23", "NIFTY", expiry="2099-01-01")
    assert d["expiry"] == "2026-11-23"


def test_fo_api_meta_and_chain_endpoints(tmp_path):
    seed_fo_chain(tmp_path)
    c = TestClient(create_app(tmp_path))
    assert c.get("/api/fo/meta", params={"date": "2026-10-23"}).json() == {"underlyings": ["NIFTY"]}
    r = c.get("/api/fo/chain", params={"date": "2026-10-23", "underlying": "NIFTY"})
    assert r.status_code == 200 and r.json()["atm_strike"] == 24500 and "lot_size" in r.json()
    assert c.get("/api/fo/chain", params={"date": "2026-10-23", "underlying": "NOSUCH"}).status_code == 404
    assert c.get("/api/fo/chain", params={"date": "2026-10-23", "underlying": "../etc"}).status_code == 422
    assert c.get("/api/fo/chain", params={"date": "2026-10-23", "underlying": "NIFTY", "expiry": "bad"}).status_code == 422
    assert c.get("/api/fo/meta", params={"date": "not-a-date"}).status_code == 422
    # the viewer sends expiry="" (not omitted) before a user has picked one; that must not 422
    empty = c.get("/api/fo/chain", params={"date": "2026-10-23", "underlying": "NIFTY", "expiry": ""})
    assert empty.status_code == 200 and empty.json()["expiry"] == "2026-11-23"


def test_api_symbols_and_overview_exclude_fo_contracts(tmp_path):
    seed_fo_chain(tmp_path)
    tick_store.write_bars([Bar(OPEN, "A.NS", "IN", 1, 2, 0.5, 1.5, 3, 1.2, 1, None, None, None, None)], "1s", tmp_path)
    tick_store.compact_day("1s", DAY, tmp_path)
    c = TestClient(create_app(tmp_path))
    syms = {s["symbol"] for s in c.get("/api/symbols", params={"date": "2026-10-23"}).json()}
    assert syms == {"A.NS"}                                                     # F&O contracts filtered out
    rows = {r["symbol"] for r in c.get("/api/overview", params={"date": "2026-10-23"}).json()}
    assert rows == {"A.NS"}
    # the instrument-level chart still works for an F&O symbol directly (the F&O tab links to it)
    assert c.get("/api/day", params={"date": "2026-10-23", "symbol": "NIFTY_FUT_23_NOV_26"}).status_code == 200


# -- verification of futures bars --------------------------------------------------------
def contracts(*syms):
    return pd.DataFrame({"symbol": list(syms)})


def lake_with(tmp_path, bars):
    tick_store.write_bars(bars, "1s", tmp_path)
    tick_store.compact_day("1s", DAY, tmp_path)
    return Lake(tmp_path)


def test_verify_fo_passes_a_healthy_day(tmp_path, capsys):
    bars = fut_bars(200, oi0=1000) + fut_bars(200, oi0=50, sym="NIFTY_24500_CE_27_OCT_26")
    # make OI move through the day for both contracts
    for i, b in enumerate(bars):
        b.oi += i % 40
    rep = verify.verify_fo(lake_with(tmp_path, bars), DAY, contracts("NIFTY_FUT_27_OCT_26", "NIFTY_24500_CE_27_OCT_26"))
    out = capsys.readouterr().out
    assert rep.fails == 0 and "[WARN]" not in out and "open interest coverage" in out


def test_verify_fo_fails_without_open_interest_and_warns_on_gaps(tmp_path, capsys):
    plain = fut_bars(50)
    for b in plain:
        b.oi = None
    rep = verify.verify_fo(lake_with(tmp_path / "a", plain), DAY, contracts("NIFTY_FUT_27_OCT_26"))
    assert rep.fails == 1 and "no open interest" in capsys.readouterr().out
    mixed = fut_bars(100)
    for b in mixed[:30]:
        b.oi = None
    rep = verify.verify_fo(lake_with(tmp_path / "b", mixed), DAY, contracts("NIFTY_FUT_27_OCT_26", "NEVER_TRADED"))
    out = capsys.readouterr().out
    assert rep.fails == 0 and "under 90%" in out and "NEVER_TRADED" in out and out.count("[WARN]") >= 2


def test_verify_fo_edge_cases(tmp_path, capsys):
    assert verify.verify_fo(Lake(tmp_path), DAY).fails == 1                     # no view
    lake = lake_with(tmp_path / "x", fut_bars(10))
    assert verify.verify_fo(lake, date(2026, 10, 24)).fails == 1                # no rows that day
    rep = verify.verify_fo(lake, DAY)                                           # no contract list: found by having OI
    assert rep.fails == 0
    bad = fut_bars(10)
    bad[0].low, bad[1].oi, bad[2].bid, bad[2].ask = 200, -5, 101.0, 100.0
    rep = verify.verify_fo(lake_with(tmp_path / "y", bad), DAY)
    out = capsys.readouterr().out
    assert rep.fails == 2 and "negative open interest rows: 1" in out and "crossed books: 1" in out


def test_verify_fo_cli_reads_the_contract_file_or_all(world, monkeypatch, capsys):
    world.tick_store.write_bars(fut_bars(30), "1s")
    world.tick_store.compact_day("1s", DAY)
    pd.DataFrame({"symbol": ["NIFTY_FUT_27_OCT_26"], "instrument_key": ["NSE_FO|1"], "underlying": ["NIFTY"],
                  "instrument_type": ["FUT"], "expiry": ["2026-10-27"], "strike": [np.nan], "lot_size": [75]}
                 ).to_csv(world.data / "fo_universe.csv", index=False)
    for argv in (["verify", "fo", "--date", "2026-10-23"], ["verify", "fo", "--date", "2026-10-23", "--all"]):
        monkeypatch.setattr(sys, "argv", argv)
        with pytest.raises(SystemExit) as e:
            verify.main()
        assert e.value.code == 0
    assert "futures/options bars" in capsys.readouterr().out


# -- the feed with --segment fo --------------------------------------------------------
def test_feed_main_segment_fo_uses_the_selected_contracts(world, monkeypatch, tmp_path):
    monkeypatch.setenv("UPSTOX_ACCESS_TOKEN", "TOK")
    monkeypatch.chdir(tmp_path)
    seen = {}

    async def fake_run_forever(make, once):
        f = make()
        seen.update(keys=sorted(f.keys), once=once)
    monkeypatch.setattr(feed, "run_forever", fake_run_forever)
    monkeypatch.setattr(feed, "segment_key_map", lambda seg, fo_file=None: seen.update(seg=seg, file=fo_file) or
                        {"NSE_FO|1": "A_FUT", "NSE_FO|2": "B_FUT"})
    monkeypatch.setattr(sys, "argv", ["feed", "--once", "--segment", "fo", "--fo-file", "x.csv", "--limit", "1"])
    feed.main()
    assert seen["seg"] == "fo" and seen["file"] == "x.csv" and seen["keys"] == ["NSE_FO|1"]
    monkeypatch.setattr(feed, "segment_key_map", lambda seg, fo_file=None: {})
    monkeypatch.setattr(sys, "argv", ["feed", "--segment", "fo"])
    with pytest.raises(SystemExit, match="fo_instruments import"):
        feed.main()


def test_candles_main_segment_fo(world, monkeypatch):
    monkeypatch.setenv("UPSTOX_ACCESS_TOKEN", "TOK")
    seen = {}
    monkeypatch.setattr(candles, "segment_key_map", lambda seg, fo_file=None: seen.update(seg=seg) or {})
    monkeypatch.setattr(sys, "argv", ["candles", "--date", "2026-10-23", "--segment", "fo"])
    with pytest.raises(SystemExit, match="fo_instruments import"):
        candles.main()
    assert seen["seg"] == "fo"


# -- option Greeks ---------------------------------------------------------------
def _greeks_frame(key, with_greeks=True):
    r = pb.FeedResponse(type=pb.live_feed)
    m = r.feeds[key].fullFeed.marketFF
    m.ltpc.ltp, m.ltpc.ltt, m.oi, m.vtt = 120.5, ms(4, 0, 0), 5000, 100
    q = m.marketLevel.bidAskQuote.add()
    q.bidP, q.bidQ, q.askP, q.askQ = 120.4, 50, 120.6, 75
    if with_greeks:
        m.iv = 14.25
        m.optionGreeks.delta, m.optionGreeks.theta, m.optionGreeks.gamma = 0.52, -8.1, 0.0021
        m.optionGreeks.vega, m.optionGreeks.rho = 11.3, 4.2
    return r.SerializeToString()


def test_decode_reads_all_greeks_and_iv_from_a_full_frame():
    key = "NSE_FO|1"
    t = list(decode.ticks_from_response(decode.parse_frame(_greeks_frame(key)), {key: "OPT"}))[0]
    assert (t.iv, t.delta, t.theta, t.gamma, t.vega, t.rho) == pytest.approx((14.25, 0.52, -8.1, 0.0021, 11.3, 4.2))
    assert (t.bid, t.bid_qty, t.ask, t.ask_qty, t.oi) == (120.4, 50, 120.6, 75, 5000)    # level one still kept


def test_frames_without_greeks_leave_them_empty_and_storage_round_trips(tmp_path):
    key = "NSE_FO|1"
    ticks = [list(decode.ticks_from_response(decode.parse_frame(_greeks_frame(key, g)), {key: s}))[0]
             for s, g in (("OPT", True), ("EQ.NS", False))]
    assert ticks[1].delta is None and ticks[1].iv is None
    tick_store.write_ticks(ticks, tmp_path)
    tick_store.write_ticks(ticks[1:], tmp_path)                      # an equity-only segment unions cleanly
    day = ticks[0].ts.date()
    assert tick_store.compact_ticks_day(day, tmp_path) == 3
    import duckdb
    rows = duckdb.sql(f"SELECT symbol, delta, iv FROM read_parquet('{tmp_path}/ticks/date={day}/ticks.parquet') ORDER BY symbol, delta").fetchall()
    assert rows[0][0] == "EQ.NS" and rows[0][1] is None
    assert rows[-1][1] == pytest.approx(0.52, rel=1e-4) and rows[-1][2] == pytest.approx(14.25, rel=1e-4)


# -- what the feed keeps (tick policy) ---------------------------------------------
from data_layer.tick_policy import TickPolicy, policy_from_env  # noqa: E402


def _opt(sec_ms, px=100.0, bid=99.9, delta=0.5, iv=14.0, oi=1000, sym="OPT"):
    return Tick(OPEN + timedelta(milliseconds=sec_ms), sym, px, 10, bid, 5, bid + 0.2, 6, oi,
                iv=iv, delta=delta, theta=-1.23456789, gamma=0.00123456, vega=9.87654321, rho=2.3456789)


def test_full_keeps_every_tick_even_duplicates():
    p = TickPolicy()
    assert [p.apply(_opt(i * 100)) is not None for i in range(5)] == [True] * 5 and p.dropped == 0


def test_throttle_keeps_one_per_symbol_per_interval():
    p = TickPolicy("throttle", min_interval_s=1.0)
    kept = [p.apply(_opt(ms_)) is not None for ms_ in (0, 300, 999, 1000, 1500, 2100)]
    assert kept == [True, False, False, True, False, True]
    assert p.apply(_opt(100, sym="OTHER")) is not None               # per symbol, not global


def test_changes_ignores_greeks_only_updates():
    p = TickPolicy("a")
    assert p.apply(_opt(0)) is not None
    assert p.apply(_opt(100, delta=0.51, iv=14.1)) is None           # only Greeks moved
    assert p.apply(_opt(200, px=100.05)) is not None                 # price moved
    assert p.apply(_opt(300, px=100.05, oi=1010)) is not None        # open interest moved
    assert p.dropped == 1 and p.kept == 3


def test_greeks_selection_and_rounding():
    t = TickPolicy(greeks="iv", decimals=2).apply(_opt(0))
    assert t.iv == 14.0 and t.delta is t.theta is t.gamma is t.vega is t.rho is None
    t = TickPolicy(greeks="none").apply(_opt(0))
    assert t.iv is None and t.delta is None and t.bid == 99.9
    t = TickPolicy(decimals=3).apply(_opt(0))
    assert (t.theta, t.gamma, t.vega) == (-1.235, 0.001, 9.877)


def test_policy_from_env_precedence_and_validation():
    env = {"JARVIS_TICK_STORE": "b", "JARVIS_TICK_STORE_FO": "changes", "JARVIS_TICK_MIN_INTERVAL_S": "2",
           "JARVIS_GREEKS": "iv", "JARVIS_GREEKS_DECIMALS": "4"}
    fo, eq = policy_from_env("fo", env), policy_from_env("eq", env)
    assert (fo.mode, eq.mode, eq.min_interval_s, fo.greeks, fo.decimals) == ("changes", "throttle", 2.0, "iv", 4)
    assert policy_from_env("fo", {}).describe() == "mode=full greeks=all"
    with pytest.raises(ValueError):
        TickPolicy("sometimes")
    with pytest.raises(ValueError):
        TickPolicy(greeks="most")


def test_processor_applies_the_policy_before_storage():
    key = "NSE_FO|1"
    for mode, expect in (("full", 2), ("changes", 1)):
        p = feed.FeedProcessor({key: "OPT"}, TickPolicy(mode))
        for _ in range(2):
            p.handle(_greeks_frame(key), datetime(2026, 10, 23, 5, 0))
        assert len(p.drain()) == expect
