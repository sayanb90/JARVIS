"""Lake loader + DuckDB query tests on synthetic CSVs (no network)."""
import csv
import importlib

import pytest


@pytest.fixture()
def env(tmp_path, monkeypatch):
    monkeypatch.setenv("JARVIS_ROOT", str(tmp_path))
    import data_layer.lake as lake
    importlib.reload(lake)
    (tmp_path / "data/fundamentals").mkdir(parents=True)
    (tmp_path / "data/ohlc").mkdir(parents=True)
    return tmp_path, lake


def w(path, header, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        cw = csv.writer(f)
        cw.writerow(header)
        cw.writerows(rows)


def test_bars_upsert_idempotent_and_utc(env):
    root, lake = env
    h = ["ts", "symbol", "market", "open", "high", "low", "close", "volume"]
    p = root / "data/bars/5m/in/2026-10-23.csv"
    w(p, h, [["2026.10.23D13:30:00.000000000", "RELIANCE.NS", "IN", 1, 2, 0.5, 1.5, 100],
             ["2026.10.23D13:35:00.000000000", "RELIANCE.NS", "IN", 1.5, 2, 1, 1.8, 50]])
    lake.load_bars("5m", since=lake.date(2026, 10, 1))
    lake.load_bars("5m", since=lake.date(2026, 10, 1))          # re-run: no dupes
    w(p, h, [["2026.10.23D13:30:00.000000000", "RELIANCE.NS", "IN", 1, 2, 0.5, 9.0, 100]])  # correction
    lake.load_bars("5m", since=lake.date(2026, 10, 1))
    from data_layer.query import Lake
    df = Lake(root / "lake").sql("SELECT * FROM bars_5m ORDER BY symbol, ts")
    assert len(df) == 2                                           # corrected bar kept, no duplicates
    assert df[df.symbol == "RELIANCE.NS"].close.tolist() == [9.0, 1.8]
    assert str(df.ts.iloc[0]) == "2026-10-23 13:30:00"


def test_ohlc_year_partitions_and_daily_upsert(env):
    root, lake = env
    h = ["date", "symbol", "market", "open", "high", "low", "close", "volume"]
    w(root / "data/ohlc/ohlc_2025_in.csv", h, [["2025.12.31", "RELIANCE.NS", "IN", 1, 2, 1, 1.5, 10]])
    w(root / "data/ohlc/ohlc_2026_in.csv", h, [["2026.01.02", "RELIANCE.NS", "IN", 2, 3, 2, 2.5, 11]])
    lake.load_ohlc()
    w(root / "data/ohlc/ohlc_daily_in.csv", h, [["2026.01.02", "RELIANCE.NS", "IN", 2, 3, 2, 2.9, 11],
                                                 ["2026.01.05", "RELIANCE.NS", "IN", 3, 4, 3, 3.5, 12]])
    lake.load_ohlc(daily=True)
    from data_layer.query import Lake
    q = Lake(root / "lake")
    assert q.ohlc("RELIANCE.NS").close.tolist() == [1.5, 2.9, 3.5]
    assert q.latest_price("RELIANCE.NS") == 3.5
    assert {p.parent.name for p in (root / "lake/ohlc").glob("*/ohlc.parquet")} == {"year=2025", "year=2026"}


def seed_fundamentals(root, strong=True):
    d = root / "data/fundamentals"
    inc = [["2025.12.31", "GOOD.NS", "IN", 200, 60, 80], ["2021.12.31", "GOOD.NS", "IN", 100, 30, 40]]
    bal = [["2025.12.31", "GOOD.NS", "IN", 1000, 400, 50, 600]]
    cf = [["2025.12.31", "GOOD.NS", "IN", 70], ["2021.12.31", "GOOD.NS", "IN", 20]]
    ra = [["2025.12.31", "GOOD.NS", "IN", 0.15, 90], ["2021.12.31", "GOOD.NS", "IN", 0.1, 100]]
    w(d / "income_statement_in.csv", ["date", "symbol", "market", "revenue", "net_income", "ebit"], inc)
    w(d / "balance_sheet_in.csv", ["date", "symbol", "market", "total_assets", "total_liabilities",
                                   "long_term_debt", "total_equity"], bal)
    w(d / "cash_flow_in.csv", ["date", "symbol", "market", "free_cash_flow"], cf)
    w(d / "ratios_in.csv", ["date", "symbol", "market", "roic", "shares_outstanding"], ra)
    w(d / "pillar_metrics_in.csv",
      ["symbol", "market", "avg_net_income_5y", "avg_fcf_5y", "latest_revenue", "latest_ebit",
       "latest_total_assets", "latest_total_liabilities", "latest_long_term_debt",
       "latest_total_equity", "latest_roic", "latest_shares_outstanding"],
      [["GOOD.NS", "IN", 45, 45, 200, 80, 1000, 400, 50, 600, 0.15, 90],
       ["NOPX.NS", "IN", 45, 45, 200, 80, 1000, 400, 50, 600, 0.15, 90]])
    w(root / "data/fund_meta.csv", ["symbol", "market", "last_updated"], [["GOOD.NS", "IN", "2026.10.20"]])


def test_fundamentals_upsert_accumulates_and_scores_8(env):
    root, lake = env
    seed_fundamentals(root)
    h = ["date", "symbol", "market", "open", "high", "low", "close", "volume"]
    w(root / "data/ohlc/ohlc_2026_in.csv", h, [["2026.10.20", "GOOD.NS", "IN", 10, 10, 10, 10, 1]])
    lake.load_all()
    # rotation: a later CSV with only another ticker must NOT drop GOOD
    w(root / "data/fundamentals/cash_flow_in.csv", ["date", "symbol", "market", "free_cash_flow"],
      [["2025.12.31", "OTHER.NS", "IN", 5]])
    lake.load_fundamentals()
    from data_layer.query import Lake
    q = Lake(root / "lake")
    assert set(q.sql("SELECT DISTINCT symbol FROM cash_flow").symbol) == {"GOOD.NS", "OTHER.NS"}
    # price 10, shares 90 -> mktcap 900; P/E 20 <22.5, P/FCF 20 (not <20)... use price 9
    assert q.score_ticker("GOOD.NS", 9.0) == 8
    assert q.score_ticker("GOOD.NS", 10.0) == 7          # P/FCF = 900/45 = 20 fails strict < 20
    assert q.score_ticker("MISSING.NS", 9.0) == 0
    s = q.score_all("IN")
    assert s.loc[s.symbol == "NOPX.NS", "score"].iloc[0] == 0   # no price -> 0
    assert q.sql("SELECT last_updated FROM fund_meta").last_updated.iloc[0].strftime("%Y-%m-%d") == "2026-10-20"


# ---------------------------------------------------------------------------
# Tick -> bar aggregation and segment storage
# ---------------------------------------------------------------------------
from datetime import date, datetime  # noqa: E402

from data_layer.ticks import BarAggregator, Tick  # noqa: E402


def T(h, m, s, px, vtt=None, **kw):
    return Tick(datetime(2026, 10, 23, h, m, s), "RELIANCE.NS", px, vtt, **kw)


def test_aggregator_ohlc_volume_vwap_and_session_filter():
    ag = BarAggregator(60)
    bars = []
    bars += ag.add(T(3, 40, 0, 99.0, 500))                  # pre-open: dropped
    bars += ag.add(T(3, 45, 1, 100.0, 1000))                # first tick: baseline only
    bars += ag.add(T(3, 45, 20, 102.0, 1100, bid=101.9, bid_qty=5, ask=102.1, ask_qty=7))
    bars += ag.add(T(3, 45, 50, 101.0, 1300))
    assert bars == []                                       # bar still open
    bars += ag.add(T(3, 46, 2, 103.0, 1350))                # next bucket closes 03:45 bar
    assert len(bars) == 1
    b = bars[0]
    assert (b.ts, b.open, b.high, b.low, b.close) == (datetime(2026, 10, 23, 3, 45), 100.0, 102.0, 100.0, 101.0)
    assert b.volume == 300 and b.n_ticks == 3
    assert b.vwap == pytest.approx((102.0 * 100 + 101.0 * 200) / 300)
    assert (b.bid, b.ask, b.bid_qty, b.ask_qty) == (101.9, 102.1, 5, 7)
    assert ag.add(T(3, 45, 59, 100.5, 1360)) == []          # late tick for a closed bucket: ignored
    assert ag.add(T(10, 0, 0, 100.0, 9)) == []              # at/after close: dropped
    assert [x.ts.minute for x in ag.flush(datetime(2026, 10, 23, 3, 47))] == [46]


def test_aggregator_60m_bars_start_at_session_open():
    ag = BarAggregator(3600)
    ag.add(T(3, 50, 0, 10.0, 1))
    ag.add(T(4, 30, 0, 12.0, 2))
    out = ag.add(T(4, 50, 0, 11.0, 3))
    assert [b.ts for b in out] == [datetime(2026, 10, 23, 3, 45)]


def test_segments_compact_and_query(env):
    root, lake = env
    from data_layer import tick_store
    from data_layer.query import Lake
    ld = root / "lake"
    ag = BarAggregator(60)
    bars = ag.add(T(3, 45, 0, 100.0, 10)) + ag.add(T(3, 46, 0, 101.0, 20)) + ag.flush_all()
    assert tick_store.write_bars(bars, "1m", ld) == 2
    corrected = [b for b in bars if b.ts.minute == 46]
    corrected[0].close = 555.0
    tick_store.write_bars(corrected, "1m", ld)               # later segment wins on (symbol, ts)
    q = Lake(ld)
    assert len(q.sql("SELECT * FROM bars_1m")) == 3          # not yet compacted: duplicate visible
    assert tick_store.compact_day("1m", date(2026, 10, 23), ld) == 2
    df = Lake(ld).sql("SELECT * FROM bars_1m ORDER BY ts")
    assert df.close.tolist() == [100.0, 555.0]
    assert list((ld / "bars/interval=1m/date=2026-10-23").glob("seg-*")) == []
    assert tick_store.write_ticks([T(3, 45, 0, 100.0, 10, bid=99.9)], ld) == 1


def test_ohlc_all_markets_but_bars_india_only(env):
    root, lake = env
    h = ["date", "symbol", "market", "open", "high", "low", "close", "volume"]
    for m, sym in (("in", "INFY.NS"), ("us", "AAPL"), ("uk", "SHEL.L"), ("eu", "SAP.DE")):
        w(root / f"data/ohlc/ohlc_2026_{m}.csv", h, [["2026.10.23", sym, m.upper(), 1, 2, 1, 1.5, 10]])
    bh = ["ts", "symbol", "market", "open", "high", "low", "close", "volume"]
    w(root / "data/bars/5m/in/2026-10-23.csv", bh, [["2026.10.23D03:45:00.000000000", "INFY.NS", "IN", 1, 2, 1, 1.5, 10]])
    w(root / "data/bars/5m/us/2026-10-23.csv", bh, [["2026.10.23D13:30:00.000000000", "AAPL", "US", 1, 2, 1, 1.5, 10]])
    lake.load_ohlc()
    lake.load_bars("5m", since=lake.date(2026, 10, 1))           # default markets = India only
    from data_layer.query import Lake
    q = Lake(root / "lake")
    assert set(q.sql("SELECT DISTINCT market FROM ohlc").market) == {"IN", "US", "UK", "EU"}
    assert set(q.sql("SELECT DISTINCT market FROM bars_5m").market) == {"IN"}
    assert lake.BARS_MARKETS == ("IN",) and lake.FUND_MARKETS == ("IN",)
