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
    p = root / "data/bars/5m/us/2026-10-23.csv"
    w(p, h, [["2026.10.23D13:30:00.000000000", "AAPL", "US", 1, 2, 0.5, 1.5, 100],
             ["2026.10.23D13:35:00.000000000", "AAPL", "US", 1.5, 2, 1, 1.8, 50]])
    lake.load_bars("5m", since=lake.date(2026, 10, 1))
    lake.load_bars("5m", since=lake.date(2026, 10, 1))          # re-run: no dupes
    w(p, h, [["2026.10.23D13:30:00.000000000", "AAPL", "US", 1, 2, 0.5, 9.0, 100]])  # correction
    w(root / "data/bars/5m/in/2026-10-23.csv", h,
      [["2026.10.23D03:45:00.000000000", "INFY.NS", "IN", 10, 11, 9, 10.5, 7]])
    lake.load_bars("5m", since=lake.date(2026, 10, 1))
    from data_layer.query import Lake
    df = Lake(root / "lake").sql("SELECT * FROM bars_5m ORDER BY symbol, ts")
    assert len(df) == 3                                           # 2 AAPL (corrected one kept) + 1 INFY
    assert df[df.symbol == "AAPL"].close.tolist() == [9.0, 1.8]
    assert str(df.ts.iloc[0]) == "2026-10-23 13:30:00"


def test_ohlc_year_partitions_and_daily_upsert(env):
    root, lake = env
    h = ["date", "symbol", "market", "open", "high", "low", "close", "volume"]
    w(root / "data/ohlc/ohlc_2025_us.csv", h, [["2025.12.31", "AAPL", "US", 1, 2, 1, 1.5, 10]])
    w(root / "data/ohlc/ohlc_2026_us.csv", h, [["2026.01.02", "AAPL", "US", 2, 3, 2, 2.5, 11]])
    lake.load_ohlc()
    w(root / "data/ohlc/ohlc_daily_us.csv", h, [["2026.01.02", "AAPL", "US", 2, 3, 2, 2.9, 11],
                                                 ["2026.01.05", "AAPL", "US", 3, 4, 3, 3.5, 12]])
    lake.load_ohlc(daily=True)
    from data_layer.query import Lake
    q = Lake(root / "lake")
    assert q.ohlc("AAPL").close.tolist() == [1.5, 2.9, 3.5]
    assert q.latest_price("AAPL") == 3.5
    assert {p.parent.name for p in (root / "lake/ohlc").glob("*/ohlc.parquet")} == {"year=2025", "year=2026"}


def seed_fundamentals(root, strong=True):
    d = root / "data/fundamentals"
    inc = [["2025.12.31", "GOOD", "US", 200, 60, 80], ["2021.12.31", "GOOD", "US", 100, 30, 40]]
    bal = [["2025.12.31", "GOOD", "US", 1000, 400, 50, 600]]
    cf = [["2025.12.31", "GOOD", "US", 70], ["2021.12.31", "GOOD", "US", 20]]
    ra = [["2025.12.31", "GOOD", "US", 0.15, 90], ["2021.12.31", "GOOD", "US", 0.1, 100]]
    w(d / "income_statement_us.csv", ["date", "symbol", "market", "revenue", "net_income", "ebit"], inc)
    w(d / "balance_sheet_us.csv", ["date", "symbol", "market", "total_assets", "total_liabilities",
                                   "long_term_debt", "total_equity"], bal)
    w(d / "cash_flow_us.csv", ["date", "symbol", "market", "free_cash_flow"], cf)
    w(d / "ratios_us.csv", ["date", "symbol", "market", "roic", "shares_outstanding"], ra)
    w(d / "pillar_metrics_us.csv",
      ["symbol", "market", "avg_net_income_5y", "avg_fcf_5y", "latest_revenue", "latest_ebit",
       "latest_total_assets", "latest_total_liabilities", "latest_long_term_debt",
       "latest_total_equity", "latest_roic", "latest_shares_outstanding"],
      [["GOOD", "US", 45, 45, 200, 80, 1000, 400, 50, 600, 0.15, 90],
       ["NOPX", "US", 45, 45, 200, 80, 1000, 400, 50, 600, 0.15, 90]])
    w(root / "data/fund_meta.csv", ["symbol", "market", "last_updated"], [["GOOD", "US", "2026.10.20"]])


def test_fundamentals_upsert_accumulates_and_scores_8(env):
    root, lake = env
    seed_fundamentals(root)
    h = ["date", "symbol", "market", "open", "high", "low", "close", "volume"]
    w(root / "data/ohlc/ohlc_2026_us.csv", h, [["2026.10.20", "GOOD", "US", 10, 10, 10, 10, 1]])
    lake.load_all()
    # rotation: a later CSV with only another ticker must NOT drop GOOD
    w(root / "data/fundamentals/cash_flow_us.csv", ["date", "symbol", "market", "free_cash_flow"],
      [["2025.12.31", "OTHER", "US", 5]])
    lake.load_fundamentals()
    from data_layer.query import Lake
    q = Lake(root / "lake")
    assert set(q.sql("SELECT DISTINCT symbol FROM cash_flow").symbol) == {"GOOD", "OTHER"}
    # price 10, shares 90 -> mktcap 900; P/E 20 <22.5, P/FCF 20 (not <20)... use price 9
    assert q.score_ticker("GOOD", 9.0) == 8
    assert q.score_ticker("GOOD", 10.0) == 7          # P/FCF = 900/45 = 20 fails strict < 20
    assert q.score_ticker("MISSING", 9.0) == 0
    s = q.score_all("US")
    assert s.loc[s.symbol == "NOPX", "score"].iloc[0] == 0   # no price -> 0
    assert q.sql("SELECT last_updated FROM fund_meta").last_updated.iloc[0].strftime("%Y-%m-%d") == "2026-10-20"
